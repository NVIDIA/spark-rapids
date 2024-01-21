/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.window

import ai.rapids.cudf
import ai.rapids.cudf.OrderByArg
import com.nvidia.spark.rapids.{GpuAlias, GpuBindReferences, GpuBoundReference, GpuColumnVector, GpuExpression, GpuLiteral, GpuMetric, GpuProjectExec, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.window.TableAndBatchUtils.{debug, getTableSlice, sliceAndMakeSpillable, toSpillableBatch}
import java.util

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.{CudfAggregate, GpuAggregateExpression, GpuAggregateFunction, GpuCount}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch


// It is not really simple to do a single iterator that can do the splits and retries along with
// The data as needed. Instead we are going to decompose the problem into multiple iterators that
// feed into each other.
// The first pass iterator will take in a batch of data and produce one or more aggregated result
// pairs that include the ridealong columns with the aggregation results for that batch.
// Note that it is assumed that the aggregation was done as a sort based aggregation, so
// the ridealong columns and the aggregation result should both be sorted by the partition by
// columns.  Also the aggregation result must have a count column so it can be expanded using
// repeat to get back to the size of the ridealong columns.
case class FirstPassAggResult(rideAlongColumns: SpillableColumnarBatch,
                              aggResult: SpillableColumnarBatch) extends AutoCloseable {
  override def close(): Unit = {
    rideAlongColumns.close()
    aggResult.close()
  }
}

object TableAndBatchUtils {
  def toSpillableBatch(cb: ColumnarBatch): SpillableColumnarBatch = {
    SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  def toSpillableBatch(table: cudf.Table, types: Seq[DataType]): SpillableColumnarBatch = {
    toSpillableBatch(GpuColumnVector.from(table, types.toArray))
  }

  def getTableSlice(tbl: cudf.Table,
                    beginRow: Int, endRow: Int,
                    beginCol: Int, endCol: Int): cudf.Table = {
    val projectedColumns =
      Range(beginCol, endCol).map { i => tbl.getColumn(i).slice(beginRow, endRow).head }
    new cudf.Table(projectedColumns.toArray: _*)
  }

  def getTableSlice(tbl: cudf.Table, beginRow: Int, endRow: Int): cudf.Table = {
    val nCols = tbl.getNumberOfColumns
    val columns = Range(0, nCols).map { i => tbl.getColumn(i).slice(beginRow, endRow).head }
    new cudf.Table(columns.toArray: _*)
  }

  def sliceAndMakeSpillable(tbl: cudf.Table,
                            rowBegin: Int,
                            rowEnd: Int,
                            dataTypes: Seq[DataType]): SpillableColumnarBatch = {
    val sliced = getTableSlice(tbl, rowBegin, rowEnd)
    toSpillableBatch(GpuColumnVector.from(sliced, dataTypes.toArray))
  }

  // TODO: CALEB: Remove.
  def debug(msg: String, scb: Option[SpillableColumnarBatch]): Unit = {
    scb.foreach { scb =>
      withResource(scb.getColumnarBatch()) { cb =>
        System.err.println()
        GpuColumnVector.debug(msg, cb)
        System.err.println()
        System.err.println()
      }
    }
  }
}

class GpuUnboundedToUnboundedAggWindowFirstPassIterator(
    input: Iterator[ColumnarBatch],
    boundStages: GpuUnboundedToUnboundedAggStages,
    opTime: GpuMetric) extends Iterator[FirstPassAggResult] {
  private var subIterator: Option[Iterator[FirstPassAggResult]] = None
  override def hasNext: Boolean = subIterator.exists(_.hasNext) || input.hasNext

  private def getSpillableInputBatch(): SpillableColumnarBatch = {
    toSpillableBatch(input.next)
  }

  private def upscaleCountResults(unfixed: cudf.Table): cudf.Table = {
    // The group "count" result is in the last column, with type INT32.
    // Cast this up to INT64.  Return the other columns unchanged.
    val nCols = unfixed.getNumberOfColumns
    withResource(unfixed) { _ =>
      val fixedCols = Range(0, nCols).map {
        case i if i != nCols-1 => unfixed.getColumn(i).incRefCount()
        case _ => unfixed.getColumn(nCols - 1).castTo(cudf.DType.INT64)
      }
      new cudf.Table(fixedCols: _*)
    }
  }

  private def groupByAggregate(inputCB: ColumnarBatch) = {
    val groupByOptions = cudf.GroupByOptions.builder()
                                            .withIgnoreNullKeys(false)
                                            .withKeysSorted(true)
                                            .build()
    val cudfAggregates = boundStages.cudfUpdateAggregates
    val aggInputOrdinals = boundStages.aggInputOrdinals
    val cudfAggsOnColumns = cudfAggregates.zip(aggInputOrdinals).map {
      case (cudfAgg, ord) => cudfAgg.groupByAggregate.onColumn(ord)
    }

    withResource(GpuColumnVector.from(inputCB)) { inputTable =>
      val aggResults = inputTable.groupBy(groupByOptions, boundStages.groupColumnOrdinals: _*)
                                 .aggregate(cudfAggsOnColumns: _*)
//      cudf.TableDebug.get.debug("Agg results: ", aggResults)
      upscaleCountResults(aggResults)
    }
  }

  override def next(): FirstPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next()).get
    } else {
      val currIter = withRetry(getSpillableInputBatch(), splitSpillableInHalfByRows) { scb =>
        withResource(scb.getColumnarBatch()) { cb =>
          GpuColumnVector.debug("CALEB: FirstIter: Input: ", cb)
          val aggResultTable = groupByAggregate(cb)
          val rideAlongColumns = GpuProjectExec.project(cb, boundStages.boundRideAlong)

          GpuColumnVector.debug("CALEB: FirstIter: Output: rideAlong: ", rideAlongColumns)
          FirstPassAggResult(toSpillableBatch(rideAlongColumns),
                             toSpillableBatch(aggResultTable,
                               boundStages.groupingColumnTypes ++ boundStages.aggResultTypes))
        }
      }
      val result = currIter.next()
      subIterator = Some(currIter)
      result
    }
  }
}

case class PartitionedFirstPassAggResult(firstPassAggResult: FirstPassAggResult,
                                         boundStages: GpuUnboundedToUnboundedAggStages) {
  var firstGroupAggResult: Option[SpillableColumnarBatch] = None
  var firstGroupRideAlong: Option[SpillableColumnarBatch] = None
  var intermediateGroupAggResult: Option[SpillableColumnarBatch] = None
  var intermediateGroupRideAlong: Option[SpillableColumnarBatch] = None
  var lastGroupAggResult: Option[SpillableColumnarBatch] = None
  var lastGroupRideAlong: Option[SpillableColumnarBatch] = None

  val numGroupingKeys: Int = boundStages.boundPartitionSpec.size
  val numGroups: Int = firstPassAggResult.aggResult.numRows()
  val numRideAlongRows: Int = firstPassAggResult.rideAlongColumns.numRows()

  withResource(firstPassAggResult.rideAlongColumns.getColumnarBatch()) {
    cb => GpuColumnVector.debug("CALEB: Partitioning input: ", cb)
  }

  if (numGroups < 1) {
      throw new IllegalStateException("Expected at least one result group.")
  }


  // TODO: This table needs closing.
  private val groupTable =
    GpuProjectExec.project(firstPassAggResult.rideAlongColumns.getColumnarBatch(),
                           boundStages.boundPartitionSpec)
  GpuColumnVector.debug("ANOTHER groupTable post extraction: ", groupTable)

  private def getIndexForGroupMarginAtRowNumber(aggResultTable: cudf.Table,
                                                rowNumber: Int,
                                                isLowerBound: Boolean): Int = {
    withResource(getTableSlice(aggResultTable,
                               beginRow = rowNumber,
                               endRow = rowNumber + 1,
                               beginCol = 0,
                               endCol = numGroupingKeys)) { lastGroup =>
      cudf.TableDebug.get.debug("LastGroup: ", lastGroup)

      // TODO: Transmit sort-orders. Hard-coding for now.
      val orderBys = Range(0, numGroupingKeys).map(i => OrderByArg.asc(i))
      withResource(GpuColumnVector.from(groupTable)) { groupTable =>
        val groupMargin = if (isLowerBound) {
          groupTable.lowerBound(lastGroup, orderBys: _*)
        } else {
          groupTable.upperBound(lastGroup, orderBys: _*)
        }
        withResource(groupMargin.copyToHost()) { groupMarginHost =>
          groupMarginHost.getInt(0)
        }
      }
    }
  }

  if (firstPassAggResult.aggResult.numRows() < 2) {
    throw new IllegalStateException("Should have at least 2 groups at this point.")
  }
  // TODO: There is some unnesting we can do here.
  withResource(firstPassAggResult.aggResult.getColumnarBatch()) { cb =>
    withResource(GpuColumnVector.from(cb)) { aggResultTable =>

      val lastGroupBeginIdx =
        getIndexForGroupMarginAtRowNumber(aggResultTable,
                                          aggResultTable.getRowCount.asInstanceOf[Int] - 1,
                                          isLowerBound = true)
      System.err.println(s"CALEB: LastGroup begins at $lastGroupBeginIdx, in ride-along.")

      val firstGroupEndIdx = getIndexForGroupMarginAtRowNumber(aggResultTable,
                                                               0,
                                                               isLowerBound = false)
      System.err.println(s"CALEB: FirstGroup ends at $firstGroupEndIdx, in ride-along.")

      withResource(firstPassAggResult.rideAlongColumns.getColumnarBatch()) { rideAlongCB =>
        withResource(GpuColumnVector.from(rideAlongCB)) { rideAlongTable =>
          // Slice and dice!
          val aggResultTypes = boundStages.groupingColumnTypes ++ boundStages.aggResultTypes
          val rideAlongTypes = boundStages.rideAlongColumnTypes
          firstGroupAggResult = Some(sliceAndMakeSpillable(aggResultTable, 0, 1,
                                                           aggResultTypes))
          debug("CALEB: FirstGroup Agg: ", firstGroupAggResult)
          firstGroupRideAlong = Some(sliceAndMakeSpillable(rideAlongTable, 0, firstGroupEndIdx,
                                                           rideAlongTypes))
          debug("CALEB: FirstGroup RideAlong: ", firstGroupRideAlong)
          lastGroupAggResult = Some(sliceAndMakeSpillable(aggResultTable, numGroups - 1, numGroups,
                                                          aggResultTypes))
          debug("CALEB: LastGroup Agg: ", lastGroupAggResult)
          lastGroupRideAlong = Some(sliceAndMakeSpillable(rideAlongTable, lastGroupBeginIdx,
                                                          numRideAlongRows, rideAlongTypes))
          debug("CALEB: LastGroup RideAlong: ", lastGroupRideAlong)
          if (numGroups > 2) {
            // Lump the remaining groups together.
            intermediateGroupAggResult = Some(sliceAndMakeSpillable(aggResultTable,
                                                                    1,
                                                                    numGroups - 1,
                                                                    aggResultTypes))
            debug("CALEB: InterGroup Agg: ", intermediateGroupAggResult)
            intermediateGroupRideAlong = Some(sliceAndMakeSpillable(aggResultTable,
                                                                    firstGroupEndIdx,
                                                                    lastGroupBeginIdx,
                                                                    rideAlongTypes))
            debug("CALEB: InterGroup RideAlong: ", intermediateGroupRideAlong)
          }
        }
      }
    }
  }
} // class PartitionedFirstPassAggResult.

// The second pass through the data will take the output of the first pass. It will slice
// the result depending on if it knows that the group by keys is complete or not.
// Completed data will have the aggregation results merged into a single aggregation result
// Note that this aggregation result needs to remain sorted.  The result is returned as
// an iterator of ridealong columns, and the full agg results for those columns. It is not
// the responsibility of the second stage to try and combine small batches or split up large
// ones, beyond what the retry framework might do.
case class SecondPassAggResult(rideAlongColumns: util.LinkedList[SpillableColumnarBatch],
                               aggResult: SpillableColumnarBatch) {
}

class GpuUnboundedToUnboundedAggWindowSecondPassIterator(
    input: Iterator[FirstPassAggResult],
    boundStages: GpuUnboundedToUnboundedAggStages,
    opTime: GpuMetric) extends Iterator[SecondPassAggResult] {
  // input data where we don't know if the results are done yet
  // TODO this should probably be a var once we start using it
  private val rideAlongColumnsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()
  // Agg results where the input keys are not fully complete yet. They will need to be combined
  // together before being returned.
  // TODO this should be uncommented once we start using it
//  private val aggResultsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()

  override def hasNext: Boolean = (!rideAlongColumnsPendingCompletion.isEmpty) || input.hasNext

  private def removeGroupColumns(aggResults: SpillableColumnarBatch): SpillableColumnarBatch = {
    withResource(aggResults.getColumnarBatch()) { cb =>
      val numColumnsToSkip = boundStages.boundPartitionSpec.size
      withResource(GpuColumnVector.from(cb)) { tbl =>
        val groupColumnsRemoved = GpuColumnVector.from(
          tbl, boundStages.aggResultTypes.toArray, numColumnsToSkip, tbl.getNumberOfColumns
        )
        SpillableColumnarBatch(groupColumnsRemoved, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
  }

  /*
  private def firstRowMatchesCachedGroup(newAggResult: SpillableColumnarBatch)
    : Boolean = {

    val firstRowGroups =
      withResource(newAggResult.getColumnarBatch()) { newCB =>
        withResource(GpuColumnVector.from(newCB)) { newTable =>

        }
      }

  }
   */

  override def next(): SecondPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    var output: Option[SecondPassAggResult] = None
    while (output.isEmpty) {
      if (input.hasNext) {
        withResource(input.next()) { newData =>

          /*
          if (newData.aggResult.numRows() == 1) {
            // All the aggregation results are for the same group.
            // Add the lot to "incomplete", and go again.
            rideAlongColumnsPendingCompletion.add(newData.rideAlongColumns.incRefCount())
            aggResultsPendingCompletion.add(newData.aggResult.incRefCount())
          }
          else {
            val partitioned = PartitionedFirstPassAggResult(newData, boundStages)
            // There are at least two aggregation result rows.

            // The first agg result might complete the previously incomplete group.
            if (!aggResultsPendingCompletion.isEmpty) {
              // Check if the first agg-result row belongs to the same group.
            }
          }
           */
          PartitionedFirstPassAggResult(newData, boundStages)

          val resultRideAlongs = new util.LinkedList[SpillableColumnarBatch]()
          resultRideAlongs.add(newData.rideAlongColumns.incRefCount())

          output = Some(
            SecondPassAggResult(resultRideAlongs, removeGroupColumns(newData.aggResult))
          )
          // TODO newData should be sliced based off of which rows are known to be completed and
          //  which are not. If there are parts that are done it should be combined with
          //  the data pending completion and put into output. Then the incomplete data
          //  should be put into the pending completion queues.
        }
      } else {
        throw new IllegalStateException("Merge aggResultsPendingCompletion")
        // TODO There is no more data, so we need to merge the aggResultsPendingCompletion
        //  into a single SpillableColumnarBatch, and put the result output along with
        //  the rideAlongColumnPendingCompletion
      }
    }
    output.get
  }
}

// The final step is to take the original input data along with the agg data, estimate how
// to split/combine the input batches to output batches that are close to the target batch size
// Then expand the data to match that size, combine everything together and return the result.

class GpuUnboundedToUnboundedAggFinalIterator(
    input: Iterator[SecondPassAggResult],
    boundStages: GpuUnboundedToUnboundedAggStages,
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {
  private var pending: Option[SecondPassAggResult] = None

  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closePending()
    }
  }

  private def hasMoreInPending: Boolean = pending.exists(!_.rideAlongColumns.isEmpty)
  private def pendingAggResults: SpillableColumnarBatch = pending.get.aggResult.incRefCount()
  private def nextPendingRideAlong: SpillableColumnarBatch = pending.get.rideAlongColumns.pop
  private def closePending(): Unit = {
    pending.foreach(_.aggResult.close())
    pending.foreach(_.rideAlongColumns.forEach(_.close()))
    pending = None
  }
  private def replacePending(p: SecondPassAggResult): Unit = {
    closePending()
    pending = Some(p)
  }

  override def hasNext: Boolean =  hasMoreInPending || input.hasNext

  override def next(): ColumnarBatch = {
    // TODO we need to add in the retry code, and pre-splitting of the data if possible, but
    //  for now we are just going to try it.
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    while (!hasMoreInPending) {
      replacePending(input.next())
    }

    // TODO this is a very dumb version right now that is not checking for size
    //  That will be added later on.

    // TODO fix this. We don't want just one batch of ride along columns, and we don't
    //  want to leak anything if we run out of memory
    var rideAlongCombined: ColumnarBatch = null
    while (hasMoreInPending) {
      val cb = withResource(nextPendingRideAlong) { scb =>
        scb.getColumnarBatch()
      }
      withResource(cb) { _ =>
        if (rideAlongCombined == null) {
          rideAlongCombined = GpuColumnVector.incRefCounts(cb)
        } else {
          rideAlongCombined.close()
          throw new IllegalStateException("Concat not implemented yet...")
        }
      }
    }

    // The first stage is to expand the aggregate based on the count column
    val combined = withResource(rideAlongCombined) { _ =>
      val repeatedCb = withResource(pendingAggResults) { scb =>
        opTime.ns {
          withResource(scb.getColumnarBatch()) { cb =>
            GpuColumnVector.debug("FinalIter: PendingAggResult CB == ", cb)
            withResource(boundStages.boundCount.columnarEval(cb)) { counts =>
              cudf.TableDebug.get.debug("FinalIter: Counts: ", counts.getBase)
              withResource(GpuProjectExec.project(cb, boundStages.boundAggsToRepeat)) { toRepeat =>
                withResource(GpuColumnVector.from(toRepeat)) { table =>
                  withResource(table.repeat(counts.getBase)) { repeated =>
                    GpuColumnVector.from(repeated,
                      boundStages.boundAggsToRepeat.map(_.dataType).toArray)
                  }
                }
              }
            }
          }
        }
      }
      // Second step is to stitch the two together
      withResource(repeatedCb) { _ =>
        opTime.ns {
          GpuColumnVector.appendColumns(rideAlongCombined,
            GpuColumnVector.extractColumns(repeatedCb): _*)
        }
      }
    }
    withResource(combined) { _ =>
      opTime.ns {
        closeOnExcept(GpuProjectExec.project(combined, boundStages.boundFinalProject)) { ret =>
          numOutputBatches += 1
          numOutputRows += ret.numRows()
          ret
        }
      }
    }
  }
}

/**
 * Holds the bound references for various aggregation stages
 * @param boundRideAlong used for a project that pulls out columns that are passing through
 *                       unchanged.
 * @param boundAggregations aggregations to be done. NOTE THIS IS WIP
 * @param boundCount The column that contains the count in it for the number of aggregations
 * @param boundAggsToRepeat the columns to get that need to be repeated by the amount in count
 * @param boundFinalProject the final project to get the output in the right order
 */
case class GpuUnboundedToUnboundedAggStages(
    boundPartitionSpec: Seq[GpuExpression],
    boundRideAlong: Seq[GpuExpression],
    boundAggregations: Seq[GpuExpression],
    boundCount: GpuExpression,
    boundAggsToRepeat: Seq[GpuExpression],
    boundFinalProject: Seq[GpuExpression]) extends Serializable {

  val groupingColumnTypes: Seq[DataType] = boundPartitionSpec.map {
    _.dataType
  }
  val groupColumnOrdinals: Seq[Int] = boundPartitionSpec.map {
    case GpuBoundReference(ordinal, _, _) => ordinal
  }
  val aggregateFunctions: Seq[GpuAggregateFunction] = boundAggregations.map {
    _.asInstanceOf[GpuAlias].child.asInstanceOf[GpuAggregateFunction]
  }
  val aggResultTypes: Seq[DataType] = aggregateFunctions.map{ _.dataType }
  val aggInputProjections: Seq[Expression] = aggregateFunctions.flatMap{ _.inputProjection }
  val aggInputOrdinals: Seq[Int] = aggInputProjections.map {
    case GpuBoundReference(ordinal, _, _) => ordinal
    case GpuLiteral(_, _) => 0
    case _ => throw new IllegalStateException("Unexpected expression")
  }
  val cudfUpdateAggregates: Seq[CudfAggregate] = aggregateFunctions.flatMap {
    _.updateAggregates
  }
  val cudfMergeAggregates: Seq[CudfAggregate] = aggregateFunctions.flatMap {
    _.mergeAggregates
  }
  val rideAlongColumnTypes: Seq[DataType] = boundRideAlong.map { _.dataType }
}

/**
 * An iterator that can do unbounded to unbounded window aggregations as group by aggregations
 * followed by an expand/join.
 */
object GpuUnboundedToUnboundedAggWindowIterator {

  private def rideAlongProjection(windowOps: Seq[NamedExpression],
                                  childOutput: Seq[Attribute])
  : (Seq[Attribute], Seq[GpuExpression]) = {
    val rideAlong = windowOps.filter {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => true
      case _ => false
    }
    val rideAlongOutput = rideAlong.map(_.toAttribute)
    val boundRideAlong = GpuBindReferences.bindGpuReferences(rideAlong, childOutput)
    (rideAlongOutput, boundRideAlong)
  }

  private def tmpAggregationOps(windowOps: Seq[NamedExpression],
                                childOutput: Seq[Attribute])
  : (Seq[Attribute], Seq[GpuExpression]) = {
    //  TODO I don't know what this is really going to look like. I am just doing an approximation
    //    here so I can get the output of the aggregations after everything is done for the
    //    repeat. Please fill this in/split it apart, whatever to make it work for you
    val windowAggs = windowOps.flatMap {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => None

      case ga@GpuAlias(GpuWindowExpression(agg: GpuUnboundedToUnboundedWindowAgg, _), _) =>
        // We don't care about the spec, they are all unbounded to unbounded so just get the func
        // We do care that we keep the expression id so we can line it up at the very end
        Some(GpuAlias(agg, ga.name)(ga.exprId))

      case ga@GpuAlias(GpuWindowExpression(
                         GpuAggregateExpression(
                           agg: GpuUnboundedToUnboundedWindowAgg, _, _, _, _), _), _) =>
        // TODO should I verify distinct, filter, etc
        // We don't care about the spec, they are all unbounded to unbounded so just get the func
        // We do care that we keep the expression id so we can line it up at the very end
        Some(GpuAlias(agg, ga.name)(ga.exprId))

      case other =>
        // This should only happen if we did something wrong with how this was created.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    } :+ GpuAlias(GpuCount(Seq(GpuLiteral(1L))), "_count")()
    // Later code by conventions "knows" that the last column is a count and that it can be
    // thrown away. If we ever dedupe this with a COUNT(1) that already exists, then
    // we need to update the output of this to have a clean way to say what is the count,
    // and if that count is needed see repeatOps

    val aggregationsOutput = windowAggs.map(_.toAttribute)
    val boundAggregations = GpuBindReferences.bindGpuReferences(windowAggs, childOutput)
    (aggregationsOutput, boundAggregations)
  }

  private def repeatOps(aggregationsOutput: Seq[Attribute])
    : (GpuExpression, Seq[Attribute], Seq[GpuExpression]) = {
    // It is assumed that the last aggregation column is a count that we will use for repeat
    // If that ever changes, this code needs to be updated.
    val aggOutputExpressions = aggregationsOutput.map { attr =>
      GpuAlias(
        AttributeReference(attr.name, attr.dataType, attr.nullable)(attr.exprId),
        attr.name)(attr.exprId)
    }
    val boundAggOutputExpressions =
      GpuBindReferences.bindGpuReferences(aggOutputExpressions, aggregationsOutput)

    val boundCount = boundAggOutputExpressions.last
    val aggsToRepeat = boundAggOutputExpressions.slice(0, boundAggOutputExpressions.length - 1)
    val aggsToRepeatOutput = aggregationsOutput.slice(0, aggregationsOutput.length - 1)
    (boundCount, aggsToRepeatOutput, aggsToRepeat)
  }

  def computeFinalProject(rideAlongOutput: Seq[Attribute],
      aggsToRepeatOutput: Seq[Attribute],
      windowOps: Seq[NamedExpression]): Seq[GpuExpression] = {
    val combinedOutput = rideAlongOutput ++ aggsToRepeatOutput
    val remapped = windowOps.map { expr =>
      GpuAlias(AttributeReference(expr.name, expr.dataType, expr.nullable)(expr.exprId),
        expr.name)(expr.exprId)
    }
    GpuBindReferences.bindGpuReferences(remapped, combinedOutput)
  }

  /**
   * Break up the window operations into the various needed stages and bind them.
   * @param gpuPartitionSpec the partition spec for the GPU
   * @param windowOps the window operations (along with the pass-through columns)
   * @param childOutput what the output of the operation feeding this looks like
   * @return
   */
  def breakUpAggregations(gpuPartitionSpec: Seq[Expression],
                          windowOps: Seq[NamedExpression],
                          childOutput: Seq[Attribute])
                          : GpuUnboundedToUnboundedAggStages = {

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, childOutput)

    // STEP 1. project that will pull out the columns that are output unchanged.
    val (rideAlongOutput, boundRideAlong) = rideAlongProjection(windowOps, childOutput)

    // STEP 2. project that will pull out the columns needed for the aggregation.
    val (aggregationsOutput, boundAggregations) = tmpAggregationOps(windowOps, childOutput)

    // STEP N: Given the output of the aggregations get count column and the other
    //  columns so we can do the repeat call.
    val (boundCount, aggsToRepeatOutput, aggsToRepeat) = repeatOps(aggregationsOutput)

    // STEP N + 1: After the repeat is done the repeated columns are put at the end of the
    //  rideAlong columns and then we need to do a project that would put them all in the
    //  proper output order, according to the windowOps
    val finalProject = computeFinalProject(rideAlongOutput, aggsToRepeatOutput, windowOps)

    GpuUnboundedToUnboundedAggStages(boundPartitionSpec,
                                     boundRideAlong, boundAggregations,
                                     boundCount, aggsToRepeat, finalProject)
  }

  def apply(input: Iterator[ColumnarBatch],
      boundStages: GpuUnboundedToUnboundedAggStages,
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    val firstPass = new GpuUnboundedToUnboundedAggWindowFirstPassIterator(input, boundStages,
      opTime)
    val secondPass = new GpuUnboundedToUnboundedAggWindowSecondPassIterator(firstPass,
      boundStages, opTime)
    new GpuUnboundedToUnboundedAggFinalIterator(secondPass, boundStages,
      numOutputBatches, numOutputRows, opTime)
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries. This is specifically for unbounded to unbounded window
 * operations that can be replaced with an aggregation and then expanded out/joined with
 * the original input data.
 */
case class GpuUnboundedToUnboundedAggWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  // For this we only need the data to be sorted by the partition columns, but
  //  we don't change the input sort from the CPU yet. In some cases we might even
  //  be able to remove the sort entirely. https://github.com/NVIDIA/spark-rapids/issues/9989
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(cpuPartitionOrdering)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundStages = GpuUnboundedToUnboundedAggWindowIterator.breakUpAggregations(
      gpuPartitionSpec, windowOps, child.output)

    child.executeColumnar().mapPartitions { iter =>
      GpuUnboundedToUnboundedAggWindowIterator(iter, boundStages,
        numOutputBatches, numOutputRows, opTime)
    }
  }
}