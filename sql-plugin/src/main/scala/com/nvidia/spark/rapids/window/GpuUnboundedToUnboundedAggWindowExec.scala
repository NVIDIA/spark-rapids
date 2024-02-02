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
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.window.TableAndBatchUtils.{getTableSlice, sliceAndMakeSpillable, toSpillableBatch, toTable}
import java.util
import scala.collection.mutable.ListBuffer

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.{CudfAggregate, GpuAggregateExpression, GpuAggregateFunction, GpuCount}
import org.apache.spark.sql.types.{DataType, IntegerType}
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

  def toTable(cb: ColumnarBatch): cudf.Table = GpuColumnVector.from(cb)

  def getTableSlice(tbl: cudf.Table,
                    beginRow: Int, endRow: Int,
                    beginCol: Int, endCol: Int): cudf.Table = {
    val projectedColumns =
      Range(beginCol, endCol).map { i => tbl.getColumn(i).slice(beginRow, endRow).head }
    withResource(projectedColumns) { _ =>
      new cudf.Table(projectedColumns.toArray: _*)
    }
  }

  def getTableSlice(tbl: cudf.Table, beginRow: Int, endRow: Int): cudf.Table = {
    getTableSlice(tbl, beginRow, endRow, beginCol=0, endCol=tbl.getNumberOfColumns)
  }

  def sliceAndMakeSpillable(tbl: cudf.Table,
                            rowBegin: Int,
                            rowEnd: Int,
                            dataTypes: Seq[DataType]): SpillableColumnarBatch = {
    withResource(getTableSlice(tbl, rowBegin, rowEnd)) { sliced =>
      toSpillableBatch(GpuColumnVector.from(sliced, dataTypes.toArray))
    }
  }

  // TODO: Remove, when merging the PR.
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

  def debug(msg: String, table: cudf.Table): Unit = {
    System.err.println()
    cudf.TableDebug.get.debug(msg, table)
    System.err.println()
    System.err.println()
  }
}

class GpuUnboundedToUnboundedAggWindowFirstPassIterator(
    input: Iterator[ColumnarBatch],
    boundStages: GpuUnboundedToUnboundedAggStages,
    opTime: GpuMetric) extends Iterator[FirstPassAggResult] {
  private var subIterator: Option[Iterator[FirstPassAggResult]] = None
  override def hasNext: Boolean = subIterator.exists(_.hasNext) || input.hasNext

  private def getSpillableInputBatch: SpillableColumnarBatch = {
    toSpillableBatch(input.next)
  }

  // "Fixes up" the count aggregate results, by casting up to INT64.
  // TODO: This should not be required after fixing project after update.
  private def upscaleCountResults(unfixed: cudf.Table): cudf.Table = {
    // The group "count" result is in the last column, with type INT32.
    // Cast this up to INT64.  Return the other columns unchanged.
    val nCols = unfixed.getNumberOfColumns
    val fixedCols = Range(0, nCols).map {
      case i if i != nCols-1 => unfixed.getColumn(i).incRefCount()
      case _ => unfixed.getColumn(nCols - 1).castTo(cudf.DType.INT64)
    }
    withResource(fixedCols) { _ =>
      new cudf.Table(fixedCols: _*)
    }
  }

  // Append column at the end to account for the added `GpuCount(1)`.
  private def preProcess(inputCB: ColumnarBatch): ColumnarBatch = {
    withResource(GpuColumnVector.from(inputCB)) { inputTable =>
      withResource(cudf.Scalar.fromInt(1)) { one =>
        withResource(
          cudf.ColumnVector.fromScalar(one,
            inputTable.getColumn(0).getRowCount.asInstanceOf[Int])) { ones =>
          val columns = Range(0, inputTable.getNumberOfColumns)
                          .map {inputTable.getColumn} :+ ones
          withResource(new cudf.Table(columns: _*)) { preProcessedTable =>
            GpuColumnVector.from(preProcessedTable,
                                 boundStages.inputTypes.toArray :+ IntegerType)
          }
        }
      }
    }
  }

  private def groupByAggregate(inputCB: ColumnarBatch) = {
    // Note: The data is always ordered first by the grouping keys,
    // as ASC NULLS FIRST, regardless of how the order-by columns
    // are ordered.  This happens prior to the window exec, since
    // the GpuSortOrder is upstream from the window exec.
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
      // The COUNT aggregate result (at the end) is returned from libcudf as an INT32,
      // while Spark expects an `INT64`.  This needs to be scaled up.
      withResource(aggResults) {
        upscaleCountResults
      }
    }
  }

  override def next(): FirstPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next()).get
    } else {
      opTime.ns {
        val currIter = withRetry(getSpillableInputBatch, splitSpillableInHalfByRows) { scb =>
          withResource(scb.getColumnarBatch()) { cb =>
            withResource(preProcess(cb)) { preProcessedInput =>
              withResource(groupByAggregate(preProcessedInput)) { aggResultTable =>
                val rideAlongColumns = GpuProjectExec.project(preProcessedInput,
                                                              boundStages.boundRideAlong)

                FirstPassAggResult(
                  toSpillableBatch(rideAlongColumns),
                  toSpillableBatch(aggResultTable,
                    boundStages.groupingColumnTypes ++ boundStages.aggResultTypes))
              }
            }
          }
        }
        val result = currIter.next()
        subIterator = Some(currIter)
        result
      }
    }
  }
}

case class PartitionedFirstPassAggResult(firstPassAggResult: FirstPassAggResult,
                                         boundStages: GpuUnboundedToUnboundedAggStages) {
  var lastGroupAggResult: Option[SpillableColumnarBatch] = None
  var lastGroupRideAlong: Option[SpillableColumnarBatch] = None
  var otherGroupAggResult: Option[SpillableColumnarBatch] = None
  var otherGroupRideAlong: Option[SpillableColumnarBatch] = None

  val numGroups: Int = firstPassAggResult.aggResult.numRows()
  private val numGroupingKeys: Int = boundStages.boundPartitionSpec.size
  private val numRideAlongRows: Int = firstPassAggResult.rideAlongColumns.numRows()

  if (numGroups < 2) {
      throw new IllegalStateException("Expected at least two result groups.")
  }

  /**
   * The `rideAlongGroupsTable` is the projection of the group rows from "rideAlong" columns
   * from the first pass aggregation.  There could well be repeats of the group values,
   * once for every "rideAlong" row in the same group.
   * The `aggResultTable` has one row per group; no repeats.
   * This helper function finds the beginning index (in groupTable) for the last group
   * in `aggResultTable`.
   */
  private def getStartIndexForLastGroup(aggResultTable: cudf.Table,
                                        rideAlongGroupsTable: cudf.Table): Int = {
    val lastRowIndex = aggResultTable.getRowCount.asInstanceOf[Int] - 1
    withResource(getTableSlice(aggResultTable,
                               beginRow = lastRowIndex,
                               endRow = lastRowIndex + 1,
                               beginCol = 0,
                               endCol = numGroupingKeys)) { group =>
      // The grouping keys are always ordered ASC NULLS FIRST,
      // regardless of how the order-by columns are ordered.
      // Searching for a group does not involve the order-by columns in any way.
      // A simple `lowerBound` does the trick.
      val orderBys = Range(0, numGroupingKeys).map(i => OrderByArg.asc(i, true))
      withResource(rideAlongGroupsTable.lowerBound(group, orderBys: _*)) { groupMargin =>
        withResource(groupMargin.copyToHost()) { groupMarginHost =>
          groupMarginHost.getInt(0)
        }
      }
    }
  }

  withResource(firstPassAggResult.rideAlongColumns.getColumnarBatch()) { rideAlongCB =>
    withResource(GpuProjectExec.project(rideAlongCB, boundStages.boundPartitionSpec)) { rideGrpCB =>
      withResource(GpuColumnVector.from(rideGrpCB)) { rideAlongGroupsTable =>
        withResource(firstPassAggResult.aggResult.getColumnarBatch()) { aggResultsCB =>
          withResource(GpuColumnVector.from(aggResultsCB)) { aggResultTable =>
            val lastGroupBeginIdx = getStartIndexForLastGroup(aggResultTable,
                                                              rideAlongGroupsTable)
            withResource(GpuColumnVector.from(rideAlongCB)) { rideAlongTable =>
              // Slice and dice!
              val aggResultTypes = boundStages.groupingColumnTypes ++ boundStages.aggResultTypes
              val rideAlongTypes = boundStages.rideAlongColumnTypes

              lastGroupAggResult = Some(sliceAndMakeSpillable(aggResultTable,
                                                              numGroups - 1,
                                                              numGroups,
                                                              aggResultTypes))
              lastGroupRideAlong = Some(sliceAndMakeSpillable(rideAlongTable,
                                                              lastGroupBeginIdx,
                                                              numRideAlongRows,
                                                              rideAlongTypes))

              otherGroupAggResult = Some(sliceAndMakeSpillable(aggResultTable,
                                                               0,
                                                               numGroups - 1,
                                                               aggResultTypes))
              otherGroupRideAlong = Some(sliceAndMakeSpillable(rideAlongTable,
                                                               0,
                                                               lastGroupBeginIdx,
                                                               rideAlongTypes))
            }
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
  private var rideAlongColumnsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()
  // Agg results where the input keys are not fully complete yet. They will need to be combined
  // together before being returned.
  private var aggResultsPendingCompletion = ListBuffer.empty[SpillableColumnarBatch]

  // Register cleanup for incomplete shutdown.
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      Range(0, rideAlongColumnsPendingCompletion.size).foreach { i =>
        rideAlongColumnsPendingCompletion.get(i).close()
      }
      aggResultsPendingCompletion.foreach{_.close}
    }
  }

  override def hasNext: Boolean = (!rideAlongColumnsPendingCompletion.isEmpty) || input.hasNext

  private def removeGroupColumns(aggResults: SpillableColumnarBatch): SpillableColumnarBatch = {
    val aggResultTable = withResource(aggResults.getColumnarBatch()) { toTable }
    val numColumnsToSkip = boundStages.boundPartitionSpec.size
    val groupResultsRemovedCB = withResource(aggResultTable) {
      GpuColumnVector.from(_, boundStages.aggResultTypes.toArray, numColumnsToSkip,
                           aggResultTable.getNumberOfColumns)
    }
    SpillableColumnarBatch(groupResultsRemovedCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  /**
   * Concatenates all underlying tables in `inputSCB`, and returns
   * a SpillableColumnarBatch of the result.
   */
  private def concat(inputSCB: Seq[SpillableColumnarBatch],
                     schema: Seq[DataType])
    : SpillableColumnarBatch = {

    val tables = inputSCB.map { scb =>
      withResource(scb.getColumnarBatch()) {
        toTable
      }
    }.toArray

    val resultTable = if (tables.length == 1) {
      tables.head
    }
    else {
      withResource(tables) { _ =>
        cudf.Table.concatenate(tables: _*)
      }
    }

    withResource(resultTable) {
      toSpillableBatch(_, schema)
    }
  }

  private def groupByMerge(aggResultSCB: SpillableColumnarBatch) = {
    // Note: The data is always ordered first by the grouping keys,
    // as ASC NULLS FIRST, regardless of how the order-by columns
    // are ordered.  This happens prior to the window exec, since
    // the GpuSortOrder is upstream from the window exec.
    val groupByOptions = cudf.GroupByOptions.builder()
                                            .withIgnoreNullKeys(false)
                                            .withKeysSorted(true)
                                            .build
    val numGroupColumns = boundStages.groupingColumnTypes.size
    val cudfMergeAggregates = boundStages.cudfMergeAggregates
    val cudfAggsOnColumns = cudfMergeAggregates.zipWithIndex.map {
      case (mergeAgg, ord) => mergeAgg.groupByAggregate.onColumn(ord + numGroupColumns)
    }
    val aggResultTable = withResource(aggResultSCB.getColumnarBatch()) { toTable }
    val mergeResults = withResource(aggResultTable) {
      _.groupBy(groupByOptions, Range(0, numGroupColumns).toArray: _*)
       .aggregate(cudfAggsOnColumns: _*)
    }
    withResource(mergeResults) { _ =>
      val mergeResultTypes = boundStages.groupingColumnTypes ++ boundStages.aggResultTypes
      val cb = GpuColumnVector.from(mergeResults, mergeResultTypes.toArray)
      SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    }
  }

  private def processNewData(newData: FirstPassAggResult): Option[SecondPassAggResult] = {

    if (newData.aggResult.numRows() == 1) {
      // All the aggregation results are for the same group.
      // Add the lot to "incomplete".  No results with this input.
      rideAlongColumnsPendingCompletion.add(newData.rideAlongColumns.incRefCount())
      aggResultsPendingCompletion += newData.aggResult.incRefCount()
      None
    }
    else {
      opTime.ns {
        val partitioned = withRetryNoSplit(newData) {
          PartitionedFirstPassAggResult(_, boundStages)
        }
        // There are at least two aggregation result rows.

        // 1. Set aside the last agg result row (incomplete), and its rideAlong.
        // 2. Append the rest of the results together.  Run agg merge.
        // 3. Save last agg result and rideAlong as currently incomplete.
        // 4. Return merge results as the result batch.

        val completedAggResults =
          aggResultsPendingCompletion ++ partitioned.otherGroupAggResult

        val result = withRetryNoSplit(completedAggResults) { _ =>
          withResource(concat(completedAggResults,
                              boundStages.groupingColumnTypes ++
                                boundStages.aggResultTypes)) { concatAggResults =>
            withResource(groupByMerge(concatAggResults)) { mergedAggResults =>
              val completedRideAlongBatches =
                rideAlongColumnsPendingCompletion.clone // Cloned for exception/retry safety.
                  .asInstanceOf[util.LinkedList[SpillableColumnarBatch]]
              completedRideAlongBatches.add(partitioned.otherGroupRideAlong.get)
              val groupsRemoved = removeGroupColumns(mergedAggResults)
              SecondPassAggResult(completedRideAlongBatches,
                                  groupsRemoved)
            }
          }
        }

        // Output has been calculated. Set last group's data in "pendingCompletion".
        rideAlongColumnsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()
        rideAlongColumnsPendingCompletion.add(partitioned.lastGroupRideAlong.get)
        aggResultsPendingCompletion = ListBuffer.tabulate(1) { _ =>
          partitioned.lastGroupAggResult.get
        }

        Some(result)
      }
    }
  }

  override def next(): SecondPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    var output: Option[SecondPassAggResult] = None
    while (output.isEmpty) {
      if (input.hasNext) {
        withResource(input.next()) { newData =>
          output = processNewData(newData)
        }
      } else {
        opTime.ns {
          // No more input. All pending batches can now be assumed complete.
          output = withRetryNoSplit(aggResultsPendingCompletion) { _ =>
            withResource(concat(aggResultsPendingCompletion,
                                boundStages.groupingColumnTypes ++
                                  boundStages.aggResultTypes)) { concatAggResults =>
              withResource(groupByMerge(concatAggResults)) { mergedAggResults =>
                Some(SecondPassAggResult(rideAlongColumnsPendingCompletion,
                                         removeGroupColumns(mergedAggResults)))
              }
            }
          }
          // Final output has been calculated. It is safe to reset the buffers.
          aggResultsPendingCompletion = ListBuffer.empty[SpillableColumnarBatch]
          rideAlongColumnsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]
        }
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
            withResource(boundStages.boundCount.columnarEval(cb)) { counts =>
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
    inputTypes: Seq[DataType],
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
    // TODO: Cannot assume GpuLiteral is always for GpuCount. Maybe check agg type first.
    case GpuLiteral(_, _) => inputTypes.size // An all 1s column appended at the end.
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

    val childTypes = childOutput.map{_.dataType}

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

    GpuUnboundedToUnboundedAggStages(childTypes,
                                     boundPartitionSpec,
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