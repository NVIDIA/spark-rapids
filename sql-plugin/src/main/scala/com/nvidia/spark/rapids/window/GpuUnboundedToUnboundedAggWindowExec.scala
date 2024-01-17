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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf
import com.nvidia.spark.rapids.{ConcatAndConsumeAll, GpuAlias, GpuBindReferences, GpuColumnVector, GpuExpression, GpuLiteral, GpuMetric, GpuProjectExec, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import java.util

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.{GpuAggregateExpression, GpuCount}
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


/**
 * Just a simple wrapper to make working with buffers of AutoClosable things play
 * nicely with withResource.
 */
class AutoClosableArrayBuffer[T <: AutoCloseable]() extends AutoCloseable {
  private val data = new ArrayBuffer[T]()

  def append(scb: T): Unit = data.append(scb)

  def last: T = data.last

  def removeLast(): T = data.remove(data.length - 1)

  def foreach[U](f: T => U): Unit = data.foreach(f)

  def toArray[B >: T : ClassTag]: Array[B] = data.toArray

  override def toString: String = s"AutoCloseable(${super.toString})"

  override def close(): Unit = {
    data.foreach(_.close())
    data.clear()
  }
}

// It is not really simple to do a single iterator that can do the splits and retries along with
// The data as needed. Instead we are going to decompose the problem into multiple iterators that
// feed into each other.
// The first pass iterator will take in a batch of data and produce one or more aggregated result
// pairs that include the ride-along columns with the aggregation results for that batch.
// Note that it is assumed that the aggregation was done as a sort based aggregation, so
// the ride-along columns and the aggregation result should both be sorted by the partition by
// columns.  Also the aggregation result must have a count column so it can be expanded using
// repeat to get back to the size of the ride-along columns.
case class FirstPassAggResult(rideAlongColumns: SpillableColumnarBatch,
    aggResult: SpillableColumnarBatch) extends AutoCloseable {
  override def close(): Unit = {
    rideAlongColumns.close()
    aggResult.close()
  }
}

class GpuUnboundedToUnboundedAggWindowFirstPassIterator(
    input: Iterator[ColumnarBatch],
    boundStages: GpuUnboundedToUnboundedAggStages,
    opTime: GpuMetric) extends Iterator[FirstPassAggResult] {
  private var subIterator: Option[Iterator[FirstPassAggResult]] = None
  override def hasNext: Boolean = subIterator.exists(_.hasNext) || input.hasNext

  override def next(): FirstPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next()).get
    } else {
      val currIter = withRetry(
        SpillableColumnarBatch(input.next(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
        splitSpillableInHalfByRows) { scb =>
        withResource(scb.getColumnarBatch()) { cb =>
          // TODO actually do the agg
          throw new IllegalStateException("Do the agg!!!")
        }
      }
      val result = currIter.next()
      subIterator = Some(currIter)
      result
    }
  }
}

// The second pass through the data will take the output of the first pass. It will slice
// the result depending on if it knows that the group by keys is complete or not.
// Completed data will have the aggregation results merged into a single aggregation result
// Note that this aggregation result needs to remain sorted.  The result is returned as
// an iterator of ride-along columns, and the full agg results for those columns. It is not
// the responsibility of the second stage to try and combine small batches or split up large
// ones, beyond what the retry framework might do.
case class SecondPassAggResult(rideAlongColumns: util.LinkedList[SpillableColumnarBatch],
    aggResult: SpillableColumnarBatch) extends AutoCloseable {
  override def close(): Unit = {
    rideAlongColumns.forEach(_.close())
    rideAlongColumns.clear()
    aggResult.close()
  }
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

  override def next(): SecondPassAggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    var output: Option[SecondPassAggResult] = None
    while (output.isEmpty) {
      if (input.hasNext) {
        withResource(input.next()) { newData =>
          // TODO remove this line. It is here to avoid compile warnings becoming errors
          output = None
          throw new IllegalStateException("Actually split the inputs")
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

// The next to final step is to take the original input data along with the agg data, estimate how
// to split/combine the input batches to output batches that are close to the target batch size.

case class SlicedBySize(rideAlongColumns: SpillableColumnarBatch,
    aggResults: SpillableColumnarBatch) extends AutoCloseable {
  override def close(): Unit = {
    rideAlongColumns.close()
    aggResults.close()
  }
}

object PendingSecondAggResults {
  def apply(result: SecondPassAggResult,
      boundStages: GpuUnboundedToUnboundedAggStages,
      targetSizeBytes: Long,
      opTime: GpuMetric): PendingSecondAggResults = {
    closeOnExcept(result) { _ =>
      new PendingSecondAggResults(result.rideAlongColumns, result.aggResult,
        boundStages, targetSizeBytes, opTime)
    }
  }

  def makeBatch(columns: Array[cudf.ColumnVector], types: Array[DataType]): ColumnarBatch = {
    val tmp = columns.zip(types).map {
      case (c, t) => GpuColumnVector.from(c, t).asInstanceOf[ColumnVector]
    }
    new ColumnarBatch(tmp, columns(0).getRowCount.toInt)
  }

  def splitCb(cb: ColumnarBatch, inclusiveCutPoint: Int): (ColumnarBatch, ColumnarBatch) = {
    // First save the types
    val types = GpuColumnVector.extractTypes(cb)
    // Slice is at the column level, not at a table level
    closeOnExcept(new ArrayBuffer[cudf.ColumnVector]()) { before =>
      val afterCb = closeOnExcept(new ArrayBuffer[cudf.ColumnVector]()) { after =>
        GpuColumnVector.extractBases(cb).foreach { base =>
          val result = base.split(inclusiveCutPoint)
          before.append(result(0))
          after.append(result(1))
          assert(result.length == 2)
        }
        makeBatch(after.toArray, types)
      }
      closeOnExcept(afterCb) { _ =>
        (makeBatch(before.toArray, types), afterCb)
      }
    }
  }

  def sliceInclusiveCb(cb: ColumnarBatch, inclusiveStart: Int, inclusiveEnd: Int): ColumnarBatch = {
    // First save the types
    val types = GpuColumnVector.extractTypes(cb)
    // Slice is at the column level, not at a table level
    closeOnExcept(new ArrayBuffer[cudf.ColumnVector]()) { cbs =>
      GpuColumnVector.extractBases(cb).foreach { base =>
        val result = base.slice(inclusiveStart, inclusiveEnd + 1)
        cbs.append(result(0))
        assert(result.length == 1)
      }
      makeBatch(cbs.toArray, types)
    }
  }

  /**
   * Makes a boolean vector where only one row is true.
   * @param trueRow the row that should be true
   * @param size the total number of rows.
   */
  def makeSingleRowMask(trueRow: Int, size: Int): cudf.ColumnVector = {
    assert(size > trueRow, s"$size > $trueRow")
    // TODO probably want an optimization if the size is really small
    val rowsBefore = trueRow
    val rowsAfter = size - trueRow - 1
    if (rowsBefore == 0 && rowsAfter == 0) {
      // Special Case where we cannot concat
      cudf.ColumnVector.fromBooleans(true)
    } else {
      withResource(new AutoClosableArrayBuffer[cudf.ColumnView]) { toConcat =>
        withResource(cudf.Scalar.fromBool(false)) { fs =>
          if (rowsBefore > 0) {
            toConcat.append(cudf.ColumnVector.fromScalar(fs, rowsBefore))
          }
          toConcat.append(cudf.ColumnVector.fromBooleans(true))
          if (rowsAfter > 0) {
            toConcat.append(cudf.ColumnVector.fromScalar(fs, rowsAfter))
          }
        }
        cudf.ColumnVector.concatenate(toConcat.toArray: _*)
      }
    }
  }

  def replaceCountInAggAt(cb: ColumnarBatch, countRow: Int, newCount: Long): ColumnarBatch = {
    // TODO I'm sure there is a lot we can do to optimize this, but this works...
    withResource(AggResultBatchConventions.getRepeatedAggColumns(cb)) { aggColumns =>
      val newCountCv = withResource(AggResultBatchConventions.getCount(cb)) { count =>
        withResource(makeSingleRowMask(countRow, count.getRowCount.toInt)) { mask =>
          withResource(cudf.Scalar.fromLong(newCount)) { ncScalar =>
            mask.ifElse(ncScalar, count.getBase)
          }
        }
      }
      withResource(newCountCv) { _ =>
        AggResultBatchConventions.appendCountColumn(aggColumns, newCountCv)
      }
    }
  }

  def concatBatchesAndClose(toConcat: AutoClosableArrayBuffer[SpillableColumnarBatch],
      opTime: GpuMetric): SpillableColumnarBatch = {
    val cb = withRetryNoSplit(toConcat) { _ =>
      opTime.ns {
        val ready = closeOnExcept(new AutoClosableArrayBuffer[ColumnarBatch]) { cbs =>
          toConcat.foreach { scb =>
            cbs.append(scb.getColumnarBatch())
          }
          cbs.toArray
        }
        // This consumes/closes the array of batches
        ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(ready,
          GpuColumnVector.extractTypes(ready.head))
      }
    }
    SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
  }

  def splitAggResultByRepeatedRows(aggResult: SpillableColumnarBatch,
      targetRows: Int,
      totalRows: Long): (SpillableColumnarBatch, SpillableColumnarBatch) = {
    // We have high confidence that we need to split this in two, but even then we don't
    // have enough information here to know that we don't need to split it without
    // processing the batch
    withResource(aggResult.getColumnarBatch()) { cb =>
      if (cb.numRows() == 1) {
        // This is a very common special case where there is one and only one row, so
        // we need to keep all of the columns the same, but slice the count row accordingly.
        withResource(AggResultBatchConventions.getRepeatedAggColumns(cb)) { aggs =>
          // The aggs are just repeated, but the count is new
          val firstPart = withResource(cudf.ColumnVector.fromLongs(targetRows)) { count =>
            AggResultBatchConventions.appendCountColumn(aggs, count)
          }
          val secondPart = closeOnExcept(firstPart) { _ =>
            withResource(cudf.ColumnVector.fromLongs(totalRows - targetRows)) {
              count =>
                AggResultBatchConventions.appendCountColumn(aggs, count)
            }
          }
          (SpillableColumnarBatch(firstPart, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
              SpillableColumnarBatch(secondPart, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
        }
      } else {
        // This is a little complicated in the general case. We need to find which row
        // in the aggregation we need to split on. The only way to do that is to get a
        // running sum of the counts, and then do an upper bound on that column
        withResource(AggResultBatchConventions.getCount(cb)) { counts =>
          val (splitIndex, countToKeep, countForNextTime) =
            withResource(counts.getBase.prefixSum()) { runningCount =>
              val splitIndex = withResource(new cudf.Table(runningCount)) { runningCountTable =>
                withResource(cudf.ColumnVector.fromLongs(targetRows)) { tr =>
                  withResource(new cudf.Table(tr)) { targetRowsTable =>
                    runningCountTable.lowerBound(Array(true), targetRowsTable, Array(false))
                  }
                }
              }
              withResource(splitIndex) { _ =>
                val indexToLookAt = withResource(splitIndex.getScalarElement(0)) { s =>
                  s.getInt
                }
                val totalRowsUpToIndex = withResource(
                  runningCount.getScalarElement(indexToLookAt)) { s =>
                  s.getLong
                }
                val countInRow = withResource(counts.getBase.getScalarElement(indexToLookAt)) { s =>
                  s.getLong
                }
                val countToKeep = targetRows - (totalRowsUpToIndex - countInRow)
                val countForNextTime = countInRow - countToKeep
                (indexToLookAt, countToKeep, countForNextTime)
              }
            }
          if (countForNextTime == 0) {
            // We got lucky and it is on an agg boundary
            val (a, b) = splitCb(cb, splitIndex + 1)
            (SpillableColumnarBatch(a, SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
                SpillableColumnarBatch(b, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
          } else {
            val scbFirst = withResource(sliceInclusiveCb(cb, 0, splitIndex)) { first =>
              SpillableColumnarBatch(replaceCountInAggAt(first, splitIndex, countToKeep),
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            }
            closeOnExcept(scbFirst) { _ =>
              val scbSecond = withResource(sliceInclusiveCb(cb, splitIndex, cb.numRows() - 1)) {
                second =>
                  SpillableColumnarBatch(replaceCountInAggAt(second, 0, countForNextTime),
                    SpillPriorities.ACTIVE_BATCHING_PRIORITY)
              }
              (scbFirst, scbSecond)
            }
          }
        }
      }
    }
  }
}

class PendingSecondAggResults private(
    private val rideAlongColumns: util.LinkedList[SpillableColumnarBatch],
    private var aggResult: SpillableColumnarBatch,
    private val boundStages: GpuUnboundedToUnboundedAggStages,
    private val targetSizeBytes: Long,
    opTime: GpuMetric) extends Iterator[SlicedBySize] with AutoCloseable {
  import PendingSecondAggResults._

  private var totalRowsInAgg = {
    var total = 0L
    rideAlongColumns.forEach(total += _.numRows())
    total
  }

  override def hasNext: Boolean = !rideAlongColumns.isEmpty

  /**
   * We want to estimate the average size per row that the aggregations will add. This
   * does not have to be perfect because we will back it up with a split and retry handling
   * that can slice the output in half. We are also going to include the count column because
   * I don't want to read the data back, if it spilled.
   */
  private def estimateAggSizePerRow: Double =
    aggResult.sizeInBytes.toDouble / aggResult.numRows()

  /**
   * Gets the next batch of ride along columns to process.
   */
  private def getRideAlongToProcess(): SpillableColumnarBatch = {
    val averageAggSizePerRow = estimateAggSizePerRow
    var currentSize = 0L
    var numRowsTotal = 0

    // First pull in the batches that might be enough to process
    val toProcess = new AutoClosableArrayBuffer[SpillableColumnarBatch]()
    closeOnExcept(toProcess) { _ =>
      while (currentSize < targetSizeBytes && !rideAlongColumns.isEmpty) {
        val scb = rideAlongColumns.pop()
        toProcess.append(scb)
        val numRows = scb.numRows()
        val estimatedSize = (scb.sizeInBytes + (numRows * averageAggSizePerRow)).toLong
        numRowsTotal += numRows
        currentSize += estimatedSize
      }

      if (currentSize > targetSizeBytes) {
        // If we buffered too much data we need to decide how to slice it, but we only
        // want to slice the last batch in toProcess because we know that the batch before
        // it was not large enough to send us over the limit. We do this by estimating how
        // many rows we need from toProcess and hence how many rows we need to remove.
        val avgSizePerRow = currentSize.toDouble / numRowsTotal
        val estimatedRowsToKeep = math.ceil(targetSizeBytes / avgSizePerRow).toLong
        val estimatedRowsToRemove = numRowsTotal - estimatedRowsToKeep

        // If we need to remove more rows, than the last batch has, we just remove the last batch
        val numRowsToRemove = if (estimatedRowsToRemove >= toProcess.last.numRows) {
          val theLastOne = toProcess.removeLast()
          rideAlongColumns.addFirst(theLastOne)
          // We probably don't need to update numRowsTotal, but it is just to be defensive
          numRowsTotal -= theLastOne.numRows()
          0
        } else {
          numRowsTotal - estimatedRowsToKeep
        }

        if (numRowsToRemove > 0) {
          // We need to slice the last batch
          val theLastOne = toProcess.removeLast()
          val numRowsToKeepInLastBatch = (theLastOne.numRows() - numRowsToRemove).toInt
          val (keep, forNextTime) = withRetryNoSplit(theLastOne) { _ =>
            opTime.ns {
              withResource(theLastOne.getColumnarBatch()) { cb =>
                splitCb(cb, numRowsToKeepInLastBatch)
              }
            }
          }
          rideAlongColumns.addFirst(SpillableColumnarBatch(forNextTime,
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY))

          toProcess.append(SpillableColumnarBatch(keep,
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
        }
      }
    }
    concatBatchesAndClose(toProcess, opTime)
  }

  def getSlicedAggResultByRepeatedRows(numDesiredRows: Int): SpillableColumnarBatch = {
    val (ret, keep) = withRetryNoSplit(aggResult) { _ =>
      splitAggResultByRepeatedRows(aggResult, numDesiredRows, totalRowsInAgg)
    }
    totalRowsInAgg -= numDesiredRows
    aggResult = keep
    ret
  }

  override def next(): SlicedBySize = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    closeOnExcept(getRideAlongToProcess()) { rideAlongScb =>
      if (rideAlongColumns.isEmpty) {
        // This is the last batch so we don't need to even figure out where to slice
        // the AggResult
        SlicedBySize(rideAlongScb, aggResult.incRefCount())
      } else {
        SlicedBySize(rideAlongScb, getSlicedAggResultByRepeatedRows(rideAlongScb.numRows()))
      }
    }
  }

  override def close(): Unit = {
    rideAlongColumns.forEach(_.close())
    rideAlongColumns.clear()
    aggResult.close()
  }
}

/**
 * Try to slice the input batches into right sized output.
 */
class GpuUnboundedToUnboundedAggSliceBySizeIterator(
    input: Iterator[SecondPassAggResult],
    boundStages: GpuUnboundedToUnboundedAggStages,
    targetSizeBytes: Long,
    opTime: GpuMetric) extends Iterator[SlicedBySize] {

  private var pending: Option[PendingSecondAggResults] = None
  private def pendingHasNext: Boolean = pending.exists(_.hasNext)

  override def hasNext: Boolean = pendingHasNext || input.hasNext

  override def next(): SlicedBySize = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }

    if (!pendingHasNext) {
      pending = Some(PendingSecondAggResults(input.next(), boundStages, targetSizeBytes, opTime))
    }
    val ret = pending.get.next()
    // avoid leaks in the tests
    if (!pendingHasNext) {
      pending.get.close()
      pending = None
    }
    ret
  }

  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      close()
    }
  }

  def close(): Unit = {
    pending.foreach(_.close())
    pending = None
  }
}

// The final step is to expand the data to match that size, combine everything together and
// return the result.

class GpuUnboundedToUnboundedAggFinalIterator(
    input: Iterator[SlicedBySize],
    boundStages: GpuUnboundedToUnboundedAggStages,
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = input.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    // TODO we need to add in the split to the retry

    withRetryNoSplit(input.next()) { toExpand =>
      opTime.ns {
        // The first stage is to expand the aggregate based on the count column
        val repeatedAggs = withResource(toExpand.aggResults.getColumnarBatch()) { cb =>
          withResource(AggResultBatchConventions.getCount(cb)) { counts =>
            withResource(AggResultBatchConventions.getRepeatedAggColumns(cb)) { toRepeat =>
              val dataTypes = GpuColumnVector.extractTypes(toRepeat)
              withResource(GpuColumnVector.from(toRepeat)) { table =>
                withResource(table.repeat(counts.getBase)) { repeated =>
                  GpuColumnVector.from(repeated, dataTypes)
                }
              }
            }
          }
        }
        // Second step is to stitch the two together
        val combined = withResource(repeatedAggs) { _ =>
          withResource(toExpand.rideAlongColumns.getColumnarBatch()) { rideAlong =>
            GpuColumnVector.appendColumns(rideAlong,
              GpuColumnVector.extractColumns(repeatedAggs): _*)
          }
        }
        withResource(combined) { _ =>
          closeOnExcept(GpuProjectExec.project(combined, boundStages.boundFinalProject)) { ret =>
            numOutputBatches += 1
            numOutputRows += ret.numRows()
            ret
          }
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
 * @param boundFinalProject the final project to get the output in the right order
 */
case class GpuUnboundedToUnboundedAggStages(
    boundRideAlong: Seq[GpuExpression],
    boundAggregations: Seq[GpuExpression],
    boundFinalProject: Seq[GpuExpression]) extends Serializable

object AggResultBatchConventions {
  private def getColumnFromBatch(cb: ColumnarBatch, colId: Int): ColumnVector = {
    val ret = cb.column(colId)
    ret.asInstanceOf[GpuColumnVector].incRefCount()
    ret
  }

  def getCount(cb: ColumnarBatch): GpuColumnVector = {
    // By convention the last column is the count column
    getColumnFromBatch(cb, cb.numCols() - 1).asInstanceOf[GpuColumnVector]
  }

  def getRepeatedAggColumns(cb: ColumnarBatch): ColumnarBatch = {
    // By convention all of the columns, except the last one are agg columns
    val columns = (0 until cb.numCols() - 1).safeMap { index =>
      getColumnFromBatch(cb, index)
    }
    new ColumnarBatch(columns.toArray, cb.numRows())
  }

  def appendCountColumn(repeatedAggColumns: ColumnarBatch,
      counts: cudf.ColumnVector): ColumnarBatch = {
    val countCol = GpuColumnVector.fromChecked(counts, LongType)
    GpuColumnVector.appendColumns(repeatedAggColumns, countCol)
  }
}

/**
 * An iterator that can do unbounded to unbounded window aggregations as group by aggregations
 * followed by an expand/join.
 */
object GpuUnboundedToUnboundedAggWindowIterator {
  def rideAlongProjection(windowOps: Seq[NamedExpression],
      childOutput: Seq[Attribute]): (Seq[Attribute], Seq[GpuExpression]) = {
    val rideAlong = windowOps.filter {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => true
      case _ => false
    }
    val rideAlongOutput = rideAlong.map(_.toAttribute)
    val boundRideAlong = GpuBindReferences.bindGpuReferences(rideAlong, childOutput)
    (rideAlongOutput, boundRideAlong)
  }


  def tmpAggregationOps(windowOps: Seq[NamedExpression],
      childOutput: Seq[Attribute]): (Seq[Attribute], Seq[GpuExpression]) = {
    //  TODO I don't know what this is really going to look like. I am just doing an approximation
    //    here so I can get the output of the aggregations after everything is done for the
    //    repeat. Please fill this in/split it apart, whatever to make it work for you
    val windowAggs = windowOps.flatMap {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => None
      case ga@GpuAlias(GpuWindowExpression(agg: GpuUnboundedToUnboundedWindowAgg, _), _) =>
        // We don't care about the spec, they are all unbounded to unbounded so just get the func
        // We do care that we keep the expression id so we can line it up at the very end
        Some(GpuAlias(agg, ga.name)(ga.exprId))
      case ga@GpuAlias(GpuWindowExpression(GpuAggregateExpression(
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
    // thrown away. We should never try and dedupe this count with an existing count column,
    // because if we need to slice the aggregation results we will modify the count column
    // to do that. This will not work if we are going to output that count column.

    val aggregationsOutput = windowAggs.map(_.toAttribute)
    val boundAggregations = GpuBindReferences.bindGpuReferences(windowAggs, childOutput)
    (aggregationsOutput, boundAggregations)
  }

  def repeatOps(aggregationsOutput: Seq[Attribute]): Seq[Attribute] = {
    // By convention the last column in the aggs is the count column we want to use
    aggregationsOutput.slice(0, aggregationsOutput.length - 1)
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
      childOutput: Seq[Attribute]): GpuUnboundedToUnboundedAggStages = {
    // STEP 1. project that will pull out the columns that are output unchanged.
    val (rideAlongOutput, boundRideAlong) = rideAlongProjection(windowOps, childOutput)

    // STEP 2. project that will pull out the columns needed for the aggregation.
    val (aggregationsOutput, boundAggregations) = tmpAggregationOps(windowOps, childOutput)

    // STEP N: Given the output of the aggregations get the aggregations without that count.
    // The count and aggs locations is by convention.
    val aggsToRepeatOutput = repeatOps(aggregationsOutput)

    // STEP N + 1: After the repeat is done the repeated columns are put at the end of the
    //  rideAlong columns and then we need to do a project that would put them all in the
    //  proper output order, according to the windowOps
    val finalProject = computeFinalProject(rideAlongOutput, aggsToRepeatOutput, windowOps)

    GpuUnboundedToUnboundedAggStages(boundRideAlong, boundAggregations, finalProject)
  }

  def apply(input: Iterator[ColumnarBatch],
      boundStages: GpuUnboundedToUnboundedAggStages,
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric,
      targetSizeBytes: Long): Iterator[ColumnarBatch] = {
    val firstPass = new GpuUnboundedToUnboundedAggWindowFirstPassIterator(input, boundStages,
      opTime)
    val secondPass = new GpuUnboundedToUnboundedAggWindowSecondPassIterator(firstPass,
      boundStages, opTime)
    val slicedBySize = new GpuUnboundedToUnboundedAggSliceBySizeIterator(secondPass,
      boundStages, targetSizeBytes, opTime)
    new GpuUnboundedToUnboundedAggFinalIterator(slicedBySize, boundStages,
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
    override val cpuOrderSpec: Seq[SortOrder],
    targetSizeBytes: Long) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] =
    cpuPartitionSpec :: cpuOrderSpec :: targetSizeBytes.asInstanceOf[java.lang.Long] :: Nil

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
        numOutputBatches, numOutputRows, opTime, targetSizeBytes)
    }
  }
}