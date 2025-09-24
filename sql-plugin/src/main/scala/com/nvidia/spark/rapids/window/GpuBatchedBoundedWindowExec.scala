/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, NvtxColor, Table => CudfTable}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBatchedBoundedWindowIterator(
  input: Iterator[ColumnarBatch],
  override val boundWindowOps: Seq[GpuExpression],
  override val boundPartitionSpec: Seq[GpuExpression],
  override val boundOrderSpec: Seq[SortOrder],
  val outputTypes: Array[DataType],
  minPreceding: Int,
  maxFollowing: Int,
  numOutputBatches: GpuMetric,
  numOutputRows: GpuMetric,
  opTime: GpuMetric) extends Iterator[ColumnarBatch] with BasicWindowCalc with Logging {

  override def isRunningBatched: Boolean = false  // Not "Running Window" optimized.
                                                  // This is strictly for batching.

  override def hasNext: Boolean = numUnprocessedInCache > 0 || input.hasNext

  var cached: Option[Array[CudfColumnVector]] = None  // For processing with the next batch.

  private var numUnprocessedInCache: Int = 0  // numRows at the bottom not processed completely.
  private var numPrecedingRowsAdded: Int = 0  // numRows at the top, added for preceding context.

  // Register handler to clean up cache when task completes.
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      clearCached()
    }
  }

  // Caches input column schema on first read.
  var inputTypes: Option[Array[DataType]] = None

  // Clears cached column vectors, after consumption.
  private def clearCached(): Unit = {
    cached.foreach(_.foreach(_.close))
    cached = None
  }

  private def getNextInputBatch: SpillableColumnarBatch = {
    // Sets column batch types using the types cached from the
    // first input column read.
    def optionallySetInputTypes(inputCB: ColumnarBatch): Unit = {
      if (inputTypes.isEmpty) {
        inputTypes = Some(GpuColumnVector.extractTypes(inputCB))
      }
    }

    // Reads fresh batch from iterator, initializes input data-types if necessary.
    def getFreshInputBatch: ColumnarBatch = {
      val fresh_batch = input.next()
      optionallySetInputTypes(fresh_batch)
      fresh_batch
    }

    def concatenateColumns(cached: Array[CudfColumnVector],
                           freshBatchTable: CudfTable)
    : Array[CudfColumnVector] = {

      if (cached.length != freshBatchTable.getNumberOfColumns) {
        throw new IllegalArgumentException("Expected the same number of columns " +
          "in input batch and cached batch.")
      }
      cached.zipWithIndex.map { case (cachedCol, idx) =>
        CudfColumnVector.concatenate(cachedCol, freshBatchTable.getColumn(idx))
      }
    }

    // Either cached has unprocessed rows, or input.hasNext().
    if (input.hasNext) {
      if (cached.isDefined) {
        // Cached input AND new input rows exist.  Return concat-ed rows.
        withResource(getFreshInputBatch) { freshBatchCB =>
          withResource(GpuColumnVector.from(freshBatchCB)) { freshBatchTable =>
            withResource(concatenateColumns(cached.get, freshBatchTable)) { concat =>
              clearCached()
              SpillableColumnarBatch(convertToBatch(inputTypes.get, concat),
                SpillPriorities.ACTIVE_BATCHING_PRIORITY)
            }
          }
        }
      } else {
          // No cached input available. Return fresh input rows, only.
          SpillableColumnarBatch(getFreshInputBatch,
                                 SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
    else {
      // No fresh input available. Return cached input.
      val cachedCB = convertToBatch(inputTypes.get, cached.get)
      clearCached()
      SpillableColumnarBatch(cachedCB,
                             SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    }
  }

  /**
   * Helper to trim specified number of rows off the top and bottom,
   * of all specified columns.
   */
  private def trim(columns: Array[CudfColumnVector],
                    offTheTop: Int,
                    offTheBottom: Int): Array[CudfColumnVector] = {

    def checkValidSizes(col: CudfColumnVector): Unit =
      if ((offTheTop + offTheBottom) > col.getRowCount) {
        throw new IllegalArgumentException(s"Cannot trim column of size ${col.getRowCount} by " +
          s"$offTheTop rows at the top, and $offTheBottom rows at the bottom.")
      }

    columns.map{ col =>
      checkValidSizes(col)
      col.subVector(offTheTop, col.getRowCount.toInt - offTheBottom)
    }
  }

  private def resetInputCache(newCache: Option[Array[CudfColumnVector]],
                              newPrecedingAdded: Int): Unit= {
    cached.foreach(_.foreach(_.close))
    cached = newCache
    numPrecedingRowsAdded = newPrecedingAdded
  }

  override def next(): ColumnarBatch = {
    var outputBatch: ColumnarBatch = null
    while (outputBatch == null  &&  hasNext) {
      withResource(getNextInputBatch) { inputCbSpillable =>
        withResource(inputCbSpillable.getColumnarBatch()) { inputCB =>

          val inputRowCount = inputCB.numRows()
          val noMoreInput = !input.hasNext
          numUnprocessedInCache = if (noMoreInput) {
            // If there are no more input rows expected,
            // this is the last output batch.
            // Consider all rows in the batch as processed.
            0
          } else {
            // More input rows expected. The last `maxFollowing` rows can't be finalized.
            // Cannot exceed `inputRowCount`.
            if (maxFollowing < 0) { // E.g. LAG(3) => [ preceding=-3, following=-3 ]
              // -ve following => No need to wait for more following rows.
              // All "following" context is already available in the current batch.
              0
            } else {
              maxFollowing min inputRowCount
            }
          }

          if (numPrecedingRowsAdded + numUnprocessedInCache >= inputRowCount) {
            // No point calling windowing kernel: the results will simply be ignored.
            logWarning("Not enough rows! Cannot output a batch.")
          } else {
            withResource(new NvtxWithMetrics("window", NvtxColor.CYAN, opTime)) { _ =>
              withResource(computeBasicWindow(inputCB)) { outputCols =>
                outputBatch =  withResource(
                                  trim(outputCols,
                                    numPrecedingRowsAdded, numUnprocessedInCache)) { trimmed =>
                                  convertToBatch(outputTypes, trimmed)
                }
              }
            }
          }

          // Compute new cache using current input.
          numPrecedingRowsAdded = if (minPreceding > 0) { // E.g. LEAD(3) => [prec=3, foll=3]
            // preceding > 0 => No "preceding" rows need be carried forward.
            // Only the rows that need to be recomputed.
            0
          } else {
            Math.abs(minPreceding) min (inputRowCount - numUnprocessedInCache)
          }
          val inputCols = Range(0, inputCB.numCols()).map {
            inputCB.column(_).asInstanceOf[GpuColumnVector].getBase
          }.toArray

          val newCached = trim(inputCols,
            inputRowCount - (numPrecedingRowsAdded + numUnprocessedInCache),
            0)
          resetInputCache(Some(newCached), numPrecedingRowsAdded)
        }
      }
    }
    numOutputBatches += 1
    numOutputRows += outputBatch.numRows()
    outputBatch
  }
}

/// Window Exec used exclusively for batching bounded window functions.
class GpuBatchedBoundedWindowExec(
    override val windowOps: Seq[NamedExpression],
    override val gpuPartitionSpec: Seq[Expression],
    override val gpuOrderSpec: Seq[SortOrder],
    override val child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder],
    minPreceding: Integer,
    maxFollowing: Integer
) extends GpuWindowExec(windowOps,
                        gpuPartitionSpec,
                        gpuOrderSpec,
                        child)(cpuPartitionSpec, cpuOrderSpec) {

  override def otherCopyArgs: Seq[AnyRef] =
    cpuPartitionSpec :: cpuOrderSpec :: minPreceding :: maxFollowing :: Nil

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq.fill(children.size)(null)

  override def outputBatching: CoalesceGoal = null

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME_LEGACY)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
      new GpuBatchedBoundedWindowIterator(iter, boundWindowOps, boundPartitionSpec,
        boundOrderSpec, output.map(_.dataType).toArray, minPreceding, maxFollowing,
        numOutputBatches, numOutputRows, opTime)
    }
  }
}