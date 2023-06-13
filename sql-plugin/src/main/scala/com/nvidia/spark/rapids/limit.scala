/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec with ShimUnaryExecNode {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME)
  )

  override def output: Seq[Attribute] = child.output

  // The same as what feeds us, even though we might make it smaller
  // the reality is that nothing is coming out after this, so it does fit
  // the requirements
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    sliceRDD(child.executeColumnar(), limit, 0)
  }

  def sliceRDD(rdd: RDD[ColumnarBatch], limit: Int, offset: Int): RDD[ColumnarBatch] = {
    val opTime = gpuLongMetric(OP_TIME)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    rdd.mapPartitions { iter =>
      new Iterator[ColumnarBatch] {
        private var remainingLimit = limit - offset
        private var remainingOffset = offset

        override def hasNext: Boolean = (limit == -1 || remainingLimit > 0) && iter.hasNext

        override def next(): ColumnarBatch = {
          if (!this.hasNext) {
            throw new NoSuchElementException("Next on empty iterator")
          }

          var batch = iter.next()
          val numCols = batch.numCols()

          // In each partition, we need to skip `offset` rows
          while (batch != null && remainingOffset >= batch.numRows()) {
            remainingOffset -= batch.numRows()
            batch.safeClose()
            batch = if (this.hasNext) { iter.next() } else { null }
          }

          // If the last batch is null, then we have offset >= numRows in this partition.
          // In such case, we should return an empty batch
          if (batch == null || batch.numRows() == 0) {
            return new ColumnarBatch(new ArrayBuffer[GpuColumnVector](numCols).toArray, 0)
          }

          // Here 0 <= remainingOffset < batch.numRow(), we need to get batch[remainingOffset:]
          withResource(new NvtxWithMetrics("limit and offset", NvtxColor.ORANGE, opTime)) { _ =>
            var result: ColumnarBatch = null
            // limit < 0 (limit == -1) denotes there is no limitation, so when
            // (remainingOffset == 0 && (remainingLimit >= batch.numRows() || limit < 0)) is true,
            // we can take this batch completely
            if (remainingOffset == 0 && (remainingLimit >= batch.numRows() || limit < 0)) {
              result = batch
            } else {
              // otherwise, we need to slice batch with (remainingOffset, remainingLimit).
              // And remainingOffset > 0 will be used only once, for the latter batches in this
              // partition, set remainingOffset = 0
              val length = if (remainingLimit >= batch.numRows() || limit < 0) {
                batch.numRows()
              } else {
                remainingLimit
              }
              result = sliceBatchWithOffset(batch, remainingOffset, length)
              remainingOffset = 0
            }
            numOutputBatches += 1
            numOutputRows += result.numRows()
            remainingLimit -= result.numRows()
            result
          }
        }

        def sliceBatchWithOffset(batch: ColumnarBatch, offset: Int, limit: Int): ColumnarBatch = {
          val numCols = batch.numCols()
          val end = Math.min(offset + limit, batch.numRows())
          withResource(batch) { _ =>
            // result buffer need to be closed when there is an exception
            closeOnExcept(new ArrayBuffer[GpuColumnVector](numCols)) { result =>
              if (numCols > 0) {
                withResource(GpuColumnVector.from(batch)) { table =>
                  (0 until numCols).zip(output).foreach{ case (i, attr) =>
                    val subVector = table.getColumn(i).subVector(offset, end)
                    assert(subVector != null)
                    result.append(GpuColumnVector.from(subVector, attr.dataType))
                  }
                }
              }
              new ColumnarBatch(result.toArray, end - offset)
            }
          }
        }
      }
    }
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class GpuLocalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GpuGlobalLimitExec(limit: Int = -1, child: SparkPlan,
                              offset: Int = 0) extends GpuBaseLimitExec {
  // In CPU code of spark, there is an assertion 'limit >= 0 || (limit == -1 && offset > 0)'.

  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch]  = {
    super.sliceRDD(child.executeColumnar(), limit, offset)
  }
}

class GpuCollectLimitMeta(
                      collectLimit: CollectLimitExec,
                      conf: RapidsConf,
                      parent: Option[RapidsMeta[_, _, _]],
                      rule: DataFromReplacementRule)
  extends SparkPlanMeta[CollectLimitExec](collectLimit, conf, parent, rule) {
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(collectLimit.outputPartitioning, conf, Some(this)))

  override def convertToGpu(): GpuExec =
    GpuGlobalLimitExec(collectLimit.limit,
      GpuShuffleExchangeExec(
        GpuSinglePartitioning,
        GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
        ENSURE_REQUIREMENTS
      )(SinglePartition), 0)
}

object GpuTopN {
  private[this] def concatAndClose(a: SpillableColumnarBatch,
      b: ColumnarBatch,
      concatTime: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("readNConcat", NvtxColor.CYAN, concatTime)) { _ =>
      val dataTypes = GpuColumnVector.extractTypes(b)
      val aTable = withResource(a) { a =>
        withResource(a.getColumnarBatch()) { aBatch =>
          GpuColumnVector.from(aBatch)
        }
      }
      val ret = withResource(aTable) { aTable =>
        withResource(b) { b =>
          withResource(GpuColumnVector.from(b)) { bTable =>
            Table.concatenate(aTable, bTable)
          }
        }
      }
      withResource(ret) { ret =>
        GpuColumnVector.from(ret, dataTypes)
      }
    }
  }

  private[this] def sliceBatch(batch: ColumnarBatch, begin: Int, limit: Int): ColumnarBatch = {
    val end = Math.min(limit, batch.numRows())
    val start = Math.max(0, Math.min(begin, end))
    val numColumns = batch.numCols()
    closeOnExcept(new Array[ColumnVector](numColumns)) { columns =>
      val bases = GpuColumnVector.extractBases(batch)
      (0 until numColumns).foreach { i =>
        columns(i) =
          GpuColumnVector.from(bases(i).subVector(start, end), batch.column(i).dataType())
      }
      new ColumnarBatch(columns, end - start)
    }
  }

  private[this] def takeN(batch: ColumnarBatch, count: Int): ColumnarBatch = {
    sliceBatch(batch, 0, count)
  }


  private[this] def applyOffset(batch: ColumnarBatch, offset: Int): ColumnarBatch = {
    sliceBatch(batch, offset, batch.numRows())
  }

  def apply(limit: Int,
      sorter: GpuSorter,
      batch: ColumnarBatch,
      sortTime: GpuMetric): ColumnarBatch = {
    withResource(sorter.fullySortBatch(batch, sortTime)) { sorted =>
      takeN(sorted, limit)
    }
  }

  def apply(limit: Int,
      sorter: GpuSorter,
      iter: Iterator[ColumnarBatch],
      opTime: GpuMetric,
      sortTime: GpuMetric,
      concatTime: GpuMetric,
      inputBatches: GpuMetric,
      inputRows: GpuMetric,
      outputBatches: GpuMetric,
      outputRows: GpuMetric,
      offset: Int): Iterator[ColumnarBatch] =
    new Iterator[ColumnarBatch]() {
      override def hasNext: Boolean = iter.hasNext

      private[this] var pending: Option[SpillableColumnarBatch] = None

      override def next(): ColumnarBatch = {
        while (iter.hasNext) {
          val input = iter.next()
          withResource(new NvtxWithMetrics("TOP N", NvtxColor.ORANGE, opTime)) { _ =>
            inputBatches += 1
            inputRows += input.numRows()
            lazy val totalSize = GpuColumnVector.getTotalDeviceMemoryUsed(input) +
                pending.map(_.sizeInBytes).getOrElse(0L)

            val runningResult = if (pending.isEmpty) {
              withResource(input) { input =>
                apply(limit, sorter, input, sortTime)
              }
            } else if (totalSize > Integer.MAX_VALUE) {
              // The intermediate size is likely big enough we don't want to risk an overflow,
              // so sort/slice before we concat and sort/slice again.
              val tmp = withResource(input) { input =>
                apply(limit, sorter, input, sortTime)
              }
              withResource(concatAndClose(pending.get, tmp, concatTime)) { concat =>
                apply(limit, sorter, concat, sortTime)
              }
            } else {
              // The intermediate size looks like we could never overflow the indexes so
              // do it the more efficient way and concat first followed by the sort/slice
              withResource(concatAndClose(pending.get, input, concatTime)) { concat =>
                apply(limit, sorter, concat, sortTime)
              }
            }
            pending =
                Some(SpillableColumnarBatch(runningResult, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
          }
        }
        val tmp = pending.get.getColumnarBatch()
        pending.get.close()
        pending = None
        val ret = if (offset > 0) {
          withResource(tmp) { _ =>
            applyOffset(tmp, offset)
          }
        } else {
          tmp
        }
        outputBatches += 1
        outputRows += ret.numRows()
        ret
      }
    }
}

/**
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a Limit operator after a `SortExec` operator,
 * or having a `ProjectExec` operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
case class GpuTopN(
    limit: Int,
    gpuSortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0)(
    cpuSortOrder: Seq[SortOrder]) extends GpuBaseLimitExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuSortOrder :: Nil

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    SORT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SORT_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  )

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val sorter = new GpuSorter(gpuSortOrder, child.output)
    val boundProjectExprs = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val opTime = gpuLongMetric(OP_TIME)
    val inputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val inputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val outputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val sortTime = gpuLongMetric(SORT_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val localLimit = limit
    val localProjectList = projectList
    val childOutput = child.output

    child.executeColumnar().mapPartitions { iter =>
      val topN = GpuTopN(localLimit, sorter, iter, opTime, sortTime, concatTime,
        inputBatches, inputRows, outputBatches, outputRows, offset)
      if (localProjectList != childOutput) {
        topN.map { batch =>
          GpuProjectExec.projectAndClose(batch, boundProjectExprs, opTime)
        }
      } else {
        topN
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def outputOrdering: Seq[SortOrder] = cpuSortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(gpuSortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"GpuTopN(limit=$limit, orderBy=$orderByString, output=$outputString, offset=$offset)"
  }
}
