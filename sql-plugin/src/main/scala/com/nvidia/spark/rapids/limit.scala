/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{CollectLimitExec, LimitExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    GPU_OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_OP_TIME)
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val gpuOpTime = gpuLongMetric(GPU_OP_TIME)

    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      new Iterator[ColumnarBatch] {
        private var remainingLimit = limit

        override def hasNext: Boolean = remainingLimit > 0 && cbIter.hasNext

        override def next(): ColumnarBatch = {
          val batch = cbIter.next()
          withResource(new NvtxWithMetrics("limit", NvtxColor.ORANGE, gpuOpTime)) { _ =>
            val result = if (batch.numRows() > remainingLimit) {
              sliceBatch(batch)
            } else {
              batch
            }
            numOutputBatches += 1
            numOutputRows += result.numRows()
            remainingLimit -= result.numRows()
            result
          }
        }

        def sliceBatch(batch: ColumnarBatch): ColumnarBatch = {
          val numColumns = batch.numCols()
          val resultCVs = new ArrayBuffer[GpuColumnVector](numColumns)
          var exception: Throwable = null
          var table: Table = null
          try {
            if (numColumns > 0) {
              table = GpuColumnVector.from(batch)
              (0 until numColumns).zip(output).foreach{ case (i, attr) =>
                val subVector = table.getColumn(i).subVector(0, remainingLimit)
                assert(subVector != null)
                resultCVs.append(GpuColumnVector.from(subVector, attr.dataType))
                assert(subVector.getRowCount == remainingLimit,
                  s"result rowcount ${subVector.getRowCount} is not equal to the " +
                    s"remainingLimit $remainingLimit")
              }
            }
            new ColumnarBatch(resultCVs.toArray, remainingLimit)
          } catch {
            case e: Throwable => exception = e
              throw e
          } finally {
            if (exception != null) {
              resultCVs.foreach(gpuVector => gpuVector.safeClose(exception))
            }
            if (table != null) {
              table.safeClose(exception)
            }
            batch.safeClose(exception)
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
case class GpuGlobalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec {
  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
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
      ShimLoader.getSparkShims.getGpuShuffleExchangeExec(GpuSinglePartitioning,
        GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded())))

}

object GpuTopN extends Arm {
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

  private[this] def takeN(batch: ColumnarBatch, count: Int): ColumnarBatch = {
    val end = Math.min(count, batch.numRows())
    val numColumns = batch.numCols()
    closeOnExcept(new Array[ColumnVector](numColumns)) { columns =>
      val bases = GpuColumnVector.extractBases(batch)
      (0 until numColumns).foreach { i =>
        columns(i) = GpuColumnVector.from(bases(i).subVector(0, end), batch.column(i).dataType())
      }
      new ColumnarBatch(columns, end)
    }
  }

  def apply(limit: Int,
      sorter: GpuSorter,
      batch: ColumnarBatch,
      sortTime: GpuMetric): ColumnarBatch = {
    withResource(sorter.fullySortBatch(batch, sortTime, NoopMetric)) { sorted =>
      takeN(sorted, limit)
    }
  }

  def apply(limit: Int,
      sorter: GpuSorter,
      iter: Iterator[ColumnarBatch],
      totalTime: GpuMetric,
      sortTime: GpuMetric,
      concatTime: GpuMetric,
      inputBatches: GpuMetric,
      inputRows: GpuMetric,
      outputBatches: GpuMetric,
      outputRows: GpuMetric,
      spillCallback: RapidsBuffer.SpillCallback): Iterator[ColumnarBatch] =
    new Iterator[ColumnarBatch]() {
      override def hasNext: Boolean = iter.hasNext

      private[this] var pending: Option[SpillableColumnarBatch] = None

      override def next(): ColumnarBatch = {
        while (iter.hasNext) {
          val input = iter.next()
          withResource(new NvtxWithMetrics("TOP N", NvtxColor.ORANGE, totalTime)) { _ =>
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
                Some(SpillableColumnarBatch(runningResult, SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
                  spillCallback))
          }
        }
        val ret = pending.get.getColumnarBatch()
        pending.get.close()
        pending = None
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
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan) extends GpuExec with UnaryExecNode {

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME),
    CONCAT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME)
  ) ++ spillMetrics

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sorter = new GpuSorter(sortOrder, child.output)
    val boundProjectExprs = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val inputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val inputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val outputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val outputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val sortTime = gpuLongMetric(SORT_TIME)
    val concatTime = gpuLongMetric(CONCAT_TIME)
    val callback = GpuMetric.makeSpillCallback(allMetrics)
    child.executeColumnar().mapPartitions { iter =>
      val topN = GpuTopN(limit, sorter, iter, totalTime, sortTime, concatTime,
        inputBatches, inputRows, outputBatches, outputRows, callback)
      if (projectList != child.output) {
        topN.map { batch =>
          GpuProjectExec.projectAndClose(batch, boundProjectExprs, totalTime)
        }
      } else {
        topN
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"GpuTopN(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }
}