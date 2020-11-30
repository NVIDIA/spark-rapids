/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.util

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Coalesces serialized tables on the host up to the target batch size before transferring
 * the coalesced result to the GPU. This reduces the overhead of copying data to the GPU
 * and also helps avoid holding onto the GPU semaphore while shuffle I/O is being performed.
 * @note This should ALWAYS appear in the plan after a GPU shuffle when RAPIDS shuffle is
 *       not being used.
 */
case class GpuShuffleCoalesceExec(child: SparkPlan, @transient rapidsConf: RapidsConf)
    extends UnaryExecNode with GpuExec {

  import GpuMetricNames._
  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_BATCHES),
    "collectTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "collect batch time"),
    "concatTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "concat batch time")
  )

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val metricsMap = metrics
    val targetBatchByteSize = rapidsConf.gpuTargetBatchSizeBytes
    val sparkSchema = GpuColumnVector.extractTypes(schema)

    child.executeColumnar().mapPartitions { iter =>
      new GpuShuffleCoalesceIterator(iter, targetBatchByteSize, sparkSchema, metricsMap)
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * [[SerializedTableColumn]]. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before the data
 * is transferred to the GPU.
 */
class GpuShuffleCoalesceIterator(
    batchIter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    sparkSchema: Array[DataType],
    metricsMap: Map[String, SQLMetric])
    extends Iterator[ColumnarBatch] with Arm with AutoCloseable {
  private[this] val totalTimeMetric = metricsMap(GpuMetricNames.TOTAL_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetricNames.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetricNames.NUM_INPUT_ROWS)
  private[this] val outputBatchesMetric = metricsMap(GpuMetricNames.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetricNames.NUM_OUTPUT_ROWS)
  private[this] val collectTimeMetric = metricsMap("collectTime")
  private[this] val concatTimeMetric = metricsMap("concatTime")
  private[this] val serializedTables = new util.ArrayDeque[SerializedTableColumn]
  private[this] var numTablesInBatch: Int = 0
  private[this] var numRowsInBatch: Int = 0
  private[this] var batchByteSize: Long = 0L

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def hasNext: Boolean = {
    withResource(new MetricRange(totalTimeMetric)) { _ =>
      bufferNextBatch()
      numTablesInBatch > 0
    }
  }

  override def next(): ColumnarBatch = {
    withResource(new MetricRange(totalTimeMetric)) { _ =>
      if (!hasNext) {
        throw new NoSuchElementException("No more columnar batches")
      }
      concatenateBatch()
    }
  }

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
  }

  private def batchIterHasNext: Boolean = {
    withResource(new MetricRange(collectTimeMetric)) { _ =>
      batchIter.hasNext
    }
  }

  private def batchIterNext(): ColumnarBatch = {
    withResource(new MetricRange(collectTimeMetric)) { _ =>
      batchIter.next()
    }
  }

  private def bufferNextBatch(): Unit = {
    if (numTablesInBatch == serializedTables.size()) {
      var batchCanGrow = batchByteSize < targetBatchByteSize
      while (batchCanGrow && batchIterHasNext) {
        closeOnExcept(batchIterNext()) { batch =>
          inputBatchesMetric += 1
          // don't bother tracking empty tables
          if (batch.numRows > 0) {
            inputRowsMetric += batch.numRows
            val tableColumn = batch.column(0).asInstanceOf[SerializedTableColumn]
            batchCanGrow = canAddToBatch(tableColumn.header)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableColumn.header.getNumRows
              batchByteSize += tableColumn.header.getDataLen
            }
          } else {
            batch.close()
          }
        }
      }
    }
  }

  private def canAddToBatch(nextTable: SerializedTableHeader): Boolean = {
    if (batchByteSize + nextTable.getDataLen > targetBatchByteSize) {
      return false
    }
    if (numRowsInBatch.toLong + nextTable.getNumRows > Integer.MAX_VALUE) {
      return false
    }
    true
  }

  private def concatenateBatch(): ColumnarBatch = {
    // about to start using the GPU in this task
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val firstHeader = serializedTables.peekFirst().header
    val batch = withResource(new MetricRange(concatTimeMetric)) { _ =>
      if (firstHeader.getNumColumns == 0) {
        (0 until numTablesInBatch).foreach(_ => serializedTables.removeFirst())
        new ColumnarBatch(Array.empty, numRowsInBatch)
      } else {
        concatenateTablesBatch()
      }
    }

    outputBatchesMetric += 1
    outputRowsMetric += batch.numRows

    // update the stats for the next batch in progress
    numTablesInBatch = serializedTables.size
    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1, "should only track at most one buffer that is not in a batch")
      val header = serializedTables.peekFirst().header
      batchByteSize = header.getDataLen
      numRowsInBatch = header.getNumRows
    }

    batch
  }

  private def concatenateTablesBatch(): ColumnarBatch = {
    val headers = new Array[SerializedTableHeader](numTablesInBatch)
    withResource(new Array[HostMemoryBuffer](numTablesInBatch)) { buffers =>
      headers.indices.foreach { i =>
        val serializedTable = serializedTables.removeFirst()
        headers(i) = serializedTable.header
        buffers(i) = serializedTable.hostBuffer
      }

      withResource(new NvtxRange("Concat+Load Batch", NvtxColor.YELLOW)) { _ =>
        withResource(JCudfSerialization.concatToContiguousTable(headers, buffers)) { table =>
          GpuColumnVectorFromBuffer.from(table, sparkSchema)
        }
      }
    }
  }
}
