/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import scala.reflect.ClassTag

import ai.rapids.cudf.{JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Coalesces serialized tables on the host up to the target batch size before transferring
 * the coalesced result to the GPU. This reduces the overhead of copying data to the GPU
 * and also helps avoid holding onto the GPU semaphore while shuffle I/O is being performed.
 * @note This should ALWAYS appear in the plan after a GPU shuffle when RAPIDS shuffle is
 *       not being used.
 */
case class GpuShuffleCoalesceExec(child: SparkPlan, targetBatchByteSize: Long)
    extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  )

  override protected val outputBatchesLevel = MODERATE_LEVEL

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val metricsMap = allMetrics
    val targetSize = targetBatchByteSize
    val dataTypes = GpuColumnVector.extractTypes(schema)
    val readOption = CoalesceReadOption(new RapidsConf(conf))

    child.executeColumnar().mapPartitions { iter =>
      GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(iter, targetSize, dataTypes,
        readOption, metricsMap)
    }
  }
}

/** A case class to pack some options. Now it has only one, but may have more in the future */
case class CoalesceReadOption private(kudoEnabled: Boolean)

object CoalesceReadOption {
  def apply(conf: RapidsConf): CoalesceReadOption = {
    // TODO get the value from conf
    CoalesceReadOption(false)
  }
}

object GpuShuffleCoalesceUtils {
  def getGpuShuffleCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      targetSize: Long,
      dataTypes: Array[DataType],
      readOption: CoalesceReadOption,
      metricsMap: Map[String, GpuMetric],
      prefetchFirstBatch: Boolean = false): Iterator[ColumnarBatch] = {
    val hostIter = if (readOption.kudoEnabled) {
      // TODO replace with the actual Kudo host iterator
      Iterator.empty
    } else {
      new HostShuffleCoalesceIterator(iter, targetSize, metricsMap)
    }
    val maybeBufferedIter = if (prefetchFirstBatch) {
      val bufferedIter = new CloseableBufferedIterator(hostIter)
      withResource(new NvtxRange("fetch first batch", NvtxColor.YELLOW)) { _ =>
        // Force a coalesce of the first batch before we grab the GPU semaphore
        bufferedIter.headOption
      }
      bufferedIter
    } else {
      hostIter
    }
    new GpuShuffleCoalesceIterator(maybeBufferedIter, dataTypes, metricsMap)
  }

  /**
   * Return an iterator that can coalesce serialized batches if they are just
   * returned from the Shuffle deserializer. Otherwise, None will be returned.
   */
  def getHostShuffleCoalesceIterator(
      iter: BufferedIterator[ColumnarBatch],
      targetSize: Long,
      coalesceMetrics: Map[String, GpuMetric]): Option[Iterator[AutoCloseable]] = {
    var retIter: Option[Iterator[AutoCloseable]] = None
    if (iter.hasNext && iter.head.numCols() == 1) {
      iter.head.column(0) match {
        // TODO add the Kudo case
        case _: SerializedTableColumn =>
          retIter = Some(new HostShuffleCoalesceIterator(iter, targetSize, coalesceMetrics))
        case _ => // should be gpu batches
      }
    }
    retIter
  }

  /** Get the buffer size of a serialized batch just returned by the Shuffle deserializer */
  def getSerializedBufferSize(cb: ColumnarBatch): Long = {
    assert(cb.numCols() == 1)
    val hmb = cb.column(0) match {
      // TODO add the Kudo case
      case serCol: SerializedTableColumn => serCol.hostBuffer
      case o => throw new IllegalStateException(s"unsupported type: ${o.getClass}")
    }
    if (hmb != null) hmb.getLength else 0L
  }

  /**
   * Get the buffer size of the coalesced result, it accepts either a concatenated
   * host buffer from the Shuffle coalesce exec, or a coalesced GPU batch.
   */
  def getCoalescedBufferSize(concated: AnyRef): Long = concated match {
    case c: HostConcatResult => c.getTableHeader.getDataLen
    case g => GpuColumnVector.getTotalDeviceMemoryUsed(g.asInstanceOf[ColumnarBatch])
  }

  /** Try to convert a coalesced host buffer to a GPU batch. */
  def coalescedResultToGpuIfAllowed(
      coalescedResult: AnyRef,
      dataTypes: Array[DataType]): ColumnarBatch = coalescedResult match {
    case c: HostConcatResult =>
      cudf_utils.HostConcatResultUtil.getColumnarBatch(c, dataTypes)
    case o =>
      throw new IllegalArgumentException(s"unsupported type: ${o.getClass}")
  }
}

/**
 * A trait defining some operations on the table T.
 * This is used by HostCoalesceIteratorBase to separate the table operations from
 * the shuffle read process.
 */
sealed trait TableOperator[T <: AutoCloseable, C] {
  def getDataLen(table: T): Long
  def getNumRows(table: T): Int
  def concatOnHost(tables: Array[T]): C
}

class CudfTableOperator extends TableOperator[SerializedTableColumn, HostConcatResult] {
  override def getDataLen(table: SerializedTableColumn): Long = table.header.getDataLen
  override def getNumRows(table: SerializedTableColumn): Int = table.header.getNumRows

  override def concatOnHost(tables: Array[SerializedTableColumn]): HostConcatResult = {
    assert(tables.nonEmpty, "no tables to be concatenated")
    val numCols = tables.head.header.getNumColumns
    if (numCols == 0) {
      val totalRowsNum = tables.map(getNumRows).sum
      cudf_utils.HostConcatResultUtil.rowsOnlyHostConcatResult(totalRowsNum)
    } else {
      val (headers, buffers) = tables.map(t => (t.header, t.hostBuffer)).unzip
      JCudfSerialization.concatToHostBuffer(headers, buffers)
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * serialized tables from shuffle. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
abstract class HostCoalesceIteratorBase[T <: AutoCloseable: ClassTag, C](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric]) extends Iterator[C] with AutoCloseable {
  private[this] val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
  private[this] val inputBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
  private[this] val inputRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
  private[this] val serializedTables = new util.ArrayDeque[T]
  private[this] var numTablesInBatch: Int = 0
  private[this] var numRowsInBatch: Int = 0
  private[this] var batchByteSize: Long = 0L

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  protected def tableOperator: TableOperator[T, C]

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
  }

  private def concatenateTablesInHost(): C = {
    val result = withResource(new MetricRange(concatTimeMetric)) { _ =>
      withResource(new Array[T](numTablesInBatch)) { tables =>
        tables.indices.foreach(i => tables(i) = serializedTables.removeFirst())
        tableOperator.concatOnHost(tables)
      }
    }

    // update the stats for the next batch in progress
    numTablesInBatch = serializedTables.size
    batchByteSize = 0
    numRowsInBatch = 0
    if (numTablesInBatch > 0) {
      require(numTablesInBatch == 1,
        "should only track at most one buffer that is not in a batch")
      val firstTable = serializedTables.peekFirst()
      batchByteSize = tableOperator.getDataLen(firstTable)
      numRowsInBatch = tableOperator.getNumRows(firstTable)
    }
    result
  }

  private def bufferNextBatch(): Unit = {
    if (numTablesInBatch == serializedTables.size()) {
      var batchCanGrow = batchByteSize < targetBatchByteSize
      while (batchCanGrow && iter.hasNext) {
        closeOnExcept(iter.next()) { batch =>
          inputBatchesMetric += 1
          // don't bother tracking empty tables
          if (batch.numRows > 0) {
            inputRowsMetric += batch.numRows()
            val tableColumn = batch.column(0).asInstanceOf[T]
            batchCanGrow = canAddToBatch(tableColumn)
            serializedTables.addLast(tableColumn)
            // always add the first table to the batch even if its beyond the target limits
            if (batchCanGrow || numTablesInBatch == 0) {
              numTablesInBatch += 1
              numRowsInBatch += tableOperator.getNumRows(tableColumn)
              batchByteSize += tableOperator.getDataLen(tableColumn)
            }
          } else {
            batch.close()
          }
        }
      }
    }
  }

  override def hasNext(): Boolean = {
    bufferNextBatch()
    numTablesInBatch > 0
  }

  override def next(): C = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }
    concatenateTablesInHost()
  }

  private def canAddToBatch(nextTable: T): Boolean = {
    if (batchByteSize + tableOperator.getDataLen(nextTable) > targetBatchByteSize) {
      return false
    }
    if (numRowsInBatch.toLong + tableOperator.getNumRows(nextTable) > Integer.MAX_VALUE) {
      return false
    }
    true
  }
}

class HostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    metricsMap: Map[String, GpuMetric])
  extends HostCoalesceIteratorBase[SerializedTableColumn, HostConcatResult](iter,
    targetBatchByteSize, metricsMap) {
  override protected def tableOperator = new CudfTableOperator
}

/**
 * Iterator that expects only the coalesced host buffers as the input, and transfers
 * the host buffers to GPU.
 */
class GpuShuffleCoalesceIterator(iter: Iterator[AnyRef],
    dataTypes: Array[DataType],
    metricsMap: Map[String, GpuMetric]) extends Iterator[ColumnarBatch] {
  private[this] val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
  private[this] val outputBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
  private[this] val outputRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more columnar batches")
    }
    withResource(new NvtxRange("Concat+Load Batch", NvtxColor.YELLOW)) { _ =>
      val hostConcatResult = withResource(new MetricRange(opTimeMetric)) { _ =>
        // op time covers concat time performed in `iter.next()`.
        // Note the concat runs on CPU.
        // GPU time = opTime - concatTime
        iter.next()
      }

      withResourceIfAllowed(hostConcatResult) { _ =>
        // We acquire the GPU regardless of whether `hostConcatResult`
        // is an empty batch or not, because the downstream tasks expect
        // the `GpuShuffleCoalesceIterator` to acquire the semaphore and may
        // generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        withResource(new MetricRange(opTimeMetric)) { _ =>
          val batch = GpuShuffleCoalesceUtils.coalescedResultToGpuIfAllowed(
            hostConcatResult, dataTypes)
          outputBatchesMetric += 1
          outputRowsMetric += batch.numRows()
          batch
        }
      }
    }
  }
}
