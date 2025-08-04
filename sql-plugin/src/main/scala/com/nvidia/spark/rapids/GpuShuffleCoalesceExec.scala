/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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
import java.util.concurrent.{Future, TimeUnit}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.{JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.FileUtils.createTempFile
import com.nvidia.spark.rapids.GpuMetric.{ASYNC_READ_TIME, SYNC_READ_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.io.async.{ThrottlingExecutor, TrafficController}
import com.nvidia.spark.rapids.jni.kudo.{DumpOption, KudoHostMergeResultWrapper, KudoSerializer, MergeOptions}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Coalesces serialized tables on the host up to the target batch size before transferring
 * the coalesced result to the GPU. This reduces the overhead of copying data to the GPU
 * and also helps avoid holding onto the GPU semaphore while shuffle I/O is being performed.
 *
 * @note This should ALWAYS appear in the plan after a GPU shuffle when RAPIDS shuffle is
 *       not being used.
 */
case class GpuShuffleCoalesceExec(child: SparkPlan, targetBatchByteSize: Long)
  extends ShimUnaryExecNode with GpuExec {

  import GpuMetric._
  import GpuShuffleCoalesceUtils._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    SYNC_READ_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SYNC_READ_TIME),
    ASYNC_READ_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_ASYNC_READ_TIME),
    READ_THROTTLING_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_READ_THROTTLING_TIME),
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
    val readOption = CoalesceReadOption(conf)

    child.executeColumnar().mapPartitions { iter =>
      getGpuShuffleCoalesceIterator(iter, targetSize, dataTypes,
        readOption, metricsMap)
    }
  }
}

/** A case class to pack some options. */
case class CoalesceReadOption private(
    kudoEnabled: Boolean, kudoDebugMode: DumpOption, kudoDebugDumpPrefix: Option[String],
    useAsync: Boolean)

object CoalesceReadOption {
  def apply(conf: SQLConf): CoalesceReadOption = {
    val dumpOption = RapidsConf.SHUFFLE_KUDO_SERIALIZER_DEBUG_MODE.get(conf) match {
      case "NEVER" => DumpOption.Never
      case "ALWAYS" => DumpOption.Always
      case "ONFAILURE" => DumpOption.OnFailure
    }
    CoalesceReadOption(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.get(conf),
      dumpOption,
      RapidsConf.SHUFFLE_KUDO_SERIALIZER_DEBUG_DUMP_PREFIX.get(conf),
      RapidsConf.SHUFFLE_ASYNC_READ_ENABLED.get(conf))
  }

  def apply(conf: RapidsConf): CoalesceReadOption = {
    CoalesceReadOption(conf.shuffleKudoSerializerEnabled,
      conf.shuffleKudoSerializerDebugMode,
      conf.shuffleKudoSerializerDebugDumpPrefix,
      conf.shuffleAsyncReadEnabled)
  }
}

object GpuShuffleCoalesceUtils {
  /**
   * Return an iterator that will pull in batches from the input iterator,
   * concatenate them up to the "targetSize" and move the concatenated result
   * to the GPU for each output batch.
   * The input iterator is expected to contain only serialized host batches just
   * returned from the Shuffle deserializer. Otherwise, it will blow up.
   *
   * @param iter               the input iterator containing only serialized host batches
   * @param targetSize         the target batch size for coalescing
   * @param dataTypes          the schema of the input batches
   * @param readOption         the coalesce read option
   * @param metricsMap         metrics map
   * @param prefetchFirstBatch whether prefetching the first bundle of serialized
   *                           batches with the total size up to the "targetSize". The
   *                           prefetched batches will be cached on host until the "next()"
   *                           is called. This is for some optimization cases in join.
   */
  def getGpuShuffleCoalesceIterator(
      iter: Iterator[ColumnarBatch],
      targetSize: Long,
      dataTypes: Array[DataType],
      readOption: CoalesceReadOption,
      metricsMap: Map[String, GpuMetric],
      prefetchFirstBatch: Boolean = false): Iterator[ColumnarBatch] = {
    val concatTimeMetric = metricsMap(GpuMetric.CONCAT_TIME)
    val inBatchesMetric = metricsMap(GpuMetric.NUM_INPUT_BATCHES)
    val inRowsMetric = metricsMap(GpuMetric.NUM_INPUT_ROWS)
    val outBatchesMetric = metricsMap(GpuMetric.NUM_OUTPUT_BATCHES)
    val outRowsMetric = metricsMap(GpuMetric.NUM_OUTPUT_ROWS)
    val opTimeMetric = metricsMap(GpuMetric.OP_TIME)
    val readThrottlingMetric = metricsMap(GpuMetric.READ_THROTTLING_TIME)
    val hostIter = if (readOption.kudoEnabled) {
      new KudoHostShuffleCoalesceIterator(iter, targetSize, dataTypes, concatTimeMetric,
        inBatchesMetric, inRowsMetric, readOption)
    } else {
      new HostShuffleCoalesceIterator(iter, targetSize, concatTimeMetric, inBatchesMetric,
        inRowsMetric)
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
    if (readOption.useAsync) {
      new GpuShuffleAsyncCoalesceIterator(maybeBufferedIter, dataTypes, targetSize,
        outBatchesMetric, outRowsMetric, metricsMap(ASYNC_READ_TIME), opTimeMetric,
        readThrottlingMetric)
    } else {
      new GpuShuffleCoalesceIterator(maybeBufferedIter, dataTypes, outBatchesMetric,
        outRowsMetric, metricsMap(SYNC_READ_TIME), opTimeMetric)
    }
  }

  /** Get the buffer size of a serialized batch just returned by the Shuffle deserializer */
  def getSerializedBufferSize(cb: ColumnarBatch): Long = {
    assert(cb.numCols() == 1)
    cb.column(0) match {
      case col: KudoSerializedTableColumn => col.spillableKudoTable.length
      case serCol: SerializedTableColumn => {
        val hmb = serCol.hostBuffer
        if (hmb != null) hmb.getLength else 0L
      }
      case o => throw new IllegalStateException(s"unsupported type: ${o.getClass}")
    }
  }
}

/**
 * A trait representing the shuffle coalesced result by the Shuffle coalesce iterator.
 */
trait CoalescedHostResult extends AutoCloseable {
  /** Convert itself to a GPU batch */
  def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch

  /** Get the data size */
  def getDataSize: Long
}

/**
 * A trait defining some operations on the table T.
 * This is used by HostCoalesceIteratorBase to separate the table operations from
 * the shuffle read process.
 */
sealed trait SerializedTableOperator[T <: AutoCloseable] {
  def getDataLen(table: T): Long

  def getNumRows(table: T): Int

  def concatOnHost(tables: Array[T]): CoalescedHostResult
}

class JCudfCoalescedHostResult(hostConcatResult: HostConcatResult) extends CoalescedHostResult {
  assert(hostConcatResult != null, "hostConcatResult should not be null")

  override def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch =
    cudf_utils.HostConcatResultUtil.getColumnarBatch(hostConcatResult, dataTypes)

  override def close(): Unit = hostConcatResult.close()

  override def getDataSize: Long = hostConcatResult.getTableHeader.getDataLen
}

class JCudfTableOperator extends SerializedTableOperator[SerializedTableColumn] {
  override def getDataLen(table: SerializedTableColumn): Long = table.header.getDataLen

  override def getNumRows(table: SerializedTableColumn): Int = table.header.getNumRows

  override def concatOnHost(tables: Array[SerializedTableColumn]): CoalescedHostResult = {
    assert(tables.nonEmpty, "no tables to be concatenated")
    val numCols = tables.head.header.getNumColumns
    val ret = if (numCols == 0) {
      val totalRowsNum = tables.map(getNumRows).sum
      cudf_utils.HostConcatResultUtil.rowsOnlyHostConcatResult(totalRowsNum)
    } else {
      val (headers, buffers) = tables.map(t => (t.header, t.hostBuffer)).unzip
      JCudfSerialization.concatToHostBuffer(headers, buffers)
    }
    new JCudfCoalescedHostResult(ret)
  }
}

case class RowCountOnlyMergeResult(rowCount: Int) extends CoalescedHostResult {
  override def toGpuBatch(dataTypes: Array[DataType]): ColumnarBatch = {
    new ColumnarBatch(Array.empty, rowCount)
  }

  override def getDataSize: Long = 0

  override def close(): Unit = {}
}

class KudoTableOperator(kudo: Option[KudoSerializer], readOption: CoalesceReadOption,
    taskIdentifier: String)
  extends SerializedTableOperator[KudoSerializedTableColumn] {
  require(kudo != null, "kudo serializer should not be null")

  override def getDataLen(column: KudoSerializedTableColumn): Long =
    column.spillableKudoTable.header
    .getTotalDataLen

  override def getNumRows(column: KudoSerializedTableColumn): Int =
    column.spillableKudoTable.header
    .getNumRows

  private def buildMergeOptions(): MergeOptions = {
    val dumpOption = readOption.kudoDebugMode
    val dumpPrefix = readOption.kudoDebugDumpPrefix
    if (dumpOption != DumpOption.Never && dumpPrefix.isDefined) {
      val updatedPrefix = s"${dumpPrefix.get}_${taskIdentifier}"
      lazy val (out, path) = createTempFile(new Configuration(), updatedPrefix, ".bin")
      new MergeOptions(dumpOption, () => out, path.toString)
    } else {
      new MergeOptions(dumpOption, null, null)
    }
  }

  override def concatOnHost(columns: Array[KudoSerializedTableColumn]): CoalescedHostResult = {
    require(columns.nonEmpty, "no tables to be concatenated")
    val numCols = columns.head.spillableKudoTable.header.getNumColumns
    if (numCols == 0) {
      val totalRowsNum = columns.map(getNumRows).sum
      RowCountOnlyMergeResult(totalRowsNum)
    } else {
      // "lock" all input tables in memory before merge
      withResource(columns.safeMap(_.spillableKudoTable.makeKudoTable)) { kudoTables =>
        val result = kudo.get.mergeOnHost(kudoTables, buildMergeOptions())
        KudoHostMergeResultWrapper(result)
      }
    }
  }
}

/**
 * Iterator that coalesces columnar batches that are expected to only contain
 * serialized tables from shuffle. The serialized tables within are collected up
 * to the target batch size and then concatenated on the host before handing
 * them to the caller on `.next()`
 */
abstract class HostCoalesceIteratorBase[T <: AutoCloseable : ClassTag](
    iter: Iterator[ColumnarBatch],
    targetBatchByteSize: Long,
    concatTimeMetric: GpuMetric,
    inputBatchesMetric: GpuMetric,
    inputRowsMetric: GpuMetric,
    useAsync: Boolean = false
) extends Iterator[CoalescedHostResult] with AutoCloseable {
  private[this] val serializedTables = new util.ArrayDeque[T]
  @volatile private[this] var numTablesInBatch: Int = 0
  @volatile private[this] var numRowsInBatch: Int = 0
  @volatile private[this] var batchByteSize: Long = 0L

  private var executor: Option[ThrottlingExecutor] = None

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc)(close())
  }

  // Don't try to call TaskContext.get().taskAttemptId() in the backend thread
  private val taskAttemptID = Option(TaskContext.get()).
    map(_.taskAttemptId().toString).getOrElse("unknown")

  private var bufferingFuture: Option[Future[_]] = None

  protected val tableOperator: SerializedTableOperator[T]

  override def close(): Unit = {
    serializedTables.forEach(_.close())
    serializedTables.clear()
    executor.foreach(e => e.shutdownNow(10, TimeUnit.SECONDS))
  }

  private def concatenateTablesInHost(): CoalescedHostResult = {
    val result = withResource(new MetricRange(concatTimeMetric)) { _ =>
      val input = new ArrayBuffer[T]()
      for (_ <- 0 until numTablesInBatch)
        input += serializedTables.removeFirst()

      // Update the stats for the next batch in progress.
      // Note that the modification of these variables will happen before
      // the modifications in async bufferNextBatch(), we just need to ensure
      // their visibility to the next thread.
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

      if (useAsync && iter.hasNext) {
        if (executor.isEmpty) {
          executor =
            Some(new ThrottlingExecutor(
              TrampolineUtil.newDaemonCachedThreadPool(
                "async buffer thread for " + Thread.currentThread().getName, 1, 1),
              TrafficController.getReadInstance,
              _ => {
                // This is a no-op for now, but we can add stats collection here in the future.
              }
            ))
        }
        bufferingFuture = Option(executor.get.submit(
          () => {
            val nvRangeName = s"Task ${taskAttemptID}-Async Buffer Next (Backend)"
            withResource(new NvtxRange(nvRangeName, NvtxColor.ORANGE)) { _ =>
              bufferNextBatch()
            }
          },
          targetBatchByteSize // This is just a estimation, may overestimate.
        ))
      }

      withRetryNoSplit(input.toSeq) { tables =>
        tableOperator.concatOnHost(tables.toArray)
      }
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
    // Don't do any heavy things here to support the async read by
    // GpuShuffleAsyncCoalesceIterator.
    // Suppose "iter.hasNext" reads in only a header which should be small
    // enough to make this a very lightweight operation.
    bufferingFuture.isDefined || !serializedTables.isEmpty || iter.hasNext
  }

  override def next(): CoalescedHostResult = {
    if (!hasNext()) {
      throw new NoSuchElementException("No more host batches to concatenate")
    }
    bufferingFuture.map(f => {
      val nvRangeName = s"Task ${taskAttemptID} - Async Buffer Next (Frontend)"
      withResource(new NvtxRange(nvRangeName, NvtxColor.ORANGE)) { _ =>
        f.get()
      }
    }).getOrElse({
      val nvRangeName = s"Task ${taskAttemptID} - Sync Buffer Next (Frontend)"
      withResource(new NvtxRange(nvRangeName, NvtxColor.ORANGE)) { _ =>
        bufferNextBatch()
      }
    }
    )
    bufferingFuture = None

    val nvRangeName = s"Task ${taskAttemptID} - Concat in CPU"
    withResource(new NvtxRange(nvRangeName, NvtxColor.PURPLE)) { _ =>
      concatenateTablesInHost()
    }
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
    targetBatchSize: Long,
    concatTimeMetric: GpuMetric = NoopMetric,
    inputBatchesMetric: GpuMetric = NoopMetric,
    inputRowsMetric: GpuMetric = NoopMetric)
  extends HostCoalesceIteratorBase[SerializedTableColumn](iter, targetBatchSize,
    concatTimeMetric, inputBatchesMetric, inputRowsMetric) {
  override protected val tableOperator = new JCudfTableOperator
}

class KudoHostShuffleCoalesceIterator(
    iter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    dataTypes: Array[DataType],
    concatTimeMetric: GpuMetric = NoopMetric,
    inputBatchesMetric: GpuMetric = NoopMetric,
    inputRowsMetric: GpuMetric = NoopMetric,
    readOption: CoalesceReadOption
    )
  extends HostCoalesceIteratorBase[KudoSerializedTableColumn](iter, targetBatchSize,
    concatTimeMetric, inputBatchesMetric, inputRowsMetric, readOption.useAsync) {

  // Capture TaskContext info during RDD execution when it's available
  private val taskIdentifier = Option(TaskContext.get()) match {
    case Some(tc) => s"stage_${tc.stageId()}_task_${tc.taskAttemptId()}"
    case None => java.util.UUID.randomUUID().toString
  }

  override protected val tableOperator = {
    val kudoSer = if (dataTypes.nonEmpty) {
      Some(new KudoSerializer(GpuColumnVector.from(dataTypes)))
    } else {
      None
    }
    new KudoTableOperator(kudoSer, readOption, taskIdentifier)
  }
}


/**
 * Iterator that expects only "CoalescedHostResult"s as the input, and transfers
 * them to GPU.
 */
class GpuShuffleCoalesceIterator(iter: Iterator[CoalescedHostResult],
    dataTypes: Array[DataType],
    outputBatchesMetric: GpuMetric = NoopMetric,
    outputRowsMetric: GpuMetric = NoopMetric,
    readTimeMetric: GpuMetric = NoopMetric,
    opTimeMetric: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {

  override def hasNext: Boolean = GpuMetric.ns(readTimeMetric, opTimeMetric) {
    iter.hasNext
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more columnar batches")
    }
    withResource(new NvtxRange("Concat+Load Batch", NvtxColor.YELLOW)) { _ =>
      val hostCoalescedResult = GpuMetric.ns(readTimeMetric, opTimeMetric) {
        // It covers the time of i/o, deser and concat
        iter.next()
      }
      withResource(hostCoalescedResult) { _ =>
        // We acquire the GPU regardless of whether `hostConcatResult`
        // is an empty batch or not, because the downstream tasks expect
        // the `GpuShuffleCoalesceIterator` to acquire the semaphore and may
        // generate GPU data from batches that are empty.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        GpuMetric.ns(opTimeMetric) {
          val batch = hostCoalescedResult.toGpuBatch(dataTypes)
          outputBatchesMetric += 1
          outputRowsMetric += batch.numRows()
          batch
        }
      }
    }
  }
}