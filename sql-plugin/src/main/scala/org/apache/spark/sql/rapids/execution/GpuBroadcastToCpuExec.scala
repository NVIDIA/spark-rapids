/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids.execution

import java.util
import java.util.Optional
import java.util.concurrent.{Callable, Future}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf.{DType, HostColumnVector, HostColumnVectorCore, HostMemoryBuffer, NvtxColor}
import ai.rapids.cudf.JCudfSerialization.{SerializedColumnHeader, SerializedTableHeader}
import com.nvidia.spark.rapids.{Arm, GpuMetric, MetricRange, NvtxWithMetrics, RapidsHostColumnVector, ShimLoader}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

/**
 * This is a specialized version of GpuColumnarToRow that wraps a GpuBroadcastExchange and
 * converts the columnar results containing cuDF tables into Spark rows so that the results
 * can feed a CPU BroadcastHashJoin. This is required for exchange reuse in AQE.
 *
 * @param mode Broadcast mode
 * @param child Input to broadcast
 */
case class GpuBroadcastToCpuExec(mode: BroadcastMode, child: SparkPlan)
    extends GpuBroadcastExchangeExecBase(mode, child) {

  import GpuMetric._

  @transient
  override lazy val relationFuture: Future[Broadcast[Any]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val collectTime = gpuLongMetric(COLLECT_TIME)
    val buildTime = gpuLongMetric(BUILD_TIME)
    val broadcastTime = gpuLongMetric("broadcastTime")
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val dataTypes = child.output.map(_.dataType).toArray

    val task = new Callable[Broadcast[Any]]() {
      override def call(): Broadcast[Any] = {
        // This will run in another thread. Set the execution id so that we can connect these jobs
        // with the correct execution.
        SQLExecution.withExecutionId(sparkSession, executionId) {
          val totalRange = new MetricRange(totalTime)
          try {
            // Setup a job group here so later it may get cancelled by groupId if necessary.
            sparkContext.setJobGroup(_runId.toString, s"broadcast exchange (runId ${_runId})",
              interruptOnCancel = true)

            // run code on executors to serialize batches
            val serializedBatches: Array[SerializeBatchDeserializeHostBuffer] = withResource(
                new NvtxWithMetrics("broadcast collect", NvtxColor.GREEN, collectTime)) { _ =>
              val data = child.executeColumnar().map(cb => try {
                new SerializeBatchDeserializeHostBuffer(cb)
              } finally {
                cb.close()
              })
              data.collect()
            }

            // deserialize to host buffers in the driver and then convert to rows
            val gpuBatches = withResource(serializedBatches) { _ =>
              serializedBatches.safeMap { cb =>
                val hostColumns = GpuBroadcastToCpuExec.buildHostColumns(cb.header, cb.buffer,
                  dataTypes)
                val rowCount = cb.header.getNumRows
                new ColumnarBatch(hostColumns.toArray, rowCount)
              }
            }

            val broadcasted = try {
              val numRows = gpuBatches.map(_.numRows).sum
              checkRowLimit(numRows)
              numOutputRows += numRows

              val relation = withResource(new NvtxWithMetrics(
                "broadcast build", NvtxColor.DARK_GREEN, buildTime)) { _ =>
                val toUnsafe = UnsafeProjection.create(output, output)
                val unsafeRows = gpuBatches.flatMap {
                  _.rowIterator().asScala.map(r => toUnsafe(r).copy())
                }
                val relation = ShimLoader.getSparkShims
                    .broadcastModeTransform(mode, unsafeRows.toArray)

                val dataSize = relation match {
                  case map: KnownSizeEstimation =>
                    map.estimatedSize
                  case arr: Array[InternalRow] =>
                    arr.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
                  case _ =>
                    throw new SparkException("[BUG] BroadcastMode.transform returned unexpected " +
                        s"type: ${relation.getClass.getName}")
                }
                longMetric("dataSize") += dataSize
                if (dataSize >= MAX_BROADCAST_TABLE_BYTES) {
                  throw new SparkException(
                    s"Cannot broadcast the table that is larger than " +
                        s"${MAX_BROADCAST_TABLE_BYTES >> 30}GB: ${dataSize >> 30} GB")
                }
                relation
              }

              withResource(
                new NvtxWithMetrics("broadcast", NvtxColor.CYAN, broadcastTime)) {
                // Broadcast the relation
                _ => sparkContext.broadcast(relation)
              }
            } finally {
              gpuBatches.foreach(_.close())
            }

            val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
            SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
            promise.trySuccess(broadcasted)
            broadcasted

          } catch {
            // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
            // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
            // will catch this exception and re-throw the wrapped fatal throwable.
            case oe: OutOfMemoryError =>
              val ex = createOutOfMemoryException(oe)
              promise.failure(ex)
              throw ex
            case e if !NonFatal(e) =>
              val ex = new Exception(e)
              promise.failure(ex)
              throw ex
            case e: Throwable =>
              promise.failure(e)
              throw e
          } finally {
            totalRange.close()
          }
        }
      }
    }
    GpuBroadcastExchangeExecBase.executionContext.submit[Broadcast[Any]](task)
  }

  override def doCanonicalize(): SparkPlan = {
    GpuBroadcastToCpuExec(mode.canonicalized, child.canonicalized)
  }
}

object GpuBroadcastToCpuExec extends Arm {
  case class ColumnOffsets(validity: Long, offsets: Long, data: Long, dataLen: Long)

  private def buildHostColumns(
      header: SerializedTableHeader,
      buffer: HostMemoryBuffer,
      dataTypes: Array[DataType]): Array[RapidsHostColumnVector] = {
    assert(dataTypes.length == header.getNumColumns)
    val columnOffsets = buildColumnOffsets(header, buffer)
    closeOnExcept(new ArrayBuffer[RapidsHostColumnVector](header.getNumColumns)) { hostColumns =>
      (0 until header.getNumColumns).foreach { i =>
        val columnHeader = header.getColumnHeader(i)
        val hcv = buildHostColumn(columnHeader, columnOffsets, buffer)
        hostColumns += new RapidsHostColumnVector(dataTypes(i), hcv)
      }
      assert(columnOffsets.isEmpty)
      hostColumns.toArray
    }
  }

  // TODO: The methods below either replicate private functionality in cudf
  //       or should be moved to cudf.

  private def buildHostColumn(
      columnHeader: SerializedColumnHeader,
      columnOffsets: util.ArrayDeque[ColumnOffsets],
      buffer: HostMemoryBuffer): HostColumnVector = {
    val offsetsInfo = columnOffsets.remove()
    closeOnExcept(new ArrayBuffer[HostColumnVectorCore](columnHeader.getNumChildren)) { children =>
      val childHeaders = columnHeader.getChildren
      if (childHeaders != null) {
        childHeaders.foreach { childHeader =>
          children += buildHostColumn(childHeader, columnOffsets, buffer)
        }
      }
      val dtype = columnHeader.getType
      val numRows = columnHeader.getRowCount
      val nullCount = columnHeader.getNullCount

      // Slice up the host buffer for this column vector's buffers.
      val dataBuffer = if (dtype.isNestedType) {
        null
      } else {
        buffer.slice(offsetsInfo.data, offsetsInfo.dataLen)
      }
      val validityBuffer = if (nullCount > 0) {
        // one bit per row
        val validitySize = (numRows + 7) / 8
        buffer.slice(offsetsInfo.validity, validitySize)
      } else {
        null
      }
      val offsetsBuffer = if (dtype.hasOffsets) {
        // one 32-bit integer offset per row plus one additional offset at the end
        val offsetsSize = if (numRows > 0) (numRows + 1) * Integer.BYTES else 0
        buffer.slice(offsetsInfo.offsets, offsetsSize)
      } else {
        null
      }

      new HostColumnVector(dtype, numRows, Optional.of(nullCount), dataBuffer, validityBuffer,
        offsetsBuffer, children.asJava)
    }
  }

  /** Build a list of column offset descriptors using a pre-order traversal of the columns. */
  private def buildColumnOffsets(
      header: SerializedTableHeader,
      buffer: HostMemoryBuffer): util.ArrayDeque[ColumnOffsets] = {
    val numTopColumns = header.getNumColumns
    val offsets = new util.ArrayDeque[ColumnOffsets]
    var bufferOffset = 0L
    (0 until numTopColumns).foreach { i =>
      val columnHeader = header.getColumnHeader(i)
      bufferOffset = buildColumnOffsetsForColumn(columnHeader, buffer, offsets, bufferOffset)
    }
    offsets
  }

  /** Append a list of column offset descriptors using a pre-order traversal of the column. */
  private def buildColumnOffsetsForColumn(
      columnHeader: SerializedColumnHeader,
      buffer: HostMemoryBuffer,
      offsetsList: util.ArrayDeque[ColumnOffsets],
      startBufferOffset: Long): Long = {
    var bufferOffset = startBufferOffset
    val rowCount = columnHeader.getRowCount
    var validity = 0L
    var offsets = 0L
    var data = 0L
    var dataLen = 0L
    if (columnHeader.getNullCount > 0) {
      val validityLen = padFor64ByteAlignment((rowCount + 7) / 8)
      validity = bufferOffset
      bufferOffset += validityLen
    }

    val dtype = columnHeader.getType
    if  (dtype.hasOffsets) {
      if (rowCount > 0) {
        val offsetsLen = (rowCount + 1) * Integer.BYTES
        offsets = bufferOffset
        val startOffset = buffer.getInt(bufferOffset)
        val endOffset = buffer.getInt(bufferOffset + (rowCount * Integer.BYTES))
        bufferOffset += padFor64ByteAlignment(offsetsLen)
        if (dtype.equals(DType.STRING)) {
          dataLen = endOffset - startOffset
          data = bufferOffset
          bufferOffset += padFor64ByteAlignment(dataLen)
        }
      }
    } else if (dtype.getSizeInBytes > 0) {
      dataLen = dtype.getSizeInBytes * rowCount
      data = bufferOffset
      bufferOffset += padFor64ByteAlignment(dataLen)
    }
    offsetsList.add(ColumnOffsets(
      validity = validity,
      offsets = offsets,
      data = data,
      dataLen = dataLen))

    val children = columnHeader.getChildren
    if (children != null) {
      children.foreach { child =>
        bufferOffset = buildColumnOffsetsForColumn(child, buffer, offsetsList, bufferOffset)
      }
    }

    bufferOffset
  }

  private def padFor64ByteAlignment(addr: Long): Long = ((addr + 63) / 64) * 64
}
