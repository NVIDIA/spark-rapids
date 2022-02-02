/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, Future}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostMemoryBuffer, JCudfSerialization, NvtxColor}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.{Arm, GpuMetric, NvtxWithMetrics, RapidsHostColumnVector}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.shims.v2.SparkShimImpl

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
    val dataTypes = child.output.map(_.dataType).toArray

    val task = new Callable[Broadcast[Any]]() {
      override def call(): Broadcast[Any] = {
        // This will run in another thread. Set the execution id so that we can connect these jobs
        // with the correct execution.
        SQLExecution.withExecutionId(sparkSession, executionId) {
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
                val relation = SparkShimImpl
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
  private def buildHostColumns(
      header: SerializedTableHeader,
      buffer: HostMemoryBuffer,
      dataTypes: Array[DataType]): Array[RapidsHostColumnVector] = {
    assert(dataTypes.length == header.getNumColumns)
    closeOnExcept(JCudfSerialization.unpackHostColumnVectors(header, buffer)) { hostColumns =>
      assert(hostColumns.length == dataTypes.length)
      dataTypes.zip(hostColumns).safeMap { case (dataType, hostColumn) =>
        new RapidsHostColumnVector(dataType, hostColumn)
      }
    }
  }
}
