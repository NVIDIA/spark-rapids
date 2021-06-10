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

import java.util.Optional
import java.util.concurrent.{Callable, Future}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostColumnVector, NvtxColor}
import com.nvidia.spark.rapids.{GpuMetric, MetricRange, NvtxWithMetrics, RapidsHostColumnVector}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES
import org.apache.spark.sql.execution.metric.SQLMetrics
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
case class GpuBroadcastToCpuExec(override val mode: BroadcastMode, child: SparkPlan)
    extends GpuBroadcastExchangeExecBaseWithFuture(mode, child) {

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

    val task = new Callable[Broadcast[Any]]() {
      override def call(): Broadcast[Any] = {
        // This will run in another thread. Set the execution id so that we can connect these jobs
        // with the correct execution.
        SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
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
            val dataTypes = child.output.map(_.dataType)

            val gpuBatches = serializedBatches.safeMap { cb =>
              val hostColumns = (0 until cb.header.getNumColumns).map { i =>
                val columnHeader = cb.header.getColumnHeader(i)
                val hcv = new HostColumnVector(
                  columnHeader.getType,
                  columnHeader.rowCount,
                  Optional.of(columnHeader.nullCount),
                  cb.buffer,
                  null, null, List.empty.asJava)
                new RapidsHostColumnVector(dataTypes(i), hcv)
              }
              val rowCount = hostColumns.headOption.map(_.getRowCount.toInt).getOrElse(0)
              new ColumnarBatch(hostColumns.toArray, rowCount)
            }

            val broadcasted = try {
              val rows = new ListBuffer[InternalRow]()
              gpuBatches.foreach(cb => rows.appendAll(cb.rowIterator().asScala))

              val numRows = rows.length
              checkRowLimit(numRows)
              numOutputRows += numRows

              val relation = withResource(new NvtxWithMetrics(
                "broadcast build", NvtxColor.DARK_GREEN, buildTime)) { _ =>
                val toUnsafe = UnsafeProjection.create(output, output)
                val unsafeRows = rows.iterator.map(toUnsafe)
                val relation = mode.transform(unsafeRows.toArray)

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
                    s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
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
    GpuBroadcastExchangeExec.executionContext.submit[Broadcast[Any]](task)
  }

}