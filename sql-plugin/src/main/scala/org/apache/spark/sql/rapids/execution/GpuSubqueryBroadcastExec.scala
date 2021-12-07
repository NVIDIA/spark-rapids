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

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{GpuExec, GpuMetric, NvtxWithMetrics}
import com.nvidia.spark.rapids.GpuMetric.{COLLECT_TIME, DESCRIPTION_COLLECT_TIME, ESSENTIAL_LEVEL}
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan, SQLExecution}
import org.apache.spark.util.ThreadUtils


case class GpuSubqueryBroadcastExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan) extends BaseSubqueryExec with GpuExec with ShimUnaryExecNode {

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    COLLECT_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_COLLECT_TIME))

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val collectTime = gpuLongMetric(COLLECT_TIME)

    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkSession, executionId) {
        withResource(new NvtxWithMetrics("broadcast collect", NvtxColor.GREEN, collectTime)) { _ =>

          val batchBc = child.executeBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

          gpuLongMetric("dataSize") += batchBc.value.dataSize

          val toUnsafe = UnsafeProjection.create(output, output)
          withResource(batchBc.value.hostBatches) { hostBatches =>
            hostBatches.flatMap { cb =>
              cb.rowIterator().asScala
                  .map(toUnsafe(_).copy().asInstanceOf[InternalRow])
            }
          }
        }
      }
    }(GpuSubqueryBroadcastExec.executionContext)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuSubqueryBroadcastExec does not support the execute() code path.")
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }
}

object GpuSubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("dynamicpruning", 16))
}
