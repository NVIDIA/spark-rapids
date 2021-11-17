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

import com.nvidia.spark.rapids.{GpuColumnVector, GpuExec}
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.execution.{BaseSubqueryExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

case class GpuSubqueryBroadcastExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan) extends BaseSubqueryExec with GpuExec with ShimUnaryExecNode {

  require(child.isInstanceOf[GpuBroadcastExchangeExec])

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    Future {
      val serBatch = child.executeBroadcast[SerializeConcatHostBuffersDeserializeBatch]().value

      val hostCols = GpuColumnVector.extractColumns(serBatch.batch).map(_.copyToHost())
      withResource(new ColumnarBatch(hostCols.toArray, serBatch.numRows)) { hostCb =>
        val toUnsafe = UnsafeProjection.create(output, output)
        hostCb.rowIterator().asScala
            .map(toUnsafe(_).copy().asInstanceOf[InternalRow])
            .toArray
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
