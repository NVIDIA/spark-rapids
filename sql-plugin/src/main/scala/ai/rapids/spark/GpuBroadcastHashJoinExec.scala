/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastHashJoinExec(
    leftKeys: Seq[GpuExpression],
    rightKeys: Seq[GpuExpression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[GpuExpression],
    left: SparkPlan,
    right: SparkPlan) extends BroadcastHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right)
  with GpuHashJoin {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  def broadcastExchange: GpuBroadcastExchangeExec = buildPlan match {
    case gpu: GpuBroadcastExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExec]
  }

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = broadcastExchange
      .executeColumnarBroadcast[SerializableGpuColumnarBatch]()

    val rdd = streamedPlan.executeColumnar()
    rdd.mapPartitions(it => new Iterator[ColumnarBatch] {
      @transient private lazy val builtTable = {
        // TODO clean up intermediate results...
        val keys = GpuProjectExec.project(broadcastRelation.value.batch, gpuBuildKeys)
        val combined = combine(keys, broadcastRelation.value.batch)
        val ret = GpuColumnVector.from(combined)
        // Don't warn for a leak, because we cannot control when we are done with this
        (0 until ret.getNumberOfColumns).foreach(ret.getColumn(_).noWarnLeakExpected())
        ret
      }

      override def hasNext: Boolean = it.hasNext

      override def next(): ColumnarBatch = {
        val cb = it.next()
        val ret = doJoin(builtTable, cb)
        numOutputRows += ret.numRows()
        ret
      }
    })
  }
}
