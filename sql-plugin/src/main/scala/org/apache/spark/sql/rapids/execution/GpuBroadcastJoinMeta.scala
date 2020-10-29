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
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{ConfKeysAndIncompat, GpuExec, RapidsConf, RapidsMeta, SparkPlanMeta}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

abstract class GpuBroadcastJoinMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  def canBuildSideBeReplaced(buildSide: SparkPlanMeta[_]): Boolean = {
    buildSide.wrapped match {
      case b: BroadcastQueryStageExec =>
        b.plan.isInstanceOf[GpuExec]
      case _ =>
        buildSide.canThisBeReplaced
    }
  }

  def verifyBuildSideWasReplaced(buildSide: SparkPlan): Unit = {
    val buildSideOnGpu = buildSide match {
      case BroadcastQueryStageExec(_, gpu: GpuBroadcastExchangeExecBase) => true
      case BroadcastQueryStageExec(_, reused: ReusedExchangeExec) =>
        reused.child.isInstanceOf[GpuBroadcastExchangeExecBase]
      case gpu: GpuBroadcastExchangeExecBase => true
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExecBase]
    }
    if (!buildSideOnGpu) {
      throw new IllegalStateException(s"the broadcast must be on the GPU too: ${buildSide}")
    }
  }
}
