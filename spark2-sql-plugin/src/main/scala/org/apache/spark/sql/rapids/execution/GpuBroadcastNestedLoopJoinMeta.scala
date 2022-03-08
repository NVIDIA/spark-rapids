/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, BuildLeft, BuildRight, BuildSide}

class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val gpuBuildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.nonEquiJoinMeta(conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = conditionMeta.toSeq

  override def tagPlanForGpu(): Unit = {
    JoinTypeChecks.tagForGpu(join.joinType, this)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | RightOuter | LeftSemi | LeftAnti =>
        conditionMeta.foreach(requireAstForGpuOn)
      case _ => willNotWorkOnGpu(s"${join.joinType} currently is not supported")
    }
    join.joinType match {
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        willNotWorkOnGpu(s"build left not supported for ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        willNotWorkOnGpu(s"build right not supported for ${join.joinType}")
      case _ =>
    }

    val Seq(leftPlan, rightPlan) = childPlans
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => leftPlan
      case GpuBuildRight => rightPlan
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu(
        "the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
  }
}
