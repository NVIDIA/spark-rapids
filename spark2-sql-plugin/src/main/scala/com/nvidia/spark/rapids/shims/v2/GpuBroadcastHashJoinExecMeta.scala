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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2._

import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, JoinTypeChecks}

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends GpuBroadcastJoinMeta[BroadcastHashJoinExec](join, conf, parent, rule) {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val condition: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, condition)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      join.condition)
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }

    if (!canBuildSideBeReplaced(buildSideMeta)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
  }
}
