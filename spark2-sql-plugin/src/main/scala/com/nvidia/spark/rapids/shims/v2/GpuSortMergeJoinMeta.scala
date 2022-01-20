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

import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, JoinTypeChecks}

class GpuSortMergeJoinMeta(
    join: SortMergeJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[SortMergeJoinExec](join, conf, parent, rule) {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val condition: Option[BaseExprMeta[_]] = join.condition.map(
    GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = if (GpuHashJoin.canBuildRight(join.joinType)) {
    GpuBuildRight
  } else if (GpuHashJoin.canBuildLeft(join.joinType)) {
    GpuBuildLeft
  } else {
    throw new IllegalStateException(s"Cannot build either side for ${join.joinType} join")
  }

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, condition)

  override def tagPlanForGpu(): Unit = {
    // Use conditions from Hash Join
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      join.condition)

    if (!conf.enableReplaceSortMergeJoin) {
      willNotWorkOnGpu(s"Not replacing sort merge join with hash join, " +
        s"see ${RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key}")
    }

    // make sure this is the last check - if this is SortMergeJoin, the children can be Sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // ShuffleHashJoin
    if (canThisBeReplaced) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu(s"can't replace sortMergeJoin because one of the SortExec's before " +
              s"can't be replaced.")
          } else {
            plan.shouldBeRemoved("replacing sortMergeJoin with shuffleHashJoin")
          }
        }
      }
    }
  }
}
