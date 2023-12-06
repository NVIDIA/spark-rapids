/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.AstUtil.{JoinCondSplitAsPostFilter, JoinCondSplitAsProject, JoinCondSplitStrategy, NoopJoinCondSplit}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBroadcastHashJoinMetaBase(join, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSideMeta)
    // First to check whether we can extract some non-supported AST conditions. If not, will do a
    // post-join filter right after hash join node. Otherwise, do split as project.
    val nonAstJoinCond = if (!canJoinCondAstAble()) {
      JoinCondSplitAsPostFilter(conditionMeta.map(_.convertToGpu()), GpuHashJoin.output(
        join.joinType, left.output, right.output), left.output, right.output, buildSide)
    } else {
      val (remain, leftExpr, rightExpr) = AstUtil.extractNonAstFromJoinCond(
        conditionMeta, left.output, right.output, true)
      if(leftExpr.isEmpty && rightExpr.isEmpty) {
        NoopJoinCondSplit(remain, left.output, right.output, buildSide)
      } else {
        JoinCondSplitAsProject(
          remain, left.output, leftExpr, right.output, rightExpr,
          GpuHashJoin.output(join.joinType, left.output, right.output), buildSide)
      }
    }

    GpuBroadcastHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      buildSide,
      nonAstJoinCond.astCondition(),
      nonAstJoinCond,
      left, right)
  }
}

case class GpuBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    override val joinCondSplitStrategy: JoinCondSplitStrategy,
    left: SparkPlan,
    right: SparkPlan) extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, joinCondSplitStrategy, left, right)
