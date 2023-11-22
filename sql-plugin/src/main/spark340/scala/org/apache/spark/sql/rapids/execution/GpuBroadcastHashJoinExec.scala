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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
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
    // post-join filter right after hash join node.
    if (canJoinCondAstAble()) {
      val (remain, leftExpr, rightExpr) = AstUtil.extractNonAstFromJoinCond(conditionMeta, left
          .output, right.output, true)

      // Reconstruct the child with wrapped project node if needed.
      val leftChild = if (!leftExpr.isEmpty && buildSide != GpuBuildLeft) {
        GpuProjectExec(leftExpr ++ left.output, left)(true)
      } else {
        left
      }
      val rightChild = if (!rightExpr.isEmpty && buildSide == GpuBuildLeft) {
        GpuProjectExec(rightExpr ++ right.output, right)(true)
      } else {
        right
      }
      val (postBuildAttr, postBuildCondition) = if (buildSide == GpuBuildLeft) {
        (left.output.toList, leftExpr ++ left.output)
      } else {
        (right.output.toList, rightExpr ++ right.output)
      }

      val joinExec = GpuBroadcastHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        buildSide,
        remain,
        postBuildCondition,
        postBuildAttr,
        leftChild, rightChild)
      if (leftExpr.isEmpty && rightExpr.isEmpty) {
        joinExec
      } else {
        // Remove the intermediate attributes from left and right side project nodes. Output
        // attributes need to be updated based on types
        GpuProjectExec(
          GpuHashJoin.output(join.joinType, left.output, right.output).toList,
          joinExec)(false)
      }
    } else {
      val joinExec = GpuBroadcastHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        buildSide,
        None,
        List.empty,
        List.empty,
        left, right)
      // For inner joins we can apply a post-join condition for any conditions that cannot be
      // evaluated directly in a mixed join that leverages a cudf AST expression
      conditionMeta.map(_.convertToGpu()).map(c => GpuFilterExec(c, joinExec)()).getOrElse(joinExec)
    }
  }
}

case class GpuBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    postBuildCondition: List[NamedExpression],
    postBuildAttr: List[Attribute],
    left: SparkPlan,
    right: SparkPlan) extends GpuBroadcastHashJoinExecBase(leftKeys, rightKeys, joinType,
      buildSide, condition, postBuildCondition, postBuildAttr, left, right)
