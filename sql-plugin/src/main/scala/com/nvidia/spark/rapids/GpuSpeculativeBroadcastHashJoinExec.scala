/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi,
  RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution,
  UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

// Note: SpeculativeBroadcastJoinStrategy is defined in StrategyRules.scala

/**
 * A CPU placeholder join operator for speculative broadcast optimization.
 *
 * This is NOT a GpuExec - it's a CPU plan marker that gets transformed by
 * queryStageOptimizerRules into either BroadcastHashJoinExec or ShuffledHashJoinExec,
 * which are then converted to GPU versions by ColumnarOverrides.
 *
 * KEY DESIGN:
 * This join has ASYMMETRIC requiredChildDistribution:
 * - Build side: ClusteredDistribution (requires shuffle)
 * - Stream side: UnspecifiedDistribution (no shuffle required initially)
 *
 * EXECUTION FLOW:
 * 1. Physical planning creates this join when build side size is uncertain
 * 2. EnsureRequirements adds ShuffleExchange ONLY for build side
 * 3. AQE executes build side shuffle as a QueryStage
 * 4. queryStageOptimizerRules checks actual shuffle size:
 *    - If small: transforms to BroadcastHashJoinExec (no stream side shuffle)
 *    - If large: transforms to ShuffledHashJoinExec (adds stream side shuffle)
 * 5. ColumnarOverrides converts the resulting join to GPU version
 *
 * BENEFIT: Stream side shuffle is completely avoided when build side is small.
 */
case class SpeculativeBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    broadcastThreshold: Long)
  extends BinaryExecNode {

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike =>
      left.output ++ right.output
    case LeftOuter =>
      left.output ++ right.output.map(_.withNullability(true))
    case RightOuter =>
      left.output.map(_.withNullability(true)) ++ right.output
    case LeftSemi | LeftAnti =>
      left.output
    case _ =>
      throw new IllegalArgumentException(s"Unsupported join type: $joinType")
  }

  /**
   * CRITICAL: Asymmetric distribution requirement.
   * Build side requires ClusteredDistribution (will get shuffle).
   * Stream side requires UnspecifiedDistribution (no shuffle initially).
   */
  override def requiredChildDistribution: Seq[Distribution] = {
    val buildKeys = buildSide match {
      case BuildLeft => leftKeys
      case BuildRight => rightKeys
    }
    val buildDist = ClusteredDistribution(buildKeys)
    buildSide match {
      case BuildLeft => Seq(buildDist, UnspecifiedDistribution)
      case BuildRight => Seq(UnspecifiedDistribution, buildDist)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // This should never be called - the plan must be transformed by AQE rules
    throw new UnsupportedOperationException(
      "SpeculativeBroadcastHashJoinExec must be transformed by queryStageOptimizerRules. " +
      "Ensure AQE is enabled and spark.rapids.sql.speculativeBroadcast.enabled=true")
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SparkPlan = {
    copy(left = newLeft, right = newRight)
  }
}
