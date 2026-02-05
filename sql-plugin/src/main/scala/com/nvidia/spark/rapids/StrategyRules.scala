/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.delta.DeltaProvider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, DynamicPruningSubquery,
  Expression, In, InSet, IsNotNull, MultiLikeBase, Not, Or, StringPredicate,
  StringRegexExpression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeExec}

/**
 * Provides a Strategy that can implement rules for translating
 * custom logical plan nodes to physical plan nodes.
 * @note This is instantiated via reflection from ShimLoader.
 */
class StrategyRules extends SparkStrategy with Logging {

  private lazy val deltaStrategies: Seq[SparkStrategy] = {
    // Custom plan nodes that originate from DeltaLake
    DeltaProvider().getStrategyRules
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val rapidsConf = new RapidsConf(plan.conf)

    // Speculative broadcast works independently of GPU execution
    // It creates a CPU plan that gets transformed by AQE rules
    if (rapidsConf.isSpeculativeBroadcastEnabled) {
      val specBroadcastResult = planSpeculativeBroadcastJoin(plan, rapidsConf)
      if (specBroadcastResult.nonEmpty) {
        return specBroadcastResult
      }
    }

    // Other strategies require GPU to be enabled
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU) {
      deltaStrategies.view.map(_(plan)).find(_.nonEmpty).getOrElse(Nil)
    } else {
      Nil
    }
  }

  /**
   * Plan speculative broadcast join if the plan is a candidate.
   *
   * Strict Candidate Selection Criteria:
   * 1. AQE must be enabled (required for runtime decision)
   * 2. Neither side's subtree can contain another shuffle-based Join
   * 3. Large side: only IS NOT NULL filters allowed (no substantive filtering)
   * 4. Small side: must have "likely selective" filter (EqualTo, DPP, etc.)
   * 5. Small side size estimate: > autoBroadcastThreshold AND < targetThreshold * 100
   *
   * It creates SpeculativeBroadcastHashJoinExec with asymmetric distribution:
   * - Build side (small): ClusteredDistribution (will get shuffle)
   * - Stream side (large): UnspecifiedDistribution (no shuffle initially)
   */
  private def planSpeculativeBroadcastJoin(
      plan: LogicalPlan,
      conf: RapidsConf): Seq[SparkPlan] = {

    // Check if AQE is enabled (required for speculative broadcast to work)
    if (!plan.conf.adaptiveExecutionEnabled) {
      logDebug("SpeculativeBroadcast: AQE is disabled, skipping")
      return Nil
    }

    val autoBroadcastThreshold = plan.conf.autoBroadcastJoinThreshold
    val targetThreshold = conf.speculativeBroadcastTargetThreshold
    val largeSideMinSize = conf.speculativeBroadcastLargeSideMinSize

    plan match {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition,
          _, left, right, _) =>
        logDebug(s"SpeculativeBroadcast: Found equi-join, joinType=$joinType")

        // Check subtree simplicity first
        if (containsShuffleJoin(left) || containsShuffleJoin(right)) {
          logDebug("SpeculativeBroadcast: Subtree contains shuffle-based join, skipping")
          return Nil
        }

        // Try to identify large side and small side
        val candidateOpt = identifyLargeAndSmallSide(
          left, right, joinType, autoBroadcastThreshold, targetThreshold, largeSideMinSize)

        candidateOpt.flatMap { case (largeSide, smallSide, buildSide) =>
          val smallSideSize = smallSide.stats.sizeInBytes.toLong
          val largeSideSize = largeSide.stats.sizeInBytes.toLong

          // Only apply to the join with the largest large side in this query.
          // Find all candidate joins in the plan and check if this one has the largest.
          val allCandidates = findAllCandidateJoins(
            plan, autoBroadcastThreshold, targetThreshold, largeSideMinSize)
          val largeSideSizes = allCandidates.map(_._2)
          val maxLargeSideSize = if (largeSideSizes.nonEmpty) largeSideSizes.max else 0L

          if (largeSideSize < maxLargeSideSize) {
            logDebug(s"SpeculativeBroadcast: Skipping join because large side " +
              s"($largeSideSize) is not the largest ($maxLargeSideSize)")
            return Nil
          }

          logInfo(s"SpeculativeBroadcast: Candidate found! " +
            s"largeSide=${largeSide.nodeName} (size=$largeSideSize bytes), " +
            s"smallSide=${smallSide.nodeName} (size=$smallSideSize bytes), " +
            s"buildSide=$buildSide, targetThreshold=$targetThreshold")

          // Plan children first
          val leftPlan = planLater(left)
          val rightPlan = planLater(right)

          // IMPORTANT: We must add ShuffleExchangeExec explicitly here, rather than
          // relying on EnsureRequirements to add it based on requiredChildDistribution.
          //
          // Reason: EnsureRequirements only adds shuffle when child's outputPartitioning
          // does NOT satisfy the required distribution. If build side already has compatible
          // partitioning (e.g., from a previous aggregation), EnsureRequirements won't add
          // any shuffle. But we NEED a ShuffleExchangeExec so that:
          // 1. AQE wraps it as ShuffleQueryStageExec
          // 2. The shuffle stage gets materialized independently
          // 3. We can obtain runtime statistics (mapStats) with actual data size
          //
          // Without explicit shuffle, there's no ShuffleQueryStageExec, no mapStats,
          // and SpeculativeBroadcastRule cannot make the runtime decision.
          val numPartitions = plan.conf.numShufflePartitions
          val (plannedLeft, plannedRight) = buildSide match {
            case BuildLeft =>
              val partitioning = HashPartitioning(leftKeys, numPartitions)
              val buildShuffle = ShuffleExchangeExec(partitioning, leftPlan, ENSURE_REQUIREMENTS)
              (buildShuffle, rightPlan)
            case BuildRight =>
              val partitioning = HashPartitioning(rightKeys, numPartitions)
              val buildShuffle = ShuffleExchangeExec(partitioning, rightPlan, ENSURE_REQUIREMENTS)
              (leftPlan, buildShuffle)
          }

          logInfo(s"SpeculativeBroadcast: Creating join with explicit shuffle on " +
            s"$buildSide side (small side), numPartitions=$numPartitions")

          Some(SpeculativeBroadcastHashJoinExec(
            leftKeys, rightKeys, joinType, buildSide, otherCondition,
            plannedLeft, plannedRight, targetThreshold) :: Nil)
        }.getOrElse(Nil)

      case _ => Nil
    }
  }

  /**
   * Find all candidate joins in the plan tree and return their large side sizes.
   * Used to ensure only the join with the largest large side is selected per query.
   */
  private def findAllCandidateJoins(
      plan: LogicalPlan,
      autoBroadcastThreshold: Long,
      targetThreshold: Long,
      largeSideMinSize: Long): Seq[(LogicalPlan, Long)] = {

    val candidates = scala.collection.mutable.ArrayBuffer[(LogicalPlan, Long)]()

    plan.foreach {
      case j @ Join(left, right, joinType, _, _) =>
        // Check subtree simplicity
        if (!containsShuffleJoin(left) && !containsShuffleJoin(right)) {
          // Try left as large side
          if (isValidLargeSide(left, largeSideMinSize) &&
              isValidSmallSide(right, autoBroadcastThreshold, targetThreshold) &&
              canBuildRight(joinType)) {
            candidates += ((j, left.stats.sizeInBytes.toLong))
          }
          // Try right as large side
          else if (isValidLargeSide(right, largeSideMinSize) &&
              isValidSmallSide(left, autoBroadcastThreshold, targetThreshold) &&
              canBuildLeft(joinType)) {
            candidates += ((j, right.stats.sizeInBytes.toLong))
          }
        }
      case _ =>
    }

    candidates.toSeq
  }

  /**
   * Identify which side is "large" (stream side) and which is "small" (build side).
   * Returns Some((largeSide, smallSide, buildSide)) if valid candidate, None otherwise.
   */
  private def identifyLargeAndSmallSide(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      autoBroadcastThreshold: Long,
      targetThreshold: Long,
      largeSideMinSize: Long): Option[(LogicalPlan, LogicalPlan, BuildSide)] = {

    // Try left as large side, right as small side
    if (isValidLargeSide(left, largeSideMinSize) &&
        isValidSmallSide(right, autoBroadcastThreshold, targetThreshold) &&
        canBuildRight(joinType)) {
      return Some((left, right, BuildRight))
    }

    // Try right as large side, left as small side
    if (isValidLargeSide(right, largeSideMinSize) &&
        isValidSmallSide(left, autoBroadcastThreshold, targetThreshold) &&
        canBuildLeft(joinType)) {
      return Some((right, left, BuildLeft))
    }

    // No valid candidate
    None
  }

  /**
   * Check if a plan subtree is valid as "large side" (stream side).
   * Requirements:
   * - Has a FileScan as leaf
   * - Only IS NOT NULL filters allowed (no substantive filtering)
   * - Size estimate >= largeSideMinSize (must be big enough to benefit from optimization)
   */
  private def isValidLargeSide(plan: LogicalPlan, largeSideMinSize: Long): Boolean = {
    if (!hasFileScan(plan)) {
      logDebug(s"SpeculativeBroadcast: Large side ${plan.nodeName} has no FileScan")
      return false
    }

    if (!hasOnlyIsNotNullFilter(plan)) {
      logDebug(s"SpeculativeBroadcast: Large side ${plan.nodeName} has non-IS-NOT-NULL filter")
      return false
    }

    val estimate = plan.stats.sizeInBytes.toLong
    if (estimate < largeSideMinSize) {
      logDebug(s"SpeculativeBroadcast: Large side too small ($estimate < $largeSideMinSize)")
      return false
    }

    true
  }

  /**
   * Check if a plan subtree is valid as "small side" (build side).
   * Requirements:
   * - Has a FileScan as leaf
   * - Has "likely selective" filter (EqualTo, DPP, etc.)
   * - Size estimate > autoBroadcastThreshold (otherwise Spark would broadcast anyway)
   * - Size estimate < targetThreshold * 100 (filter must be selective enough)
   */
  private def isValidSmallSide(
      plan: LogicalPlan,
      autoBroadcastThreshold: Long,
      targetThreshold: Long): Boolean = {
    if (!hasFileScan(plan)) {
      logDebug(s"SpeculativeBroadcast: Small side ${plan.nodeName} has no FileScan")
      return false
    }

    if (!hasLikelySelectiveFilter(plan)) {
      logDebug(s"SpeculativeBroadcast: Small side ${plan.nodeName} has no selective filter")
      return false
    }

    val estimate = plan.stats.sizeInBytes.toLong
    val maxAllowed = targetThreshold * 100

    // Skip if size unknown or too small (would broadcast anyway) or too large
    if (estimate < 0) {
      logDebug(s"SpeculativeBroadcast: Small side size unknown")
      return false
    }
    // If autoBroadcastThreshold > 0 and estimate <= threshold, Spark would broadcast anyway
    if (autoBroadcastThreshold > 0 && estimate <= autoBroadcastThreshold) {
      logDebug(s"SpeculativeBroadcast: Small side too small ($estimate <= $autoBroadcastThreshold)")
      return false
    }
    if (estimate > maxAllowed) {
      logDebug(s"SpeculativeBroadcast: Small side too large ($estimate > $maxAllowed)")
      return false
    }

    true
  }

  /**
   * Check if the plan subtree contains a shuffle-based join.
   * BroadcastHashJoin is allowed (no shuffle), but SortMergeJoin/ShuffledHashJoin are not.
   */
  private def containsShuffleJoin(plan: LogicalPlan): Boolean = {
    plan.exists {
      case j: Join =>
        // Check hint to see if it's broadcast join
        // If no broadcast hint, it will likely become shuffle join
        val hasBroadcastHint = j.hint.leftHint.exists(_.strategy.contains(
          org.apache.spark.sql.catalyst.plans.logical.BROADCAST)) ||
          j.hint.rightHint.exists(_.strategy.contains(
            org.apache.spark.sql.catalyst.plans.logical.BROADCAST))
        !hasBroadcastHint
      case _ => false
    }
  }

  /**
   * Check if the plan has a FileScan (or LogicalRelation) as leaf.
   * LeafNode is a trait that includes LogicalRelation, FileSourceScanExec, etc.
   */
  private def hasFileScan(plan: LogicalPlan): Boolean = {
    plan.exists(_.isInstanceOf[LeafNode])
  }

  /**
   * Check if all Filter nodes in the plan only have IS NOT NULL conditions.
   * Returns false if there's any Filter with non-IS-NOT-NULL conditions.
   */
  private def hasOnlyIsNotNullFilter(plan: LogicalPlan): Boolean = {
    !plan.exists {
      case f: Filter => !isOnlyIsNotNullCondition(f.condition)
      case _ => false
    }
  }

  /**
   * Check if the expression is only composed of IS NOT NULL and AND.
   */
  private def isOnlyIsNotNullCondition(expr: Expression): Boolean = expr match {
    case IsNotNull(_) => true
    case And(l, r) => isOnlyIsNotNullCondition(l) && isOnlyIsNotNullCondition(r)
    case _ => false
  }

  /**
   * Check if the plan has at least one Filter with "likely selective" condition.
   * This is based on Spark's isLikelySelective logic from predicates.scala,
   * plus DynamicPruningSubquery (DPP) check.
   */
  private def hasLikelySelectiveFilter(plan: LogicalPlan): Boolean = {
    plan.exists {
      case f: Filter =>
        isLikelySelective(f.condition) || hasDPP(f.condition)
      case _ => false
    }
  }

  /**
   * Check if expression is likely selective.
   * Borrowed from Spark's PredicateHelper.isLikelySelective.
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _: MultiLikeBase => true
    case _ => false
  }

  /**
   * Check if expression contains DynamicPruningSubquery (DPP).
   */
  private def hasDPP(expr: Expression): Boolean = {
    expr.exists(_.isInstanceOf[DynamicPruningSubquery])
  }

  /**
   * Check if the join type allows building on the left side.
   */
  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: org.apache.spark.sql.catalyst.plans.InnerLike => true
    case RightOuter => true
    case _ => false
  }

  /**
   * Check if the join type allows building on the right side.
   */
  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: org.apache.spark.sql.catalyst.plans.InnerLike => true
    case LeftOuter | LeftSemi | LeftAnti => true
    case _ => false
  }
}
