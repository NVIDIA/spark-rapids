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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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
   * This identifies joins where:
   * 1. Build side size is uncertain (near the broadcast threshold)
   * 2. AQE is enabled (required for runtime decision)
   * 3. Join type supports broadcast
   *
   * It creates SpeculativeBroadcastHashJoinExec with asymmetric distribution:
   * - Build side: ClusteredDistribution (will get shuffle)
   * - Stream side: UnspecifiedDistribution (no shuffle initially)
   */
  private def planSpeculativeBroadcastJoin(
      plan: LogicalPlan,
      conf: RapidsConf): Seq[SparkPlan] = {

    // Check if AQE is enabled (required for speculative broadcast to work)
    if (!plan.conf.adaptiveExecutionEnabled) {
      logInfo("SpeculativeBroadcast: AQE is disabled, skipping")
      return Nil
    }

    // Get threshold, with a sensible default if autoBroadcastJoinThreshold is disabled (-1)
    val configuredThreshold = conf.get(RapidsConf.SPECULATIVE_BROADCAST_THRESHOLD)
    val autoThreshold = plan.conf.autoBroadcastJoinThreshold
    val threshold = configuredThreshold.getOrElse {
      if (autoThreshold > 0) autoThreshold else 10L * 1024 * 1024 // 10MB default
    }

    logInfo(s"SpeculativeBroadcast: Checking plan ${plan.getClass.getSimpleName}, " +
      s"threshold=$threshold (configured=$configuredThreshold, auto=$autoThreshold)")

    plan match {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition,
          _, left, right, _) =>
        logInfo(s"SpeculativeBroadcast: Found equi-join, joinType=$joinType")

        // Get size estimates
        val leftSize = left.stats.sizeInBytes.toLong
        val rightSize = right.stats.sizeInBytes.toLong

        // Check if this is a candidate for speculative broadcast
        val buildSide = chooseBuildSide(leftSize, rightSize, joinType)
        buildSide.flatMap { side =>
          val buildSize = if (side == BuildLeft) leftSize else rightSize
          if (isSpeculativeCandidate(buildSize, threshold)) {
            // Plan children first
            val leftPlan = planLater(left)
            val rightPlan = planLater(right)

            // Add shuffle on build side explicitly (AQE won't add it otherwise)
            // Use default shuffle partitions from config
            val numPartitions = plan.conf.numShufflePartitions
            val (_, plannedLeft, plannedRight) = side match {
              case BuildLeft =>
                val partitioning = HashPartitioning(leftKeys, numPartitions)
                val buildShuffle = ShuffleExchangeExec(partitioning, leftPlan, ENSURE_REQUIREMENTS)
                (leftKeys, buildShuffle, rightPlan)
              case BuildRight =>
                val partitioning = HashPartitioning(rightKeys, numPartitions)
                val buildShuffle = ShuffleExchangeExec(partitioning, rightPlan, ENSURE_REQUIREMENTS)
                (rightKeys, leftPlan, buildShuffle)
            }

            logInfo(s"SpeculativeBroadcast: Creating join with explicit shuffle on " +
              s"${side} side, numPartitions=$numPartitions")

            Some(SpeculativeBroadcastHashJoinExec(
              leftKeys, rightKeys, joinType, side, otherCondition,
              plannedLeft, plannedRight, threshold) :: Nil)
          } else {
            None
          }
        }.getOrElse(Nil)

      case _ => Nil
    }
  }

  private def isSpeculativeCandidate(
      buildSizeEstimate: Long,
      threshold: Long): Boolean = {
    // Candidate if:
    // - Size unknown (negative)
    // - Size estimate is below threshold * 2 (might actually fit after shuffle)
    // We use 2x threshold as upper bound because estimates can be inaccurate
    buildSizeEstimate < 0 || buildSizeEstimate < threshold * 2
  }

  private def chooseBuildSide(
      leftSize: Long,
      rightSize: Long,
      joinType: org.apache.spark.sql.catalyst.plans.JoinType
  ): Option[org.apache.spark.sql.catalyst.optimizer.BuildSide] = {
    import org.apache.spark.sql.rapids.execution.GpuHashJoin

    val canBuildLeft = GpuHashJoin.canBuildLeft(joinType)
    val canBuildRight = GpuHashJoin.canBuildRight(joinType)

    if (canBuildLeft && canBuildRight) {
      if (leftSize >= 0 && rightSize >= 0) {
        Some(if (leftSize <= rightSize) BuildLeft else BuildRight)
      } else if (leftSize >= 0) {
        Some(BuildLeft)
      } else if (rightSize >= 0) {
        Some(BuildRight)
      } else {
        Some(BuildRight) // Both unknown, default to right
      }
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }
}
