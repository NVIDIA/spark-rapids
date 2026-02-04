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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{RapidsConf, SpeculativeBroadcastHashJoinExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode,
  ShuffledHashJoinExec}

/**
 * A queryStageOptimizerRule that transforms SpeculativeBroadcastHashJoinExec
 * based on actual runtime shuffle statistics.
 *
 * This rule runs after build side shuffle completes. It checks the actual size:
 * - If small enough: converts to BroadcastHashJoin (stream side avoids shuffle)
 * - If too large: converts to ShuffledHashJoin (adds shuffle for stream side)
 *
 * This achieves TRUE speculative broadcast: stream side shuffle is completely
 * avoided when build side turns out to be small at runtime.
 */
case class GpuSpeculativeBroadcastRule(spark: SparkSession) extends Rule[SparkPlan] with Logging {

  private def rapidsConf: RapidsConf = new RapidsConf(spark.sessionState.conf)

  private def isEnabled: Boolean = rapidsConf.isSpeculativeBroadcastEnabled

  private def broadcastThreshold: Long = {
    rapidsConf.get(RapidsConf.SPECULATIVE_BROADCAST_THRESHOLD)
      .getOrElse(spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isEnabled) {
      return plan
    }

    plan.transformUp {
      // Handle SpeculativeBroadcastHashJoinExec - the main target
      case specJoin: SpeculativeBroadcastHashJoinExec =>
        transformSpeculativeJoin(specJoin)
    }
  }

  /**
   * Transform SpeculativeBroadcastHashJoinExec based on runtime statistics.
   * Build side should have a materialized ShuffleQueryStageExec.
   */
  private def transformSpeculativeJoin(
      specJoin: SpeculativeBroadcastHashJoinExec): SparkPlan = {

    val (buildChild, streamChild) = specJoin.buildSide match {
      case BuildLeft => (specJoin.left, specJoin.right)
      case BuildRight => (specJoin.right, specJoin.left)
    }

    // Find the build side shuffle stage
    findShuffleStageInfo(buildChild) match {
      case Some((buildStage, buildSize)) =>
        if (buildSize < broadcastThreshold) {
          // Small enough - use broadcast join (no stream side shuffle!)
          logInfo(s"GpuSpeculativeBroadcastRule: Build side small ($buildSize bytes < " +
            s"$broadcastThreshold), using BroadcastHashJoin - stream side avoids shuffle!")

          val buildKeys = specJoin.buildSide match {
            case BuildLeft => specJoin.leftKeys
            case BuildRight => specJoin.rightKeys
          }
          val mode = HashedRelationBroadcastMode(buildKeys)
          val broadcastExchange = BroadcastExchangeExec(mode, buildStage)

          val (newLeft, newRight) = specJoin.buildSide match {
            case BuildLeft => (broadcastExchange, streamChild)
            case BuildRight => (streamChild, broadcastExchange)
          }

          BroadcastHashJoinExec(
            specJoin.leftKeys, specJoin.rightKeys, specJoin.joinType,
            specJoin.buildSide, specJoin.condition, newLeft, newRight)

        } else {
          // Too large - fall back to shuffled hash join (need stream side shuffle)
          logInfo(s"GpuSpeculativeBroadcastRule: Build side too large ($buildSize bytes >= " +
            s"$broadcastThreshold), falling back to ShuffledHashJoin")

          val streamKeys = specJoin.buildSide match {
            case BuildLeft => specJoin.rightKeys
            case BuildRight => specJoin.leftKeys
          }

          // Add shuffle for stream side
          val partitioning = HashPartitioning(
            streamKeys, spark.sessionState.conf.numShufflePartitions)
          val streamShuffle = ShuffleExchangeExec(partitioning, streamChild)

          val (newLeft, newRight) = specJoin.buildSide match {
            case BuildLeft => (buildStage, streamShuffle)
            case BuildRight => (streamShuffle, buildStage)
          }

          ShuffledHashJoinExec(
            specJoin.leftKeys, specJoin.rightKeys, specJoin.joinType,
            specJoin.buildSide, specJoin.condition, newLeft, newRight)
        }

      case None =>
        // Build side not ready yet, keep as-is (should not happen normally)
        logWarning("GpuSpeculativeBroadcastRule: Build side shuffle not ready")
        specJoin
    }
  }

  /**
   * Find shuffle stage info by looking through wrapper nodes.
   */
  private def findShuffleStageInfo(plan: SparkPlan): Option[(SparkPlan, Long)] = {
    plan match {
      case stage: ShuffleQueryStageExec if stage.isMaterialized =>
        stage.mapStats.map { stats =>
          (stage, stats.bytesByPartitionId.sum)
        }
      case read @ AQEShuffleReadExec(stage: ShuffleQueryStageExec, _)
          if stage.isMaterialized =>
        stage.mapStats.map { stats =>
          (read, stats.bytesByPartitionId.sum)
        }
      case unary: org.apache.spark.sql.execution.UnaryExecNode =>
        findShuffleStageInfo(unary.child)
      case _ => None
    }
  }
}

/**
 * Companion object with factory method for SparkSessionExtensions.
 */
object GpuSpeculativeBroadcastRule {
  def apply(spark: SparkSession): Rule[SparkPlan] = new GpuSpeculativeBroadcastRule(spark)
}
