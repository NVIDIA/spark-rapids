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

import com.nvidia.spark.rapids.{GpuOverrides, GpuTransitionOverrides, RapidsConf,
  SpeculativeBroadcastHashJoinExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin,
  HashedRelationBroadcastMode, ShuffledHashJoinExec}

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
    val configuredThreshold = rapidsConf.get(RapidsConf.SPECULATIVE_BROADCAST_THRESHOLD)
    val autoThreshold = spark.sessionState.conf.autoBroadcastJoinThreshold
    configuredThreshold.getOrElse {
      if (autoThreshold > 0) autoThreshold else 10L * 1024 * 1024 // 10MB default
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    logInfo(s"GpuSpeculativeBroadcastRule.apply called, isEnabled=$isEnabled, " +
      s"plan=${plan.getClass.getSimpleName}")
    if (!isEnabled) {
      return plan
    }

    plan.transformUp {
      case specJoin: SpeculativeBroadcastHashJoinExec =>
        logInfo(s"GpuSpeculativeBroadcastRule: Found SpeculativeBroadcastHashJoinExec, " +
          s"buildSide=${specJoin.buildSide}, left=${specJoin.left.getClass.getSimpleName}, " +
          s"right=${specJoin.right.getClass.getSimpleName}")
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

    // Log detailed attribute info for debugging
    logInfo(s"GpuSpeculativeBroadcastRule: specJoin.left.output = ${specJoin.left.output}")
    logInfo(s"GpuSpeculativeBroadcastRule: specJoin.right.output = ${specJoin.right.output}")
    logInfo(s"GpuSpeculativeBroadcastRule: specJoin.leftKeys = ${specJoin.leftKeys}")
    logInfo(s"GpuSpeculativeBroadcastRule: specJoin.rightKeys = ${specJoin.rightKeys}")

    // Find the build side shuffle stage
    findShuffleStageInfo(buildChild) match {
      case Some((buildStage, buildSize)) =>
        logInfo(s"GpuSpeculativeBroadcastRule: buildChild.output = ${buildChild.output}")
        logInfo(s"GpuSpeculativeBroadcastRule: buildStage.output = ${buildStage.output}")
        logInfo(s"GpuSpeculativeBroadcastRule: streamChild.output = ${streamChild.output}")

        // Create position-based attribute mapping
        // Maps attribute by finding its position in child output and using corresponding
        // position in stage output
        val (originalBuildOutput, originalStreamOutput) = specJoin.buildSide match {
          case BuildLeft => (specJoin.left.output, specJoin.right.output)
          case BuildRight => (specJoin.right.output, specJoin.left.output)
        }

        // Build index map: exprId -> position in original output
        val buildPosMap = originalBuildOutput.zipWithIndex.map {
          case (a, i) => a.exprId -> i
        }.toMap
        val streamPosMap = originalStreamOutput.zipWithIndex.map {
          case (a, i) => a.exprId -> i
        }.toMap

        logInfo(s"GpuSpeculativeBroadcastRule: buildPosMap = $buildPosMap")
        logInfo(s"GpuSpeculativeBroadcastRule: streamPosMap = $streamPosMap")

        // Remap expression by finding matching attribute in output by exprId.id
        // Note: We compare only exprId.id, not the full ExprId (which includes jvmId),
        // because different AttributeReference objects can have different jvmIds
        def remapBuildExpr(expr: Expression): Expression = {
          expr.transform {
            case a: AttributeReference =>
              // Find attribute in buildStage.output with same exprId.id
              val newAttr = buildStage.output.find(_.exprId.id == a.exprId.id).getOrElse(a)
              if (newAttr ne a) {
                logInfo(s"GpuSpeculativeBroadcastRule: Remapping build attr " +
                  s"${a.name}#${a.exprId.id} (jvm=${a.exprId}) -> " +
                  s"${newAttr.name}#${newAttr.exprId.id} (jvm=${newAttr.exprId})")
              } else {
                logInfo(s"GpuSpeculativeBroadcastRule: No match in buildStage for " +
                  s"${a.name}#${a.exprId.id}, keeping original")
              }
              newAttr
          }
        }

        def remapStreamExpr(expr: Expression): Expression = {
          expr.transform {
            case a: AttributeReference =>
              // Find attribute in streamChild.output with same exprId.id
              val newAttr = streamChild.output.find(_.exprId.id == a.exprId.id).getOrElse(a)
              if (newAttr ne a) {
                logInfo(s"GpuSpeculativeBroadcastRule: Remapping stream attr " +
                  s"${a.name}#${a.exprId.id} (jvm=${a.exprId}) -> " +
                  s"${newAttr.name}#${newAttr.exprId.id} (jvm=${newAttr.exprId})")
              } else {
                logInfo(s"GpuSpeculativeBroadcastRule: No match in streamChild for " +
                  s"${a.name}#${a.exprId.id}, keeping original")
              }
              newAttr
          }
        }

        if (buildSize < broadcastThreshold) {
          // Small enough - use broadcast join (no stream side shuffle!)
          logInfo(s"GpuSpeculativeBroadcastRule: Build side small ($buildSize bytes < " +
            s"$broadcastThreshold), using BroadcastHashJoin - stream side avoids shuffle!")

          // Remap build keys to buildStage.output attributes for HashedRelationBroadcastMode
          val originalBuildKeys = specJoin.buildSide match {
            case BuildLeft => specJoin.leftKeys
            case BuildRight => specJoin.rightKeys
          }
          val buildKeys = originalBuildKeys.map(remapBuildExpr)

          logInfo(s"GpuSpeculativeBroadcastRule: originalBuildKeys = $originalBuildKeys")
          logInfo(s"GpuSpeculativeBroadcastRule: remapped buildKeys = $buildKeys")

          // CRITICAL: HashJoin.rewriteKeyExpr rewrites IntegralType keys to Long for optimization.
          // BroadcastHashJoinExec codegen uses rewriteKeyExpr on streamedKeys, so it expects
          // the HashedRelation to also use rewritten keys. If we don't rewrite, the codegen
          // will call get(Long) but the relation only supports get(InternalRow).
          val rewrittenBuildKeys = HashJoin.rewriteKeyExpr(buildKeys)
          logInfo(s"GpuSpeculativeBroadcastRule: rewrittenBuildKeys = $rewrittenBuildKeys")

          // Bind the rewritten keys to the buildStage output schema
          val boundBuildKeys = BindReferences.bindReferences(rewrittenBuildKeys, buildStage.output)
          logInfo(s"GpuSpeculativeBroadcastRule: boundBuildKeys = $boundBuildKeys")

          val broadcastExchange = BroadcastExchangeExec(
            HashedRelationBroadcastMode(boundBuildKeys), buildStage)

          val (newLeft, newRight) = specJoin.buildSide match {
            case BuildLeft => (broadcastExchange, streamChild)
            case BuildRight => (streamChild, broadcastExchange)
          }

          // For BroadcastHashJoinExec: build side uses remapped keys, stream side unchanged
          val (newLeftKeys, newRightKeys) = specJoin.buildSide match {
            case BuildLeft => (buildKeys, specJoin.rightKeys.map(remapStreamExpr))
            case BuildRight => (specJoin.leftKeys.map(remapStreamExpr), buildKeys)
          }

          // Remap condition - only remap attributes that belong to build side
          val newCondition = specJoin.condition.map { c =>
            c.transform {
              case a: AttributeReference =>
                if (buildPosMap.contains(a.exprId)) {
                  remapBuildExpr(a)
                } else if (streamPosMap.contains(a.exprId)) {
                  remapStreamExpr(a)
                } else {
                  a
                }
            }
          }

          logInfo(s"GpuSpeculativeBroadcastRule: newLeftKeys=$newLeftKeys, " +
            s"newRightKeys=$newRightKeys, newCondition=$newCondition")

          val cpuJoin = BroadcastHashJoinExec(
            newLeftKeys, newRightKeys, specJoin.joinType,
            specJoin.buildSide, newCondition, newLeft, newRight)

          // Apply GpuOverrides to convert CPU join to GPU join.
          // This is necessary because GpuOverrides runs before this rule,
          // so it cannot convert SpeculativeBroadcastHashJoinExec directly.
          convertToGpu(cpuJoin)

        } else {
          // Too large - fall back to shuffled hash join (need stream side shuffle)
          logInfo(s"GpuSpeculativeBroadcastRule: Build side too large ($buildSize bytes >= " +
            s"$broadcastThreshold), falling back to ShuffledHashJoin")

          // Stream keys don't need remapping for shuffle partitioning (uses original refs)
          val streamKeys = specJoin.buildSide match {
            case BuildLeft => specJoin.rightKeys.map(remapStreamExpr)
            case BuildRight => specJoin.leftKeys.map(remapStreamExpr)
          }

          // Add shuffle for stream side
          val partitioning = HashPartitioning(
            streamKeys, spark.sessionState.conf.numShufflePartitions)
          val streamShuffle = ShuffleExchangeExec(partitioning, streamChild)

          val (newLeft, newRight) = specJoin.buildSide match {
            case BuildLeft => (buildStage, streamShuffle)
            case BuildRight => (streamShuffle, buildStage)
          }

          // Remap build side keys
          val (newLeftKeys, newRightKeys) = specJoin.buildSide match {
            case BuildLeft =>
              (specJoin.leftKeys.map(remapBuildExpr), specJoin.rightKeys.map(remapStreamExpr))
            case BuildRight =>
              (specJoin.leftKeys.map(remapStreamExpr), specJoin.rightKeys.map(remapBuildExpr))
          }

          // Remap condition
          val newCondition = specJoin.condition.map { c =>
            c.transform {
              case a: AttributeReference =>
                if (buildPosMap.contains(a.exprId)) {
                  remapBuildExpr(a)
                } else if (streamPosMap.contains(a.exprId)) {
                  remapStreamExpr(a)
                } else {
                  a
                }
            }
          }

          logInfo(s"GpuSpeculativeBroadcastRule: Fallback newLeftKeys=$newLeftKeys, " +
            s"newRightKeys=$newRightKeys")

          val cpuJoin = ShuffledHashJoinExec(
            newLeftKeys, newRightKeys, specJoin.joinType,
            specJoin.buildSide, newCondition, newLeft, newRight)

          // Apply GpuOverrides to convert CPU join to GPU join
          convertToGpu(cpuJoin)
        }

      case None =>
        // Build side not ready yet, keep as-is (should not happen normally)
        logWarning("GpuSpeculativeBroadcastRule: Build side shuffle not ready")
        specJoin
    }
  }

  /**
   * Convert a CPU plan to GPU using GpuOverrides and GpuTransitionOverrides.
   * This is called after creating BroadcastHashJoinExec or ShuffledHashJoinExec
   * to ensure they run on GPU.
   *
   * We need to apply both rules because:
   * 1. GpuOverrides converts CPU operators to GPU operators
   * 2. GpuTransitionOverrides handles transitions between CPU and GPU data formats
   *
   * This is necessary because the normal ColumnarRule execution runs before AQE,
   * but this rule runs during AQE after query stages are materialized.
   */
  private def convertToGpu(plan: SparkPlan): SparkPlan = {
    val conf = rapidsConf
    if (conf.isSqlEnabled && conf.isSqlExecuteOnGPU) {
      logInfo(s"GpuSpeculativeBroadcastRule: Converting to GPU: " +
        s"${plan.getClass.getSimpleName}")
      // First apply GpuOverrides to convert operators
      val gpuPlan = GpuOverrides().apply(plan)
      logInfo(s"GpuSpeculativeBroadcastRule: After GpuOverrides: " +
        s"${gpuPlan.getClass.getSimpleName}")
      // Then apply GpuTransitionOverrides to handle data format transitions
      val transitionedPlan = new GpuTransitionOverrides().apply(gpuPlan)
      logInfo(s"GpuSpeculativeBroadcastRule: After transitions: " +
        s"${transitionedPlan.getClass.getSimpleName}")
      transitionedPlan
    } else {
      logInfo("GpuSpeculativeBroadcastRule: GPU disabled, keeping CPU plan")
      plan
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
