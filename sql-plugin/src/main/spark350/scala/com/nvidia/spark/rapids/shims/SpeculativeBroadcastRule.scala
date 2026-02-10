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

import com.nvidia.spark.rapids.{GpuOverrides, RapidsConf, SpeculativeBroadcastHashJoinExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode,
  HashJoin, ShuffledHashJoinExec}

/**
 * A queryStageOptimizerRule that transforms SpeculativeBroadcastHashJoinExec
 * based on actual runtime shuffle statistics.
 *
 * This rule runs after build side shuffle completes. It checks the actual size:
 * - If small enough: converts to BroadcastHashJoin (stream side avoids shuffle)
 * - If too large: converts to ShuffledHashJoin (adds shuffle for stream side)
 *
 * This rule is CPU/GPU neutral - it produces standard Spark CPU operators
 * (BroadcastHashJoinExec or ShuffledHashJoinExec). The subsequent GpuOverrides
 * pass will convert these to GPU operators if applicable.
 */
case class SpeculativeBroadcastRule(spark: SparkSession) extends Rule[SparkPlan] with Logging {

  private def rapidsConf: RapidsConf = new RapidsConf(spark.sessionState.conf)

  private def isEnabled: Boolean = rapidsConf.isSpeculativeBroadcastEnabled

  private def targetThreshold: Long = rapidsConf.speculativeBroadcastTargetThreshold

  override def apply(plan: SparkPlan): SparkPlan = {
    // Check if plan contains SpeculativeBroadcastHashJoinExec
    val specJoins = plan.collect { case s: SpeculativeBroadcastHashJoinExec => s }
    logDebug(s"SpeculativeBroadcastRule.apply called, isEnabled=$isEnabled, " +
      s"plan=${plan.getClass.getSimpleName}, containsSpecJoin=${specJoins.nonEmpty}, " +
      s"specJoinCount=${specJoins.size}")
    if (specJoins.nonEmpty) {
      logDebug(s"SpeculativeBroadcastRule: Plan tree:\n${plan.treeString}")
    }
    if (!isEnabled) {
      return plan
    }

    val result = plan.transformUp {
      case specJoin: SpeculativeBroadcastHashJoinExec =>
        logDebug(s"SpeculativeBroadcastRule: Found SpeculativeBroadcastHashJoinExec, " +
          s"buildSide=${specJoin.buildSide}, left=${specJoin.left.getClass.getSimpleName}, " +
          s"right=${specJoin.right.getClass.getSimpleName}")
        transformSpeculativeJoin(specJoin)
    }

    // Verify transformation
    val remaining = result.collect { case s: SpeculativeBroadcastHashJoinExec => s }
    logDebug(s"SpeculativeBroadcastRule: After transform, remaining SpecJoins: " +
      s"${remaining.size}, result root: ${result.getClass.getSimpleName}")
    if (remaining.nonEmpty) {
      logDebug(s"SpeculativeBroadcastRule: Result tree:\n${result.treeString}")
    }
    result
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
    logDebug(s"SpeculativeBroadcastRule: specJoin.left.output = ${specJoin.left.output}")
    logDebug(s"SpeculativeBroadcastRule: specJoin.right.output = ${specJoin.right.output}")
    logDebug(s"SpeculativeBroadcastRule: specJoin.leftKeys = ${specJoin.leftKeys}")
    logDebug(s"SpeculativeBroadcastRule: specJoin.rightKeys = ${specJoin.rightKeys}")

    // Find the build side shuffle stage
    findShuffleStageInfo(buildChild) match {
      case Some((buildStage, buildSize)) =>
        logDebug(s"SpeculativeBroadcastRule: buildChild.output = ${buildChild.output}")
        logDebug(s"SpeculativeBroadcastRule: buildStage.output = ${buildStage.output}")
        logDebug(s"SpeculativeBroadcastRule: streamChild.output = ${streamChild.output}")

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

        logInfo(s"SpeculativeBroadcastRule: buildPosMap = $buildPosMap")
        logInfo(s"SpeculativeBroadcastRule: streamPosMap = $streamPosMap")

        // Remap expression by finding matching attribute in output by exprId.id
        // Note: We compare only exprId.id, not the full ExprId (which includes jvmId),
        // because different AttributeReference objects can have different jvmIds
        def remapBuildExpr(expr: Expression): Expression = {
          expr.transform {
            case a: AttributeReference =>
              // Find attribute in buildStage.output with same exprId.id
              val newAttr = buildStage.output.find(_.exprId.id == a.exprId.id).getOrElse(a)
              if (newAttr ne a) {
                logInfo(s"SpeculativeBroadcastRule: Remapping build attr " +
                  s"${a.name}#${a.exprId.id} (jvm=${a.exprId}) -> " +
                  s"${newAttr.name}#${newAttr.exprId.id} (jvm=${newAttr.exprId})")
              } else {
                logInfo(s"SpeculativeBroadcastRule: No match in buildStage for " +
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
                logInfo(s"SpeculativeBroadcastRule: Remapping stream attr " +
                  s"${a.name}#${a.exprId.id} (jvm=${a.exprId}) -> " +
                  s"${newAttr.name}#${newAttr.exprId.id} (jvm=${newAttr.exprId})")
              } else {
                logInfo(s"SpeculativeBroadcastRule: No match in streamChild for " +
                  s"${a.name}#${a.exprId.id}, keeping original")
              }
              newAttr
          }
        }

        if (buildSize < targetThreshold) {
          // Small enough - use broadcast join (no stream side shuffle!)
          logInfo(s"SpeculativeBroadcastRule: Build side small ($buildSize bytes < " +
            s"$targetThreshold), using BroadcastHashJoin - stream side avoids shuffle!")

          // Remap build keys to buildStage.output attributes for HashedRelationBroadcastMode
          val originalBuildKeys = specJoin.buildSide match {
            case BuildLeft => specJoin.leftKeys
            case BuildRight => specJoin.rightKeys
          }
          val buildKeys = originalBuildKeys.map(remapBuildExpr)

          logInfo(s"SpeculativeBroadcastRule: originalBuildKeys = $originalBuildKeys")
          logInfo(s"SpeculativeBroadcastRule: remapped buildKeys = $buildKeys")

          // CRITICAL: HashJoin.rewriteKeyExpr rewrites IntegralType keys to Long for optimization.
          // BroadcastHashJoinExec codegen uses rewriteKeyExpr on streamedKeys, so it expects
          // the HashedRelation to also use rewritten keys. If we don't rewrite, the codegen
          // will call get(Long) but the relation only supports get(InternalRow).
          val rewrittenBuildKeys = HashJoin.rewriteKeyExpr(buildKeys)
          logInfo(s"SpeculativeBroadcastRule: rewrittenBuildKeys = $rewrittenBuildKeys")

          // Bind the rewritten keys to the buildStage output schema
          val boundBuildKeys = BindReferences.bindReferences(rewrittenBuildKeys, buildStage.output)
          logInfo(s"SpeculativeBroadcastRule: boundBuildKeys = $boundBuildKeys")

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

          logInfo(s"SpeculativeBroadcastRule: newLeftKeys=$newLeftKeys, " +
            s"newRightKeys=$newRightKeys, newCondition=$newCondition")

          val cpuJoin = BroadcastHashJoinExec(
            newLeftKeys, newRightKeys, specJoin.joinType,
            specJoin.buildSide, newCondition, newLeft, newRight)

          // Since queryStageOptimizerRules run after postStageCreationRules (where GpuOverrides
          // normally runs), we need to manually invoke GpuOverrides to convert this new join
          // to GPU if applicable.
          logDebug(s"SpeculativeBroadcastRule: Created BroadcastHashJoinExec, " +
            s"invoking GpuOverrides to convert to GPU")
          val gpuConverted = GpuOverrides().apply(cpuJoin)
          logDebug(s"SpeculativeBroadcastRule: After GpuOverrides: " +
            s"${gpuConverted.getClass.getSimpleName}")
          gpuConverted

        } else {
          // Too large - fall back to shuffled hash join (need stream side shuffle)
          logInfo(s"SpeculativeBroadcastRule: Build side too large ($buildSize bytes >= " +
            s"$targetThreshold), falling back to ShuffledHashJoin")

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

          logInfo(s"SpeculativeBroadcastRule: Fallback newLeftKeys=$newLeftKeys, " +
            s"newRightKeys=$newRightKeys")

          val cpuJoin = ShuffledHashJoinExec(
            newLeftKeys, newRightKeys, specJoin.joinType,
            specJoin.buildSide, newCondition, newLeft, newRight)

          // Since queryStageOptimizerRules run after postStageCreationRules (where GpuOverrides
          // normally runs), we need to manually invoke GpuOverrides to convert this new join
          // to GPU if applicable.
          logDebug(s"SpeculativeBroadcastRule: Created ShuffledHashJoinExec, " +
            s"invoking GpuOverrides to convert to GPU")
          val gpuConverted = GpuOverrides().apply(cpuJoin)
          logDebug(s"SpeculativeBroadcastRule: After GpuOverrides: " +
            s"${gpuConverted.getClass.getSimpleName}")
          gpuConverted
        }

      case None =>
        // Build side not ready yet, keep as-is (should not happen normally)
        logDebug("SpeculativeBroadcastRule: Build side shuffle not ready")
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
object SpeculativeBroadcastRule {
  def apply(spark: SparkSession): Rule[SparkPlan] = new SpeculativeBroadcastRule(spark)
}
