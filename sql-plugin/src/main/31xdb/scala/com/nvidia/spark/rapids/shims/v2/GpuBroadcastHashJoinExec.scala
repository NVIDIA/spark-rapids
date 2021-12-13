/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{FullOuter, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.rapids.execution.{GpuBroadcastExchangeExec, GpuBroadcastHelper, GpuHashJoin, JoinTypeChecks}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends GpuBroadcastJoinMeta[BroadcastHashJoinExec](join, conf, parent, rule) {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  def isAstCondition(): Boolean = {
    join.condition.isDefined && conditionMeta.forall(_.canThisBeAst)
  }

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override def tagPlanForGpu(): Unit = {
    val joinType = join.joinType
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)
    val gpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

    def unSupportNonEqualCondition(): Unit = if (join.condition.isDefined) {
      this.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
    }
    def unSupportNonEqualNonAst(): Unit = if (join.condition.isDefined) {
      conditionMeta.foreach(requireAstForGpuOn)
    }
    def unSupportStructKeys(): Unit = if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
      this.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
    }

    JoinTypeChecks.tagForGpu(joinType, this)
    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti =>
        unSupportNonEqualNonAst()
      case FullOuter =>
        unSupportNonEqualCondition()
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        unSupportStructKeys()
      case _ =>
        this.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
    // If there is an AST condition, we need to make sure the build side works for
    // GpuBroadcastNestedLoopJoin
    if (isAstCondition()) {
      joinType match {
        case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
          willNotWorkOnGpu(s"build left not supported for conditional ${joinType}")
        case RightOuter if gpuBuildSide == GpuBuildRight =>
          willNotWorkOnGpu(s"build right not supported for conditional ${joinType}")
        case _ =>
      }
    }

    val Seq(leftChild, rightChild) = childPlans
    val buildSide = join.buildSide match {
      case BuildLeft => leftChild
      case BuildRight => rightChild
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val gpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSide = join.buildSide match {
      case BuildLeft => left
      case BuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)

    val substituteBroadcastNestedLoopJoin: Boolean = join.joinType match {
      case RightOuter | LeftOuter | LeftSemi | LeftAnti  =>
        isAstCondition()
      case _ => false
    }

    if (substituteBroadcastNestedLoopJoin) {
      ShimLoader.getSparkShims.getGpuBroadcastNestedLoopJoinShim(
        left, right, gpuBuildSide,
        join.joinType,
        conditionMeta.map(_.convertToGpu()),
        conf.gpuTargetBatchSizeBytes)
    } else {
      val joinExec = GpuBroadcastHashJoinExec(
        leftKeys.map(_.convertToGpu()),
        rightKeys.map(_.convertToGpu()),
        join.joinType,
        GpuJoinUtils.getGpuBuildSide(join.buildSide),
        None,
        left, right)
      // The GPU does not yet support conditional joins, so conditions are implemented
      // as a filter after the join when possible.
      conditionMeta.map(c => GpuFilterExec(c.convertToGpu(), joinExec)).getOrElse(joinExec)
    }
  }
}

case class GpuBroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with GpuHashJoin {
  import GpuMetric._

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME)) ++ spillMetrics

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case GpuBuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case GpuBuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = (joinType, buildSide) match {
    // For FullOuter join require a single batch for the side that is not the broadcast, because it
    // will be a single batch already
    case (FullOuter, GpuBuildLeft) => Seq(null, RequireSingleBatch)
    case (FullOuter, GpuBuildRight) => Seq(RequireSingleBatch, null)
    case (_, _) => Seq(null, null)
  }

  def broadcastExchange: GpuBroadcastExchangeExec = buildPlan match {
    case BroadcastQueryStageExec(_, gpu: GpuBroadcastExchangeExec, _) => gpu
    case BroadcastQueryStageExec(_, reused: ReusedExchangeExec, _) =>
      reused.child.asInstanceOf[GpuBroadcastExchangeExec]
    case gpu: GpuBroadcastExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExec]
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(
      "GpuBroadcastHashJoin does not support row-based processing")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)

    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)

    val broadcastRelation = broadcastExchange.executeColumnarBroadcast[Any]()

    val rdd = streamedPlan.executeColumnar()
    val buildSchema = buildPlan.schema
    rdd.mapPartitions { it =>
      val stIt = new CollectTimeIterator("broadcast join stream", it, streamTime)
      withResource(
          GpuBroadcastHelper.getBroadcastBatch(broadcastRelation, buildSchema)) { builtBatch =>
        doJoin(builtBatch, stIt, targetSize, spillCallback,
          numOutputRows, joinOutputRows, numOutputBatches, opTime, joinTime)
      }
    }
  }
}
