/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{GpuBroadcastJoinMeta, ShimBinaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuBroadcastHashJoinMetaBase(
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
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
    GpuHashJoin.tagBuildSide(this, join.joinType, buildSide)
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }

    if (!canBuildSideBeReplaced(buildSideMeta)) {
      if (conf.isSqlExplainOnlyEnabled && wrapped.conf.adaptiveExecutionEnabled) {
        willNotWorkOnGpu("explain only mode with AQE, we cannot determine " +
          "if the broadcast for this join is on the GPU too")
      } else {
        willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
      }
    }

    if (!canThisBeReplaced) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
  }

  // Called in runAfterTagRules for a special post tagging for this broadcast join.
  def checkTagForBuildSide(): Unit = {
    val Seq(leftChild, rightChild) = childPlans
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => leftChild
      case GpuBuildRight => rightChild
    }
    // Check both of the conditions to avoid duplicate reason string.
    if (!canThisBeReplaced && canBuildSideBeReplaced(buildSideMeta)) {
      buildSideMeta.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
    if (canThisBeReplaced && !canBuildSideBeReplaced(buildSideMeta)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }
  }

  def convertToGpu(): GpuExec
}

abstract class GpuBroadcastHashJoinExecBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends ShimBinaryExecNode with GpuHashJoin {
  import GpuMetric._

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val opMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME))

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case GpuBuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case GpuBuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  def broadcastExchange: GpuBroadcastExchangeExec = buildPlan match {
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] =>
      bqse.plan.asInstanceOf[GpuBroadcastExchangeExec]
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuBroadcastExchangeExec]
    case gpu: GpuBroadcastExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExec]
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(
      "GpuBroadcastHashJoin does not support row-based processing")

  protected def doColumnarBroadcastJoin(): RDD[ColumnarBatch] = {
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)

    val broadcastRelation = broadcastExchange.executeColumnarBroadcast[Any]()

    val rdd = streamedPlan.executeColumnar()
    val buildSchema = buildPlan.schema
    rdd.mapPartitions { it =>
      val (builtBatch, streamIter) =
        GpuBroadcastHelper.getBroadcastBuiltBatchAndStreamIter(
          broadcastRelation,
          buildSchema,
          new CollectTimeIterator("broadcast join stream", it, streamTime))
      // builtBatch will be closed in doJoin
      doJoin(builtBatch, streamIter, targetSize, NoopMetric, NoopMetric, opTime, joinTime)
    }
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    doColumnarBroadcastJoin()
  }
}
