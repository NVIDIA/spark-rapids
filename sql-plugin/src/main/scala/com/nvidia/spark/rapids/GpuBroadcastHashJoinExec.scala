/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode}
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
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
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

  override def convertToGpu(): GpuExec = {
    val condition = conditionMeta.map(_.convertToGpu())
    val (joinCondition, filterCondition) = if (conditionMeta.forall(_.canThisBeAst)) {
      (condition, None)
    } else {
      (None, condition)
    }
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSideMeta = buildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSideMeta)
    val joinExec = GpuBroadcastHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      buildSide,
      joinCondition,
      left, right)
    // For inner joins we can apply a post-join condition for any conditions that cannot be
    // evaluated directly in a mixed join that leverages a cudf AST expression
    filterCondition.map(c => GpuFilterExec(c, joinExec)).getOrElse(joinExec)
  }
}

case class GpuBroadcastHashJoinExec(
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

  /**
   * Gets the ColumnarBatch for the build side and the stream iterator by
   * acquiring the GPU only after first stream batch has been streamed to GPU.
   *
   * `broadcastRelation` represents the broadcasted build side table on the host. The code
   * in this function peaks at the stream side, after having wrapped it in a closeable
   * buffered iterator, to cause the stream side to produce the first batch. This delays
   * acquiring the semaphore until after the stream side performs all the steps needed
   * (including IO) to produce that first batch. Once the first stream batch is produced,
   * the build side is materialized to the GPU (while holding the semaphore).
   *
   * TODO: This could try to trigger the broadcast materialization on the host before
   *   getting started on the stream side (e.g. call `broadcastRelation.value`).
   */
  private def getBroadcastBuiltBatchAndStreamIter(
      broadcastRelation: Broadcast[Any],
      buildSchema: StructType,
      streamIter: Iterator[ColumnarBatch],
      coalesceMetricsMap: Map[String, GpuMetric]): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    val semWait = coalesceMetricsMap(GpuMetric.SEMAPHORE_WAIT_TIME)

    val bufferedStreamIter = new CloseableBufferedIterator(streamIter.buffered)
    closeOnExcept(bufferedStreamIter) { _ =>
      withResource(new NvtxRange("first stream batch", NvtxColor.RED)) { _ =>
        if (bufferedStreamIter.hasNext) {
          bufferedStreamIter.head
        } else {
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), semWait)
        }
      }

      val buildBatch =
        GpuBroadcastHelper.getBroadcastBatch(broadcastRelation, buildSchema)
      (buildBatch, bufferedStreamIter)
    }
  }

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
      val (builtBatch, streamIter) =
        getBroadcastBuiltBatchAndStreamIter(
          broadcastRelation,
          buildSchema,
          new CollectTimeIterator("broadcast join stream", it, streamTime),
          allMetrics)
      withResource(builtBatch) { _ =>
        doJoin(builtBatch, streamIter, targetSize, spillCallback,
          numOutputRows, joinOutputRows, numOutputBatches, opTime, joinTime)
      }
    }
  }
}
