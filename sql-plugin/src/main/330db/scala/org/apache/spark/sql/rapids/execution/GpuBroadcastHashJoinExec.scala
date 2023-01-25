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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ExecutorBroadcastMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBroadcastHashJoinMetaBase(join, conf, parent, rule) {

  // TODO: This will be moved to a shimmed version of GpuBroadcastJoinMeta[_] when
  // BNLJ will support EXECUTOR_BROADCAST in addition to BHJ
  override def canBuildSideBeReplaced(buildSide: SparkPlanMeta[_]): Boolean = {
    buildSide.wrapped match {
      case bqse: BroadcastQueryStageExec => bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] ||
          bqse.plan.isInstanceOf[ReusedExchangeExec] &&
          bqse.plan.asInstanceOf[ReusedExchangeExec]
              .child.isInstanceOf[GpuBroadcastExchangeExec]
      case sqse: ShuffleQueryStageExec => sqse.plan.isInstanceOf[GpuShuffleExchangeExecBase] ||
          sqse.plan.isInstanceOf[ReusedExchangeExec] &&
          sqse.plan.asInstanceOf[ReusedExchangeExec]
              .child.isInstanceOf[GpuShuffleExchangeExecBase]
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec] ||
          reused.child.isInstanceOf[GpuShuffleExchangeExecBase]
      case _: GpuBroadcastExchangeExec | _: GpuShuffleExchangeExecBase => true
      case _ => buildSide.canThisBeReplaced
    }
  }

  override def verifyBuildSideWasReplaced(buildSide: SparkPlan): Unit = {
    val buildSideOnGpu = buildSide match {
      case bqse: BroadcastQueryStageExec => bqse.plan.isInstanceOf[GpuBroadcastExchangeExec] ||
          bqse.plan.isInstanceOf[ReusedExchangeExec] &&
              bqse.plan.asInstanceOf[ReusedExchangeExec]
                  .child.isInstanceOf[GpuBroadcastExchangeExec]
      case sqse: ShuffleQueryStageExec => sqse.plan.isInstanceOf[GpuShuffleExchangeExecBase] ||
          sqse.plan.isInstanceOf[ReusedExchangeExec] &&
          sqse.plan.asInstanceOf[ReusedExchangeExec]
              .child.isInstanceOf[GpuShuffleExchangeExecBase]
      case reused: ReusedExchangeExec => reused.child.isInstanceOf[GpuBroadcastExchangeExec] ||
          reused.child.isInstanceOf[GpuShuffleExchangeExecBase]
      case _: GpuBroadcastExchangeExec | _: GpuShuffleExchangeExecBase => true
      case _ => false
    }
    if (!buildSideOnGpu) {
      throw new IllegalStateException(s"the broadcast must be on the GPU too")
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
      left,
      right,
      join.isExecutorBroadcast)
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
    right: SparkPlan, 
    executorBroadcast: Boolean)
      extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right) {
  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  ) ++ semaphoreMetrics ++ spillMetrics

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isExecutorBroadcast) {
      buildSide match {
        case GpuBuildLeft =>
          BroadcastDistribution(ExecutorBroadcastMode) :: UnspecifiedDistribution :: Nil
        case GpuBuildRight =>
          UnspecifiedDistribution :: BroadcastDistribution(ExecutorBroadcastMode) :: Nil
      } 
    } else {
      super.requiredChildDistribution
    }
  }

  def isExecutorBroadcast(): Boolean = {
    executorBroadcast
  }

  def shuffleExchange: GpuShuffleExchangeExec = buildPlan match {
    case bqse: ShuffleQueryStageExec if bqse.plan.isInstanceOf[GpuShuffleExchangeExec] =>
      bqse.plan.asInstanceOf[GpuShuffleExchangeExec]
    case bqse: ShuffleQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuShuffleExchangeExec]
    case gpu: GpuShuffleExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuShuffleExchangeExec]
  }


  private def getExecutorBuiltBatchAndStreamIter(
      buildRelation: RDD[ColumnarBatch],
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      streamIter: Iterator[ColumnarBatch],
      coalesceMetricsMap: Map[String, GpuMetric]): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    val semWait = coalesceMetricsMap(GpuMetric.SEMAPHORE_WAIT_TIME)
    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val metricsMap = allMetrics

    val bufferedStreamIter = new CloseableBufferedIterator(streamIter.buffered)
    closeOnExcept(bufferedStreamIter) { _ =>
      withResource(new NvtxRange("first stream batch", NvtxColor.RED)) { _ =>
        if (bufferedStreamIter.hasNext) {
          bufferedStreamIter.head
        } else {
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), semWait)
        }
      }
      val buildBatch = GpuExecutorBroadcastHelper.getExecutorBroadcastBatch(buildRelation,
          buildSchema, buildOutput, metricsMap, targetSize)
      (buildBatch, bufferedStreamIter)
    }
  }

  private def doColumnarExecutorBroadcastJoin(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)

    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)

    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)

    val buildRelation = shuffleExchange.executeColumnar()

    val rdd = streamedPlan.executeColumnar()
    val localBuildSchema = buildPlan.schema
    val localBuildOutput = buildPlan.output
    rdd.mapPartitions { it =>
      val (builtBatch, streamIter) =
        getExecutorBuiltBatchAndStreamIter(
          buildRelation,
          localBuildSchema,
          localBuildOutput,
          new CollectTimeIterator("executor broadcast join stream", it, streamTime),
          allMetrics)
      withResource(builtBatch) { _ =>
        doJoin(builtBatch, streamIter, targetSize, spillCallback,
          numOutputRows, joinOutputRows, numOutputBatches, opTime, joinTime)
      }
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (isExecutorBroadcast) {
      doColumnarExecutorBroadcastJoin()
    } else {
      doColumnarBroadcastJoin()
    }
  }
}