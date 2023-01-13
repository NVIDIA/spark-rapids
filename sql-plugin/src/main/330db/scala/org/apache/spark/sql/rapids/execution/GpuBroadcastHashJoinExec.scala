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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ExecutorBroadcastMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastHashJoinMeta(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBroadcastHashJoinMetaBase(join, conf, parent, rule) {

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

  // Because of the nature of the executor-side broadcast, every executor needs to read the 
  // shuffle data directly from the executor that produces the broadcast data. 
  // Note that we can't call mapPartitions here because we're on an executor, and we don't 
  // have access to a SparkContext.
  private def shuffleCoalesceIterator(
    buildRelation: RDD[ColumnarBatch],
    buildSchema: StructType): Iterator[ColumnarBatch] = {

    val rapidsConf = new RapidsConf(conf)
    val targetSize = rapidsConf.gpuTargetBatchSizeBytes
    val dataTypes = GpuColumnVector.extractTypes(buildSchema)
    val metricsMap = allMetrics

    buildRelation.partitions.map { part =>
      val it = buildRelation.compute(part, TaskContext.get())
      new GpuShuffleCoalesceIterator(
        new HostShuffleCoalesceIterator(it, targetSize, dataTypes, metricsMap),
          dataTypes, metricsMap).asInstanceOf[Iterator[ColumnarBatch]]
    }.reduceLeft(_ ++ _)
  }

  private def getExecutorBatch(
      buildRelation: RDD[ColumnarBatch], 
      buildSchema: StructType): ColumnarBatch = {

    val it = shuffleCoalesceIterator(buildRelation, buildSchema)
    if (it.hasNext) {
      it.next
    } else {
      GpuColumnVector.emptyBatch(buildSchema)
    }
  }

  private def getExecutorBuiltBatchAndStreamIter(
      buildRelation: RDD[ColumnarBatch],
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

      val buildBatch = getExecutorBatch(buildRelation, buildSchema)
      (buildBatch, bufferedStreamIter)
    }
  }

  private def doExecutorBroadcastJoin(): RDD[ColumnarBatch] = {
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
    val buildSchema = buildPlan.schema
    rdd.mapPartitions { it =>
      val (builtBatch, streamIter) =
        getExecutorBuiltBatchAndStreamIter(
          buildRelation,
          buildSchema,
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
      doExecutorBroadcastJoin()
    } else {
      doColumnarBroadcastJoin()
    }
  }
}