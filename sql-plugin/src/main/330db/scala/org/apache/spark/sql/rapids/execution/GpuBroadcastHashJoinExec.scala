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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ExecutorBroadcast, ExecutorBroadcastMode, HashedRelation}
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
    val execBroadcast = if (join.isExecutorBroadcast) {
      Some(join.executorBroadcast)
    } else {
      None
    }

    val joinExec = GpuBroadcastHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      buildSide,
      joinCondition,
      left, right)(execBroadcast)
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
    right: SparkPlan)(executorBroadcast: Option[ExecutorBroadcast[HashedRelation]])
      extends GpuBroadcastHashJoinExecBase(
      leftKeys, rightKeys, joinType, buildSide, condition, left, right) {
  import GpuMetric._

  override def otherCopyArgs: Seq[AnyRef] = executorBroadcast :: Nil

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
    executorBroadcast.isDefined
  }

  private def getExecutorBatch(
      buildRelation: RDD[ColumnarBatch], 
      buildSchema: StructType): ColumnarBatch = {
    
    // For now, we only support SinglePartition, so we should only need to get one of the
    // partitions from the iterator
    val iter = buildRelation.toLocalIterator
    if (iter.hasNext) {
      iter.next
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

    val buildRelation = buildPlan.executeColumnar()

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