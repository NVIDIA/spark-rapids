/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.shims.v2.ShimBinaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuShuffledNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    val condition: Option[Expression],
    val isSkewJoin: Boolean)(
    cpuLeftKeys: Seq[Expression],
    cpuRightKeys: Seq[Expression]) extends ShimBinaryExecNode with GpuExec {

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case GpuBuildLeft => (left, right)
    case GpuBuildRight => (right, left)
  }

  override def otherCopyArgs: Seq[AnyRef] = cpuLeftKeys :: cpuRightKeys :: Nil
  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS)) ++ spillMetrics

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(cpuLeftKeys) :: HashClusteredDistribution(cpuRightKeys) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledNestedLoopJoin does not support the execute() code path.")
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = (joinType, buildSide) match {
    case (FullOuter, _) => Seq(RequireSingleBatch, RequireSingleBatch)
    case (_, GpuBuildLeft) => Seq(RequireSingleBatch, null)
    case (_, GpuBuildRight) => Seq(null, RequireSingleBatch)
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"ShuffledNestedLoopJoin should not take $x as the JoinType")
    }
  }

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val streamTime = gpuLongMetric(STREAM_TIME)
    val targetSizeBytes = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val localBuildOutput: Seq[Attribute] = buildPlan.output
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
    val streamAttributes = streamedPlan.output
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
    val nestedLoopJoinType = joinType

    // Determine which table will be first in the join and bind the references accordingly
    // so the AST column references match the appropriate table.
    val (firstTable, secondTable) = joinType match {
      case RightOuter => (right, left)
      case _ => (left, right)
    }
    val numFirstTableColumns = firstTable.output.size
    val boundCondition = condition.map {
      GpuBindReferences.bindGpuReference(_, firstTable.output ++ secondTable.output)
    }

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        val spillableBuiltBatch = withResource(
          ConcatAndConsumeAll.getSingleBatchWithVerification(buildIter, localBuildOutput)) {
          builtBatch => {
            LazySpillableColumnarBatch(builtBatch, spillCallback, "built")
          }
        }
        val lazyStream = streamIter.map { cb =>
          withResource(cb) { cb =>
            LazySpillableColumnarBatch(cb, spillCallback, "stream_batch")
          }
        }
        GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(
          nestedLoopJoinType, buildSide, numFirstTableColumns,
          spillableBuiltBatch,
          lazyStream, streamAttributes, targetSizeBytes, boundCondition, spillCallback,
          numOutputRows = numOutputRows,
          joinOutputRows = joinOutputRows,
          numOutputBatches = numOutputBatches,
          opTime = opTime,
          joinTime = joinTime)
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}
