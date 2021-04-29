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

package com.nvidia.spark.rapids

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.rapids.execution.GpuHashJoin
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuShuffledHashJoinBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: GpuBuildSide,
    condition: Option[Expression],
    val isSkewJoin: Boolean) extends BinaryExecNode with GpuHashJoin {
  import GpuMetric._

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    BUILD_DATA_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    FILTER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_FILTER_TIME))

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledHashJoin does not support the execute() code path.")
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = (joinType, buildSide) match {
    case (FullOuter, _) => Seq(RequireSingleBatch, RequireSingleBatch)
    case (_, GpuBuildLeft) => Seq(RequireSingleBatch, null)
    case (_, GpuBuildRight) => Seq(null, RequireSingleBatch)
  }

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val buildTime = gpuLongMetric(BUILD_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val filterTime = gpuLongMetric(FILTER_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)

    val boundCondition = condition.map(GpuBindReferences.bindReference(_, output))

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        var combinedSize = 0

        val startTime = System.nanoTime()
        val builtTable = withResource(ConcatAndConsumeAll.getSingleBatchWithVerification(
          buildIter, localBuildOutput)) { buildBatch: ColumnarBatch =>
          withResource(GpuProjectExec.project(buildBatch, gpuBuildKeys)) { keys =>
            val combined = GpuHashJoin.incRefCount(combine(keys, buildBatch))
            withResource(combined) { combined =>
              combinedSize =
                  GpuColumnVector.extractColumns(combined)
                      .map(_.getBase.getDeviceMemorySize).sum.toInt
              GpuColumnVector.from(combined)
            }
          }
        }

        val delta = System.nanoTime() - startTime
        buildTime += delta
        totalTime += delta
        buildDataSize += combinedSize
        val context = TaskContext.get()
        context.addTaskCompletionListener[Unit](_ => builtTable.close())

        doJoin(builtTable, streamIter, boundCondition,
          numOutputRows, joinOutputRows, numOutputBatches,
          streamTime, joinTime, filterTime, totalTime)
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}
