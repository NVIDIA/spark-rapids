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

package com.nvidia.spark.rapids.shims.spark311

import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, GpuShuffledHashJoinBase}
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuJoinUtils {
  def getGpuBuildSide(buildSide: BuildSide): GpuBuildSide = {
    buildSide match {
      case BuildRight => GpuBuildRight
      case BuildLeft => GpuBuildLeft
      case _ => throw new Exception("unknown buildSide Type")
    }
  }
}

/**
 *  Spark 3.1 changed packages of BuildLeft, BuildRight, BuildSide
 */
class GpuShuffledHashJoinMeta(
    join: ShuffledHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ShuffledHashJoinExec](join, conf, parent, rule) {
  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val condition: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ condition

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, join.leftKeys, join.rightKeys, join.condition)
  }

  override def convertToGpu(): GpuExec =
    GpuShuffledHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      GpuJoinUtils.getGpuBuildSide(join.buildSide),
      condition.map(_.convertToGpu()),
      childPlans(0).convertIfNeeded(),
      childPlans(1).convertIfNeeded(),
      isSkewJoin = false)
}

case class GpuShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    override val isSkewJoin: Boolean)
  extends GpuShuffledHashJoinBase(
    leftKeys,
    rightKeys,
    joinType,
    condition,
    left,
    right,
    isSkewJoin)
  with GpuHashJoin {
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

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = buildSide match {
    case GpuBuildLeft => Seq(RequireSingleBatch, null)
    case GpuBuildRight => Seq(null, RequireSingleBatch)
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
}
