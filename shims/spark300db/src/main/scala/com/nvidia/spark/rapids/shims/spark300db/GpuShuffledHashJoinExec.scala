/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark300db

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetricNames._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    rule: ConfKeysAndIncompat)
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
      join.buildSide,
      condition.map(_.convertToGpu()),
      childPlans(0).convertIfNeeded(),
      childPlans(1).convertIfNeeded())
}

case class GpuShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with GpuHashJoin {

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "build side size"),
    "buildTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "build time"),
    "joinTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "join time"),
    "joinOutputRows" -> SQLMetrics.createMetric(sparkContext, "join output rows"),
    "filterTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "filter time"))

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledHashJoin does not support the execute() code path.")
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = buildSide match {
    case BuildLeft => Seq(RequireSingleBatch, null)
    case BuildRight => Seq(null, RequireSingleBatch)
  }

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val buildDataSize = longMetric("buildDataSize")
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val buildTime = longMetric("buildTime")
    val joinTime = longMetric("joinTime")
    val filterTime = longMetric("filterTime")
    val joinOutputRows = longMetric("joinOutputRows")

    val boundCondition = condition.map(GpuBindReferences.bindReference(_, output))

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        var combinedSize = 0

        val startTime = System.nanoTime()
        val builtTable = withResource(ConcatAndConsumeAll.getSingleBatchWithVerification(
          buildIter, localBuildOutput)) { buildBatch: ColumnarBatch =>
          withResource(GpuProjectExec.project(buildBatch, gpuBuildKeys)) { keys =>
            val combined = GpuHashJoin.incRefCount(combine(keys, buildBatch))
            val filtered = filterBuiltTableIfNeeded(combined)
            combinedSize =
                GpuColumnVector.extractColumns(filtered)
                    .map(_.getBase.getDeviceMemorySize).sum.toInt
            withResource(filtered) { filtered =>
              GpuColumnVector.from(filtered)
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
          joinTime, filterTime, totalTime)
      }
    }
  }
}
