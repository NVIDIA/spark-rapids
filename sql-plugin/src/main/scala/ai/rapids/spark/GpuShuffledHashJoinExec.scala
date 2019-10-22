/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.spark.GpuMetricNames._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, ShuffledHashJoinExec}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import scala.math.max

class GpuShuffledHashJoinMeta(
    join: ShuffledHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[ShuffledHashJoinExec](join, conf, parent, rule) {
  val leftKeys: Seq[ExprMeta[_]] = join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[ExprMeta[_]] = join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val condition: Option[ExprMeta[_]] = join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[ExprMeta[_]] = leftKeys ++ rightKeys ++ condition

  override def tagPlanForGpu(): Unit = {
    if (!GpuHashJoin.isJoinTypeAllowed(join.joinType)) {
      willNotWorkOnGpu(s" ${join.joinType} is not currently supported")
    }
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
    leftKeys: Seq[GpuExpression],
    rightKeys: Seq[GpuExpression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[GpuExpression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with GpuHashJoin {

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "build side size"),
    "buildTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "build time"),
    "joinTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "join time"),
    "joinOutputRows" -> SQLMetrics.createMetric(sparkContext, "join output rows"),
    "filterTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "filter time"),
    "peakDevMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak device memory"))

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
        val buildBatch = ConcatAndConsumeAll.getSingleBatchWithVerification(buildIter, localBuildOutput)
        val asStringCatBatch = try {
          GpuColumnVector.convertToStringCategoriesIfNeeded(buildBatch)
        } finally {
          buildBatch.close()
        }
        val builtTable = try {
          val keys = GpuProjectExec.project(asStringCatBatch, gpuBuildKeys)
          try {
            // Combine does not inc any reference counting
            val combined = combine(keys, asStringCatBatch)
            combinedSize = GpuColumnVector.extractColumns(combined).map(_.dataType().defaultSize).sum * combined.numRows
            GpuColumnVector.from(combined)
          } finally {
            keys.close()
          }
        } finally {
          asStringCatBatch.close()
        }

        val delta = System.nanoTime() - startTime
        buildTime += delta
        totalTime += delta
        //TODO this does not take into account strings correctly.
        buildDataSize += combinedSize
        val context = TaskContext.get()
        context.addTaskCompletionListener[Unit](_ => builtTable.close())

        streamIter.map(cb => {
          val startTime = System.nanoTime()
          val ret = doJoin(builtTable, cb, boundCondition, joinOutputRows, numOutputRows,
            numOutputBatches, joinTime, filterTime)
          totalTime += (System.nanoTime() - startTime)
          ret
        })
      }
    }
  }
}
