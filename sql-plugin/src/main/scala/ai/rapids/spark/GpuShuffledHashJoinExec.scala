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
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

case class GpuShuffledHashJoinExec(
    leftKeys: Seq[GpuExpression],
    rightKeys: Seq[GpuExpression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[GpuExpression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with GpuHashJoin {

  override val additionalMetrics: Map[String, SQLMetric] = Map(
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "time to build hash map"))

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
          val ret = doJoin(builtTable, cb)
          totalTime += (System.nanoTime() - startTime)
          numOutputRows += ret.numRows()
          numOutputBatches += 1
          ret
        })
      }
    }
  }
}
