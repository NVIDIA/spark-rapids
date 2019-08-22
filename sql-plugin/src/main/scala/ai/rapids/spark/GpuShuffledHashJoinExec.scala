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

import java.util.concurrent.TimeUnit.NANOSECONDS

import ai.rapids.cudf.Table

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.joins.{BuildSide, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute

class GpuShuffledHashJoinExec(
    leftKeys: Seq[GpuExpression],
    rightKeys: Seq[GpuExpression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[GpuExpression],
    left: SparkPlan,
    right: SparkPlan) extends ShuffledHashJoinExec(leftKeys, rightKeys, joinType,
  buildSide, condition, left, right) with GpuHashJoin {

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        var combinedSize = 0
        val start = System.nanoTime()
        val buildBatch = ConcatAndConsumeAll(buildIter, localBuildOutput)
        val builtTable = try {
          val keys = GpuProjectExec.project(buildBatch, gpuBuildKeys)
          try {
            // Combine does not inc any reference counting
            val combined = combine(keys, buildBatch)
            combinedSize = GpuColumnVector.extractColumns(combined).map(_.dataType().defaultSize).sum * combined.numRows
            GpuColumnVector.from(combined)
          } finally {
            keys.close()
          }
        } finally {
          buildBatch.close()
        }

        buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
        //TODO this does not take into account strings correctly.
        buildDataSize += combinedSize
        val context = TaskContext.get()
        context.addTaskCompletionListener[Unit](_ => builtTable.close())

        streamIter.map(cb => {
          val ret = doJoin(builtTable, cb)
          numOutputRows += ret.numRows()
          ret
        })
      }
    }
  }
}
