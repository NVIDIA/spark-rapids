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

package com.nvidia.spark.rapids

import org.apache.spark.sql.execution.command.RunnableCommand

import com.nvidia.spark.rapids.shims.ShimUnaryCommand
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

trait GpuRunnableCommand extends RunnableCommand with ShimUnaryCommand {
  lazy val basicMetrics: Map[String, SQLMetric] = GpuWriteJobStatsTracker.basicMetrics
  lazy val taskMetrics: Map[String, SQLMetric] = GpuWriteJobStatsTracker.taskMetrics

  override lazy val metrics: Map[String, SQLMetric] = basicMetrics ++ taskMetrics

  override final def run(sparkSession: SparkSession): Seq[Row] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  def runColumnar(sparkSession: SparkSession, child: SparkPlan): Seq[ColumnarBatch]

  def gpuWriteJobStatsTracker(
      hadoopConf: Configuration): GpuWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    GpuWriteJobStatsTracker(serializableHadoopConf, this.basicMetrics, this.taskMetrics)
  }

  def requireSingleBatch: Boolean
}
