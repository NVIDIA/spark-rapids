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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.GpuDataWritingCommand
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME
import org.apache.spark.util.SerializableConfiguration

/**
 * [[ColumnarWriteTaskStatsTracker]] implementation that produces `WriteTaskStats`
 * and tracks writing times per task.
 */
class GpuWriteTaskStatsTracker(
    hadoopConf: Configuration,
    taskMetrics: Map[String, SQLMetric])
    extends BasicColumnarWriteTaskStatsTracker(hadoopConf, taskMetrics.get(TASK_COMMIT_TIME)) {
  def addGpuTime(nanos: Long): Unit = {
    taskMetrics(GpuWriteJobStatsTracker.GPU_TIME_KEY) += nanos
  }

  def addWriteTime(nanos: Long): Unit = {
    taskMetrics(GpuWriteJobStatsTracker.WRITE_TIME_KEY) += nanos
  }
}

/**
 * Simple [[ColumnarWriteJobStatsTracker]] implementation that's serializable, capable of
 * instantiating [[GpuWriteTaskStatsTracker]] on executors and processing the
 * `WriteTaskStats` they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class GpuWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient driverSideMetrics: Map[String, SQLMetric],
    taskMetrics: Map[String, SQLMetric])
    extends BasicColumnarWriteJobStatsTracker(serializableHadoopConf, driverSideMetrics) {
  override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
    new GpuWriteTaskStatsTracker(serializableHadoopConf.value, taskMetrics)
  }
}

object GpuWriteJobStatsTracker {
  val GPU_TIME_KEY = "gpuTime"
  val WRITE_TIME_KEY = "writeTime"

  def basicMetrics: Map[String, SQLMetric] = BasicColumnarWriteJobStatsTracker.metrics

  def taskMetrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      GPU_TIME_KEY -> SQLMetrics.createNanoTimingMetric(sparkContext, "GPU time"),
      WRITE_TIME_KEY -> SQLMetrics.createNanoTimingMetric(sparkContext, "write time"),
      TASK_COMMIT_TIME -> basicMetrics(TASK_COMMIT_TIME)
    )
  }

  def apply(serializableHadoopConf: SerializableConfiguration,
      command: GpuDataWritingCommand): GpuWriteJobStatsTracker =
    new GpuWriteJobStatsTracker(serializableHadoopConf, command.basicMetrics, command.taskMetrics)
}
