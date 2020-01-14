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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteTaskStats}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.SparkContext

/**
 * [[ColumnarWriteTaskStatsTracker]] implementation that produces [[WriteTaskStats]]
 * and tracks writing times per task.
 */
class GpuWriteTaskStatsTracker(
    hadoopConf: Configuration,
    taskMetrics: Map[String, SQLMetric])
    extends BasicColumnarWriteTaskStatsTracker(hadoopConf) {
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
 * [[WriteTaskStats]] they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class GpuWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient basicMetrics: Map[String, SQLMetric],
    taskMetrics: Map[String, SQLMetric])
    extends BasicColumnarWriteJobStatsTracker(serializableHadoopConf, basicMetrics) {
  override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
    new GpuWriteTaskStatsTracker(serializableHadoopConf.value, taskMetrics)
  }
}

object GpuWriteJobStatsTracker {
  val GPU_TIME_KEY = "gpuTime"
  val WRITE_TIME_KEY = "writeTime"

  lazy val basicMetrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics

  lazy val taskMetrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      GPU_TIME_KEY -> SQLMetrics.createNanoTimingMetric(sparkContext, "GPU time"),
      WRITE_TIME_KEY -> SQLMetrics.createNanoTimingMetric(sparkContext, "write time")
    )
  }

  def metrics: Map[String, SQLMetric] = basicMetrics ++ taskMetrics

  def apply(serializableHadoopConf: SerializableConfiguration): GpuWriteJobStatsTracker =
    new GpuWriteJobStatsTracker(serializableHadoopConf, basicMetrics, taskMetrics)
}
