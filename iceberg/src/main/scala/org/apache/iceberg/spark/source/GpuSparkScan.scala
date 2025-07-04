/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.FieldUtils
import org.apache.iceberg.ScanTaskGroup
import org.apache.iceberg.spark.{GpuSparkReadConf, SparkReadConf}
import org.apache.iceberg.types.Types

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.types.StructType

abstract class GpuSparkScan(val cpuScan: SparkScan,
    val rapidsConf: RapidsConf,
    val queryUsesInputFile: Boolean,
) extends GpuScan with SupportsReportStatistics {
  private val readConf: GpuSparkReadConf = new GpuSparkReadConf(
    FieldUtils.readField(cpuScan, "readConf", true)
    .asInstanceOf[SparkReadConf])

  def readTimestampWithoutZone: Boolean = readConf.handleTimestampWithoutZone()

  override def readSchema(): StructType = cpuScan.readSchema()

  override def estimateStatistics(): Statistics = cpuScan.estimateStatistics()

  override def toBatch: Batch = {
    val cpuBatch = cpuScan.toBatch.asInstanceOf[SparkBatch]
    new GpuSparkBatch(cpuBatch, this)
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    throw new UnsupportedOperationException(
      "GpuSparkScan does not support toMicroBatchStream()")

  override def description(): String = cpuScan.description()

  override def reportDriverMetrics(): Array[CustomTaskMetric] = cpuScan.reportDriverMetrics()

  override def supportedCustomMetrics(): Array[CustomMetric] = cpuScan.supportedCustomMetrics()

  protected def groupingKeyType(): Types.StructType

  protected def taskGroups(): Seq[_ <: ScanTaskGroup[_]]
}
