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

import com.nvidia.spark.rapids.RapidsConf
import org.apache.iceberg.types.Types
import org.apache.iceberg.{PartitionScanTask, ScanTaskGroup}
import org.apache.spark.sql.connector.read.SupportsReportPartitioning
import org.apache.spark.sql.connector.read.partitioning.Partitioning

import scala.collection.JavaConverters._

abstract class GpuSparkPartitioningAwareScan[T <: PartitionScanTask](
    override val cpuScan: SparkPartitioningAwareScan[T],
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean,
) extends GpuSparkScan(cpuScan, rapidsConf, queryUsesInputFile) with SupportsReportPartitioning  {

  override def outputPartitioning(): Partitioning = {
    cpuScan.outputPartitioning()
  }

  override def groupingKeyType(): Types.StructType = cpuScan.groupingKeyType()

  override def taskGroups(): Seq[_ <: ScanTaskGroup[_]] = cpuScan.taskGroups().asScala.toSeq
}
