/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.SerializableConfiguration

trait AvroProvider {
  /** If the file format is supported as an external source */
  def isSupportedFormat(format: FileFormat): Boolean

  def isPerFileReadEnabledForFormat(format: FileFormat, conf: RapidsConf): Boolean

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit

  /**
   * Get a read file format for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def getReadFileFormat(format: FileFormat): FileFormat

  /**
   * Create a multi-file reader factory for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def createMultiFileReaderFactory(
      format: FileFormat,
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]

  def isSupportedScan(scan: Scan): Boolean

  def copyScanWithInputFileTrue(scan: Scan): Scan
}
