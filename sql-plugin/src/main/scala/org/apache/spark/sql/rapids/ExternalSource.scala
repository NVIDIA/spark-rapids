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

package org.apache.spark.sql.rapids

import scala.util.Try

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.iceberg.IcebergProvider

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * The subclass of AvroProvider imports spark-avro classes. This file should not imports
 * spark-avro classes because `class not found` exception may throw if spark-avro does not
 * exist at runtime. Details see: https://github.com/NVIDIA/spark-rapids/issues/5648
 */
object ExternalSource extends Logging {
  val avroScanClassName = "org.apache.spark.sql.v2.avro.AvroScan"
  lazy val hasSparkAvroJar = {
    /** spark-avro is an optional package for Spark, so the RAPIDS Accelerator
     * must run successfully without it. */
    Utils.classIsLoadable(avroScanClassName) && {
      Try(ShimLoader.loadClass(avroScanClassName)).map(_ => true)
        .getOrElse {
          logWarning("Avro library not found by the RAPIDS plugin. The Plugin jars are " +
              "likely deployed using a static classpath spark.driver/executor.extraClassPath. " +
              "Consider using --jars or --packages instead.")
          false
        }
    }
  }

  lazy val avroProvider = ShimLoader.newAvroProvider()

  private lazy val hasIcebergJar = {
    Utils.classIsLoadable(IcebergProvider.cpuScanClassName) &&
        Try(ShimLoader.loadClass(IcebergProvider.cpuScanClassName)).isSuccess
  }

  private lazy val icebergProvider = IcebergProvider()

  /** If the file format is supported as an external source */
  def isSupportedFormat(format: FileFormat): Boolean = {
    if (hasSparkAvroJar) {
      avroProvider.isSupportedFormat(format)
    } else false
  }

  def isPerFileReadEnabledForFormat(format: FileFormat, conf: RapidsConf): Boolean = {
    if (hasSparkAvroJar) {
      avroProvider.isPerFileReadEnabledForFormat(format, conf)
    } else false
  }

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (hasSparkAvroJar) {
      avroProvider.tagSupportForGpuFileSourceScan(meta)
    }
  }

  /**
   * Get a read file format for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def getReadFileFormat(format: FileFormat): FileFormat = {
    if (hasSparkAvroJar) {
      avroProvider.getReadFileFormat(format)
    } else {
      throw new IllegalArgumentException(s"${format.getClass.getCanonicalName} is not supported")
    }
  }

  /**
   * Create a multi-file reader factory for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def createMultiFileReaderFactory(
      format: FileFormat,
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    if (hasSparkAvroJar) {
      avroProvider.createMultiFileReaderFactory(format, broadcastedConf, pushedFilters,
        fileScan)
    } else {
      throw new RuntimeException(s"File format $format is not supported yet")
    }
  }

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    var scans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Map.empty
    if (hasSparkAvroJar) {
      scans = scans ++ avroProvider.getScans
    }
    if (hasIcebergJar) {
      scans = scans ++ icebergProvider.getScans
    }
    scans
  }

  /** If the scan is supported as an external source */
  def isSupportedScan(scan: Scan): Boolean = {
    if (hasSparkAvroJar && avroProvider.isSupportedScan(scan)) {
      true
    } else if (hasIcebergJar && icebergProvider.isSupportedScan(scan)) {
      true
    } else {
      false
    }
  }

  /**
   * Clone the input scan with setting 'true' to the 'queryUsesInputFile'.
   * Better to check if the scan is supported first by calling 'isSupportedScan'.
   */
  def copyScanWithInputFileTrue(scan: Scan): Scan = {
    if (hasSparkAvroJar && avroProvider.isSupportedScan(scan)) {
      avroProvider.copyScanWithInputFileTrue(scan)
    } else if (hasIcebergJar && icebergProvider.isSupportedScan(scan)) {
      // Iceberg does not yet support a coalescing reader, so nothing to change
      scan
    } else {
      throw new RuntimeException(s"Unsupported scan type: ${scan.getClass.getSimpleName}")
    }
  }
}
