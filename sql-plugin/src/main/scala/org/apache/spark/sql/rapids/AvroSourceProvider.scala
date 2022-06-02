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

import com.nvidia.spark.rapids._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.avro.{AvroFileFormat, AvroOptions}
import org.apache.spark.sql.connector.read.{PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.SerializableConfiguration

class AvroSourceProvider extends AvroProvider {

  /** If the file format is supported as an external source */
  def isSupportedFormat(format: FileFormat): Boolean = {
    format match {
      case _: AvroFileFormat => true
      case _ => false
    }
  }

  def isPerFileReadEnabledForFormat(format: FileFormat, conf: RapidsConf): Boolean = {
    format match {
      case _: AvroFileFormat => conf.isAvroPerFileReadEnabled
      case _ => false
    }
  }

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    meta.wrapped.relation.fileFormat match {
      case _: AvroFileFormat => GpuReadAvroFileFormat.tagSupport(meta)
      case f =>
        meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
    }
  }

  /**
   * Get a read file format for the input format.
   * Better to check if the format is supported first by calling 'isSupportedFormat'
   */
  def getReadFileFormat(format: FileFormat): FileFormat = {
    format match {
      case _: AvroFileFormat => new GpuReadAvroFileFormat
      case f =>
        throw new IllegalArgumentException(s"${f.getClass.getCanonicalName} is not supported")
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
    format match {
      case _: AvroFileFormat =>
        GpuAvroMultiFilePartitionReaderFactory(
          fileScan.relation.sparkSession.sessionState.conf,
          fileScan.rapidsConf,
          broadcastedConf,
          fileScan.relation.dataSchema,
          fileScan.requiredSchema,
          fileScan.relation.partitionSchema,
          new AvroOptions(fileScan.relation.options, broadcastedConf.value.value),
          fileScan.allMetrics,
          pushedFilters,
          fileScan.queryUsesInputFile)
      case _ =>
        // never reach here
        throw new RuntimeException(s"File format $format is not supported yet")
    }
  }

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    Seq(
      GpuOverrides.scan[AvroScan](
        "Avro parsing",
        (a, conf, p, r) => new ScanMeta[AvroScan](a, conf, p, r) {
          override def tagSelfForGpu(): Unit = GpuAvroScan.tagSupport(this)

          override def convertToGpu(): Scan =
            GpuAvroScan(a.sparkSession,
              a.fileIndex,
              a.dataSchema,
              a.readDataSchema,
              a.readPartitionSchema,
              a.options,
              a.pushedFilters,
              conf,
              a.partitionFilters,
              a.dataFilters)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }
}
