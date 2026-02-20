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

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids._
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.FieldUtils
import org.apache.iceberg.{BaseMetadataTable, ScanTaskGroup}
import org.apache.iceberg.spark.{GpuSparkReadConf, SparkReadConf}
import org.apache.iceberg.types.Types

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.read.{Batch, Scan, Statistics, SupportsReportStatistics}
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

  // For Iceberg tables, disable field ID matching if there are nested types
  // Iceberg v2 writes field IDs for nested type components (array elements, map keys/values),
  // but cuDF's field ID matching is designed for top-level columns only
  lazy val useFieldId: Boolean = {
    def hasNestedTypes(dataType: org.apache.spark.sql.types.DataType): Boolean = {
      dataType match {
        case _: org.apache.spark.sql.types.ArrayType => true
        case _: org.apache.spark.sql.types.MapType => true
        case s: org.apache.spark.sql.types.StructType =>
          s.fields.exists(f => hasNestedTypes(f.dataType))
        case _ => false
      }
    }

    val schema = readSchema()
    val hasNested = schema.fields.exists(f => hasNestedTypes(f.dataType))

    // Disable field ID matching for nested types in Iceberg
    !hasNested
  }

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


object GpuSparkScan {
  def isMetadataScan(scan: Scan): Boolean = {
    scan.asInstanceOf[SparkScan].table().isInstanceOf[BaseMetadataTable]
  }

  def tryConvert(cpuScan: Scan, rapidsConf: RapidsConf): Try[GpuSparkScan] = {
    Try {
      cpuScan match {
        case icebergScan: SparkBatchQueryScan =>
          new GpuSparkBatchQueryScan(icebergScan, rapidsConf, false)
        case s: SparkCopyOnWriteScan =>
          new GpuSparkCopyOnWriteScan(s, rapidsConf, false)
        case _ =>
          throw new IllegalArgumentException(
            s"Currently iceberg support only supports batch query scan and copy-on-write scan, " +
              s"but got ${cpuScan.getClass.getName}")
      }
    }
  }

  def tagForGpu(meta: ScanMeta[Scan], gpuScan: Try[GpuSparkScan]): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergReadEnabled) {
      meta.willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
    }

    FileFormatChecks.tag(meta, meta.wrapped.readSchema(), IcebergFormatType, ReadFileOp)

    Try {
      GpuSparkScan.isMetadataScan(meta.wrapped)
    } match {
      case Success(true) => meta.willNotWorkOnGpu("scan is a metadata scan")
      case Failure(e) => meta.willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
      case _ =>
    }

    gpuScan match {
      case Success(_) =>
      case Failure(e) => meta.willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
    }
  }
}
