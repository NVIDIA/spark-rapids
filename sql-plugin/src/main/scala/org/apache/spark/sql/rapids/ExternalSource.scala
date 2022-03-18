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

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids._

import org.apache.spark.sql.avro.AvroFileFormat
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.util.Utils

object ExternalSource {

  lazy val hasSparkAvroJar = {
    val loader = Utils.getContextOrSparkClassLoader

    /** spark-avro is an optional package for Spark, so the RAPIDS Accelerator
     * must run successfully without it. */
    Try(loader.loadClass("org.apache.spark.sql.v2.avro.AvroScan")) match {
      case Failure(_) => false
      case Success(_) => true
    }
  }

  def tagSupportForGpuFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (hasSparkAvroJar) {
      meta.wrapped.relation.fileFormat match {
        case _: AvroFileFormat => GpuReadAvroFileFormat.tagSupport(meta)
        case f =>
          meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
      }
    } else {
      meta.wrapped.relation.fileFormat match {
        case f =>
          meta.willNotWorkOnGpu(s"unsupported file format: ${f.getClass.getCanonicalName}")
      }
    }
  }

  def convertFileFormatForGpuFileSourceScanExec(format: FileFormat): FileFormat = {
    if (hasSparkAvroJar) {
      format match {
        case _: AvroFileFormat => new GpuReadAvroFileFormat
        case f =>
          throw new IllegalArgumentException(s"${f.getClass.getCanonicalName} is not supported")
      }
    } else {
      format match {
        case f =>
          throw new IllegalArgumentException(s"${f.getClass.getCanonicalName} is not supported")
      }
    }
  }

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    if (hasSparkAvroJar) {
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
                conf,
                a.partitionFilters,
                a.dataFilters)
          })
      ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
    } else Map.empty
  }

}
