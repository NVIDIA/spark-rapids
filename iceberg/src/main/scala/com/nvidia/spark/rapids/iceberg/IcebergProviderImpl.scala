/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.{FileFormatChecks, GpuScan, IcebergFormatType, RapidsConf, ReadFileOp, ScanMeta, ScanRule, ShimReflectionUtils}
import com.nvidia.spark.rapids.iceberg.spark.source.GpuSparkBatchQueryScan

import org.apache.spark.sql.connector.read.Scan

class IcebergProviderImpl extends IcebergProvider {
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val cpuIcebergScanClass = ShimReflectionUtils.loadClass(IcebergProvider.cpuScanClassName)
    Seq(new ScanRule[Scan](
      (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
        private lazy val convertedScan: Try[GpuSparkBatchQueryScan] = Try {
          GpuSparkBatchQueryScan.fromCpu(a, this.conf)
        }

        override def supportsRuntimeFilters: Boolean = true

        override def tagSelfForGpu(): Unit = {
          if (!this.conf.isIcebergEnabled) {
            willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG} to true")
          }

          if (!this.conf.isIcebergReadEnabled) {
            willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
          }

          FileFormatChecks.tag(this, a.readSchema(), IcebergFormatType, ReadFileOp)

          Try {
            GpuSparkBatchQueryScan.isMetadataScan(a)
          } match {
            case Success(true) => willNotWorkOnGpu("scan is a metadata scan")
            case Failure(e) => willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
            case _ =>
          }

          convertedScan match {
            case Failure(e) => willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
            case _ =>
          }
        }

        override def convertToGpu(): GpuScan = convertedScan.get
      },
      "Iceberg scan",
      ClassTag(cpuIcebergScanClass))
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }
}
