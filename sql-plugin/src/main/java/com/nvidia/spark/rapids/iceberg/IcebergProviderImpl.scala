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

package com.nvidia.spark.rapids.iceberg

import scala.reflect.ClassTag
import scala.util.{Failure, Try}

import com.nvidia.spark.rapids.{FileFormatChecks, IcebergFormatType, RapidsConf, ReadFileOp, ScanMeta, ScanRule, ShimLoader}
import com.nvidia.spark.rapids.iceberg.spark.source.GpuSparkBatchQueryScan

import org.apache.spark.sql.connector.read.Scan

class IcebergProviderImpl extends IcebergProvider {
  override def isSupportedScan(scan: Scan): Boolean = scan.isInstanceOf[GpuSparkBatchQueryScan]

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val cpuIcebergScanClass = ShimLoader.loadClass(IcebergProvider.cpuScanClassName)
    Seq(new ScanRule[Scan](
      (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
        private lazy val convertedScan: Try[GpuSparkBatchQueryScan] = Try {
          GpuSparkBatchQueryScan.fromCpu(a, conf)
        }

        override def supportsRuntimeFilters: Boolean = true

        override def tagSelfForGpu(): Unit = {
          if (!conf.isIcebergEnabled) {
            willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG} to true")
          }

          if (!conf.isIcebergReadEnabled) {
            willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
          }

          FileFormatChecks.tag(this, a.readSchema(), IcebergFormatType, ReadFileOp)

          if (GpuSparkBatchQueryScan.isMetadataScan(a)) {
            willNotWorkOnGpu("scan is a metadata scan")
          }

          convertedScan match {
            case Failure(e) => willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
            case _ =>
          }
        }

        override def convertToGpu(): Scan = convertedScan.get
      },
      "Iceberg scan",
      ClassTag(cpuIcebergScanClass))
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }
}
