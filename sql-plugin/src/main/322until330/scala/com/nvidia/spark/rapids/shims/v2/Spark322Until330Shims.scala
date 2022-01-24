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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.{GpuCSVScan, GpuOrcScanBase, GpuOverrides, GpuParquetScanBase, ScanMeta, ScanRule}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

/**
 * Shim base class that can be compiled with every supported 3.2.2+
 */
trait Spark322Until330Shims extends Spark322PlusShims with RebaseShims with Logging {
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuParquetScanBase.tagSupport(this)
          // we are being overly cautious and that Parquet does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Parquet does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan = {
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
        }
      }),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuOrcScanBase.tagSupport(this)
          // we are being overly cautious and that Orc does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Orc does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      }),
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuCSVScan.tagSupport(this)
          // we are being overly cautious and that Csv does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Csv does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf.maxReadBatchSizeRows,
            conf.maxReadBatchSizeBytes)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
}