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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuParquetScan, RapidsConf, RapidsMeta, ScanMeta}

import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

class RapidsParquetScanMeta(
    pScan: ParquetScan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ScanMeta[ParquetScan](pScan, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    GpuParquetScan.tagSupport(this)
    // we are being overly cautious and that Parquet does not support this yet
    if (pScan.isInstanceOf[SupportsRuntimeFiltering]) {
      willNotWorkOnGpu("Parquet does not support Runtime filtering (DPP)" +
        " on datasource V2 yet.")
    }

    if (pScan.options.getBoolean("mergeSchema", true) &&
      conf.parquetReaderFooterType == RapidsConf.ParquetFooterReaderType.NATIVE) {
      willNotWorkOnGpu("Native footer reader for parquet does not work when" +
        " mergeSchema is enabled")
    }
  }

  override def convertToGpu(): Scan = {
    GpuParquetScan(pScan.sparkSession,
      pScan.hadoopConf,
      pScan.fileIndex,
      pScan.dataSchema,
      pScan.readDataSchema,
      pScan.readPartitionSchema,
      pScan.pushedFilters,
      pScan.options,
      pScan.partitionFilters,
      pScan.dataFilters,
      conf)
  }
}
