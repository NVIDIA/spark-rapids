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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuCSVScan, GpuScan, RapidsConf, RapidsMeta, ScanMeta}

import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan

class RapidsCsvScanMeta(
    cScan: CSVScan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ScanMeta[CSVScan](cScan, conf, parent, rule) {
  
  override def tagSelfForGpu(): Unit = {
    GpuCSVScan.tagSupport(this)
    // we are being overly cautious and that Csv does not support this yet
    TagScanForRuntimeFiltering.tagScanForRuntimeFiltering(this, cScan)
  }

  override def convertToGpu(): GpuScan =
    GpuCSVScan(cScan.sparkSession,
      cScan.fileIndex,
      cScan.dataSchema,
      cScan.readDataSchema,
      cScan.readPartitionSchema,
      cScan.options,
      cScan.partitionFilters,
      cScan.dataFilters,
      conf.maxReadBatchSizeRows,
      conf.maxReadBatchSizeBytes,
      conf.maxGpuColumnSizeBytes)
}
