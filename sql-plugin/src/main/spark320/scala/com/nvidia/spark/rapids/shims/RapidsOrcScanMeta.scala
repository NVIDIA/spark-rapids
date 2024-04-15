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
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuOrcScan, GpuScan, RapidsConf, RapidsMeta, ScanMeta}

import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan

class RapidsOrcScanMeta(
    oScan: OrcScan,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ScanMeta[OrcScan](oScan, conf, parent, rule) {
  
  override def tagSelfForGpu(): Unit = {
    GpuOrcScan.tagSupport(this)
    // we are being overly cautious and that Orc does not support this yet
    TagScanForRuntimeFiltering.tagScanForRuntimeFiltering(this, oScan)
  }

  override def convertToGpu(): GpuScan =
    GpuOrcScan(oScan.sparkSession,
      oScan.hadoopConf,
      oScan.fileIndex,
      oScan.dataSchema,
      oScan.readDataSchema,
      oScan.readPartitionSchema,
      oScan.options,
      oScan.pushedFilters,
      oScan.partitionFilters,
      oScan.dataFilters,
      conf)
}
