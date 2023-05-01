/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "340"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuOrcScan, RapidsConf, RapidsMeta, ScanMeta}

import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeV2Filtering}
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
    // SupportsRuntimeV2Filtering is actually the parent of SupportsRuntimeFiltering
    // in 3.4.0, so this should catch both types
    if (oScan.isInstanceOf[SupportsRuntimeV2Filtering]) {
      willNotWorkOnGpu("Orc does not support Runtime filtering (DPP)" +
        " on datasource V2 yet.")
    }
    // Spark[330,_] allows aggregates to be pushed down to ORC
    if (oScan.pushedAggregate.nonEmpty) {
      willNotWorkOnGpu("aggregates pushed into ORC read, which is a metadata only operation")
    }
  }

  override def convertToGpu(): Scan =
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
