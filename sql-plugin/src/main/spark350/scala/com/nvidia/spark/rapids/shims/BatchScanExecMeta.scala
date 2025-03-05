/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class BatchScanExecMeta(p: BatchScanExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends BatchScanExecMetaBase(p, conf, parent, rule) {
  override def convertToGpu(): GpuExec = {
    val spj = p.spjParams
    GpuBatchScanExec(p.output, childScans.head.convertToGpu(), runtimeFilters,
      spj.keyGroupedPartitioning, p.ordering, p.table, spj.commonPartitionValues,
      spj.applyPartialClustering, spj.replicatePartitions)
  }
}
