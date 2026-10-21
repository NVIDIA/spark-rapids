/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import java.util.Locale

import com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.METADATA_ROW_IDX_COL

import org.apache.spark.sql.execution.datasources.HadoopFsRelation

/** DBR 18.3 wrapper for the shared native deletion-vector reader. */
case class GpuDeltaParquetFileFormatNativeDV(
    @transient relation: HadoopFsRelation,
    protocol: Protocol,
    metadata: Metadata,
    generateRowIndexFilterId: Boolean = false,
    generateRowIndexFilterColumn: Boolean = false,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
) extends GpuDeltaParquetFileFormatNativeDVBase(
    relation, protocol, metadata, generateRowIndexFilterId, generateRowIndexFilterColumn,
    nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields, optimizationsEnabled,
    tablePath, isCDCRead,
    lowShuffleMergeDiscoveryScan = false,
    lowShuffleMergeRowIndexColumn = METADATA_ROW_IDX_COL) {
  // Until this runtime supports low-shuffle MERGE, do not interpret marked scans as normal reads.
  require(!relation.options.keys.exists { key =>
    key.toLowerCase(Locale.ROOT).startsWith("spark.rapids.internal.delta.lowshufflemerge.")
  }, "Low-shuffle MERGE scan options are not supported by the DBR 18.3 native Delta reader")
}
