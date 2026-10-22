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

package com.nvidia.spark.rapids.delta.shims

import com.databricks.sql.transaction.tahoe.{DeltaParquetFileFormat, Snapshot}
import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsConf, RapidsMeta}

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.StructType

/** Fallback checks shared by DBR 17.3+ DELETE, UPDATE and MERGE with persistent DVs. */
object PersistentDeletionVectorMetaShim {
  /**
   * Tags the command for CPU fallback when the GPU cannot write persistent deletion vectors.
   *
   * The GPU bitmap generation relies on the target scan running on GPU, because only the GPU
   * reader fills the physical row index requested through the DBR metadata row index. The CPU
   * reader returns nulls for it, which DBR's bitmap aggregation silently skips.
   *
   * @return true if the command writes persistent deletion vectors
   */
  def tagForGpu(
      meta: RapidsMeta[_, _, _],
      sqlConf: SQLConf,
      snapshot: Snapshot,
      usePersistentDvs: ConfigEntry[Boolean],
      targetSchema: StructType): Boolean = {
    val enabled = DeletionVectorUtils.deletionVectorsWritable(snapshot) &&
      sqlConf.getConf(usePersistentDvs)
    if (enabled) {
      val rapidsConf = meta.conf
      if (!(sqlConf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX) &&
          rapidsConf.isDeltaDeletionVectorPredicatePushdownEnabled)) {
        meta.willNotWorkOnGpu(
          "Persistent deletion vector writes on GPU require DBR metadata row indexes and " +
            "native cuDF deletion-vector predicate pushdown")
      }
      if (!(rapidsConf.isParquetEnabled && rapidsConf.isParquetReadEnabled)) {
        meta.willNotWorkOnGpu(
          "Persistent deletion vector writes on GPU require the GPU Parquet reader, which is " +
            s"disabled by ${RapidsConf.ENABLE_PARQUET} or ${RapidsConf.ENABLE_PARQUET_READ}")
      }
      if (targetSchema.fieldNames.exists(
          sqlConf.resolver(_, DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD.name))) {
        meta.willNotWorkOnGpu(
          s"user column ${DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD.name} " +
            "conflicts with the DV row index")
      }
      // The GPU Delta reader keeps VARIANT scans on CPU when the table may contain shredded
      // files, and the CPU reader cannot produce the row index.
      if (targetSchema.exists { field =>
        TrampolineUtil.dataTypeExistsRecursively(field.dataType, GpuColumnVector.isVariantType)
      }) {
        meta.willNotWorkOnGpu(
          "Persistent deletion vector writes on GPU are not supported for VARIANT columns")
      }
    }
    enabled
  }
}
