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

import com.databricks.sql.transaction.tahoe.{DeltaParquetFileFormat, DeltaTableUtils, RowTracking}
import com.databricks.sql.transaction.tahoe.commands.DeletionVectorUtils
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.delta.{UpdateCommandEdgeMeta, UpdateCommandMeta}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}

object UpdateCommandMetaShim {
  def tagForGpu(meta: UpdateCommandMeta): Unit = {
    val deltaLog = meta.updateCmd.tahoeFileIndex.deltaLog
    val snapshot = deltaLog.unsafeVolatileSnapshot
    val dvFeatureEnabled = DeletionVectorUtils.deletionVectorsWritable(snapshot)
    val persistentDvEnabled = dvFeatureEnabled && meta.updateCmd.conf.getConf(
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS)

    if (persistentDvEnabled && !supportsPersistentDeletionVectorWrites(meta)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector writes on GPU require DBR metadata row indexes and " +
          "native cuDF deletion-vector predicate pushdown")
    }
    if (persistentDvEnabled && RowTracking.isEnabled(snapshot.protocol, snapshot.metadata) &&
        hasDataPredicate(meta.updateCmd.condition, snapshot.metadata.partitionColumns,
          meta.updateCmd.tahoeFileIndex.spark)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector UPDATE with row tracking is not supported on GPU")
    }
    if (persistentDvEnabled &&
        hasUserRowIndexColumn(
          meta.updateCmd.target.schema.fieldNames, meta.updateCmd.conf.resolver)) {
      meta.willNotWorkOnGpu(
        s"user column ${DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD.name} " +
          "conflicts with the DV row index")
    }
  }

  def tagForGpu(meta: UpdateCommandEdgeMeta): Unit = {
    val deltaLog = meta.updateCmd.tahoeFileIndex.deltaLog
    val snapshot = deltaLog.unsafeVolatileSnapshot
    val dvFeatureEnabled = DeletionVectorUtils.deletionVectorsWritable(snapshot)
    val persistentDvEnabled = dvFeatureEnabled && meta.updateCmd.conf.getConf(
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS)

    if (persistentDvEnabled && !supportsPersistentDeletionVectorWrites(meta)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector writes on GPU require DBR metadata row indexes and " +
          "native cuDF deletion-vector predicate pushdown")
    }
    if (persistentDvEnabled && RowTracking.isEnabled(snapshot.protocol, snapshot.metadata) &&
        hasDataPredicate(meta.updateCmd.condition, snapshot.metadata.partitionColumns,
          meta.updateCmd.tahoeFileIndex.spark)) {
      meta.willNotWorkOnGpu(
        "Persistent deletion vector UPDATE with row tracking is not supported on GPU")
    }
    if (persistentDvEnabled &&
        hasUserRowIndexColumn(
          meta.updateCmd.target.schema.fieldNames, meta.updateCmd.conf.resolver)) {
      meta.willNotWorkOnGpu(
        s"user column ${DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD.name} " +
          "conflicts with the DV row index")
    }
  }

  private def supportsPersistentDeletionVectorWrites(meta: UpdateCommandMeta): Boolean =
    meta.updateCmd.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX) &&
      meta.conf.isDeltaDeletionVectorPredicatePushdownEnabled

  private def supportsPersistentDeletionVectorWrites(meta: UpdateCommandEdgeMeta): Boolean =
    meta.updateCmd.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX) &&
      meta.conf.isDeltaDeletionVectorPredicatePushdownEnabled

  private def hasUserRowIndexColumn(
      fieldNames: Array[String],
      resolver: (String, String) => Boolean): Boolean = {
    fieldNames.exists(resolver(_, DeltaParquetFileFormat.ROW_INDEX_STRUCT_FIELD.name))
  }

  private def hasDataPredicate(
      condition: Option[Expression],
      partitionColumns: Seq[String],
      spark: SparkSession): Boolean = {
    val (_, dataPredicates) = DeltaTableUtils.splitMetadataAndDataPredicates(
      condition.getOrElse(Literal.TrueLiteral), partitionColumns, spark)
    dataPredicates.nonEmpty
  }
}
