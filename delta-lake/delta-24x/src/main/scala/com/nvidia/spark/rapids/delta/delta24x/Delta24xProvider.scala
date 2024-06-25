/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta24x

import com.nvidia.spark.rapids.{AtomicCreateTableAsSelectExecMeta, AtomicReplaceTableAsSelectExecMeta, GpuExec, GpuOverrides, GpuReadParquetFileFormat, RunnableCommandRule, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.DeltaIOProvider

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.DeltaParquetFileFormat.{IS_ROW_DELETED_COLUMN_NAME, ROW_INDEX_COLUMN_NAME}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, UpdateCommand}
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}
import org.apache.spark.sql.execution.datasources.v2.rapids.{GpuAtomicCreateTableAsSelectExec, GpuAtomicReplaceTableAsSelectExec}

object Delta24xProvider extends DeltaIOProvider {

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
        "Delete rows from a Delta Lake table",
        (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r))
        .disabledByDefault("Delta Lake delete support is experimental"),
      GpuOverrides.runnableCmd[MergeIntoCommand](
        "Merge of a source query/table into a Delta table",
        (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake merge support is experimental"),
      GpuOverrides.runnableCmd[UpdateCommand](
        "Update rows in a Delta Lake table",
        (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake update support is experimental")
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      val deltaFormat = format.asInstanceOf[DeltaParquetFileFormat]
      val requiredSchema = meta.wrapped.requiredSchema
      if (requiredSchema.exists(_.name == IS_ROW_DELETED_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $IS_ROW_DELETED_COLUMN_NAME is not supported")
      }
      if (requiredSchema.exists(_.name == ROW_INDEX_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $ROW_INDEX_COLUMN_NAME is not supported")
      }
      if (deltaFormat.hasDeletionVectorMap()) {
        meta.willNotWorkOnGpu("deletion vectors are not supported")
      }
      GpuReadParquetFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(format: FileFormat): FileFormat = {
    val cpuFormat = format.asInstanceOf[DeltaParquetFileFormat]
    GpuDelta24xParquetFileFormat(cpuFormat.metadata, cpuFormat.isSplittable,
      cpuFormat.disablePushDowns, cpuFormat.broadcastDvMap)
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    val cpuCatalog = cpuExec.catalog.asInstanceOf[DeltaCatalog]
    GpuAtomicCreateTableAsSelectExec(
      DeltaRuntimeShim.getGpuDeltaCatalog(cpuCatalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.plan,
      meta.childPlans.head.convertIfNeeded(),
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def convertToGpu(
      cpuExec: AtomicReplaceTableAsSelectExec,
      meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    val cpuCatalog = cpuExec.catalog.asInstanceOf[DeltaCatalog]
    GpuAtomicReplaceTableAsSelectExec(
      DeltaRuntimeShim.getGpuDeltaCatalog(cpuCatalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.plan,
      meta.childPlans.head.convertIfNeeded(),
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.orCreate,
      cpuExec.invalidateCache)
  }
}
