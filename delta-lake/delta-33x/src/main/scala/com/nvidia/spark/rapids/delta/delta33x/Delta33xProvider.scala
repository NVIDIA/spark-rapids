/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta33x

import com.nvidia.spark.rapids.{AtomicCreateTableAsSelectExecMeta, AtomicReplaceTableAsSelectExecMeta, GpuExec, RunnableCommandRule}
import com.nvidia.spark.rapids.{GpuReadParquetFileFormat, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.DeltaIOProvider

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.DeltaParquetFileFormat.{IS_ROW_DELETED_COLUMN_NAME, ROW_INDEX_COLUMN_NAME}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.v2.{AtomicCreateTableAsSelectExec, AtomicReplaceTableAsSelectExec}

object Delta33xProvider extends DeltaIOProvider {

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Map.empty[Class[_ <: RunnableCommand], RunnableCommandRule[_ <: RunnableCommand]]
  }

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      val requiredSchema = meta.wrapped.requiredSchema
      if (requiredSchema.exists(_.name == IS_ROW_DELETED_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $IS_ROW_DELETED_COLUMN_NAME is not supported")
      }
      if (requiredSchema.exists(_.name == ROW_INDEX_COLUMN_NAME)) {
        meta.willNotWorkOnGpu(
          s"reading metadata column $ROW_INDEX_COLUMN_NAME is not supported")
      }
      GpuReadParquetFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(relation: HadoopFsRelation): FileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    GpuDelta33xParquetFileFormat(fmt.protocol, fmt.metadata, fmt.nullableRowTrackingFields,
      fmt.optimizationsEnabled, fmt.tablePath, fmt.isCDCRead)
  }

  override def convertToGpu(
    cpuExec: AtomicCreateTableAsSelectExec,
    meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def convertToGpu(
    cpuExec: AtomicReplaceTableAsSelectExec,
    meta: AtomicReplaceTableAsSelectExecMeta): GpuExec = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def tagForGpu(cpuExec: AtomicCreateTableAsSelectExec,
    meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    meta.willNotWorkOnGpu("Delta write is not supported at the moment")
  }

  override def tagForGpu(cpuExec: AtomicReplaceTableAsSelectExec,
    meta: AtomicReplaceTableAsSelectExecMeta): Unit = {
    meta.willNotWorkOnGpu("Delta write is not supported at the moment")
  }

}
