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

package com.nvidia.spark.rapids.delta.delta22x

import com.nvidia.spark.rapids.{GpuOverrides, GpuReadParquetFileFormat, RunnableCommandRule, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.{DeltaIOProvider, GpuDeltaParquetFileFormat}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.commands.{DeleteCommand, MergeIntoCommand, UpdateCommand}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.util.SerializableConfiguration

object Delta22xProvider extends DeltaIOProvider {

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

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = {
    format == classOf[DeltaParquetFileFormat] || format == classOf[GpuDelta22xParquetFileFormat]
  }

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      GpuReadParquetFileFormat.tagSupport(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(format: FileFormat): FileFormat = {
    val cpuFormat = format.asInstanceOf[DeltaParquetFileFormat]
    GpuDelta22xParquetFileFormat(cpuFormat.columnMappingMode, cpuFormat.referenceSchema)
  }

  override def createMultiFileReaderFactory(
      format: FileFormat,
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val gpuFormat = fileScan.relation.fileFormat.asInstanceOf[GpuDeltaParquetFileFormat]
    gpuFormat.createMultiFileReaderFactory(broadcastedConf, pushedFilters, fileScan)
  }
}
