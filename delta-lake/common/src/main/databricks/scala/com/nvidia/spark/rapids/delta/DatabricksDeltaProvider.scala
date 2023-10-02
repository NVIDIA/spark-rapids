/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.databricks.sql.managedcatalog.UnityCatalogV2Proxy
import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaParquetFileFormat}
import com.databricks.sql.transaction.tahoe.catalog.DeltaCatalog
import com.databricks.sql.transaction.tahoe.commands.{DeleteCommand, DeleteCommandEdge, MergeIntoCommand, MergeIntoCommandEdge, UpdateCommand, UpdateCommandEdge}
import com.databricks.sql.transaction.tahoe.rapids.GpuDeltaCatalog
import com.databricks.sql.transaction.tahoe.sources.{DeltaDataSource, DeltaSourceUtils}
import com.nvidia.spark.rapids._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.StagingTableCatalog
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.AtomicCreateTableAsSelectExec
import org.apache.spark.sql.execution.datasources.v2.rapids.GpuAtomicCreateTableAsSelectExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExternalSource
import org.apache.spark.sql.sources.CreatableRelationProvider

/**
 * Common implementation of the DeltaProvider interface for all Databricks versions.
 */
object DatabricksDeltaProvider extends DeltaProviderImplBase {
  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = {
    Seq(
      ExternalSource.toCreatableRelationProviderRule[DeltaDataSource](
        "Write to Delta Lake table",
        (a, conf, p, r) => {
          require(p.isDefined, "Must provide parent meta")
          new DeltaCreatableRelationProviderMeta(a, conf, p, r)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[CreatableRelationProvider]), r)).toMap
  }

  override def getRunnableCommandRules: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[DeleteCommand](
        "Delete rows from a Delta Lake table",
        (a, conf, p, r) => new DeleteCommandMeta(a, conf, p, r))
        .disabledByDefault("Delta Lake delete support is experimental"),
      GpuOverrides.runnableCmd[DeleteCommandEdge](
        "Delete rows from a Delta Lake table",
        (a, conf, p, r) => new DeleteCommandEdgeMeta(a, conf, p, r))
        .disabledByDefault("Delta Lake delete support is experimental"),
      GpuOverrides.runnableCmd[MergeIntoCommand](
        "Merge of a source query/table into a Delta table",
        (a, conf, p, r) => new MergeIntoCommandMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake merge support is experimental"),
      GpuOverrides.runnableCmd[MergeIntoCommandEdge](
        "Merge of a source query/table into a Delta table",
        (a, conf, p, r) => new MergeIntoCommandEdgeMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake merge support is experimental"),
      GpuOverrides.runnableCmd[UpdateCommand](
        "Update rows in a Delta Lake table",
        (a, conf, p, r) => new UpdateCommandMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake update support is experimental"),
      GpuOverrides.runnableCmd[UpdateCommandEdge](
        "Update rows in a Delta Lake table",
        (a, conf, p, r) => new UpdateCommandEdgeMeta(a, conf, p, r))
          .disabledByDefault("Delta Lake update support is experimental")
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }

  override def isSupportedFormat(format: Class[_ <: FileFormat]): Boolean = {
    format == classOf[DeltaParquetFileFormat]
  }

  override def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val format = meta.wrapped.relation.fileFormat
    if (format.getClass == classOf[DeltaParquetFileFormat]) {
      GpuReadParquetFileFormat.tagSupport(meta)
      GpuDeltaParquetFileFormat.tagSupportForGpuFileSourceScan(meta)
    } else {
      meta.willNotWorkOnGpu(s"format ${format.getClass} is not supported")
    }
  }

  override def getReadFileFormat(format: FileFormat): FileFormat = {
    val cpuFormat = format.asInstanceOf[DeltaParquetFileFormat]
    GpuDeltaParquetFileFormat.convertToGpu(cpuFormat)
  }

  override def isSupportedCatalog(catalogClass: Class[_ <: StagingTableCatalog]): Boolean = {
    catalogClass == classOf[DeltaCatalog] || catalogClass == classOf[UnityCatalogV2Proxy]
  }

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    require(isSupportedCatalog(cpuExec.catalog.getClass))
    if (!meta.conf.isDeltaWriteEnabled) {
      meta.willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val properties = cpuExec.properties
    val provider = properties.getOrElse("provider",
      cpuExec.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
    if (!DeltaSourceUtils.isDeltaDataSourceName(provider)) {
      meta.willNotWorkOnGpu(s"table provider '$provider' is not a Delta Lake provider")
    }
    RapidsDeltaUtils.tagForDeltaWrite(meta, cpuExec.query.schema, None,
      cpuExec.writeOptions.asCaseSensitiveMap().asScala.toMap, cpuExec.session)
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      cpuExec.output,
      new GpuDeltaCatalog(cpuExec.catalog, meta.conf),
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.plan,
      meta.childPlans.head.convertIfNeeded(),
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }
}

class DeltaCreatableRelationProviderMeta(
    source: DeltaDataSource,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends CreatableRelationProviderMeta[DeltaDataSource](source, conf, parent, rule) {
  require(parent.isDefined, "Must provide parent meta")
  private val saveCmd = parent.get.wrapped match {
    case s: SaveIntoDataSourceCommand => s
    case s =>
      throw new IllegalStateException(s"Expected SaveIntoDataSourceCommand, found ${s.getClass}")
  }

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    val path = saveCmd.options.get("path")
    if (path.isDefined) {
      val deltaLog = DeltaLog.forTable(SparkSession.active, path.get, saveCmd.options)
      RapidsDeltaUtils.tagForDeltaWrite(this, saveCmd.query.schema, Some(deltaLog),
        saveCmd.options, SparkSession.active)
    } else {
      willNotWorkOnGpu("no path specified for Delta Lake table")
    }
  }

  override def convertToGpu(): GpuCreatableRelationProvider = new GpuDeltaDataSource(conf)
}

/**
 * Implements the Delta Probe interface for probing the Delta Lake provider on Databricks.
 * @note This is instantiated via reflection from ShimLoader.
 */
class DeltaProbeImpl extends DeltaProbe {
  // Delta Lake is built-in for Databricks instances, so no probing is necessary.
  override def getDeltaProvider: DeltaProvider = DatabricksDeltaProvider
}
