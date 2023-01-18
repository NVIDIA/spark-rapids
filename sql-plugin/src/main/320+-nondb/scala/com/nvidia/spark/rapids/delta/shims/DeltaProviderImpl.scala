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

package com.nvidia.spark.rapids.delta.shims

import com.nvidia.spark.rapids.{CreatableRelationProviderMeta, CreatableRelationProviderRule, DataFromReplacementRule, DeltaFormatType, FileFormatChecks, GpuCreatableRelationProvider, GpuParquetFileFormat, RapidsConf, RapidsMeta, WriteFileOp}
import com.nvidia.spark.rapids.delta.DeltaProviderImplBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.ExternalSource
import org.apache.spark.sql.sources.CreatableRelationProvider

/**
 * Implements the DeltaProvider interface when Delta Lake is available at runtime.
 * @note This is instantiated via reflection from ShimLoader.
 */
class DeltaProviderImpl extends DeltaProviderImplBase {
  override def getCreatableRelationRules: Map[Class[_ <: CreatableRelationProvider],
      CreatableRelationProviderRule[_ <: CreatableRelationProvider]] = {
    Seq(
      ExternalSource.toCreatableRelationProviderRule[DeltaDataSource](
        "Write to Delta Lake table",
        (a, conf, p, r) => {
          require(p.isDefined, "Must provide parent meta")
          new DeltaCreatableRelationProviderMeta(a, conf, p, r)
        }
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[CreatableRelationProvider]), r)).toMap
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

  private def checkIncompatibleConfs(deltaLog: DeltaLog, sqlConf: SQLConf): Unit = {
    def getSQLConf(key: String): Option[String] = {
      try {
        Option(sqlConf.getConfString(key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    val optimizeWriteEnabled = {
      val deltaOptions = new DeltaOptions(saveCmd.options, sqlConf)
      deltaOptions.optimizeWrite.orElse {
        getSQLConf("spark.databricks.delta.optimizeWrite.enabled").map(_.toBoolean).orElse {
          val metadata = deltaLog.snapshot.metadata
          DeltaConfigs.AUTO_OPTIMIZE.fromMetaData(metadata).orElse {
            metadata.configuration.get("delta.autoOptimize.optimizeWrite").orElse {
              getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite")
            }.map(_.toBoolean)
          }
        }
      }.getOrElse(false)
    }
    if (optimizeWriteEnabled) {
      willNotWorkOnGpu("optimized write of Delta Lake tables is not supported")
    }

    val autoCompactEnabled =
      getSQLConf("spark.databricks.delta.autoCompact.enabled").orElse {
        val metadata = deltaLog.snapshot.metadata
        metadata.configuration.get("delta.autoOptimize.autoCompact").orElse {
          getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact")
        }
      }.exists(_.toBoolean)
    if (autoCompactEnabled) {
      willNotWorkOnGpu("automatic compaction of Delta Lake tables is not supported")
    }
  }

  override def tagSelfForGpu(): Unit = {
    if (!conf.isDeltaWriteEnabled) {
      willNotWorkOnGpu("Delta Lake output acceleration has been disabled. To enable set " +
          s"${RapidsConf.ENABLE_DELTA_WRITE} to true")
    }
    FileFormatChecks.tag(this, saveCmd.query.schema, DeltaFormatType, WriteFileOp)
    val path = saveCmd.options.get("path")
    if (path.isDefined) {
      val deltaLog = DeltaLog.forTable(SparkSession.active, path.get, saveCmd.options)
      deltaLog.fileFormat() match {
        case _: DeltaParquetFileFormat =>
          GpuParquetFileFormat.tagGpuSupport(this, SparkSession.active,
            saveCmd.options, saveCmd.query.schema)
        case f =>
          willNotWorkOnGpu(s"file format $f is not supported")
      }
      checkIncompatibleConfs(deltaLog, SQLConf.get)
    } else {
      willNotWorkOnGpu("no path specified for Delta Lake table")
    }
  }

  override def convertToGpu(): GpuCreatableRelationProvider = new GpuDeltaDataSource(conf)
}
