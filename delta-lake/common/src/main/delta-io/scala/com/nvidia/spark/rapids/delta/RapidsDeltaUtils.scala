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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.{DeltaFormatType, FileFormatChecks, GpuParquetFileFormat, RapidsMeta, WriteFileOp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOptions, DeltaParquetFileFormat}
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object RapidsDeltaUtils {
  def tagForDeltaWrite(
      meta: RapidsMeta[_, _, _],
      schema: StructType,
      deltaLog: DeltaLog,
      options: Map[String, String],
      spark: SparkSession): Unit = {
    FileFormatChecks.tag(meta, schema, DeltaFormatType, WriteFileOp)
    deltaLog.fileFormat() match {
      case _: DeltaParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(meta, spark, options, schema)
      case f =>
        meta.willNotWorkOnGpu(s"file format $f is not supported")
    }
    checkIncompatibleConfs(meta, deltaLog, spark.sessionState.conf, options)
  }

  private def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: DeltaLog,
      sqlConf: SQLConf,
      options: Map[String, String]): Unit = {
    def getSQLConf(key: String): Option[String] = {
      try {
        Option(sqlConf.getConfString(key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    val optimizeWriteEnabled = {
      val deltaOptions = new DeltaOptions(options, sqlConf)
      deltaOptions.optimizeWrite.orElse {
        getSQLConf("spark.databricks.delta.optimizeWrite.enabled").map(_.toBoolean).orElse {
          val metadata = DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(deltaLog).metadata
          DeltaConfigs.AUTO_OPTIMIZE.fromMetaData(metadata).orElse {
            metadata.configuration.get("delta.autoOptimize.optimizeWrite").orElse {
              getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite")
            }.map(_.toBoolean)
          }
        }
      }.getOrElse(false)
    }
    if (optimizeWriteEnabled) {
      meta.willNotWorkOnGpu("optimized write of Delta Lake tables is not supported")
    }

    val autoCompactEnabled =
      getSQLConf("spark.databricks.delta.autoCompact.enabled").orElse {
        val metadata = DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(deltaLog).metadata
        metadata.configuration.get("delta.autoOptimize.autoCompact").orElse {
          getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact")
        }
      }.exists(_.toBoolean)
    if (autoCompactEnabled) {
      meta.willNotWorkOnGpu("automatic compaction of Delta Lake tables is not supported")
    }
  }
}
