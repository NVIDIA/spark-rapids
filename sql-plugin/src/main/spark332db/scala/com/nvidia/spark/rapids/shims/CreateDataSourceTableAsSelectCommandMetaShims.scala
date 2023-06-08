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

/*** spark-rapids-shim-json-lines
{"spark": "332db"}
{"spark": "340"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.rapids.{GpuDataSourceBase, GpuOrcFileFormat}
import org.apache.spark.sql.rapids.shims.GpuCreateDataSourceTableAsSelectCommand

final class CreateDataSourceTableAsSelectCommandMeta(
    cmd: CreateDataSourceTableAsSelectCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RunnableCommandMeta[CreateDataSourceTableAsSelectCommand](cmd, conf, parent, rule) {

  private var origProvider: Class[_] = _

  override def tagSelfForGpu(): Unit = {
    if (cmd.table.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }
    if (cmd.table.provider.isEmpty) {
      willNotWorkOnGpu("provider must be defined")
    }

    val spark = SparkSession.active
    origProvider =
      GpuDataSourceBase.lookupDataSourceWithFallback(cmd.table.provider.get,
        spark.sessionState.conf)
    origProvider.getConstructor().newInstance() match {
      case f: FileFormat if GpuOrcFileFormat.isSparkOrcFormat(f) =>
        GpuOrcFileFormat.tagGpuSupport(this, spark, cmd.table.storage.properties, cmd.query.schema)
      case _: ParquetFileFormat =>
        GpuParquetFileFormat.tagGpuSupport(this, spark,
          cmd.table.storage.properties, cmd.query.schema)
      case ds =>
        willNotWorkOnGpu(s"Data source class not supported: ${ds}")
        None
    }
  }

  override def convertToGpu(): GpuCreateDataSourceTableAsSelectCommand = {
    GpuCreateDataSourceTableAsSelectCommand(
      cmd.table,
      cmd.mode,
      cmd.query,
      cmd.outputColumnNames,
      origProvider)
  }
}