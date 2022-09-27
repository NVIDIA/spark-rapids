/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.Locale

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuOrcFileFormat}

final class OptimizedCreateHiveTableAsSelectCommandMeta(
    cmd: OptimizedCreateHiveTableAsSelectCommand,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends DataWritingCommandMeta[OptimizedCreateHiveTableAsSelectCommand](
      cmd, conf, parent, rule) {

  override def tagSelfForGpu(): Unit = {
    // It would be cleaner if we could simply call `cmd.getWritingCommand` and let
    // InsertIntoHadoopFsRelationCommandMeta tag the result, but calling getWritingCommand
    // before the table exists will crash. So this ends up replicating a portion of the logic
    // from OptimizedCreateHiveTableAsSelectCommand.getWritingCommand and underlying
    // utility methods to be able to tag whether we can support the optimized Hive write.
    val spark = SparkSession.active
    val tableDesc = cmd.tableDesc

    if (tableDesc.partitionColumnNames.nonEmpty) {
      willNotWorkOnGpu("partitioned writes are not supported")
    }

    if (tableDesc.bucketSpec.isDefined) {
      willNotWorkOnGpu("bucketing is not supported")
    }

    val serde = tableDesc.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde.contains("parquet")) {
      val mergeSchemaConfKey = "spark.sql.hive.convertMetastoreParquet.mergeSchema"
      val shouldMergeSchema = SQLConf.get.getConfString(mergeSchemaConfKey, "false").toBoolean
      if (shouldMergeSchema) {
        willNotWorkOnGpu("Merging Parquet schemas across part files is not supported, " +
            s"see $mergeSchemaConfKey")
      }
      val options = tableDesc.properties.filterKeys(isParquetProperty) ++
          tableDesc.storage.properties
      GpuParquetFileFormat.tagGpuSupport(this, spark, options, cmd.query.schema)
    } else if (serde.contains("orc")) {
      val options = tableDesc.properties.filterKeys(isOrcProperty) ++
          tableDesc.storage.properties
      GpuOrcFileFormat.tagGpuSupport(this, spark, options, cmd.query.schema)
    } else {
      willNotWorkOnGpu(s"unsupported serde detected: $serde")
    }
  }

  // Return true for Apache ORC and Hive ORC-related configuration names.
  // Note that Spark doesn't support configurations like `hive.merge.orcfile.stripe.level`.
  private def isOrcProperty(key: String) =
    key.startsWith("orc.") || key.contains(".orc.")

  private def isParquetProperty(key: String) =
    key.startsWith("parquet.") || key.contains(".parquet.")
}
