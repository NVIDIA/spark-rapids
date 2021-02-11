/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading ORC files with the GPU.
 */
class GpuReadOrcFileFormat extends OrcFileFormat with GpuReadFileFormatWithMetrics {
  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric]): PartitionedFile => Iterator[InternalRow] = {
    val sqlConf = sparkSession.sessionState.conf
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val factory = GpuOrcPartitionReaderFactory(
      sqlConf,
      broadcastedHadoopConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      filters.toArray,
      new RapidsConf(sqlConf),
      metrics)
    PartitionReaderIterator.buildReader(factory)
  }
}

object GpuReadOrcFileFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    if (fsse.relation.options.getOrElse("mergeSchema", "false").toBoolean) {
      meta.willNotWorkOnGpu("mergeSchema and schema evolution is not supported yet")
    }
    GpuOrcScanBase.tagSupport(
      fsse.sqlContext.sparkSession,
      fsse.requiredSchema,
      meta
    )
  }
}
