/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading Parquet files with the GPU.
 */
class GpuReadParquetFileFormat extends ParquetFileFormat with GpuReadFileFormatWithMetrics {
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
    val factory = GpuParquetPartitionReaderFactory(
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

object GpuReadParquetFileFormat {

  private val hiddenMetadataFields = Set(
    "file_path",
    "file_name",
    "file_size",
    "file_modification_time")

  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val hiddenColumns = meta.wrapped.expressions.map(_.sql)
      .filter(hiddenMetadataFields.contains)
      .filterNot(meta.wrapped.relation.schema.fieldNames.contains)
    if (hiddenColumns.nonEmpty) {
      meta.willNotWorkOnGpu("parquet hidden metadata columns are not supported on GPU")
    }
    val fsse = meta.wrapped
    GpuParquetScanBase.tagSupport(
      ShimLoader.getSparkShims.sessionFromPlan(fsse),
      fsse.requiredSchema,
      meta
    )
  }
}
