/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.sequencefile.GpuSequenceFilePartitionReaderFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileFormat, PartitionedFile}
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A FileFormat that allows reading Hadoop SequenceFiles and returning raw key/value bytes as
 * Spark SQL BinaryType columns.
 *
 * This is a GPU-accelerated scan format that uses CUDA kernels to parse SequenceFile records
 * directly on the GPU, providing significant performance improvements over CPU-based parsing.
 *
 * Note: Only uncompressed SequenceFiles are supported. Compressed SequenceFiles will throw
 * an UnsupportedOperationException.
 */
class GpuReadSequenceFileBinaryFormat extends FileFormat with GpuReadFileFormatWithMetrics {

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = Some(SequenceFileBinaryFileFormat.dataSchema)

  // GPU SequenceFile reader processes entire files at once
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = false

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
    val rapidsConf = new RapidsConf(sqlConf)

    val factory = GpuSequenceFilePartitionReaderFactory(
      sqlConf,
      broadcastedHadoopConf,
      requiredSchema,
      partitionSchema,
      rapidsConf,
      metrics,
      options)
    PartitionReaderIterator.buildReader(factory)
  }

  // GPU SequenceFile reader processes one file at a time
  override def isPerFileReadEnabled(conf: RapidsConf): Boolean = true

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    GpuSequenceFilePartitionReaderFactory(
      fileScan.conf,
      broadcastedConf,
      fileScan.requiredSchema,
      fileScan.readPartitionSchema,
      fileScan.rapidsConf,
      fileScan.allMetrics,
      Map.empty)
  }
}

object GpuReadSequenceFileBinaryFormat {
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    val required = fsse.requiredSchema
    // Only support reading BinaryType columns named "key" and/or "value".
    required.fields.foreach { f =>
      val isKey = f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)
      val isValue = f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)
      if ((isKey || isValue) && f.dataType != org.apache.spark.sql.types.BinaryType) {
        meta.willNotWorkOnGpu(
          s"SequenceFileBinary only supports BinaryType for " +
            s"'${SequenceFileBinaryFileFormat.KEY_FIELD}' and " +
            s"'${SequenceFileBinaryFileFormat.VALUE_FIELD}' columns, but saw " +
            s"${f.name}: ${f.dataType.catalogString}")
      }
    }
  }
}
