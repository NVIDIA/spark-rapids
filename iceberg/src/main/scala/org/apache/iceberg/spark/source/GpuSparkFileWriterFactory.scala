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

package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.{ColumnarOutputWriterFactory, GpuParquetWriter, SpillableColumnarBatch}
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.parquet.GpuIcebergParquetAppender
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.iceberg.{FileFormat, MetricsConfig, PartitionSpec, SortOrder, StructLike, Table}
import org.apache.iceberg.deletes.{EqualityDeleteWriter, PositionDeleteWriter}
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.io.{DataWriter, FileWriterFactory}

import org.apache.spark.sql.execution.datasources.GpuWriteFiles
import org.apache.spark.sql.rapids.{GpuWriteJobStatsTracker, GpuWriteTaskStatsTracker}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class GpuSparkFileWriterFactory(val table: Table,
  val dataFileFormat: FileFormat,
  val dataSparkType: StructType,
  val dataSortOrder: SortOrder,
  val deleteFileFormat: FileFormat,
  val columnarOutputWriterFactory: ColumnarOutputWriterFactory,
  val hadoopConf: SerializableConfiguration,
) extends FileWriterFactory[SpillableColumnarBatch] {
  require(dataFileFormat == FileFormat.PARQUET,
    s"GpuSparkFileWriterFactory only supports PARQUET file format, but got $dataFileFormat")
  require(deleteFileFormat == FileFormat.PARQUET,
    s"GpuSparkFileWriterFactory only supports PARQUET file format, but got $deleteFileFormat")

  private lazy val taskAttemptContext: TaskAttemptContext = GpuWriteFiles
    .calcHadoopTaskAttemptContext(hadoopConf.value)

  override def newDataWriter(file: EncryptedOutputFile,
    partitionSpec: PartitionSpec,
    partition: StructLike): DataWriter[SpillableColumnarBatch] = {
    val location = file.encryptingOutputFile().location()
    new DataWriter[SpillableColumnarBatch](
      createAppender(location),
      dataFileFormat,
      location,
      partitionSpec,
      partition,
      file.keyMetadata(),
      dataSortOrder)
  }

  override def newEqualityDeleteWriter(encryptedOutputFile: EncryptedOutputFile,
    partitionSpec: PartitionSpec, structLike: StructLike):
  EqualityDeleteWriter[SpillableColumnarBatch] = throw new IllegalStateException(
    "Spark row level deletion should not produce equality delete files")

  override def newPositionDeleteWriter(encryptedOutputFile: EncryptedOutputFile,
    partitionSpec: PartitionSpec,
    structLike: StructLike): PositionDeleteWriter[SpillableColumnarBatch] =
    throw new UnsupportedOperationException("Iceberg delete command is not supported by gpu yet")

  private def createAppender(path: String): GpuIcebergParquetAppender = {
    val gpuWriter =  columnarOutputWriterFactory.newInstance(
      path = path,
      dataSchema = dataSparkType,
      context = taskAttemptContext,
      statsTrackers = Seq(new GpuWriteTaskStatsTracker(hadoopConf.value,
        GpuWriteJobStatsTracker.taskMetrics)),
      debugOutputPath = None
    ).asInstanceOf[GpuParquetWriter]

    new GpuIcebergParquetAppender(
      gpuWriter,
      metricsConfig = MetricsConfig.forTable(table),
      fileIO = new IcebergFileIO(table.io())
    )
  }
}