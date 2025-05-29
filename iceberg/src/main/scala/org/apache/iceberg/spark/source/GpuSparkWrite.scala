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

import com.nvidia.spark.rapids.{ColumnarOutputWriterFactory, GpuWrite, SpillableColumnarBatch}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitioner
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.{FieldUtils, MethodUtils}
import org.apache.iceberg.{FileFormat, PartitionSpec, Schema, Table}
import org.apache.iceberg.io.{ClusteredDataWriter, DataWriteResult, FanoutDataWriter, FileIO, OutputFileFactory, PartitioningWriter, RollingDataWriter}
import org.apache.iceberg.spark.source.SparkWrite.TaskCommit

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, RequiresDistributionAndOrdering, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

class GpuSparkWrite(cpu: SparkWrite) extends GpuWrite with RequiresDistributionAndOrdering  {
  private val table: Table = FieldUtils.readField(cpu, "table", true).asInstanceOf[Table]
  private val format: FileFormat = FieldUtils.readField(cpu, "format", true)
    .asInstanceOf[FileFormat]

  override def toBatch: BatchWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support batch write")

  override def toStreaming: StreamingWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support streaming write")

  override def toString: String = s"GpuIcebergWrite(table=$table, format=$format)"

  private[source] def abort(messages: Array[WriterCommitMessage]): Unit = {
    MethodUtils.invokeMethod(cpu, true, "abort", messages)
  }

  override def distributionStrictlyRequired(): Boolean = cpu.distributionStrictlyRequired()

  override def requiredNumPartitions(): Int = cpu.requiredNumPartitions()

  override def advisoryPartitionSizeInBytes(): Long = cpu.advisoryPartitionSizeInBytes()

  override def requiredDistribution(): Distribution = cpu.requiredDistribution()

  override def requiredOrdering(): Array[SortOrder] = cpu.requiredOrdering()

  private[source] def createDataWriterFactory: DataWriterFactory = {

  }
}

class GpuWriterFactory(val tableBroadcast: Broadcast[Table],
    val queryId: String,
    val format: FileFormat,
    val outputSpecId: Int,
    val targetFileSize: Long,
    val writeSchema: Schema,
    val dsSchema: StructType,
    val useFanout: Boolean,
    val props: Map[String, String],
    val outputWriterFactory: ColumnarOutputWriterFactory,
    val hadoopConf: SerializableConfiguration,
) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[ColumnarBatch] = {
    val table = tableBroadcast.value
    val spec = table.specs().get(outputSpecId)
    val io = table.io()
    val operationId = s"$queryId-0"
    val outputFileFactory = OutputFileFactory
      .builderFor(table, partitionId, taskId)
      .format(format)
      .operationId(operationId)
      .build()

    val writerFactory = new GpuSparkFileWriterFactory(
      table,
      format,
      dsSchema,
      null,
      format,
      outputWriterFactory,
      hadoopConf
    )

    if (spec.isUnpartitioned) {
      new GpuUnpartitionedDataWriter(writerFactory, outputFileFactory, io, spec, targetFileSize)
    } else {
      new GpuPartitionedDataWriter(writerFactory, outputFileFactory, io, spec, writeSchema,
        dsSchema, targetFileSize, useFanout)
    }
  }
}

class GpuUnpartitionedDataWriter(
    val fileWriterFactory: GpuSparkFileWriterFactory,
    val fileFactory: OutputFileFactory,
    val io: FileIO,
    val spec: PartitionSpec,
    val targetFileSize: Long)
  extends DataWriter[ColumnarBatch] {
  private val delegate = new RollingDataWriter[SpillableColumnarBatch](
    fileWriterFactory,
    fileFactory,
    io,
    targetFileSize,
    spec,
    null)


  override def write(t: ColumnarBatch): Unit = delegate.write(SpillableColumnarBatch(t,
    ACTIVE_ON_DECK_PRIORITY))

  override def commit(): WriterCommitMessage = {
    close()

    val result = delegate.result()
    val taskCommit = new TaskCommit(result.dataFiles().toArray(new Array(0)))
    taskCommit.reportOutputMetrics()
    taskCommit
  }

  override def abort(): Unit = {
    close()

    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles())
  }

  override def close(): Unit = {
    delegate.close()
  }
}

class GpuPartitionedDataWriter(
    val writerFactory: GpuSparkFileWriterFactory,
    val fileFactory: OutputFileFactory,
    val io: FileIO,
    val spec: PartitionSpec,
    val dataSchema: Schema,
    val dataSparkType: StructType,
    val targetFileSize: Long,
    val fanoutEnabled: Boolean,
) extends DataWriter[ColumnarBatch] {

  private val delegate: PartitioningWriter[SpillableColumnarBatch, DataWriteResult] =
    if (fanoutEnabled) {
    new FanoutDataWriter[SpillableColumnarBatch](writerFactory, fileFactory, io, targetFileSize)
  } else {
    new ClusteredDataWriter[SpillableColumnarBatch](writerFactory, fileFactory, io, targetFileSize)
  }

  private val partitioner = new GpuIcebergPartitioner(spec, dataSchema, dataSparkType)

  override def write(record: ColumnarBatch): Unit = {
    partitioner.partition(record)
      .foreach { batch =>
        delegate.write(SpillableColumnarBatch(batch.batch, ACTIVE_ON_DECK_PRIORITY),
          spec, batch.partition)
      }
  }

  override def commit(): WriterCommitMessage = {
    close()

    val result = delegate.result()
    val taskCommit = new TaskCommit(result.dataFiles().toArray(new Array(0)))
    taskCommit.reportOutputMetrics()
    taskCommit
  }

  override def abort(): Unit = {
    close()

    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.dataFiles())
  }

  override def close(): Unit = {
    delegate.close()
  }
}

