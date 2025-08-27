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

import com.nvidia.spark.rapids.{ColumnarOutputWriterFactory, GpuParquetFileFormat, GpuWrite, SpillableColumnarBatch}
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitioner
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.{FieldUtils, MethodUtils}
import org.apache.iceberg.{DataFile, FileFormat, PartitionSpec, Schema, SerializableTable, SnapshotUpdate, Table}
import org.apache.iceberg.io.{ClusteredDataWriter, DataWriteResult, FanoutDataWriter, FileIO, OutputFileFactory, PartitioningWriter, RollingDataWriter}
import org.apache.iceberg.spark.source.SparkWrite.TaskCommit
import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, RequiresDistributionAndOrdering, Write, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


class GpuSparkWrite(cpu: SparkWrite) extends GpuWrite with RequiresDistributionAndOrdering  {
  private[source] val table: Table = FieldUtils.readField(cpu, "table", true).asInstanceOf[Table]
  private[source] val format: FileFormat = FieldUtils.readField(cpu, "format", true)
    .asInstanceOf[FileFormat]

  override def toBatch: BatchWrite = new GpuBatchAppend(this)

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
    val sparkContext: JavaSparkContext = FieldUtils.readField(cpu, "sparkContext", true)
      .asInstanceOf[JavaSparkContext]
    val tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table))
    val queryId = FieldUtils.readField(cpu, "queryId", true).asInstanceOf[String]
    val outputSpecId = FieldUtils.readField(cpu, "outputSpecId", true).asInstanceOf[Int]
    val targetFileSize = FieldUtils.readField(cpu, "targetFileSize", true).asInstanceOf[Long]
    val writeSchema = FieldUtils.readField(cpu, "writeSchema", true).asInstanceOf[Schema]
    val dsSchema = FieldUtils.readField(cpu, "dsSchema", true).asInstanceOf[StructType]
    val useFanout = FieldUtils.readField(cpu, "useFanoutWriter", true).asInstanceOf[Boolean]
    val writeProps = FieldUtils.readField(cpu, "writeProperties", true)
      .asInstanceOf[java.util.Map[String, String]]

    if (!format.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but got: $format")
    }

    val hadoopConf = sparkContext.hadoopConfiguration
    val job = {
      val tmpJob  = Job.getInstance(hadoopConf)
      tmpJob.setOutputKeyClass(classOf[Void])
      tmpJob.setOutputValueClass(classOf[InternalRow])
      FileOutputFormat.setOutputPath(tmpJob, new Path(table.location()))
      tmpJob
    }

    val outputWriterFactory = new GpuParquetFileFormat().prepareWrite(
      SparkSession.active,
      job,
      writeProps.asScala.toMap,
      dsSchema
    )

    new GpuWriterFactory(
      tableBroadcast,
      queryId,
      format,
      outputSpecId,
      targetFileSize,
      writeSchema,
      dsSchema,
      useFanout,
      writeProps.asScala.toMap,
      outputWriterFactory,
      new SerializableConfiguration(job.getConfiguration)
    )
  }

  private[source] def files(messages: Array[WriterCommitMessage]): Seq[DataFile] = {
    messages.filter(_ != null)
      .flatMap(_.asInstanceOf[TaskCommit].files)
      .toSeq
  }

  private[source] def commitOperation(operation: SnapshotUpdate[_], desc: String) = {
    MethodUtils.invokeMethod(cpu, true, "commitOperation", operation, desc)
  }
}

object GpuSparkWrite {
  def supports(cpuClass: Class[_ <: Write]): Boolean = {
    classOf[SparkWrite].isAssignableFrom(cpuClass)
  }

  def convert(cpuWrite: Write): GpuSparkWrite = {
    new GpuSparkWrite(cpuWrite.asInstanceOf[SparkWrite])
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
    val ignore: Map[String, String],
    val outputWriterFactory: ColumnarOutputWriterFactory,
    val hadoopConf: SerializableConfiguration,
) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
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
        .asInstanceOf[DataWriter[InternalRow]]
    } else {
      new GpuPartitionedDataWriter(writerFactory, outputFileFactory, io, spec, writeSchema,
        dsSchema, targetFileSize, useFanout)
        .asInstanceOf[DataWriter[InternalRow]]
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

