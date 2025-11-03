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

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf.{Scalar, Table => CudfTable}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.rapids.{ColumnarOutputWriterFactory, GpuColumnVector, GpuDeltaWrite, GpuParquetFileFormat, RapidsHostColumnVector, SparkPlanMeta, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingArray
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.{ColumnarBatchWithPartition, GpuIcebergPartitioner}
import com.nvidia.spark.rapids.iceberg.utils.GpuStructProjection
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.{FieldUtils, MethodUtils}
import org.apache.iceberg.{FileFormat, MetadataColumns, PartitionData, Partitioning, PartitionSpec, Schema, SerializableTable, Table}
import org.apache.iceberg.deletes.DeleteGranularity
import org.apache.iceberg.io.{DeleteSchemaUtil, DeleteWriteResult, FileIO, GpuClusteredPositionDeleteWriter, GpuFanoutPositionDeleteWriter, OutputFileFactory, PartitioningWriter}
import org.apache.iceberg.spark.GpuTypeToSparkType
import org.apache.iceberg.spark.GpuTypeToSparkType.toSparkType
import org.apache.iceberg.spark.source.GpuSparkWrite.supports
import org.apache.iceberg.spark.source.GpuWriteContext.{emptyPartitionData, positionDeleteDataTypes, positionDeleteSparkType}
import org.apache.iceberg.types.{Types => IcebergTypes}

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write.{DeltaBatchWrite, DeltaWrite, DeltaWriter, DeltaWriterFactory, RequiresDistributionAndOrdering, WriterCommitMessage}
import org.apache.spark.sql.connector.write.RowLevelOperation.Command
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.rapids.GpuWriteJobStatsTracker
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.SerializableConfiguration



/**
 * GPU version of Iceberg's SparkPositionDeltaWrite.
 * This class handles merge-on-read DELETE operations that write position delete files.
 */
class GpuSparkPositionDeltaWrite(cpu: SparkPositionDeltaWrite)
  extends GpuDeltaWrite with RequiresDistributionAndOrdering {
  private[source] val table = FieldUtils.readField(cpu, "table", true)
    .asInstanceOf[Table]

  override def toBatch: DeltaBatchWrite = {
    // Call the CPU version's toBatch to get PositionDeltaBatchWrite
    val cpuBatch = cpu.toBatch
    // Wrap it in GPU version
    new GpuPositionDeltaBatchWrite(this, cpuBatch)
  }

  override def toStreaming: StreamingWrite = throw new UnsupportedOperationException(
    "GpuSparkWrite does not support streaming write")

  override def toString: String = s"GpuSparkPositionDeltaWrite(table=$table)"
  
  private[source] def abort(messages: Array[WriterCommitMessage]): Unit = {
    MethodUtils.invokeMethod(cpu, true, "abort", messages.asInstanceOf[Array[Object]])
  }

  override def requiredDistribution(): Distribution = cpu.requiredDistribution()

  override def requiredOrdering(): Array[SortOrder] = cpu.requiredOrdering()

  override def advisoryPartitionSizeInBytes(): Long = cpu.advisoryPartitionSizeInBytes()

  private[source] def createDeltaWriterFactory: DeltaWriterFactory = {
    val sparkContext: JavaSparkContext = FieldUtils.readField(cpu, "sparkContext", true)
      .asInstanceOf[JavaSparkContext]
    val tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table))
    val command = FieldUtils.readField(cpu, "command", true).asInstanceOf[Command]
    val context = GpuWriteContext(FieldUtils.readField(cpu, "context", true))
    val writeProps = FieldUtils.readField(cpu, "writeProperties", true)
      .asInstanceOf[java.util.Map[String, String]]
      .asScala
      .toMap

    if (!context.dataFileFormat.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but data format got: ${context.dataFileFormat}")
    }

    if (!context.deleteFileFormat.equals(FileFormat.PARQUET)) {
      throw new UnsupportedOperationException(
        s"GpuSparkWrite only supports Parquet, but delete format got: ${context.deleteFileFormat}")
    }

    val hadoopConf = sparkContext.hadoopConfiguration
    val job = {
      val tmpJob  = Job.getInstance(hadoopConf)
      tmpJob.setOutputKeyClass(classOf[Void])
      tmpJob.setOutputValueClass(classOf[InternalRow])
      tmpJob
    }

    val outputWriterFactory = new GpuParquetFileFormat().prepareWrite(
      SparkSession.active,
      job,
      Map.empty,
//      writeProps,
//      context.dataSparkType
      positionDeleteSparkType
    )

    val serializedHadoopConf = new SerializableConfiguration(job.getConfiguration)
    val statsTracker = new GpuWriteJobStatsTracker(serializedHadoopConf,
      GpuWriteJobStatsTracker.basicMetrics,
      GpuWriteJobStatsTracker.taskMetrics)

    new GpuPositionDeltaWriterFactory(
      tableBroadcast,
      command,
      context,
      writeProps,
      outputWriterFactory,
      statsTracker,
      serializedHadoopConf)
  }
}



object GpuSparkPositionDeltaWrite {
  def tagForGpu(deltaWrite: DeltaWrite, meta: SparkPlanMeta[_]): Unit = {
    if (!supports(deltaWrite.getClass)) {
      meta.willNotWorkOnGpu(s"GpuSparkWrite only supports ${classOf[SparkWrite].getName}, " +
        s"but got: ${deltaWrite.getClass.getName}")
      return
    }
    val context = GpuWriteContext(FieldUtils.readField(deltaWrite, "context", true))

    val table: Table = FieldUtils.readField(deltaWrite, "table", true).asInstanceOf[Table]
    val partitionSpec = table.spec()

    // Iceberg's delta write is similar to normal write, but will write position deletes
    // additionally. Position deletes have only two data types: string + int. So it's
    // safe to use normal write tag method
    GpuSparkWrite.tagForGpuWrite(
      Option(context.dataFileFormat),
      Option(context.dataSparkType),
      Option(context.dataSchema),
      Option(context.deleteFileFormat),
      partitionSpec,
      meta)
  }

  def convert(deltaWrite: DeltaWrite): GpuSparkPositionDeltaWrite = {
    new GpuSparkPositionDeltaWrite(deltaWrite.asInstanceOf[SparkPositionDeltaWrite])
  }
}

class GpuPositionDeltaWriterFactory(
  val tableSer: Broadcast[Table],
  val command: Command,
  val context: GpuWriteContext,
  val writeProps: Map[String, String],
  val outputWriterFactory: ColumnarOutputWriterFactory,
  val statsTracker: GpuWriteJobStatsTracker,
  val hadoopConf: SerializableConfiguration) extends DeltaWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DeltaWriter[InternalRow] = {
    val table = tableSer.value

    val deleteFileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
      .format(context.deleteFileFormat)
      .operationId(context.queryId)
      .suffix("deletes")
      .build()

    val writerFactory = new GpuSparkFileWriterFactory(
      table,
      context.dataFileFormat,
      context.dataSparkType,
      table.sortOrder(),
      context.deleteFileFormat,
      positionDeleteSparkType,
      outputWriterFactory,
      statsTracker.newTaskInstance(),
      hadoopConf,
      new IcebergFileIO(table.io()))

    if (command == Command.DELETE) {
      new GpuDeleteOnlyDeltaWriter(table, writerFactory, deleteFileFactory, context)
        .asInstanceOf[DeltaWriter[InternalRow]]
    } else {
      throw new UnsupportedOperationException("Merge on read command not supported yet!")
    }
  }
}


trait GpuDeltaWriter extends DeltaWriter[ColumnarBatch] {
  protected def buildPartitionProjections(
    partitionType: IcebergTypes.StructType,
    specs: collection.Map[Integer, PartitionSpec]): Map[Int, GpuStructProjection] = {
    specs.map(e => (e._1.toInt, GpuStructProjection(partitionType, e._2.partitionType())))
      .toMap
  }

  protected def newDeleteWriter(table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    outputFileFactory: OutputFileFactory,
    context: GpuWriteContext): PartitioningWriter[SpillableColumnarBatch, DeleteWriteResult] = {

    val io = table.io()
    val inputOrdered = context.inputOrdered
    val targetFileSize = context.targetDeleteFileSize

    if (inputOrdered) {
      new GpuClusteredPositionDeleteWriter(writerFactory, outputFileFactory, io, targetFileSize)
    } else {
      new GpuFanoutPositionDeleteWriter(writerFactory, outputFileFactory, io, targetFileSize)
    }
  }

  protected def uniqueSpecIds(batch: ColumnarBatch, specIdOrdinal: Int): RapidsHostColumnVector = {
    val specIdCol = batch.column(specIdOrdinal)
      .asInstanceOf[GpuColumnVector]
      .getBase
    withResource(new CudfTable(specIdCol)) { specIdTable =>
      val uniqueSpecIdTable = specIdTable
        .dropDuplicates(Array(0), DuplicateKeepOption.KEEP_ANY, false)
      withResource(uniqueSpecIdTable) { _ =>
        new RapidsHostColumnVector(IntegerType, uniqueSpecIdTable.getColumn(0).copyToHost())
      }
    }
  }

  protected def extractToStruct(batch: ColumnarBatch, ordinal: Int): ColumnarBatch = {
    require(batch.column(ordinal).dataType().isInstanceOf[StructType],
      "Can't extract from non struct type")


    val structType = batch.column(ordinal)
      .dataType()
      .asInstanceOf[StructType]

    val innerCols = batch.column(ordinal)
      .asInstanceOf[GpuColumnVector]
      .getBase
      .getChildColumnViews
      .zip(structType.fields)
      .safeMap(p => GpuColumnVector.from(p._1.copyToColumnVector(), p._2.dataType))
      .map(_.asInstanceOf[ColumnVector])

    new ColumnarBatch(innerCols, batch.numRows())
  }

  protected def extractPositionDeletes(batch: ColumnarBatch,
                                       filePathOrdinal: Int,
                                       posOrdinal: Int): ColumnarBatch = {
    val cols = Array(filePathOrdinal, posOrdinal)
      .map(batch.column(_).asInstanceOf[GpuColumnVector])
      .safeMap(_.incRefCount().asInstanceOf[ColumnVector])

    new ColumnarBatch(cols, batch.numRows())
  }
}

/**
 * GPU version of DeleteOnlyDeltaWriter.
 * 
 * This implements position delete writes for merge-on-read DELETE operations.
 * Based on the CPU implementation:
 */
class GpuDeleteOnlyDeltaWriter(
    table: Table,
    writerFactory: GpuSparkFileWriterFactory,
    deleteFileFactory: OutputFileFactory,
    context: GpuWriteContext) extends GpuDeltaWriter {

  private val io: FileIO = table.io()
  private val specs: mutable.Map[Integer, PartitionSpec] = table.specs().asScala
  private val specSparkTypeMap: mutable.Map[Int, StructType] = mutable.Map()

  // Ordinals for extracting fields from delete records
  private val specIdOrdinal: Int = context.specIdOrdinal()
  private val partitionOrdinal: Int = context.partitionOrdinal()
  private val tablePartitionType: IcebergTypes.StructType = Partitioning.partitionType(table)
  private val tablePartitionSparkType: StructType = GpuTypeToSparkType
    .toSparkType(tablePartitionType)
  private val tablePartitionDataTypes: Array[DataType] = tablePartitionSparkType
    .fields
    .map(_.dataType)
  private val fileOrdinal: Int = context.fileOrdinal()
  private val positionOrdinal: Int = context.positionOrdinal()

  // Delegate writer based on whether the table uses fanout or clustered writing
  // GPU writers work with SpillableColumnarBatch instead of PositionDelete objects
  private val delegate: PartitioningWriter[SpillableColumnarBatch, DeleteWriteResult] =
    newDeleteWriter(table, writerFactory, deleteFileFactory, context)
  

  // Partition projections for each spec
  private val partitionProjections: Map[Int, GpuStructProjection] =
    buildPartitionProjections(tablePartitionType, specs)
  
  private var closed: Boolean = false

  override def delete(metadata: ColumnarBatch, rowId: ColumnarBatch): Unit = {
    require(metadata != null, "Metadata batch must be non null for delete-only writer")

    val partitionValueBatch = extractToStruct(metadata, partitionOrdinal)
    val positionDeletes = extractPositionDeletes(rowId, fileOrdinal, positionOrdinal)

    withResource(uniqueSpecIds(metadata, specIdOrdinal)) { specIdCol =>
      for (rowIdx <- 0 until specIdCol.getRowCount.toInt) {
        val specIdHost = specIdCol.getInt(rowIdx)
        withResource(Scalar.fromInt(specIdHost)) { specId =>
          val spec = table.specs().get(specIdHost)
          val specSparkType = specSparkTypeMap.getOrElseUpdate(spec.specId(),
            toSparkType(spec.partitionType()))

          val specIdFilter = metadata.column(specIdOrdinal)
            .asInstanceOf[GpuColumnVector]
            .getBase
            .equalTo(specId)

          withResource(specIdFilter) { _ =>
            val filteredPartitionValues = GpuColumnVector.filter(partitionValueBatch,
              tablePartitionDataTypes,
              specIdFilter)

            val specProjection = withResource(filteredPartitionValues) { _ =>
              partitionProjections(specIdHost).project(filteredPartitionValues)
            }

            val partitions = withResource(specProjection) { _ =>
              val filteredPositionDeletes = GpuColumnVector.filter(positionDeletes,
                positionDeleteDataTypes, specIdFilter)

              if (specProjection.numCols() > 0) {
                withResource(filteredPositionDeletes) { _ =>
                  GpuIcebergPartitioner.partitionBy(specProjection,
                    spec.partitionType(),
                    specSparkType,
                    filteredPositionDeletes,
                    positionDeleteDataTypes)
                }
              } else {
                  // Unpartitioned spec
                  Seq(ColumnarBatchWithPartition(
                    SpillableColumnarBatch(filteredPositionDeletes,
                      SpillPriorities.ACTIVE_BATCHING_PRIORITY),
                    emptyPartitionData
                  ))
                }
              }

            closeOnExcept(partitions.to[mutable.Queue]) { buffer =>
              while (buffer.nonEmpty) {
                val p = buffer.dequeue()
                delegate.write(p.batch, spec, p.partition)
              }
            }
          }
        }
      }
    }
  }

  override def update(
      metadata: ColumnarBatch,
      rowId: ColumnarBatch, 
      row: ColumnarBatch): Unit = {
    throw new UnsupportedOperationException("Delete-only writer does not support updates")
  }

  override def insert(row: ColumnarBatch): Unit = {
    throw new UnsupportedOperationException("Delete-only writer does not support inserts")
  }

  override def commit(): WriterCommitMessage = {
    close()
    val result = delegate.result()
    new SparkPositionDeltaWrite.DeltaTaskCommit(result)
  }

  override def abort(): Unit = {
    close()
    val result = delegate.result()
    SparkCleanupUtil.deleteTaskFiles(io, result.deleteFiles())
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }
}


case class GpuWriteContext(
  dataSchema: Schema,
  dataSparkType: StructType,
  dataFileFormat: FileFormat,
  targetDataFileSize: Long,
  deleteSparkType: StructType,
  metadataSparkType: StructType,
  deleteFileFormat: FileFormat,
  targetDeleteFileSize: Long,
  deleteGranularity: DeleteGranularity,
  queryId: String,
  useFanoutWriter: Boolean,
  inputOrdered: Boolean) {

  /**
   * Returns the ordinal of the spec ID column in the delete schema.
   * This is used to identify which partition spec the delete belongs to.
   */
  def specIdOrdinal(): Int = {
    metadataSparkType.fieldIndex(MetadataColumns.SPEC_ID.name())
  }

  /**
   * Returns the ordinal of the partition column in the delete schema.
   * Contains the partition values for the deleted rows.
   */
  def partitionOrdinal(): Int = {
    metadataSparkType.fieldIndex(MetadataColumns.PARTITION_COLUMN_NAME)
  }

  /**
   * Returns the ordinal of the file path column in the delete schema.
   * Identifies which data file the deletes apply to.
   */
  def fileOrdinal(): Int = {
    deleteSparkType.fieldIndex(MetadataColumns.FILE_PATH.name())
  }

  /**
   * Returns the ordinal of the position column in the delete schema.
   * The row position within the data file to delete.
   */
  def positionOrdinal(): Int = {
    deleteSparkType.fieldIndex(MetadataColumns.ROW_POSITION.name())
  }
}

object GpuWriteContext {
  val positionDeleteSparkType: StructType = toSparkType(DeleteSchemaUtil.pathPosSchema())

  private[iceberg] val positionDeleteDataTypes = positionDeleteSparkType.fields.map(_.dataType)
  private[iceberg] val emptyPartitionData: PartitionData =
    new PartitionData(IcebergTypes.StructType.of()) {
    override def copy(): PartitionData = {
      this
    }
  }

  /**
   * Creates a GpuWriteContext from a CPU Context object using reflection.
   * This reads all fields from the CPU SparkPositionDeltaWrite.Context.
   */
  private[iceberg] def apply(cpu: AnyRef): GpuWriteContext = {
    val dataSchema = FieldUtils.readField(cpu, "dataSchema", true)
      .asInstanceOf[Schema]
    val dataSparkType = FieldUtils.readField(cpu, "dataSparkType", true)
      .asInstanceOf[StructType]
    val dataFileFormat = FieldUtils.readField(cpu, "dataFileFormat", true)
      .asInstanceOf[FileFormat]
    val targetDataFileSize = FieldUtils.readField(cpu, "targetDataFileSize", true)
      .asInstanceOf[Long]
    val deleteSparkType = FieldUtils.readField(cpu, "deleteSparkType", true)
      .asInstanceOf[StructType]
    val metadataSparkType = FieldUtils.readField(cpu, "metadataSparkType", true)
      .asInstanceOf[StructType]
    val deleteFileFormat = FieldUtils.readField(cpu, "deleteFileFormat", true)
      .asInstanceOf[FileFormat]
    val targetDeleteFileSize = FieldUtils.readField(cpu, "targetDeleteFileSize", true)
      .asInstanceOf[Long]
    val deleteGranularity = FieldUtils.readField(cpu, "deleteGranularity", true)
      .asInstanceOf[DeleteGranularity]
    val queryId = FieldUtils.readField(cpu, "queryId", true)
      .asInstanceOf[String]
    val useFanoutWriter = FieldUtils.readField(cpu, "useFanoutWriter", true)
      .asInstanceOf[Boolean]
    val inputOrdered = FieldUtils.readField(cpu, "inputOrdered", true)
      .asInstanceOf[Boolean]
    
    GpuWriteContext(
      dataSchema,
      dataSparkType,
      dataFileFormat,
      targetDataFileSize,
      deleteSparkType,
      metadataSparkType,
      deleteFileFormat,
      targetDeleteFileSize,
      deleteGranularity,
      queryId,
      useFanoutWriter,
      inputOrdered)
  }
}
