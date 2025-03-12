package com.nvidia.spark.rapids.iceberg.spark.source

import java.nio.ByteBuffer
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.iceberg.data.GpuDeleteFilter2
import com.nvidia.spark.rapids.iceberg.parquet.{GpuCoalescingIcebergParquetReader, GpuIcebergParquetReader, GpuIcebergParquetReaderConf, GpuMultiThreadIcebergParquetReader, GpuParquetReaderConf, GpuSingleThreadIcebergParquetReader, IcebergPartitionedFile, MultiFile, MultiThread, SingleFile, ThreadConf}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.iceberg.{FileFormat, FileScanTask, MetadataColumns, Partitioning, ScanTask, ScanTaskGroup, Schema, StructLike, Table, TableProperties}
import org.apache.iceberg.encryption.EncryptedFiles
import org.apache.iceberg.mapping.NameMappingParser
import org.apache.iceberg.types.{Type, Types}
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.util.{ByteBuffers, PartitionUtil}

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String


class GpuIcebergPartitionReader(private val task: GpuSparkInputPartition,
    private val threadConf: ThreadConf,
    private val metrics: Map[String, GpuMetric],
) extends PartitionReader[ColumnarBatch] {
  private lazy val table = task.table()
  private lazy val fileIO = table.io()
  private lazy val conf = newConf()
  private lazy val (inputFiles, tasks) = collectFiles()
  private lazy val gpuDeleteFiterMap: Map[IcebergPartitionedFile, Option[GpuDeleteFilter2]] =
    tasks.map {
      case (file, task) =>
        val filter = if (task.deletes().asScala.nonEmpty) {
          Some(new GpuDeleteFilter2(table.schema(),
            inputFiles, conf, task.deletes().asScala))
        } else {
          None
        }
        file -> filter
    }
  private lazy val reader: GpuIcebergParquetReader = createDataFileParquetReader()

  private var inited = false

  override def close(): Unit = {
    if (inited) {
      reader.close()
    }
  }

  override def next: Boolean = {
    reader.hasNext
  }

  override def get(): ColumnarBatch = {
    reader.next()
  }

  private def createDataFileParquetReader() = {
    if (tasks.values.exists(_.file().format() != FileFormat.PARQUET)) {
      throw new UnsupportedOperationException("Only parquet files are supported")
    }

    val files = tasks.keys.toSeq

    inited = true

    threadConf match {
      case SingleFile =>
        new GpuSingleThreadIcebergParquetReader(files, constantsMap, gpuDeleteFiterMap, conf)
      case MultiThread(_, _) =>
        new GpuMultiThreadIcebergParquetReader(files, constantsMap, gpuDeleteFiterMap, conf)
      case MultiFile(_) =>
        new GpuCoalescingIcebergParquetReader(files, constantsMap, conf)
    }
  }

  private def collectFiles() = {
    val tasks: Seq[FileScanTask] = task.taskGroup()
      .asInstanceOf[ScanTaskGroup[ScanTask]]
      .tasks()
      .asScala
      .map(t => t.asFileScanTask())
      .toSeq

    val encryptedFiles = tasks.flatMap(t => Seq(t.file()) ++ t.deletes().asScala)
      .map(f => EncryptedFiles.encryptedInput(
        fileIO.newInputFile(f.path().toString),
        f.keyMetadata()))

    val inputFiles = table.encryption()
      .decrypt(encryptedFiles.asJava)
      .asScala
      .map(f => f.location() -> f)
      .toMap

    val taskMap = tasks.map(t => {
      val file = inputFiles(t.file().path().toString)
      val icebergFile = IcebergPartitionedFile(file,
        Some((t.start(), t.length())),
        Some(t.residual()))

      icebergFile -> t
    }).toMap

    (inputFiles, taskMap)
  }

  private def newConf(): GpuIcebergParquetReaderConf = {
    val parquetConf = GpuParquetReaderConf(
      task.isCaseSensitive,
      task.getConfiguration,
      task.getMaxBatchSizeRows,
      task.getMaxBatchSizeBytes,
      task.getTargetBatchSizeBytes,
      task.getMaxGpuColumnSizeBytes,
      task.useChunkedReader(),
      task.maxChunkedReaderMemoryUsageSizeBytes(),
      task.getParquetDebugDumpPrefix,
      task.getParquetDebugDumpAlways,
      metrics,
      threadConf,
    )

    val nameMapping = Option(table.properties()
      .get(TableProperties.DEFAULT_NAME_MAPPING))
      .map(nm => NameMappingParser.fromJson(nm))

    GpuIcebergParquetReaderConf(parquetConf, task.expectedSchema(), nameMapping)
  }

  private def constantsMap(icebergFile: IcebergPartitionedFile): java.util.Map[Integer, _] = {
    val task = tasks(icebergFile)
    val filter = gpuDeleteFiterMap(icebergFile)
    val requiredSchema = filter.map(_.requiredSchema).getOrElse(conf.expectedSchema)
    GpuIcebergPartitionReader.constantsMap(task, requiredSchema, table)
  }
}

private object GpuIcebergPartitionReader {
  def constantsMap(task: FileScanTask, readSchema: Schema, table: Table): JMap[Integer, _] = {
    if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
      val partitionType: Types.StructType = Partitioning.partitionType(table)
      PartitionUtil.constantsMap(task, partitionType, convertConstant)
    }
    else {
      PartitionUtil.constantsMap(task, convertConstant)
    }
  }

  private def convertConstant(`type`: Type, value: AnyRef): AnyRef = {
    if (value == null) {
      return null
    }
    `type`.typeId match {
      case TypeID.DECIMAL =>
        Decimal.apply(value.asInstanceOf[java.math.BigDecimal])
      case TypeID.STRING =>
        value match {
          case utf8: Utf8 =>
            UTF8String.fromBytes(utf8.getBytes, 0, utf8.getByteLength)
          case _ =>
            UTF8String.fromString(value.toString)
        }
      case TypeID.FIXED =>
        value match {
          case v: Array[Byte] => v
          case fixed: GenericData.Fixed => fixed.bytes
          case b => ByteBuffers.toByteArray(b.asInstanceOf[ByteBuffer])
        }
      case TypeID.BINARY =>
        ByteBuffers.toByteArray(value.asInstanceOf[ByteBuffer])
      case TypeID.STRUCT =>
        val structType: Types.StructType = `type`.asInstanceOf[Types.StructType]
        if (structType.fields.isEmpty) {
          return new GenericInternalRow(Array.empty[Any])
        }
        val struct: StructLike = value.asInstanceOf[StructLike]

        val values = structType.fields()
          .asScala
          .zipWithIndex
          .map {
            case (field, index) =>
              val fieldType: Type = field.`type`
              val value = struct.get(index, fieldType.typeId.javaClass).asInstanceOf[AnyRef]
              convertConstant(fieldType, value).asInstanceOf[Any]
          }
          .toArray
        new GenericInternalRow(values)
      case _ => value
    }
  }
}
