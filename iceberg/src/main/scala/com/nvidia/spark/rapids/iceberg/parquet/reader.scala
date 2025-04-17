package com.nvidia.spark.rapids.iceberg.parquet

import java.io.{IOException, UncheckedIOException}
import java.net.URI
import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{DateTimeRebaseCorrected, GpuMetric, GpuParquetUtils, ParquetFileInfoWithBlockMeta}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.mapping.NameMapping
import org.apache.iceberg.parquet.{ParquetBloomRowGroupFilter, ParquetDictionaryRowGroupFilter, ParquetMetricsRowGroupFilter, ParquetSchemaUtil}
import org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class IcebergPartitionedFile(
    file: InputFile,
    split: Option[(Long, Long)] = None,
    filter: Option[Expression] = None) {

  private lazy val _urlEncodedPath: String = new Path(file.location()).toUri.toString
  private lazy val _path: Path = new Path(new URI(_urlEncodedPath))

  def parquetReadOptions: ParquetReadOptions = {
    GpuIcebergParquetReader.buildReaderOptions(file, split)
  }

  def newReader: ParquetFileReader = {
    try {
      ParquetFileReader.open(ParquetIO.file(file), parquetReadOptions)
    } catch {
      case e: IOException =>
        throw new UncheckedIOException(s"Failed to open Parquet file: ${file.location()}", e)
    }
  }

  def sparkPartitionedFile: PartitionedFile = {
    split match {
      case Some((start, length)) =>
        PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty,
          file.location(),
          start,
          length)
      case None =>
        PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty,
          file.location(),
          0,
          file.getLength)
    }
  }

  def path: Path = _path

  def urlEncodedPath: String = _urlEncodedPath

  def start: Long = split.map(_._1).getOrElse(0L)

  def length: Long = split.map(_._2).getOrElse(file.getLength)

  override def hashCode(): Int = {
    Objects.hash(urlEncodedPath, split)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: IcebergPartitionedFile =>
        urlEncodedPath == that.urlEncodedPath &&
          split == that.split
      case _ => false
    }
  }
}

sealed trait ThreadConf

case object SingleFile extends ThreadConf

case class MultiThread(numThreads: Int, maxNumFilesProcessed: Int) extends ThreadConf

case class MultiFile(numThreads: Int) extends ThreadConf


case class GpuParquetReaderConf(
    caseSensitive: Boolean,
    conf: Configuration,
    maxBatchSizeRows: Int,
    maxBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    maxChunkedReaderMemoryUsageSizeBytes: Long,
    parquetDebugDumpPrefix: Option[String],
    parquetDebugDumpAlways: Boolean,
    metrics: Map[String, GpuMetric],
    threadConf: ThreadConf,
)


case class GpuIcebergParquetReaderConf(
    parquetConf: GpuParquetReaderConf,
    expectedSchema: Schema,
    nameMapping: Option[NameMapping],
)

trait GpuIcebergParquetReader extends Iterator[ColumnarBatch] with AutoCloseable with Logging {
  def conf: GpuIcebergParquetReaderConf

  def projectSchema(fileSchema: MessageType, requiredSchema: Schema): (MessageType, MessageType) = {
    val (typeWithIds, fileReadSchema) = if (ParquetSchemaUtil.hasIds(fileSchema)) {
      (fileSchema, ParquetSchemaUtil.pruneColumns(fileSchema, requiredSchema))
    } else if (conf.nameMapping.isDefined) {
      val typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, conf.nameMapping.get)
      (typeWithIds, ParquetSchemaUtil.pruneColumns(typeWithIds, requiredSchema))
    } else {
      val typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema)
      (typeWithIds, ParquetSchemaUtil.pruneColumnsFallback(typeWithIds, requiredSchema))
    }

    logWarning(s"Doing project schema, parquet file schema:\n$fileSchema " +
      s"\niceberg required schema:\n$requiredSchema " +
      s"\nfile reade schema:\n$fileReadSchema")
    (typeWithIds, fileReadSchema)
  }

  def filterRowGroups(reader: ParquetFileReader,
      requiredSchema: Schema,
      typeWithIds: MessageType,
      filter: Option[Expression])
  : Seq[BlockMetaData] = {
    val blocks = reader.getRowGroups.asScala

    filter.map { f =>
      val statsFilter = new ParquetMetricsRowGroupFilter(requiredSchema,
        f,
        conf.parquetConf.caseSensitive)
      val dictFilter = new ParquetDictionaryRowGroupFilter(requiredSchema,
        f,
        conf.parquetConf.caseSensitive)
      val bloomFilter = new ParquetBloomRowGroupFilter(requiredSchema,
        f,
        conf.parquetConf.caseSensitive)

      blocks.filter { rowGroup =>
          statsFilter.shouldRead(typeWithIds, rowGroup) &&
            dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)) &&
            bloomFilter.shouldRead(typeWithIds, rowGroup, reader.getBloomFilterDataReader(rowGroup))
      }
    }.getOrElse(blocks)
  }

  def clipBlocksToSchema(fileReadSchema: MessageType,
      blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    GpuParquetUtils.clipBlocksToSchema(fileReadSchema,
      blocks.asJava,
      conf.parquetConf.caseSensitive)
  }

  def filterParquetBlocks(file: IcebergPartitionedFile,
      requiredSchema: Schema): ParquetFileInfoWithBlockMeta = {
    withResource(file.newReader) { reader =>
      val fileSchema = reader.getFileMetaData.getSchema

      val rowGroupFirstRowIndices = new Array[Long](reader.getRowGroups.size())
      rowGroupFirstRowIndices(0) = 0
      var accumulatedRowCount = reader.getRowGroups.get(0).getRowCount
      for (i <- 1 until reader.getRowGroups.size()) {
        rowGroupFirstRowIndices(i) = accumulatedRowCount
        accumulatedRowCount += reader.getRowGroups.get(i).getRowCount
      }
      val (typeWithIds, fileReadSchema) = projectSchema(fileSchema, requiredSchema)
      val filteredBlocks = filterRowGroups(reader, requiredSchema, typeWithIds, file.filter)
      val blockFirstRowIndices = filteredBlocks.map(b => rowGroupFirstRowIndices(b.getOrdinal))
      val blocks = clipBlocksToSchema(fileReadSchema, filteredBlocks)

      val partReaderSparkSchema = TypeWithSchemaVisitor.visit(requiredSchema.asStruct(),
          fileReadSchema, new GpuParquetReader.SparkSchemaConverter)
        .asInstanceOf[StructType]

      ParquetFileInfoWithBlockMeta(file.path,
        blocks,
        InternalRow.empty, // Iceberg handles partition values but itself
        fileReadSchema,
        partReaderSparkSchema,
        DateTimeRebaseCorrected,
        DateTimeRebaseCorrected,
        hasInt96Timestamps = true,
        blockFirstRowIndices,
      )
    }
  }
}

object GpuIcebergParquetReader {
  private val READ_PROPERTIES_TO_REMOVE = Set(
    "parquet.read.filter",
    "parquet.private.read.filter.predicate",
    "parquet.read.support.class")

  def buildReaderOptions(file: InputFile, split: Option[(Long, Long)])
  : ParquetReadOptions = {
    var optionsBuilder: ParquetReadOptions.Builder = null
    file match {
      case hadoop: HadoopInputFile =>
        // remove read properties already set that may conflict with this read
        val conf = new Configuration(hadoop.getConf)
        for (property <- READ_PROPERTIES_TO_REMOVE) {
          conf.unset(property)
        }
        optionsBuilder = HadoopReadOptions.builder(conf)
      case _ =>
        //optionsBuilder = ParquetReadOptions.builder();
        throw new UnsupportedOperationException("Only Hadoop files are supported for now")
    }
    split.foreach { case (start, length) =>
      optionsBuilder = optionsBuilder.withRange(start, start + length)
    }
    optionsBuilder.build
  }
}