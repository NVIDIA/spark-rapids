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

package com.nvidia.spark.rapids.iceberg.parquet

import java.io.{IOException, UncheckedIOException}
import java.net.URI
import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{DateTimeRebaseCorrected, GpuMetric, ThreadPoolConfBuilder}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergInputFile
import com.nvidia.spark.rapids.iceberg.parquet.converter.FromIcebergShaded._
import com.nvidia.spark.rapids.parquet.{GpuParquetUtils, ParquetFileInfoWithBlockMeta}
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.hadoop.HadoopInputFile
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.mapping.NameMapping
import org.apache.iceberg.parquet._
import org.apache.iceberg.shaded.org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.metadata.{BlockMetaData => ShadedBlockMetaData}
import org.apache.iceberg.shaded.org.apache.parquet.schema.{MessageType => ShadedMessageType}
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class IcebergPartitionedFile(
    file: IcebergInputFile,
    split: Option[(Long, Long)] = None,
    filter: Option[Expression] = None) {

  lazy val urlEncodedPath: String = new Path(file.getDelegate.location()).toUri.toString
  lazy val path: Path = new Path(new URI(urlEncodedPath))

  def parquetReadOptions: ParquetReadOptions = {
    GpuIcebergParquetReader.buildReaderOptions(file.getDelegate, split)
  }

  def newReader: ParquetFileReader = {
    try {
      ParquetFileReader.open(GpuParquetIO.file(file.getDelegate), parquetReadOptions)
    } catch {
      case e: IOException =>
        throw new UncheckedIOException(s"Failed to newInputFile Parquet file: " +
          s"${file.getDelegate.location()}", e)
    }
  }

  def sparkPartitionedFile: PartitionedFile = {
    split match {
      case Some((start, length)) =>
        PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty,
          file.getDelegate.location(),
          start,
          length)
      case None =>
        PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty,
          file.getDelegate.location(),
          0,
          file.getLength)
    }
  }

  def start: Long = split.map(_._1).getOrElse(0L)

  def length: Long = split.map(_._2).getOrElse(file.getLength)

  def isSame(p: PartitionedFile) = {
    this.urlEncodedPath == p.filePath.urlEncoded &&
      this.start == p.start &&
      this.length == p.length
  }

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

case class MultiThread(
    poolConfBuilder: ThreadPoolConfBuilder,
    maxNumFilesProcessed: Int) extends ThreadConf

case class MultiFile(poolConfBuilder: ThreadPoolConfBuilder) extends ThreadConf


case class GpuIcebergParquetReaderConf(
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
    expectedSchema: Schema,
    nameMapping: Option[NameMapping],
)

trait GpuIcebergParquetReader extends Iterator[ColumnarBatch] with AutoCloseable with Logging {
  def conf: GpuIcebergParquetReaderConf

  def projectSchema(fileSchema: ShadedMessageType, requiredSchema: Schema):
  (ShadedMessageType, ShadedMessageType) = {
    val (typeWithIds, fileReadSchema) = if (ParquetSchemaUtil.hasIds(fileSchema)) {
      (fileSchema, ParquetSchemaUtil.pruneColumns(fileSchema, requiredSchema))
    } else if (conf.nameMapping.isDefined) {
      val typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, conf.nameMapping.get)
      (typeWithIds, ParquetSchemaUtil.pruneColumns(typeWithIds, requiredSchema))
    } else {
      val typeWithIds = ParquetSchemaUtil.addFallbackIds(fileSchema)
      (typeWithIds, ParquetSchemaUtil.pruneColumnsFallback(typeWithIds, requiredSchema))
    }

    logDebug(s"Doing project schema, parquet file schema:\n$fileSchema " +
      s"\niceberg required schema:\n$requiredSchema " +
      s"\nfile reade schema:\n$fileReadSchema")
    (typeWithIds, fileReadSchema)
  }

  def filterRowGroups(reader: ParquetFileReader,
      requiredSchema: Schema,
      typeWithIds: ShadedMessageType,
      filter: Option[Expression])
  : Seq[(ShadedBlockMetaData, Int)] = {
    val blocks = reader.getRowGroups.asScala
      .zipWithIndex

    filter.map { f =>
      val statsFilter = new ParquetMetricsRowGroupFilter(requiredSchema,
        f,
        conf.caseSensitive)
      val dictFilter = new ParquetDictionaryRowGroupFilter(requiredSchema,
        f,
        conf.caseSensitive)
      val bloomFilter = new ParquetBloomRowGroupFilter(requiredSchema,
        f,
        conf.caseSensitive)

      blocks.filter { case (rowGroup, _) =>
          statsFilter.shouldRead(typeWithIds, rowGroup) &&
            dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup)) &&
            bloomFilter.shouldRead(typeWithIds, rowGroup, reader.getBloomFilterDataReader(rowGroup))
      }
    }.getOrElse(blocks)
      .toSeq
  }

  def clipBlocksToSchema(fileReadSchema: ShadedMessageType,
      blocks: Seq[ShadedBlockMetaData]): Seq[BlockMetaData] = {
    GpuParquetUtils.clipBlocksToSchema(unshade(fileReadSchema),
      blocks.map(unshade).asJava,
      conf.caseSensitive)
  }

  def filterParquetBlocks(file: IcebergPartitionedFile,
      requiredSchema: Schema): (ParquetFileInfoWithBlockMeta, ShadedMessageType) = {
    withResource(file.newReader) { reader =>
      val fileSchema = reader.getFileMetaData.getSchema

      val rowGroupFirstRowIndices = new Array[Long](reader.getRowGroups.size())
      var accumulatedRowCount = 0L
      for (i <- 0 until reader.getRowGroups.size()) {
        rowGroupFirstRowIndices(i) = accumulatedRowCount
        accumulatedRowCount += reader.getRowGroups.get(i).getRowCount
      }
      val (typeWithIds, fileReadSchema) = projectSchema(fileSchema, requiredSchema)
      val filteredBlocks = filterRowGroups(reader, requiredSchema, typeWithIds, file.filter)
      val blockFirstRowIndices = filteredBlocks.map(b => rowGroupFirstRowIndices(b._2))
      val blocks = clipBlocksToSchema(fileReadSchema, filteredBlocks.map(_._1))

      val partReaderSparkSchema = TypeWithSchemaVisitor.visit(requiredSchema.asStruct(),
          fileReadSchema, new SparkSchemaConverter)
        .asInstanceOf[StructType]

      val parquetFileInfo = ParquetFileInfoWithBlockMeta(file.path,
        blocks,
        InternalRow.empty, // Iceberg handles partition values but itself
        unshade(fileReadSchema),
        partReaderSparkSchema,
        DateTimeRebaseCorrected,
        DateTimeRebaseCorrected,
        hasInt96Timestamps = true,
        blockFirstRowIndices,
      )
      
      (parquetFileInfo, fileReadSchema)
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
        optionsBuilder = ParquetReadOptions.builder()
    }
    split.foreach { case (start, length) =>
      optionsBuilder = optionsBuilder.withRange(start, start + length)
    }
    optionsBuilder.build
  }
}