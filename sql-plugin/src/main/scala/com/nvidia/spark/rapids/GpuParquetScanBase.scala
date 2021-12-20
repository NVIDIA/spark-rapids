/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.io.OutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}
import java.util.concurrent._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.language.implicitConversions
import scala.math.max

import ai.rapids.cudf._
import com.nvidia.spark.RebaseHelper
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.ParquetPartitionReader.CopyRange
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


/**
 * Base GpuParquetScan used for common code across Spark versions. Gpu version of
 * Spark's 'ParquetScan'.
 *
 * @param sparkSession SparkSession.
 * @param hadoopConf Hadoop configuration.
 * @param dataSchema Schema of the data.
 * @param readDataSchema Schema to read.
 * @param readPartitionSchema Partition schema.
 * @param pushedFilters Filters on non-partition columns.
 * @param rapidsConf Rapids configuration.
 * @param queryUsesInputFile This is a parameter to easily allow turning it
 *                               off in GpuTransitionOverrides if InputFileName,
 *                               InputFileBlockStart, or InputFileBlockLength are used
 */
abstract class GpuParquetScanBase(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean)
  extends ScanWithMetrics with Logging {

  def isSplitableBase(path: Path): Boolean = true

  def createReaderFactoryBase(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isParquetPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
    } else {
      GpuParquetMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        queryUsesInputFile)
    }
  }
}

object GpuParquetScanBase {
  def tagSupport(scanMeta: ScanMeta[ParquetScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def throwIfNeeded(
      table: Table,
      isCorrectedInt96Rebase: Boolean,
      isCorrectedDateTimeRebase: Boolean,
      hasInt96Timestamps: Boolean): Unit = {
    (0 until table.getNumberOfColumns).foreach { i =>
      val col = table.getColumn(i)
      // if col is a day
      if (!isCorrectedDateTimeRebase && RebaseHelper.isDateRebaseNeededInRead(col)) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
      }
      // if col is a time
      else if (hasInt96Timestamps && !isCorrectedInt96Rebase ||
          !hasInt96Timestamps && !isCorrectedDateTimeRebase) {
        if (RebaseHelper.isTimeRebaseNeededInRead(col)) {
          throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
        }
      }
    }
  }

  def tagSupport(
      sparkSession: SparkSession,
      readSchema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    val sqlConf = sparkSession.conf

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetReadEnabled) {
      meta.willNotWorkOnGpu("Parquet input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_READ} to true")
    }

    FileFormatChecks.tag(meta, readSchema, ParquetFormatType, ReadFileOp)

    val schemaHasStrings = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[StringType])
    }

    if (sqlConf.get(SQLConf.PARQUET_BINARY_AS_STRING.key,
      SQLConf.PARQUET_BINARY_AS_STRING.defaultValueString).toBoolean && schemaHasStrings) {
      meta.willNotWorkOnGpu(s"GpuParquetScan does not support" +
          s" ${SQLConf.PARQUET_BINARY_AS_STRING.key}")
    }

    val schemaHasTimestamps = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    def isTsOrDate(dt: DataType) : Boolean = dt match {
      case TimestampType | DateType => true
      case _ => false
    }
    val schemaMightNeedNestedRebase = readSchema.exists { field =>
      if (DataTypeUtils.isNestedType(field.dataType)) {
        TrampolineUtil.dataTypeExistsRecursively(field.dataType, isTsOrDate)
      } else {
        false
      }
    }

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    if (schemaHasTimestamps && sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support int96 timestamp conversion")
    }

    sqlConf.get(ShimLoader.getSparkShims.int96ParquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${ShimLoader.getSparkShims.int96ParquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${ShimLoader.getSparkShims.int96ParquetRebaseReadKey} is LEGACY")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }

    sqlConf.get(ShimLoader.getSparkShims.parquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${ShimLoader.getSparkShims.parquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${ShimLoader.getSparkShims.parquetRebaseReadKey} is LEGACY")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }
  }
}

/**
 * Base object that has common functions for both GpuParquetPartitionReaderFactory
 * and GpuParquetPartitionReaderFactory
 */
object GpuParquetPartitionReaderFactoryBase {

  def filterClippedSchema(
      clippedSchema: MessageType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean): MessageType = {
    val fs = fileSchema.asGroupType()
    val types = if (isCaseSensitive) {
      val inFile = fs.getFields.asScala.map(_.getName).toSet
      clippedSchema.asGroupType()
        .getFields.asScala.filter(f => inFile.contains(f.getName))
    } else {
      val inFile = fs.getFields.asScala
        .map(_.getName.toLowerCase(Locale.ROOT)).toSet
      clippedSchema.asGroupType()
        .getFields.asScala
        .filter(f => inFile.contains(f.getName.toLowerCase(Locale.ROOT)))
    }
    if (types.isEmpty) {
      Types.buildMessage().named("spark_schema")
    } else {
      Types
        .buildMessage()
        .addFields(types: _*)
        .named("spark_schema")
    }
  }

  // Copied from Spark
  private val SPARK_VERSION_METADATA_KEY = "org.apache.spark.version"
  // Copied from Spark
  private val SPARK_LEGACY_DATETIME = "org.apache.spark.legacyDateTime"
  // Copied from Spark
  private val SPARK_LEGACY_INT96 = "org.apache.spark.legacyINT96"

  def isCorrectedInt96RebaseMode(
      lookupFileMeta: String => String,
      isCorrectedInt96ModeConfig: Boolean): Boolean = {
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 3.0 and earlier follow the legacy hybrid calendar and we need to
      // rebase the INT96 timestamp values.
      // Files written by Spark 3.1 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version >= "3.1.0") {
        lookupFileMeta(SPARK_LEGACY_INT96) == null
      } else if (version >= "3.0.0") {
        lookupFileMeta(SPARK_LEGACY_DATETIME) == null
      } else {
        false
      }
    }.getOrElse(isCorrectedInt96ModeConfig)
  }

  def isCorrectedRebaseMode(
      lookupFileMeta: String => String,
      isCorrectedModeConfig: Boolean): Boolean = {
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and later may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      version >= "3.0.0" && lookupFileMeta(SPARK_LEGACY_DATETIME) == null
    }.getOrElse(isCorrectedModeConfig)
  }
}

// contains meta about all the blocks in a file
private case class ParquetFileInfoWithBlockMeta(filePath: Path, blocks: Seq[BlockMetaData],
    partValues: InternalRow, schema: MessageType, isCorrectedInt96RebaseMode: Boolean,
    isCorrectedRebaseMode: Boolean, hasInt96Timestamps: Boolean)

private case class GpuParquetFileFilterHandler(@transient sqlConf: SQLConf) extends Arm {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val rebaseMode = ShimLoader.getSparkShims.parquetRebaseRead(sqlConf)
  private val isCorrectedRebase = "CORRECTED" == rebaseMode
  val int96RebaseMode = ShimLoader.getSparkShims.int96ParquetRebaseRead(sqlConf)
  private val isInt96CorrectedRebase = "CORRECTED" == int96RebaseMode


  def isParquetTimeInInt96(parquetType: Type): Boolean = {
    parquetType match {
      case p:PrimitiveType =>
        p.getPrimitiveTypeName == PrimitiveTypeName.INT96
      case g:GroupType => //GroupType
        g.getFields.asScala.exists(t => isParquetTimeInInt96(t))
      case _ => false
    }
  }

  @scala.annotation.nowarn(
    "msg=constructor ParquetFileReader in class ParquetFileReader is deprecated"
  )
  def filterBlocks(
      file: PartitionedFile,
      conf : Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): ParquetFileInfoWithBlockMeta = {

    val filePath = new Path(new URI(file.filePath))
    //noinspection ScalaDeprecation
    val footer = ParquetFileReader.readFooter(conf, filePath,
      ParquetMetadataConverter.range(file.start, file.start + file.length))
    val fileSchema = footer.getFileMetaData.getSchema
    val pushedFilters = if (enableParquetFilterPushDown) {
      val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
        footer.getFileMetaData.getKeyValueMetaData.get, rebaseMode)
      val parquetFilters = ShimLoader.getSparkShims.getParquetFilters(fileSchema, pushDownDate,
        pushDownTimestamp, pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold,
        isCaseSensitive, datetimeRebaseMode)
      filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
    } else {
      None
    }

    val hasInt96Timestamps = isParquetTimeInInt96(fileSchema)

    val isCorrectedRebaseForThisFile =
      GpuParquetPartitionReaderFactoryBase.isCorrectedRebaseMode(
        footer.getFileMetaData.getKeyValueMetaData.get, isCorrectedRebase)

    val isCorrectedInt96RebaseForThisFile =
      GpuParquetPartitionReaderFactoryBase.isCorrectedInt96RebaseMode(
        footer.getFileMetaData.getKeyValueMetaData.get, isInt96CorrectedRebase)

    val blocks = if (pushedFilters.isDefined) {
      // Use the ParquetFileReader to perform dictionary-level filtering
      ParquetInputFormat.setFilterPredicate(conf, pushedFilters.get)
      //noinspection ScalaDeprecation
      withResource(new ParquetFileReader(conf, footer.getFileMetaData, filePath,
        footer.getBlocks, Collections.emptyList[ColumnDescriptor])) { parquetReader =>
        parquetReader.getRowGroups
      }
    } else {
      footer.getBlocks
    }

    val clippedSchemaTmp = ParquetReadSupport.clipParquetSchema(fileSchema, readDataSchema,
      isCaseSensitive)
    // ParquetReadSupport.clipParquetSchema does most of what we want, but it includes
    // everything in readDataSchema, even if it is not in fileSchema we want to remove those
    // for our own purposes
    val clippedSchema = GpuParquetPartitionReaderFactoryBase.filterClippedSchema(clippedSchemaTmp,
      fileSchema, isCaseSensitive)
    val columnPaths = clippedSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
    val clipped = ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala, isCaseSensitive)

    ParquetFileInfoWithBlockMeta(filePath, clipped, file.partitionValues,
      clippedSchema, isCorrectedInt96RebaseForThisFile, isCorrectedRebaseForThisFile,
      hasInt96Timestamps)
  }
}

/**
 * Similar to GpuParquetPartitionReaderFactory but extended for reading multiple files
 * in an iteration. This will allow us to read multiple small files and combine them
 * on the CPU side before sending them down to the GPU.
 */
case class GpuParquetMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val numThreads = rapidsConf.parquetMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel

  private val filterHandler = GpuParquetFileFilterHandler(sqlConf)

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isParquetCoalesceFileReadEnabled && !queryUsesInputFile

  override val canUseMultiThreadReader: Boolean = rapidsConf.isParquetMultiThreadReadEnabled

  /**
   * Build the PartitionReader for cloud reading
   *
   * @param files files to be read
   * @param conf  configuration
   * @return cloud reading PartitionReader
   */
  override def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    new MultiFileCloudParquetPartitionReader(conf, files,
      isCaseSensitive, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics, partitionSchema,
      numThreads, maxNumFileProcessed, filterHandler, filters)
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  override def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val clippedBlocks = ArrayBuffer[ParquetSingleDataBlockMeta]()
    files.map { file =>
      val singleFileInfo = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
      clippedBlocks ++= singleFileInfo.blocks.map(block =>
        ParquetSingleDataBlockMeta(
          singleFileInfo.filePath,
          ParquetDataBlock(block),
          file.partitionValues,
          ParquetSchemaWrapper(singleFileInfo.schema),
          ParquetExtraInfo(singleFileInfo.isCorrectedRebaseMode,
            singleFileInfo.isCorrectedInt96RebaseMode, singleFileInfo.hasInt96Timestamps)))
    }
    new MultiFileParquetPartitionReader(conf, files, clippedBlocks,
      isCaseSensitive, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics,
      partitionSchema, numThreads)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "Parquet"

}

case class GpuParquetPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory with Arm with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes

  private val filterHandler = new GpuParquetFileFilterHandler(sqlConf)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val reader = new PartitionReaderWithBytesRead(buildBaseColumnarParquetReader(partitionedFile))
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema)
  }

  private def buildBaseColumnarParquetReader(
      file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val singleFileInfo = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
    new ParquetPartitionReader(conf, file, singleFileInfo.filePath, singleFileInfo.blocks,
      singleFileInfo.schema, isCaseSensitive, readDataSchema,
      debugDumpPrefix, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics, singleFileInfo.isCorrectedInt96RebaseMode,
      singleFileInfo.isCorrectedRebaseMode, singleFileInfo.hasInt96Timestamps)
  }
}

trait ParquetPartitionReaderBase extends Logging with Arm with ScanWithMetrics
    with MultiFileReaderFunctions {

  // Configuration
  def conf: Configuration

  // Schema to read
  def readDataSchema: StructType

  def isSchemaCaseSensitive: Boolean

  val copyBufferSize = conf.getInt("parquet.read.allocation.size", 8 * 1024 * 1024)

  @scala.annotation.nowarn(
    "msg=constructor NullOutputStream in class NullOutputStream is deprecated"
  )
  protected def calculateParquetFooterSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType): Long = {
    // Calculate size of the footer metadata.
    // This uses the column metadata from the original file, but that should
    // always be at least as big as the updated metadata in the output.
    val out = new CountingOutputStream(new NullOutputStream)
    writeFooter(out, currentChunkedBlocks, schema)
    out.getByteCount
  }

  protected def calculateParquetOutputSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType,
      handleCoalesceFiles: Boolean): Long = {
    // start with the size of Parquet magic (at start+end) and footer length values
    var size: Long = 4 + 4 + 4

    // Calculate the total amount of column data that will be copied
    // NOTE: Avoid using block.getTotalByteSize here as that is the
    //       uncompressed size rather than the size in the file.
    size += currentChunkedBlocks.flatMap(_.getColumns.asScala.map(_.getTotalSize)).sum

    val footerSize = calculateParquetFooterSize(currentChunkedBlocks, schema)
    val extraMemory = if (handleCoalesceFiles) {
      // we want to add extra memory because the ColumnChunks saved in the Footer have 2 fields
      // file_offset and data_page_offset that get much larger when we are combining files.
      // Here we estimate that by taking the number of columns * number of blocks which should be
      // the number of column chunks and then saying there are 2 fields that could be larger and
      // assume max size of those would be 8 bytes worst case. So we probably allocate to much here
      // but it shouldn't be by a huge amount and its better then having to realloc and copy.
      val numCols = currentChunkedBlocks.head.getColumns().size()
      val numColumnChunks = numCols * currentChunkedBlocks.size
      numColumnChunks * 2 * 8
    } else {
      0
    }
    val totalSize = size + footerSize + extraMemory
    totalSize
  }

  protected def writeFooter(
      out: OutputStream,
      blocks: Seq[BlockMetaData],
      schema: MessageType): Unit = {
    val fileMeta = new FileMetaData(schema, Collections.emptyMap[String, String],
      ParquetPartitionReader.PARQUET_CREATOR)
    val metadataConverter = new ParquetMetadataConverter
    val footer = new ParquetMetadata(fileMeta, blocks.asJava)
    val meta = metadataConverter.toParquetMetadata(ParquetPartitionReader.PARQUET_VERSION, footer)
    org.apache.parquet.format.Util.writeFileMetaData(meta, out)
  }

  protected def copyDataRange(
      range: CopyRange,
      in: FSDataInputStream,
      out: OutputStream,
      copyBuffer: Array[Byte]): Unit = {
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    var bytesLeft = range.length
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      in.readFully(copyBuffer, 0, readLength)
      out.write(copyBuffer, 0, readLength)
      bytesLeft -= readLength
    }
  }

  /**
   * Computes new block metadata to reflect where the blocks and columns will appear in the
   * computed Parquet file.
   *
   * @param blocks block metadata from the original file(s) that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @param copyRangesToUpdate optional buffer to update with ranges of column data to copy
   * @return updated block metadata
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  protected def computeBlockMetaData(
      blocks: Seq[BlockMetaData],
      realStartOffset: Long,
      copyRangesToUpdate: Option[ArrayBuffer[CopyRange]] = None): Seq[BlockMetaData] = {
    var totalRows: Long = 0
    val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
    var currentCopyStart = 0L
    var currentCopyEnd = 0L
    var totalBytesToCopy = 0L
    blocks.foreach { block =>
      totalRows += block.getRowCount
      val columns = block.getColumns.asScala
      val outputColumns = new ArrayBuffer[ColumnChunkMetaData](columns.length)
      columns.foreach { column =>
        // update column metadata to reflect new position in the output file
        val startPosCol = column.getStartingPos
        val offsetAdjustment = realStartOffset + totalBytesToCopy - startPosCol
        val newDictOffset = if (column.getDictionaryPageOffset > 0) {
          column.getDictionaryPageOffset + offsetAdjustment
        } else {
          0
        }
        //noinspection ScalaDeprecation
        outputColumns += ColumnChunkMetaData.get(
          column.getPath,
          column.getPrimitiveType,
          column.getCodec,
          column.getEncodingStats,
          column.getEncodings,
          column.getStatistics,
          column.getStartingPos + offsetAdjustment,
          newDictOffset,
          column.getValueCount,
          column.getTotalSize,
          column.getTotalUncompressedSize)

        if (currentCopyEnd != column.getStartingPos) {
          if (currentCopyEnd != 0) {
            copyRangesToUpdate.foreach {
              _.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
            }
          }
          currentCopyStart = column.getStartingPos
          currentCopyEnd = currentCopyStart
        }
        currentCopyEnd += column.getTotalSize
        totalBytesToCopy += column.getTotalSize
      }
      outputBlocks += ParquetPartitionReader.newParquetBlock(block.getRowCount, outputColumns)
    }

    if (currentCopyEnd != currentCopyStart) {
      copyRangesToUpdate.foreach {
        _.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
      }
    }
    outputBlocks
  }

  /**
   * Copies the data corresponding to the clipped blocks in the original file and compute the
   * block metadata for the output. The output blocks will contain the same column chunk
   * metadata but with the file offsets updated to reflect the new position of the column data
   * as written to the output.
   *
   * @param in  the input stream for the original Parquet file
   * @param out the output stream to receive the data
   * @param blocks block metadata from the original file that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @return updated block metadata corresponding to the output
   */
  protected def copyBlocksData(
      in: FSDataInputStream,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData],
      realStartOffset: Long): Seq[BlockMetaData] = {
    val copyRanges = new ArrayBuffer[CopyRange]
    val outputBlocks = computeBlockMetaData(blocks, realStartOffset, Some(copyRanges))
    val copyBuffer = new Array[Byte](copyBufferSize)
    copyRanges.foreach(copyRange => copyDataRange(copyRange, in, out, copyBuffer))
    outputBlocks
  }

  protected def areNamesEquiv(groups: GroupType, index: Int, otherName: String,
      isCaseSensitive: Boolean): Boolean = {
    if (groups.getFieldCount > index) {
      if (isCaseSensitive) {
        groups.getFieldName(index) == otherName
      } else {
        groups.getFieldName(index).toLowerCase(Locale.ROOT) == otherName.toLowerCase(Locale.ROOT)
      }
    } else {
      false
    }
  }

  @scala.annotation.nowarn(
    "msg=method getDecimalMetadata in class PrimitiveType is deprecated"
  )
  def getPrecisionsList(fields: Seq[Type]): Seq[Int] = {
    fields.filter(field => field.getOriginalType == OriginalType.DECIMAL || !field.isPrimitive())
      .flatMap { field =>
        if (!field.isPrimitive) {
          getPrecisionsList(field.asGroupType().getFields.asScala)
        } else {
          Seq(field.asPrimitiveType().getDecimalMetadata.getPrecision)
        }
      }
  }

  protected def evolveSchemaIfNeededAndClose(
      inputTable: Table,
      filePath: String,
      clippedSchema: MessageType): Table = {

    val precisions = getPrecisionsList(clippedSchema.asGroupType().getFields.asScala)
    // check if there are cols with precision that can be stored in an int
    val hasDecimalAsInt = precisions.exists(p => p <= Decimal.MAX_INT_DIGITS)
    val hasUnsignedType = existsUnsignedType(clippedSchema.asGroupType())
    if (readDataSchema.length > inputTable.getNumberOfColumns
        || hasDecimalAsInt || hasUnsignedType) {
      // Spark+Parquet schema evolution is relatively simple with only adding/removing columns
      // To type casting or anything like that
      val clippedGroups = clippedSchema.asGroupType()
      val newColumns = new Array[ColumnVector](readDataSchema.length)
      try {
        withResource(inputTable) { table =>
          var readAt = 0
          (0 until readDataSchema.length).foreach(writeAt => {
            val readField = readDataSchema(writeAt)
            if (areNamesEquiv(clippedGroups, readAt, readField.name, isSchemaCaseSensitive)) {
              val origCol = table.getColumn(readAt)
              val col: ColumnVector = if (hasDecimalAsInt || hasUnsignedType) {
                ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(origCol, readField.dataType,
                  (dt, cv) => needDecimalCast(cv, dt) || needUnsignedToSignedCast(cv, dt),
                  (dt, cv) => decimalCastOrUnsignedCast(cv, dt))
              } else {
                origCol.incRefCount()
              }
              newColumns(writeAt) = col
              readAt += 1
            } else {
              newColumns(writeAt) =
                GpuColumnVector.columnVectorFromNull(table.getRowCount.toInt, readField.dataType)
            }
          })
          if (readAt != table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Could not find the expected columns " +
              s"$readAt out of ${table.getNumberOfColumns} from $filePath")
          }
        }
        new Table(newColumns: _*)
      } finally {
        newColumns.safeClose()
      }
    } else {
      inputTable
    }
  }

  /**
   * Need to convert cudf unsigned integer to wider signed integer that Spark expects
   * After Spark 3.2.0, Spark reads uint8 as int16, uint16 as int32, uint32 as int64
   * TODO uint64 -> Decimal(20,0) depends CUDF, see issue #3475
   *
   * @param group the schema
   * @return if has unsigned integer
   */
  def existsUnsignedType(group: GroupType): Boolean = {
    group.getFields.asScala.exists(
      field => {
        if (field.isPrimitive) {
          val t = field.getOriginalType
          (t == OriginalType.UINT_8) || (t == OriginalType.UINT_16) || (t == OriginalType.UINT_32)
        } else {
          existsUnsignedType(field.asGroupType)
        }
      }
    )
  }

  def needDecimalCast(cv: ColumnView, dt: DataType): Boolean = {
    cv.getType.isDecimalType && !GpuColumnVector.getNonNestedRapidsType(dt).equals(cv.getType())
  }

  def needUnsignedToSignedCast(cv: ColumnView, dt: DataType): Boolean = {
    (cv.getType.equals(DType.UINT8) && dt.isInstanceOf[ShortType]) ||
      (cv.getType.equals(DType.UINT16) && dt.isInstanceOf[IntegerType]) ||
      (cv.getType.equals(DType.UINT32) && dt.isInstanceOf[LongType])
  }

  // Will do cast if needDecimalCast or needUnsignedToSignedCast test is true
  // in ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB.
  // Note: The behavior of unsigned to signed is decided by the Spark,
  // this means the parameter dt is from Spark meta module.
  // This implements the requested type behavior accordingly for GPU.
  // This is suitable for all Spark versions, no need to add to shim layer.
  private def decimalCastOrUnsignedCast(cv: ColumnView, dt: DataType): ColumnView = {
    if (needDecimalCast(cv, dt)) {
      cv.castTo(DecimalUtil.createCudfDecimal(dt.asInstanceOf[DecimalType]))
    } else if (needUnsignedToSignedCast(cv, dt)) {
      cv.castTo(DType.create(GpuColumnVector.getNonNestedRapidsType(dt).getTypeId))
    } else {
      throw new IllegalStateException("Logical error: should only be " +
        "decimal cast or unsigned to signed cast")
    }
  }

  protected def readPartFile(
      blocks: Seq[BlockMetaData],
      clippedSchema: MessageType,
      filePath: Path): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Parquet buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      withResource(filePath.getFileSystem(conf).open(filePath)) { in =>
        val estTotalSize = calculateParquetOutputSize(blocks, clippedSchema, false)
        closeOnExcept(HostMemoryBuffer.allocate(estTotalSize)) { hmb =>
          val out = new HostMemoryOutputStream(hmb)
          out.write(ParquetPartitionReader.PARQUET_MAGIC)
          val outputBlocks = copyBlocksData(in, out, blocks, out.getPos)
          val footerPos = out.getPos
          writeFooter(out, outputBlocks, clippedSchema)

          BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
          out.write(ParquetPartitionReader.PARQUET_MAGIC)
          // check we didn't go over memory
          if (out.getPos > estTotalSize) {
            throw new QueryExecutionException(s"Calculated buffer size $estTotalSize is to " +
              s"small, actual written: ${out.getPos}")
          }
          (hmb, out.getPos)
        }
      }
    }
  }

  protected def populateCurrentBlockChunk(
      blockIter: BufferedIterator[BlockMetaData],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long): Seq[BlockMetaData] = {
    val currentChunk = new ArrayBuffer[BlockMetaData]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.getRowCount
            numParquetBytes += currentChunk.last.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
      s"Estimated GPU bytes: $numBytes")
    currentChunk
  }

  /**
   * Take case-sensitive into consideration when getting the data reading column names
   * before sending parquet-formatted buffer to cudf.
   *
   * @param readDataSchema Spark schema to read
   * @param fileSchema the schema of the dumped parquet-formatted buffer
   * @param isCaseSensitive if it is case sensitive
   * @return a sequence of column names following the order of readDataSchema
   */
  protected def toCudfColumnNames(
      readDataSchema: StructType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean): Seq[String] = {

    if (!isCaseSensitive) {
      val fields = fileSchema.asGroupType().getFields.asScala.map(_.getName).toSet
      val m = CaseInsensitiveMap(fields.zip(fields).toMap)
      // The schemas may be different among parquet files, so some column names may not be existing
      // in the CaseInsensitiveMap. In that case, we just use the field name of readDataSchema
      //
      // For hive special case, the readDataSchema is lower case, we need to do
      // the case insensitive conversion
      // See https://github.com/NVIDIA/spark-rapids/pull/3982#issue-770410779
      readDataSchema.fieldNames.map { name => m.get(name).getOrElse(name) }
    } else {
      readDataSchema.fieldNames.toSeq
    }
  }
}

// Singleton threadpool that is used across all the tasks.
// Please note that the TaskContext is not set in these threads and should not be used.
object ParquetMultiFileThreadPoolFactory {
  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      threadTag: String,
      numThreads: Int): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      threadPool = Some(MultiFileThreadPoolUtil.createThreadPool(threadTag, numThreads))
    }
    threadPool.get
  }

  def getThreadPool(threadTag: String, numThreads: Int): ThreadPoolExecutor = {
    threadPool.getOrElse(initThreadPool(threadTag, numThreads))
  }
}

// Parquet schema wrapper
private case class ParquetSchemaWrapper(schema: MessageType) extends SchemaBase

// Parquet BlockMetaData wrapper
private case class ParquetDataBlock(dataBlock: BlockMetaData) extends DataBlockBase {
  override def getRowCount: Long = dataBlock.getRowCount
  override def getReadDataSize: Long = dataBlock.getTotalByteSize
  override def getBlockSize: Long = dataBlock.getColumns.asScala.map(_.getTotalSize).sum
}

/** Parquet extra information containing isCorrectedRebaseMode */
case class ParquetExtraInfo(isCorrectedRebaseMode: Boolean,
    isCorrectedInt96RebaseMode: Boolean, hasInt96Timestamps: Boolean) extends ExtraInfo

// contains meta about a single block in a file
private case class ParquetSingleDataBlockMeta(
  filePath: Path,
  dataBlock: ParquetDataBlock,
  partitionValues: InternalRow,
  schema: ParquetSchemaWrapper,
  extraInfo: ParquetExtraInfo) extends SingleDataBlockInfo

/**
 * A PartitionReader that can read multiple Parquet files up to the certain size. It will
 * coalesce small files together and copy the block data in a separate thread pool to speed
 * up processing the small files before sending down to the GPU.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param splits the partitioned files to read
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param numThreads the size of the threadpool
 */
class MultiFileParquetPartitionReader(
    override val conf: Configuration,
    splits: Array[PartitionedFile],
    clippedBlocks: Seq[ParquetSingleDataBlockMeta],
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedBlocks, readDataSchema,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads, execMetrics)
  with ParquetPartitionReaderBase {

  // Some implicits to convert the base class to the sub-class and vice versa
  implicit def toMessageType(schema: SchemaBase): MessageType =
    schema.asInstanceOf[ParquetSchemaWrapper].schema

  implicit def toBlockMetaData(block: DataBlockBase): BlockMetaData =
    block.asInstanceOf[ParquetDataBlock].dataBlock

  implicit def toDataBlockBase(blocks: Seq[BlockMetaData]): Seq[DataBlockBase] =
    blocks.map(ParquetDataBlock(_))

  implicit def toBlockMetaDataSeq(blocks: Seq[DataBlockBase]): Seq[BlockMetaData] =
    blocks.map(_.asInstanceOf[ParquetDataBlock].dataBlock)

  implicit def ParquetSingleDataBlockMeta(in: ExtraInfo): ParquetExtraInfo =
    in.asInstanceOf[ParquetExtraInfo]

  // The runner to copy blocks to offset of HostMemoryBuffer
  class ParquetCopyBlocksRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long)
    extends Callable[(Seq[DataBlockBase], Long)] {

    override def call(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        val startBytesRead = fileSystemBytesRead()
        val res = withResource(outhmb) { _ =>
          withResource(new HostMemoryOutputStream(outhmb)) { out =>
            withResource(file.getFileSystem(conf).open(file)) { in =>
              copyBlocksData(in, out, blocks, offset)
            }
          }
        }
        val bytesRead = fileSystemBytesRead() - startBytesRead
        (res, bytesRead)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def checkIfNeedToSplitDataBlock(currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    // We need to ensure all files we are going to combine have the same datetime
    // rebase mode.
    if (nextBlockInfo.extraInfo.isCorrectedRebaseMode !=
        currentBlockInfo.extraInfo.isCorrectedRebaseMode &&
        nextBlockInfo.extraInfo.isCorrectedInt96RebaseMode !=
            currentBlockInfo.extraInfo.isCorrectedInt96RebaseMode) {
      logInfo(s"datetime rebase mode for the next file ${nextBlockInfo.filePath} is different " +
          s"then current file ${currentBlockInfo.filePath}, splitting into another batch.")
      return true
    }

    val schemaNextFile =
      nextBlockInfo.schema.asGroupType().getFields.asScala.map(_.getName)
    val schemaCurrentfile =
      currentBlockInfo.schema.asGroupType().getFields.asScala.map(_.getName)

    if (!schemaNextFile.sameElements(schemaCurrentfile)) {
      logInfo(s"File schema for the next file ${nextBlockInfo.filePath}" +
        s" doesn't match current ${currentBlockInfo.filePath}, splitting it into another batch!")
      return true
    }
    false
  }

  override def calculateEstimatedBlocksOutputSize(
      filesAndBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      schema: SchemaBase): Long = {
    val allBlocks = filesAndBlocks.values.flatten.toSeq
    // Some Parquet versions sanity check the block metadata, and since the blocks could be from
    // multiple files they will not pass the checks as they are.
    val blockStartOffset = ParquetPartitionReader.PARQUET_MAGIC.length
    val updatedBlocks = computeBlockMetaData(allBlocks, blockStartOffset)
    calculateParquetOutputSize(updatedBlocks, schema, true)
  }

  override def getThreadPool(numThreads: Int): ThreadPoolExecutor = {
    ParquetMultiFileThreadPoolFactory.getThreadPool(getFileFormatShortName, numThreads)
  }

  override def getBatchRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long): Callable[(Seq[DataBlockBase], Long)] = {
    new ParquetCopyBlocksRunner(taskContext, file, outhmb, blocks, offset)
  }

  override final def getFileFormatShortName: String = "Parquet"

  override def readBufferToTable(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, extraInfo: ExtraInfo): Table = {

    // Dump parquet data into a file
    dumpDataToFile(dataBuffer, dataSize, splits, Option(debugDumpPrefix), Some("parquet"))

    val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
      isSchemaCaseSensitive)
    val parseOpts = ParquetOptions.builder()
      .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
      .includeColumn(includeColumns: _*).build()

    // About to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

    val table = withResource(new NvtxWithMetrics(s"$getFileFormatShortName decode",
      NvtxColor.DARK_GREEN, metrics(GPU_DECODE_TIME))) { _ =>
      Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
    }

    closeOnExcept(table) { _ =>
      GpuParquetScanBase.throwIfNeeded(
        table,
        extraInfo.isCorrectedInt96RebaseMode,
        extraInfo.isCorrectedRebaseMode,
        extraInfo.hasInt96Timestamps)
    }
    evolveSchemaIfNeededAndClose(table, splits.mkString(","), clippedSchema)
  }

  override def writeFileHeader(buffer: HostMemoryBuffer): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      out.write(ParquetPartitionReader.PARQUET_MAGIC)
      out.getPos
    }
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: Seq[DataBlockBase], schema: SchemaBase): Long = {

    val actualFooterSize = calculateParquetFooterSize(blocks, schema)
    // 4 + 4 is for writing size and the ending PARQUET_MAGIC.
    footerOffset + actualFooterSize + 4 + 4
  }

  override def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], clippedSchema: SchemaBase): (HostMemoryBuffer, Long) = {

    val lenLeft = bufferSize - footerOffset

    val finalSize = closeOnExcept(buffer) { _ =>
      withResource(buffer.slice(footerOffset, lenLeft)) { finalizehmb =>
        withResource(new HostMemoryOutputStream(finalizehmb)) { footerOut =>
          writeFooter(footerOut, blocks, clippedSchema)
          BytesUtils.writeIntLittleEndian(footerOut, footerOut.getPos.toInt)
          footerOut.write(ParquetPartitionReader.PARQUET_MAGIC)
          footerOffset + footerOut.getPos
        }
      }
    }
    (buffer, finalSize)
  }
}

/**
 * A PartitionReader that can read multiple Parquet files in parallel. This is most efficient
 * running in a cloud environment where the I/O of reading is slow.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param numThreads the size of the threadpool
 * @param maxNumFileProcessed the maximum number of files to read on the CPU side and waiting to be
 *                            processed on the GPU. This affects the amount of host memory used.
 * @param filterHandler GpuParquetFileFilterHandler used to filter the parquet blocks
 * @param filters filters passed into the filterHandler
 */
class MultiFileCloudParquetPartitionReader(
    override val conf: Configuration,
    files: Array[PartitionedFile],
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    maxNumFileProcessed: Int,
    filterHandler: GpuParquetFileFilterHandler,
    filters: Array[Filter])
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, filters,
    execMetrics) with ParquetPartitionReaderBase {

  case class HostMemoryBuffersWithMetaData(
      override val partitionedFile: PartitionedFile,
      override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
      override val bytesRead: Long,
      isCorrectRebaseMode: Boolean,
      isCorrectInt96RebaseMode: Boolean,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      taskContext: TaskContext,
      filterHandler: GpuParquetFileFilterHandler,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends Callable[HostMemoryBuffersWithMetaDataBase] with Logging {

    private var blockChunkIter: BufferedIterator[BlockMetaData] = null

    /**
     * Returns the host memory buffers and file meta data for the file processed.
     * If there was an error then the error field is set. If there were no blocks the buffer
     * is returned as null.  If there were no columns but rows (count() operation) then the
     * buffer is null and the size is the number of rows.
     *
     * Note that the TaskContext is not set in these threads and should not be used.
     */
    override def call(): HostMemoryBuffersWithMetaData = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        doRead()
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaData = {
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      try {
        val fileBlockMeta = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
        if (fileBlockMeta.blocks.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          return HostMemoryBuffersWithMetaData(file, Array((null, 0)), bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema)
        }
        blockChunkIter = fileBlockMeta.blocks.iterator.buffered
        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryBuffersWithMetaData(file, Array((null, 0)), bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema)
        } else {
          if (readDataSchema.isEmpty) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = fileBlockMeta.blocks.map(_.getRowCount).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryBuffersWithMetaData(file, Array((null, numRows)), bytesRead,
              fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
              fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema)
          } else {
            val filePath = new Path(new URI(file.filePath))
            while (blockChunkIter.hasNext) {
              val blocksToRead = populateCurrentBlockChunk(blockChunkIter,
                maxReadBatchSizeRows, maxReadBatchSizeBytes)
              hostBuffers += readPartFile(blocksToRead, fileBlockMeta.schema, filePath)
            }
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            if (isDone) {
              // got close before finishing
              hostBuffers.foreach(_._1.safeClose())
              HostMemoryBuffersWithMetaData(file, Array((null, 0)), bytesRead,
                fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
                fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema)
            } else {
              HostMemoryBuffersWithMetaData(file, hostBuffers.toArray, bytesRead,
                fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
                fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema)
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.foreach(_._1.safeClose())
          throw e
      }
    }
  }

  /**
   * File reading logic in a Callable which will be running in a thread pool
   *
   * @param tc      task context to use
   * @param file    file to be read
   * @param conf    configuration
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(tc, filterHandler, file, conf, filters)
  }

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * @param  numThreads  max number of threads to create
   * @return ThreadPoolExecutor
   */
  override def getThreadPool(numThreads: Int): ThreadPoolExecutor = {
    ParquetMultiFileThreadPoolFactory.getThreadPool(getFileFormatShortName, numThreads)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "Parquet"

  /**
   * Decode HostMemoryBuffers by GPU
   *
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return Option[ColumnarBatch]
   */
  override def readBatch(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
      Option[ColumnarBatch] = {
    fileBufsAndMeta match {
      case buffer: HostMemoryBuffersWithMetaData =>
        val memBuffersAndSize = buffer.memBuffersAndSizes
        val (hostBuffer, size) = memBuffersAndSize.head
        val nextBatch = readBufferToTable(buffer.isCorrectRebaseMode,
          buffer.isCorrectInt96RebaseMode, buffer.hasInt96Timestamps, buffer.clippedSchema,
          buffer.partitionedFile.partitionValues,
          hostBuffer, size, buffer.partitionedFile.filePath)
        if (memBuffersAndSize.length > 1) {
          val updatedBuffers = memBuffersAndSize.drop(1)
          currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
        } else {
          currentFileHostBuffers = None
        }
        nextBatch
      case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
    }
  }


  private def readBufferToTable(
      isCorrectRebaseMode: Boolean,
      isCorrectInt96RebaseMode: Boolean,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      partValues: InternalRow,
      hostBuffer: HostMemoryBuffer,
      dataSize: Long,
      fileName: String): Option[ColumnarBatch] = {
    // not reading any data, but add in partition data if needed
    if (hostBuffer == null) {
      // Someone is going to process this data, even if it is just a row count
      GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
      val emptyBatch = new ColumnarBatch(Array.empty, dataSize.toInt)
      return addPartitionValues(Some(emptyBatch), partValues, partitionSchema)
    }
    val table = withResource(hostBuffer) { _ =>

      // Dump parquet data into a file
      dumpDataToFile(hostBuffer, dataSize, files, Option(debugDumpPrefix), Some("parquet"))

      val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
        isSchemaCaseSensitive)
      val parseOpts = ParquetOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .includeColumn(includeColumns: _*).build()

      // about to start using the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

      val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
        metrics(GPU_DECODE_TIME))) { _ =>
        Table.readParquet(parseOpts, hostBuffer, 0, dataSize)
      }
      closeOnExcept(table) { _ =>
        GpuParquetScanBase.throwIfNeeded(table, isCorrectInt96RebaseMode, isCorrectRebaseMode,
          hasInt96Timestamps)
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
        if (readDataSchema.length < table.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $fileName")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      Some(evolveSchemaIfNeededAndClose(table, fileName, clippedSchema))
    }
    try {
      val colTypes = readDataSchema.fields.map(f => f.dataType)
      val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
      maybeBatch.foreach { batch =>
        logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
      }
      // we have to add partition values here for this batch, we already verified that
      // its not different for all the blocks in this batch
      addPartitionValues(maybeBatch, partValues, partitionSchema)
    } finally {
      table.foreach(_.close())
    }
  }
}

/**
 * A PartitionReader that reads a Parquet file split on the GPU.
 *
 * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
 * in memory that contains just the column chunks that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param split the file split to read
 * @param filePath the path to the Parquet file
 * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
 *                      to only contain the column chunks to be read
 * @param clippedParquetSchema the Parquet schema from the original Parquet file that has been
 *                             clipped to contain only the columns to be read
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
 */
class ParquetPartitionReader(
    override val conf: Configuration,
    split: PartitionedFile,
    filePath: Path,
    clippedBlocks: Seq[BlockMetaData],
    clippedParquetSchema: MessageType,
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    isCorrectedInt96RebaseMode: Boolean,
    isCorrectedRebaseMode: Boolean,
    hasInt96Timestamps: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with ParquetPartitionReaderBase {

  private val blockIterator:  BufferedIterator[BlockMetaData] = clippedBlocks.iterator.buffered
  private var isFirstBatch = true

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
        metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
      } else {
        batch = readBatch()
      }
    }
    if (isFirstBatch) {
      if (batch.isEmpty) {
        // This is odd, but some operators return data even when there is no input so we need to
        // be sure that we grab the GPU if there were no batches.
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
      }
      isFirstBatch = false
    }
    batch.isDefined
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("Parquet readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes)
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.getRowCount).sum.toInt
        if (numRows == 0) {
          None
        } else {
          Some(new ColumnarBatch(Array.empty, numRows.toInt))
        }
      } else {
        val table = readToTable(currentChunkedBlocks)
        try {
          val colTypes = readDataSchema.fields.map(f => f.dataType)
          val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          maybeBatch
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(currentChunkedBlocks: Seq[BlockMetaData]): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    val (dataBuffer, dataSize) = readPartFile(currentChunkedBlocks, clippedParquetSchema, filePath)
    try {
      if (dataSize == 0) {
        None
      } else {

        // Dump parquet data into a file
        dumpDataToFile(dataBuffer, dataSize, Array(split), Option(debugDumpPrefix), Some("parquet"))

        val includeColumns = toCudfColumnNames(readDataSchema, clippedParquetSchema,
          isSchemaCaseSensitive)
        val parseOpts = ParquetOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .includeColumn(includeColumns: _*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

        val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
            metrics(GPU_DECODE_TIME))) { _ =>
          Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        }
        closeOnExcept(table) { _ =>
          GpuParquetScanBase.throwIfNeeded(table, isCorrectedInt96RebaseMode, isCorrectedRebaseMode,
            hasInt96Timestamps)
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $filePath")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(evolveSchemaIfNeededAndClose(table, filePath.toString, clippedParquetSchema))
      }
    } finally {
      dataBuffer.close()
    }
  }
}

object ParquetPartitionReader {
  private[rapids] val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private[rapids] val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private[rapids] val PARQUET_VERSION = 1

  private[rapids] case class CopyRange(offset: Long, length: Long)

  /**
   * Build a new BlockMetaData
   *
   * @param rowCount the number of rows in this block
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  private[rapids] def newParquetBlock(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }

  /**
   * Trim block metadata to contain only the column chunks that occur in the specified columns.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param columnPaths the paths of columns to preserve
   * @param blocks the block metadata from the original Parquet file
   * @param isCaseSensitive indicate if it is case sensitive
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  private[spark] def clipBlocks(columnPaths: Seq[ColumnPath],
      blocks: Seq[BlockMetaData], isCaseSensitive: Boolean): Seq[BlockMetaData] = {
    val pathSet = if (isCaseSensitive) {
      columnPaths.map(cp => cp.toDotString).toSet
    } else {
      columnPaths.map(cp => cp.toDotString.toLowerCase(Locale.ROOT)).toSet
    }
    blocks.map(oldBlock => {
      //noinspection ScalaDeprecation
      val newColumns = if (isCaseSensitive) {
        oldBlock.getColumns.asScala.filter(c =>
          pathSet.contains(c.getPath.toDotString))
      } else {
        oldBlock.getColumns.asScala.filter(c =>
          pathSet.contains(c.getPath.toDotString.toLowerCase(Locale.ROOT)))
      }
      ParquetPartitionReader.newParquetBlock(oldBlock.getRowCount, newColumns)
    })
  }
}

