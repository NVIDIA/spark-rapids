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

import java.io.{File, OutputStream}
import java.net.{URI, URISyntaxException}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Locale}
import java.util.concurrent._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, LinkedHashMap}
import scala.math.max

import ai.rapids.cudf._
import ai.rapids.cudf.DType.DTypeEnum
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.RebaseHelper
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.ParquetPartitionReader.CopyRange
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, Type, Types}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetReadSupport}
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
      field.dataType match {
        case MapType(_, _, _) | ArrayType(_, _) | StructType(_) =>
          TrampolineUtil.dataTypeExistsRecursively(field.dataType, isTsOrDate)
        case _ => false
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
    partValues: InternalRow, schema: MessageType, isCorrectedRebaseMode: Boolean)

// contains meta about a single block in a file
private case class ParquetFileInfoWithSingleBlockMeta(filePath: Path, blockMeta: BlockMetaData,
    partValues: InternalRow, schema: MessageType, isCorrectedRebaseMode: Boolean)

private case class GpuParquetFileFilterHandler(@transient sqlConf: SQLConf) extends Arm {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val isCorrectedRebase =
    "CORRECTED" == ShimLoader.getSparkShims.parquetRebaseRead(sqlConf)

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
      val parquetFilters = new ParquetFilters(fileSchema, pushDownDate, pushDownTimestamp,
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
      filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
    } else {
      None
    }

    val isCorrectedRebaseForThisFile =
    GpuParquetPartitionReaderFactoryBase.isCorrectedRebaseMode(
      footer.getFileMetaData.getKeyValueMetaData.get, isCorrectedRebase)

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
    val clipped = ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala)
    ParquetFileInfoWithBlockMeta(filePath, clipped, file.partitionValues,
      clippedSchema, isCorrectedRebaseForThisFile)
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
    queryUsesInputFile: Boolean) extends PartitionReaderFactory with Arm with Logging {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val numThreads = rapidsConf.parquetMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel
  private val canUseMultiThreadReader = rapidsConf.isParquetMultiThreadReadEnabled
  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  private val canUseCoalesceFilesReader =
    rapidsConf.isParquetCoalesceFileReadEnabled && !queryUsesInputFile

  private val configCloudSchemes = rapidsConf.getCloudSchemes
  private val CLOUD_SCHEMES = HashSet("dbfs", "s3", "s3a", "s3n", "wasbs", "gs")
  private val allCloudSchemes = CLOUD_SCHEMES ++ configCloudSchemes.getOrElse(Seq.empty)

  private val filterHandler = new GpuParquetFileFilterHandler(sqlConf)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  private def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
    } catch {
      case _: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // We expect the filePath here to always have a scheme on it,
  // if it doesn't we try using the local filesystem. If that
  // doesn't work for some reason user would need to configure
  // it directly.
  private def isCloudFileSystem(filePath: String): Boolean = {
    val uri = resolveURI(filePath)
    val scheme = uri.getScheme
    if (allCloudSchemes.contains(scheme)) {
      true
    } else {
      false
    }
  }

  private def arePathsInCloud(filePaths: Array[String]): Boolean = {
    filePaths.exists(isCloudFileSystem)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val files = filePartition.files
    val filePaths = files.map(_.filePath)
    val conf = broadcastedConf.value.value
    if (!canUseCoalesceFilesReader || (canUseMultiThreadReader && arePathsInCloud(filePaths))) {
      logInfo("Using the multi-threaded multi-file parquet reader, files: " +
        s"${filePaths.mkString(",")} task attemptid: ${TaskContext.get.taskAttemptId()}")
      buildBaseColumnarParquetReaderForCloud(files, conf)
    } else {
      logInfo("Using the coalesce multi-file parquet reader, files: " +
        s"${filePaths.mkString(",")} task attemptid: ${TaskContext.get.taskAttemptId()}")
      buildBaseColumnarParquetReader(files)
    }
  }

  private def buildBaseColumnarParquetReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    new MultiFileCloudParquetPartitionReader(conf, files,
      isCaseSensitive, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics, partitionSchema,
      numThreads, maxNumFileProcessed, filterHandler, filters)
  }

  private def buildBaseColumnarParquetReader(
      files: Array[PartitionedFile]): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val clippedBlocks = ArrayBuffer[ParquetFileInfoWithSingleBlockMeta]()
    files.map { file =>
      val singleFileInfo = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
      clippedBlocks ++= singleFileInfo.blocks.map(
        ParquetFileInfoWithSingleBlockMeta(singleFileInfo.filePath, _, file.partitionValues,
          singleFileInfo.schema, singleFileInfo.isCorrectedRebaseMode))
    }

    new MultiFileParquetPartitionReader(conf, files, clippedBlocks,
      isCaseSensitive, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics,
      partitionSchema, numThreads)
  }
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
      maxReadBatchSizeBytes, metrics, singleFileInfo.isCorrectedRebaseMode)
  }
}

/**
 * Base classes with common functions for MultiFileParquetPartitionReader and ParquetPartitionReader
 */
abstract class FileParquetPartitionReaderBase(
    override val conf: Configuration,
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    execMetrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ParquetPartitionReaderBase with ScanWithMetrics with Arm {

  protected var isDone: Boolean = false
  protected var maxDeviceMemory: Long = 0
  protected var batch: Option[ColumnarBatch] = None
  metrics = execMetrics

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    isDone = true
  }

}

trait ParquetPartitionReaderBase extends Logging with Arm with ScanWithMetrics
    with MultiFileReaderFunctions {

  def conf: Configuration

  def readDataSchema: StructType

  def isSchemaCaseSensitive: Boolean

  val copyBufferSize = conf.getInt("parquet.read.allocation.size", 8 * 1024 * 1024)

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
   * Copies the data corresponding to the clipped blocks in the original file and compute the
   * block metadata for the output. The output blocks will contain the same column chunk
   * metadata but with the file offsets updated to reflect the new position of the column data
   * as written to the output.
   *
   * @param in  the input stream for the original Parquet file
   * @param out the output stream to receive the data
   * @return updated block metadata corresponding to the output
   */
  protected def copyBlocksData(
      in: FSDataInputStream,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData],
      realStartOffset: Long): Seq[BlockMetaData] = {
    var totalRows: Long = 0
    val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
    val copyRanges = new ArrayBuffer[CopyRange]
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
            copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
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
      copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
    }
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
    val typeCastingNeeded = precisions.exists(p => p <= Decimal.MAX_INT_DIGITS)
    if (readDataSchema.length > inputTable.getNumberOfColumns || typeCastingNeeded) {
      // Spark+Parquet schema evolution is relatively simple with only adding/removing columns
      // To type casting or anyting like that
      val clippedGroups = clippedSchema.asGroupType()
      val newColumns = new Array[ColumnVector](readDataSchema.length)
      val precisionList = scala.collection.mutable.Queue(precisions: _*)
      try {
        withResource(inputTable) { table =>
          var readAt = 0
          (0 until readDataSchema.length).foreach(writeAt => {
            val readField = readDataSchema(writeAt)
            if (areNamesEquiv(clippedGroups, readAt, readField.name, isSchemaCaseSensitive)) {
              val origCol = table.getColumn(readAt)
              val col = if (typeCastingNeeded && precisionList.nonEmpty) {
                convertDecimal64ToDecimal32Wrapper(origCol, precisionList.dequeue())
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
   * This method casts the input ColumnView to a new column if it contains Decimal data that
   * could be stored in smaller data type. e.g. a DecimalType(7,2) stored as 64-bit DecimalType can
   * easily be stored in a 32-bit DecimalType
   * @param cv              The column view that could potentially have Decimal64 columns with
   *                        precision < 10
   * @param precision   precisions of all the decimal columns in this Column
   * @return
   */
  private def convertDecimal64ToDecimal32Wrapper(cv: ColumnVector, precision: Int): ColumnVector = {
    /*
     * 'convertDecimal64ToDecimal32' method returns a ColumnView that should be copied out to a
     * ColumnVector  before closing the `toClose` views otherwise it will close the returned view
     * and it's children as well.
     */
    def convertDecimal64ToDecimal32(
       cv: ColumnView,
       precision: Int,
       toClose: ArrayBuffer[ColumnView]): ColumnView = {
      val dt = cv.getType
      if (!dt.isNestedType) {
        if (dt.getTypeId == DTypeEnum.DECIMAL64 && precision <= DType.DECIMAL32_MAX_PRECISION) {
          // we want to handle the legacy case where Decimals are written as an array of bytes
          // cudf reads them back as a 64-bit Decimal
          // castTo will create a new ColumnVector which we cannot close until we have copied out
          // the entire nested type. So we store them temporarily in this array and close it after
          // `copyToColumnVector` is called
          val col = cv.castTo(DecimalUtil.createCudfDecimal(precision, -dt.getScale()))
          toClose += col
          col
        } else {
          cv
        }
      } else if (dt == DType.LIST) {
        val child = cv.getChildColumnView(0)
        toClose += child
        val newChild = convertDecimal64ToDecimal32(child, precision, toClose)
        if (child == newChild) {
          cv
        } else {
          val newView = cv.replaceListChild(newChild)
          toClose += newView
          newView
        }
      } else if (dt == DType.STRUCT) {
        val newColumns = ArrayBuilder.make[ColumnView]()
        newColumns.sizeHint(cv.getNumChildren)
        val newColIndices = ArrayBuilder.make[Int]()
        newColIndices.sizeHint(cv.getNumChildren)
        (0 until cv.getNumChildren).foreach { i =>
          val child = cv.getChildColumnView(i)
          toClose += child
          val newChild = convertDecimal64ToDecimal32(child, precision, toClose)
          if (newChild != child) {
            newColumns += newChild
            newColIndices += i
          }
        }
        val cols = newColumns.result()
        if (cols.nonEmpty) {
          // create a new struct column with the new ones
          val newView = cv.replaceChildrenWithViews(newColIndices.result(), cols)
          toClose += newView
          newView
        } else {
          cv
        }
      } else {
        throw new IllegalArgumentException(s"Unsupported data type ${dt.getTypeId}")
      }
    }

    withResource(new ArrayBuffer[ColumnView]) { toClose =>
      val tmp = convertDecimal64ToDecimal32(cv, precision, toClose)
      if (tmp != cv) {
        tmp.copyToColumnVector()
      } else {
        tmp.asInstanceOf[ColumnVector].incRefCount()
      }
    }
  }

  protected def dumpParquetData(
      hmb: HostMemoryBuffer,
      dataLength: Long,
      splits: Array[PartitionedFile],
      debugDumpPrefix: String): Unit = {
    val (out, path) = FileUtils.createTempFile(conf, debugDumpPrefix, ".parquet")
    try {
      logInfo(s"Writing Parquet split data for $splits to $path")
      val in = new HostMemoryInputStream(hmb, dataLength)
      IOUtils.copy(in, out)
    } finally {
      out.close()
    }
  }

  protected def readPartFile(
      blocks: Seq[BlockMetaData],
      clippedSchema: MessageType,
      filePath: Path): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
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

// Singleton threadpool that is used across all the tasks.
// Please note that the TaskContext is not set in these threads and should not be used.
object MultiFileThreadPoolFactory {

  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      maxThreads: Int = 20,
      keepAliveSeconds: Long = 60): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      val threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("parquet reader worker-%d")
        .setDaemon(true)
        .build()

      threadPool = Some(new ThreadPoolExecutor(
        maxThreads, // corePoolSize: max number of threads to create before queuing the tasks
        maxThreads, // maximumPoolSize: because we use LinkedBlockingDeque, this is not used
        keepAliveSeconds,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable],
        threadFactory))
      threadPool.get.allowCoreThreadTimeOut(true)
    }
    threadPool.get
  }

  def submitToThreadPool[T](task: Callable[T], numThreads: Int): Future[T] = {
    val pool = threadPool.getOrElse(initThreadPool(numThreads))
    pool.submit(task)
  }
}

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
    clippedBlocks: Seq[ParquetFileInfoWithSingleBlockMeta],
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int)
  extends FileParquetPartitionReaderBase(conf, isSchemaCaseSensitive, readDataSchema,
    debugDumpPrefix, execMetrics) {

  private val blockIterator: BufferedIterator[ParquetFileInfoWithSingleBlockMeta] =
    clippedBlocks.iterator.buffered

  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  class ParquetCopyBlocksRunner(
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[BlockMetaData],
      offset: Long)
    extends Callable[(Seq[BlockMetaData], Long)] {

    override def call(): (Seq[BlockMetaData], Long) = {
      val startBytesRead = fileSystemBytesRead()
      val out = new HostMemoryOutputStream(outhmb)
      val res = withResource(file.getFileSystem(conf).open(file)) { in =>
        copyBlocksData(in, out, blocks, offset)
      }
      outhmb.close()
      val bytesRead = fileSystemBytesRead() - startBytesRead
      (res, bytesRead)
    }
  }

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
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  private def reallocHostBufferAndCopy(
      in: HostMemoryInputStream,
      newSizeEstimate: Long): HostMemoryBuffer = {
    // realloc memory and copy
    closeOnExcept(HostMemoryBuffer.allocate(newSizeEstimate)) { newhmb =>
      val newout = new HostMemoryOutputStream(newhmb)
      IOUtils.copy(in, newout)
      newout.close()
      newhmb
    }
  }

  private def readPartFiles(
      blocks: Seq[(Path, BlockMetaData)],
      clippedSchema: MessageType): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      // ugly but we want to keep the order
      val filesAndBlocks = LinkedHashMap[Path, ArrayBuffer[BlockMetaData]]()
      blocks.foreach { case (path, block) =>
        filesAndBlocks.getOrElseUpdate(path, new ArrayBuffer[BlockMetaData]) += block
      }
      val tasks = new java.util.ArrayList[Future[(Seq[BlockMetaData], Long)]]()

      val allBlocks = blocks.map(_._2)
      val initTotalSize = calculateParquetOutputSize(allBlocks, clippedSchema, true)
      closeOnExcept(HostMemoryBuffer.allocate(initTotalSize)) { allocBuf =>
        var hmb = allocBuf
        val out = new HostMemoryOutputStream(hmb)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        var offset = out.getPos
        val allOutputBlocks = scala.collection.mutable.ArrayBuffer[BlockMetaData]()
        filesAndBlocks.foreach { case (file, blocks) =>
          val fileBlockSize = blocks.flatMap(_.getColumns.asScala.map(_.getTotalSize)).sum
          // use a single buffer and slice it up for different files if we need
          val outLocal = hmb.slice(offset, fileBlockSize)
          // copy the blocks for each file in parallel using background threads
          tasks.add(MultiFileThreadPoolFactory.submitToThreadPool(
            new ParquetCopyBlocksRunner(file, outLocal, blocks, offset),
            numThreads))
          offset += fileBlockSize
        }

        for (future <- tasks.asScala) {
          val (blocks, bytesRead) = future.get()
          allOutputBlocks ++= blocks
          TrampolineUtil.incBytesRead(inputMetrics, bytesRead)
        }

        // The footer size can change vs the initial estimated because we are combining more blocks
        //  and offsets are larger, check to make sure we allocated enough memory before writing.
        // Not sure how expensive this is, we could throw exception instead if the written
        // size comes out > then the estimated size.
        val actualFooterSize = calculateParquetFooterSize(allOutputBlocks, clippedSchema)
        // 4 + 4 is for writing size and the ending PARQUET_MAGIC.
        val bufferSizeReq = offset + actualFooterSize + 4 + 4
        out.close()
        val totalBufferSize = if (bufferSizeReq > initTotalSize) {
          logWarning(s"The original estimated size $initTotalSize is to small, " +
            s"reallocing and copying data to bigger buffer size: $bufferSizeReq")
          val prevhmb = hmb
          val in = new HostMemoryInputStream(prevhmb, offset)
          hmb = reallocHostBufferAndCopy(in, bufferSizeReq)
          prevhmb.close()
          bufferSizeReq
        } else {
          initTotalSize
        }
        val lenLeft = totalBufferSize - offset
        val finalizehmb = hmb.slice(offset, lenLeft)
        val footerOut = new HostMemoryOutputStream(finalizehmb)
        writeFooter(footerOut, allOutputBlocks, clippedSchema)
        BytesUtils.writeIntLittleEndian(footerOut, footerOut.getPos.toInt)
        footerOut.write(ParquetPartitionReader.PARQUET_MAGIC)
        val amountWritten = offset + footerOut.getPos
        footerOut.close()
        // triple check we didn't go over memory
        if (amountWritten > totalBufferSize) {
           throw new QueryExecutionException(s"Calculated buffer size $totalBufferSize is to " +
            s"small, actual written: ${amountWritten}")
        }
        if (finalizehmb != null) {
          finalizehmb.close()
        }
        (hmb, amountWritten)
      }
    }
  }

  private def buildAndConcatPartitionColumns(
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): Array[GpuColumnVector] = {
    val numCols = partitionSchema.fields.length
    val allPartCols = new Array[GpuColumnVector](numCols)
    // build the partitions vectors for all partitions within each column
    // and concatenate those together then go to the next column
    for ((field, colIndex) <- partitionSchema.fields.zipWithIndex) {
      val dataType = field.dataType
      withResource(new Array[GpuColumnVector](inPartitionValues.length)) {
        partitionColumns =>
          for ((rowsInPart, partIndex) <- rowsPerPartition.zipWithIndex) {
            val partInternalRow = inPartitionValues(partIndex)
            val partValueForCol = partInternalRow.get(colIndex, dataType)
            val partitionScalar = GpuScalar.from(partValueForCol, dataType)
            withResource(partitionScalar) { scalar =>
              partitionColumns(partIndex) = GpuColumnVector.from(
                ai.rapids.cudf.ColumnVector.fromScalar(scalar, rowsInPart.toInt),
                dataType)
            }
          }
          val baseOfCols = partitionColumns.map(_.getBase)
          allPartCols(colIndex) = GpuColumnVector.from(
            ColumnVector.concatenate(baseOfCols: _*), field.dataType)
      }
    }
    allPartCols
  }

  private def concatAndAddPartitionColsToBatch(
      cb: ColumnarBatch,
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): ColumnarBatch = {
    withResource(cb) { _ =>
      closeOnExcept(buildAndConcatPartitionColumns(rowsPerPartition, inPartitionValues)) {
        allPartCols =>
          ColumnarPartitionReaderWithPartitionValues.addGpuColumVectorsToBatch(cb, allPartCols)
      }
    }
  }

  /**
   * Add all partition values found to the batch. There could be more then one partition
   * value in the batch so we have to build up columns with the correct number of rows
   * for each partition value.
   *
   * @param batch - columnar batch to append partition values to
   * @param inPartitionValues - array of partition values
   * @param rowsPerPartition - the number of rows that require each partition value
   * @param partitionSchema - schema of the partitions
   * @return
   */
  protected def addAllPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: Array[InternalRow],
      rowsPerPartition: Array[Long],
      partitionSchema: StructType): Option[ColumnarBatch] = {
    assert(rowsPerPartition.length == inPartitionValues.length)
    if (partitionSchema.nonEmpty) {
      batch.map { cb =>
        val numPartitions = inPartitionValues.length
        if (numPartitions > 1) {
          concatAndAddPartitionColsToBatch(cb, rowsPerPartition, inPartitionValues)
        } else {
          // single partition, add like other readers
          addPartitionValues(Some(cb), inPartitionValues.head, partitionSchema).get
        }
      }
    } else {
      batch
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("Parquet readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkMeta = populateCurrentBlockChunk()
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        if (currentChunkMeta.numTotalRows == 0) {
          None
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val emptyBatch = new ColumnarBatch(Array.empty, currentChunkMeta.numTotalRows.toInt)
          addAllPartitionValues(Some(emptyBatch), currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        }
      } else {
        val table = readToTable(currentChunkMeta.currentChunk, currentChunkMeta.clippedSchema,
          currentChunkMeta.isCorrectRebaseMode)
        try {
          val colTypes = readDataSchema.fields.map(f => f.dataType)
          val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          addAllPartitionValues(maybeBatch, currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(
      currentChunkedBlocks: Seq[(Path, BlockMetaData)],
      clippedSchema: MessageType,
      isCorrectRebaseMode: Boolean): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    val (dataBuffer, dataSize) = readPartFiles(currentChunkedBlocks, clippedSchema)
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpParquetData(dataBuffer, dataSize, splits, debugDumpPrefix)
        }
        val parseOpts = ParquetOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .enableStrictDecimalType(true)
          .includeColumn(readDataSchema.fieldNames:_*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
          metrics(GPU_DECODE_TIME))) { _ =>
          Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        }
        closeOnExcept(table) { _ =>
          if (!isCorrectRebaseMode) {
            (0 until table.getNumberOfColumns).foreach { i =>
              if (RebaseHelper.isDateTimeRebaseNeededRead(table.getColumn(i))) {
                throw RebaseHelper.newRebaseExceptionInRead("Parquet")
              }
            }
          }
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $currentChunkedBlocks")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(evolveSchemaIfNeededAndClose(table, splits.mkString(","), clippedSchema))
      }
    } finally {
      dataBuffer.close()
    }
  }

  private case class CurrentChunkMeta(
      isCorrectRebaseMode: Boolean,
      clippedSchema: MessageType,
      currentChunk: Seq[(Path, BlockMetaData)],
      numTotalRows: Long,
      rowsPerPartition: Array[Long],
      allPartValues: Array[InternalRow])

  private def populateCurrentBlockChunk(): CurrentChunkMeta = {
    val currentChunk = new ArrayBuffer[(Path, BlockMetaData)]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numParquetBytes: Long = 0
    var currentFile: Path = null
    var currentPartitionValues: InternalRow = null
    var currentClippedSchema: MessageType = null
    var currentIsCorrectRebaseMode: Boolean = false
    val rowsPerPartition = new ArrayBuffer[Long]()
    var lastPartRows: Long = 0
    val allPartValues = new ArrayBuffer[InternalRow]()

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        if (currentFile == null) {
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partValues
          allPartValues += currentPartitionValues
          currentClippedSchema = blockIterator.head.schema
          currentIsCorrectRebaseMode = blockIterator.head.isCorrectedRebaseMode
        }
        val peekedRowGroup = blockIterator.head.blockMeta
        if (peekedRowGroup.getRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }

        if (numRows == 0 || numRows + peekedRowGroup.getRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.getRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            // only care to check if we are actually adding in the next chunk
            if (currentFile != blockIterator.head.filePath) {
              // We need to ensure all files we are going to combine have the same datetime
              // rebase mode.
              if (blockIterator.head.isCorrectedRebaseMode != currentIsCorrectRebaseMode) {
                logInfo("datetime rebase mode for the next file " +
                  s"${blockIterator.head.filePath} is different then current file $currentFile, " +
                  s"splitting into another batch.")
                return
              }
              val schemaNextfile =
                blockIterator.head.schema.asGroupType().getFields.asScala.map(_.getName)
              val schemaCurrentfile =
                currentClippedSchema.asGroupType().getFields.asScala.map(_.getName)
              if (!(schemaNextfile == schemaCurrentfile)) {
                logInfo(s"File schema for the next file ${blockIterator.head.filePath}" +
                  s" doesn't match current $currentFile, splitting it into another batch!")
                return
              }
              // If the partition values are different we have to track the number of rows that get
              // each partition value and the partition values themselves so that we can build
              // the full columns with different partition values later.
              if (blockIterator.head.partValues != currentPartitionValues) {
                rowsPerPartition += (numRows - lastPartRows)
                lastPartRows = numRows
                // we add the actual partition values here but then
                // the number of rows in that partition at the end or
                // when the partition changes
                allPartValues += blockIterator.head.partValues
              }
              currentFile = blockIterator.head.filePath
              currentPartitionValues = blockIterator.head.partValues
              currentClippedSchema = blockIterator.head.schema
            }

            val nextBlock = blockIterator.next()
            val nextTuple = (nextBlock.filePath, nextBlock.blockMeta)
            currentChunk += nextTuple
            numRows += currentChunk.last._2.getRowCount
            numParquetBytes += currentChunk.last._2.getTotalByteSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    rowsPerPartition += (numRows - lastPartRows)
    logDebug(s"Loaded $numRows rows from Parquet. Parquet bytes read: $numParquetBytes. " +
      s"Estimated GPU bytes: $numBytes. Number of different partitions: ${allPartValues.size}")
    CurrentChunkMeta(currentIsCorrectRebaseMode, currentClippedSchema, currentChunk,
      numRows, rowsPerPartition.toArray, allPartValues.toArray)
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
    execMetrics) with ParquetPartitionReaderBase with MultiFileReaderFunctions {

  case class HostMemoryBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
    override val bytesRead: Long,
    isCorrectRebaseMode: Boolean,
    clippedSchema: MessageType) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(filterHandler: GpuParquetFileFilterHandler,
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
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      try {
        val fileBlockMeta = filterHandler.filterBlocks(file, conf, filters, readDataSchema)
        if (fileBlockMeta.blocks.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          return HostMemoryBuffersWithMetaData(file, Array((null, 0)), bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.schema)
        }
        blockChunkIter = fileBlockMeta.blocks.iterator.buffered
        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryBuffersWithMetaData(file, Array((null, 0)), bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.schema)
        } else {
          if (readDataSchema.isEmpty) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = fileBlockMeta.blocks.map(_.getRowCount).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryBuffersWithMetaData(file, Array((null, numRows)), bytesRead,
              fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.schema)
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
                fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.schema)
            } else {
              HostMemoryBuffersWithMetaData(file, hostBuffers.toArray, bytesRead,
                fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.schema)
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
   * @param file    file to be read
   * @param conf    configuration
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(file: PartitionedFile, conf: Configuration, filters: Array[Filter]):
      Callable[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(filterHandler, file, conf, filters)
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
  override def getFileFormatShortName: String = "Parquet"

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
          buffer.clippedSchema, buffer.partitionedFile.partitionValues,
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
      clippedSchema: MessageType,
      partValues: InternalRow,
      hostBuffer: HostMemoryBuffer,
      dataSize: Long,
      fileName: String): Option[ColumnarBatch] = {
    // not reading any data, but add in partition data if needed
    if (hostBuffer == null) {
      // Someone is going to process this data, even if it is just a row count
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val emptyBatch = new ColumnarBatch(Array.empty, dataSize.toInt)
      return addPartitionValues(Some(emptyBatch), partValues, partitionSchema)
    }
    val table = withResource(hostBuffer) { _ =>
      if (debugDumpPrefix != null) {
        dumpParquetData(hostBuffer, dataSize, files, debugDumpPrefix)
      }
      val parseOpts = ParquetOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .enableStrictDecimalType(true)
        .includeColumn(readDataSchema.fieldNames: _*).build()

      // about to start using the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
        metrics(GPU_DECODE_TIME))) { _ =>
        Table.readParquet(parseOpts, hostBuffer, 0, dataSize)
      }
      closeOnExcept(table) { _ =>
        if (!isCorrectRebaseMode) {
          (0 until table.getNumberOfColumns).foreach { i =>
            if (RebaseHelper.isDateTimeRebaseNeededRead(table.getColumn(i))) {
              throw RebaseHelper.newRebaseExceptionInRead("Parquet")
            }
          }
        }
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
    isCorrectedRebaseMode: Boolean)  extends
  FileParquetPartitionReaderBase(conf,
    isSchemaCaseSensitive, readDataSchema, debugDumpPrefix, execMetrics) {

  private val blockIterator:  BufferedIterator[BlockMetaData] = clippedBlocks.iterator.buffered

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
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
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
        if (debugDumpPrefix != null) {
          dumpParquetData(dataBuffer, dataSize, Array(split), debugDumpPrefix)
        }
        val parseOpts = ParquetOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .enableStrictDecimalType(true)
          .includeColumn(readDataSchema.fieldNames:_*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
            metrics(GPU_DECODE_TIME))) { _ =>
          Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        }
        closeOnExcept(table) { _ =>
          if (!isCorrectedRebaseMode) {
            (0 until table.getNumberOfColumns).foreach { i =>
              if (RebaseHelper.isDateTimeRebaseNeededRead(table.getColumn(i))) {
                throw RebaseHelper.newRebaseExceptionInRead("Parquet")
              }
            }
          }
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
   * @return the updated block metadata with undesired column chunks removed
   */
  private[spark] def clipBlocks(columnPaths: Seq[ColumnPath],
      blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    val pathSet = columnPaths.toSet
    blocks.map(oldBlock => {
      //noinspection ScalaDeprecation
      val newColumns = oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath))
      ParquetPartitionReader.newParquetBlock(oldBlock.getRowCount, newColumns)
    })
  }
}
