/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

import java.io.{EOFException, FileNotFoundException, IOException, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Locale}
import java.util.concurrent._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.math.max

import ai.rapids.cudf._
import com.nvidia.spark.RebaseHelper
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.ParquetPartitionReader.CopyRange
import com.nvidia.spark.rapids.RapidsConf.ParquetFooterReaderType
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.ParquetFooter
import com.nvidia.spark.rapids.shims.{GpuParquetCrypto, GpuTypeShims, ParquetSchemaClipShims, ParquetStringPredShims, ShimFilePartitionReaderFactory, SparkShimImpl}
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.parquet.schema.{DecimalMetadata, GroupType, MessageType, OriginalType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, PartitioningAwareFileIndex, SchemaColumnConvertNotSupportedException}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * Base GpuParquetScan used for common code across Spark versions. Gpu version of
 * Spark's 'ParquetScan'.
 *
 * @param sparkSession SparkSession.
 * @param hadoopConf Hadoop configuration.
 * @param fileIndex File index of the relation.
 * @param dataSchema Schema of the data.
 * @param readDataSchema Schema to read.
 * @param readPartitionSchema Partition schema.
 * @param pushedFilters Filters on non-partition columns.
 * @param options Parquet option settings.
 * @param partitionFilters Filters on partition columns.
 * @param dataFilters File source metadata filters.
 * @param rapidsConf Rapids configuration.
 * @param queryUsesInputFile This is a parameter to easily allow turning it
 *                               off in GpuTransitionOverrides if InputFileName,
 *                               InputFileBlockStart, or InputFileBlockLength are used
 */
case class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
  extends ScanWithMetrics with FileScan with Logging {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isParquetPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        options.asScala.toMap)
    } else {
      GpuParquetMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        queryUsesInputFile)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: GpuParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
          equivalentFilters(pushedFilters, p.pushedFilters) && rapidsConf == p.rapidsConf &&
          queryUsesInputFile == p.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  // overrides nothing in 330
  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}

object GpuParquetScan {
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

    sqlConf.get(SparkShimImpl.int96ParquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${SparkShimImpl.int96ParquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${SparkShimImpl.int96ParquetRebaseReadKey} is LEGACY")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }

    sqlConf.get(SparkShimImpl.parquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${SparkShimImpl.parquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${SparkShimImpl.parquetRebaseReadKey} is LEGACY")
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

  /**
   * Remove the columns not in `fileSchema` from `clippedSchema`.
   * Note: if `useFieldId` is true, `clippedSchema` can contain `_fake_name_UUID` columns
   * that do not exist in `fileSchema`.
   */
  def filterClippedSchema(
      clippedSchema: MessageType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean,
      useFieldId: Boolean): MessageType = {
    val idMap = ParquetSchemaClipShims.fieldIdToFieldMap(useFieldId, fileSchema)

    val fs = fileSchema.asGroupType()
    val types = if (isCaseSensitive) {
      val inFile = fs.getFields.asScala.map(_.getName).toSet
      clippedSchema.asGroupType().getFields.asScala.filter { f =>
        if (useFieldId && f.getId != null) {
          // if use fieldID configuration and read schema field has specified `parquet.field.id`
          idMap.contains(f.getId.intValue())
        } else {
          inFile.contains(f.getName)
        }
      }
    } else {
      val inFile = fs.getFields.asScala
        .map(_.getName.toLowerCase(Locale.ROOT)).toSet
      clippedSchema.asGroupType().getFields.asScala.filter { f =>
        if (useFieldId && f.getId != null) {
          // if has `fieldID` configuration and read schema field has specified `parquet.field.id`
          idMap.contains(f.getId.intValue())
        } else {
          inFile.contains(f.getName.toLowerCase(Locale.ROOT))
        }
      }
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

/**
 * A parquet compatible stream that allows reading from a HostMemoryBuffer to Parquet.
 * The majority of the code here was copied from Parquet's DelegatingSeekableInputStream with
 * minor modifications to have it be make it Scala and call into the
 * HostMemoryInputStreamMixIn's state.
 */
class HMBSeekableInputStream(
    val hmb: HostMemoryBuffer,
    val hmbLength: Long) extends SeekableInputStream
    with HostMemoryInputStreamMixIn {
  private val temp = new Array[Byte](8192)

  override def seek(offset: Long): Unit = {
    pos = offset
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte]): Unit = {
    val amountRead = read(buffer)
    val remaining = buffer.length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte], offset: Int, length: Int): Unit = {
    val amountRead = read(buffer, offset, length)
    val remaining = length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def read(buf: ByteBuffer): Int =
    if (buf.hasArray) {
      readHeapBuffer(buf)
    } else {
      readDirectBuffer(buf)
    }

  @throws[IOException]
  override def readFully(buf: ByteBuffer): Unit = {
    if (buf.hasArray) {
      readFullyHeapBuffer(buf)
    } else {
      readFullyDirectBuffer(buf)
    }
  }

  private def readHeapBuffer(buf: ByteBuffer) = {
    val bytesRead = read(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    if (bytesRead < 0) {
      bytesRead
    } else {
      buf.position(buf.position() + bytesRead)
      bytesRead
    }
  }

  private def readFullyHeapBuffer(buf: ByteBuffer): Unit = {
    readFully(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    buf.position(buf.limit)
  }

  private def readDirectBuffer(buf: ByteBuffer): Int = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var totalBytesRead = 0
    var bytesRead = 0
    totalBytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (bytesRead == temp.length) {
      buf.put(temp)
      totalBytesRead += bytesRead

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        -1
      } else {
        totalBytesRead
      }
    } else {
      buf.put(temp, 0, bytesRead)
      totalBytesRead += bytesRead
      totalBytesRead
    }
  }

  private def readFullyDirectBuffer(buf: ByteBuffer): Unit = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var bytesRead = 0
    bytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (nextReadLength > 0 && bytesRead >= 0) {
      buf.put(temp, 0, bytesRead)

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0 && buf.remaining > 0) {
      throw new EOFException("Reached the end of stream with " +
          buf.remaining + " bytes left to read")
    }
  }
}

class HMBInputFile(buffer: HostMemoryBuffer) extends InputFile {

  override def getLength: Long = buffer.getLength

  override def newStream(): SeekableInputStream = new HMBSeekableInputStream(buffer, getLength)
}

private case class GpuParquetFileFilterHandler(@transient sqlConf: SQLConf) extends Arm {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  // From Spark 340, more string predicates are supported as push-down filters, so this
  // flag is renamed to 'xxxxStringPredicate' and specified by another config.
  private val pushDownStringPredicate = ParquetStringPredShims.pushDown(sqlConf)
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val rebaseMode = SparkShimImpl.parquetRebaseRead(sqlConf)
  private val isCorrectedRebase = "CORRECTED" == rebaseMode
  val int96RebaseMode = SparkShimImpl.int96ParquetRebaseRead(sqlConf)
  private val isInt96CorrectedRebase = "CORRECTED" == int96RebaseMode
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  private val ignoreMissingParquetFieldId = ParquetSchemaClipShims.ignoreMissingIds(sqlConf)
  private val timestampNTZEnabled = ParquetSchemaClipShims.timestampNTZEnabled(sqlConf)

  private val PARQUET_ENCRYPTION_CONFS = Seq("parquet.encryption.kms.client.class",
    "parquet.encryption.kms.client.class", "parquet.crypto.factory.class")
  private val PARQUET_MAGIC_ENCRYPTED = "PARE".getBytes(StandardCharsets.US_ASCII)

  def isParquetTimeInInt96(parquetType: Type): Boolean = {
    parquetType match {
      case p:PrimitiveType =>
        p.getPrimitiveTypeName == PrimitiveTypeName.INT96
      case g:GroupType => //GroupType
        g.getFields.asScala.exists(t => isParquetTimeInInt96(t))
      case _ => false
    }
  }

  /**
   * Convert the spark data type to something that the native processor can understand.
   */
  private def convertToParquetNative(schema: DataType): ParquetFooter.SchemaElement = {
    schema match {
      case cst: StructType =>
        val schemaBuilder = ParquetFooter.StructElement.builder()
        cst.fields.foreach { field =>
          schemaBuilder.addChild(field.name, convertToParquetNative(field.dataType))
        }
        schemaBuilder.build()
      case _: NumericType | BinaryType | BooleanType | DateType | TimestampType | StringType =>
        new ParquetFooter.ValueElement()
      case at: ArrayType =>
        new ParquetFooter.ListElement(convertToParquetNative(at.elementType))
      case mt: MapType =>
        new ParquetFooter.MapElement(
          convertToParquetNative(mt.keyType),
          convertToParquetNative(mt.valueType))
      case other =>
        throw new UnsupportedOperationException(s"Need some help here $other...")
    }
  }

  def convertToFooterSchema(schema: StructType): ParquetFooter.StructElement = {
    convertToParquetNative(schema).asInstanceOf[ParquetFooter.StructElement]
  }

  def readAndFilterFooter(
      file: PartitionedFile,
      conf : Configuration,
      readDataSchema: StructType,
      filePath: Path): ParquetFooter = {
    val footerSchema = convertToFooterSchema(readDataSchema)
    val fs = filePath.getFileSystem(conf)
    val stat = fs.getFileStatus(filePath)
    // Much of this code came from the parquet_mr projects ParquetFileReader, and was modified
    // to match our needs
    val fileLen = stat.getLen
    val FOOTER_LENGTH_SIZE = 4
    // MAGIC + data + footer + footerIndex + MAGIC
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    val footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length
    val footerBuffer = withResource(fs.open(filePath)) { inputStream =>
      withResource(new NvtxRange("ReadFooterBytes", NvtxColor.YELLOW)) { _ =>
        inputStream.seek(footerLengthIndex)
        val footerLength = readIntLittleEndian(inputStream)
        val magic = new Array[Byte](MAGIC.length)
        inputStream.readFully(magic)
        if (!util.Arrays.equals(MAGIC, magic)) {
          if (util.Arrays.equals(PARQUET_MAGIC_ENCRYPTED, magic)) {
            throw new RuntimeException("The GPU does not support reading encrypted Parquet " +
              "files. To read encrypted or columnar encrypted files, disable the GPU Parquet " +
              s"reader via ${RapidsConf.ENABLE_PARQUET_READ.key}.")
          } else {
            throw new RuntimeException(s"$filePath is not a Parquet file. " +
              s"Expected magic number at tail ${util.Arrays.toString(MAGIC)} " +
              s"but found ${util.Arrays.toString(magic)}")
          }
        }
        val footerIndex = footerLengthIndex - footerLength
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
          throw new RuntimeException(s"corrupted file: the footer index is not within " +
              s"the file: $footerIndex")
        }
        inputStream.seek(footerIndex)
        closeOnExcept(HostMemoryBuffer.allocate(footerLength, false)) { outBuffer =>
          val out = new HostMemoryOutputStream(outBuffer)
          val tmpBuffer = new Array[Byte](4096)
          var bytesLeft = footerLength
          while (bytesLeft > 0) {
            val readLength = Math.min(bytesLeft, tmpBuffer.length)
            inputStream.readFully(tmpBuffer, 0, readLength)
            out.write(tmpBuffer, 0, readLength)
            bytesLeft -= readLength
          }
          outBuffer
        }
      }
    }
    withResource(footerBuffer) { footerBuffer =>
      withResource(new NvtxRange("Parse and filter footer by range", NvtxColor.RED)) { _ =>
        val len = if (fileLen <= file.length) {
          // secret signal to skip filtering
          -1
        } else {
          file.length
        }
        ParquetFooter.readAndFilter(footerBuffer, file.start, len,
          footerSchema, !isCaseSensitive)
      }
    }
  }

  @scala.annotation.nowarn(
    "msg=method readFooter in class ParquetFileReader is deprecated"
  )
  def readAndSimpleFilterFooter(
      file: PartitionedFile,
      conf : Configuration,
      filePath: Path): ParquetMetadata = {
    //noinspection ScalaDeprecation
    withResource(new NvtxRange("readFooter", NvtxColor.YELLOW)) { _ =>
      ParquetFileReader.readFooter(conf, filePath,
        ParquetMetadataConverter.range(file.start, file.start + file.length))
    }
  }

  @scala.annotation.nowarn
  def filterBlocks(
      footerReader: ParquetFooterReaderType.Value,
      file: PartitionedFile,
      conf : Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): ParquetFileInfoWithBlockMeta = {
    withResource(new NvtxRange("filterBlocks", NvtxColor.PURPLE)) { _ =>
      val filePath = new Path(new URI(file.filePath))
      // Make sure we aren't trying to read encrypted files. For now, remove the related
      // parquet confs from the hadoop configuration and try to catch the resulting
      // exception and print a useful message
      PARQUET_ENCRYPTION_CONFS.foreach { encryptConf =>
        if (conf.get(encryptConf) != null) {
          conf.unset(encryptConf)
        }
      }
      val footer = try {
         footerReader match {
          case ParquetFooterReaderType.NATIVE =>
            val serialized = withResource(readAndFilterFooter(file, conf,
              readDataSchema, filePath)) { tableFooter =>
                if (tableFooter.getNumColumns <= 0) {
                  // Special case because java parquet reader does not like having 0 columns.
                  val numRows = tableFooter.getNumRows
                  val block = new BlockMetaData()
                  block.setRowCount(numRows)
                  val schema = new MessageType("root")
                  return ParquetFileInfoWithBlockMeta(filePath, Seq(block), file.partitionValues,
                    schema, false, false, false)
                }

                tableFooter.serializeThriftFile()
            }
            withResource(serialized) { serialized =>
              withResource(new NvtxRange("readFilteredFooter", NvtxColor.YELLOW)) { _ =>
                val inputFile = new HMBInputFile(serialized)

                // We already filtered the ranges so no need to do more here...
                ParquetFileReader.readFooter(inputFile, ParquetMetadataConverter.NO_FILTER)
              }
            }
          case _ =>
            readAndSimpleFilterFooter(file, conf, filePath)
        }
      } catch {
        case e if GpuParquetCrypto.isColumnarCryptoException(e) =>
          throw new RuntimeException("The GPU does not support reading encrypted Parquet " +
            "files. To read encrypted or columnar encrypted files, disable the GPU Parquet " +
            s"reader via ${RapidsConf.ENABLE_PARQUET_READ.key}.", e)
      }

      val fileSchema = footer.getFileMetaData.getSchema

      // check spark.sql.parquet.fieldId.read.ignoreMissing
      ParquetSchemaClipShims.checkIgnoreMissingIds(ignoreMissingParquetFieldId, fileSchema,
        readDataSchema)

      val pushedFilters = if (enableParquetFilterPushDown) {
        val parquetFilters = SparkShimImpl.getParquetFilters(fileSchema, pushDownDate,
          pushDownTimestamp, pushDownDecimal, pushDownStringPredicate, pushDownInFilterThreshold,
          isCaseSensitive, footer.getFileMetaData.getKeyValueMetaData.get, rebaseMode)
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
        withResource(new NvtxRange("getBlocksWithFilter", NvtxColor.CYAN)) { _ =>
          // Use the ParquetFileReader to perform dictionary-level filtering
          ParquetInputFormat.setFilterPredicate(conf, pushedFilters.get)
          //noinspection ScalaDeprecation
          withResource(new ParquetFileReader(conf, footer.getFileMetaData, filePath,
            footer.getBlocks, Collections.emptyList[ColumnDescriptor])) { parquetReader =>
            parquetReader.getRowGroups
          }
        }
      } else {
        footer.getBlocks
      }

      val (clipped, clippedSchema) =
        withResource(new NvtxRange("clipSchema", NvtxColor.DARK_GREEN)) { _ =>
          val clippedSchemaTmp = ParquetSchemaClipShims.clipSchema(fileSchema, readDataSchema,
            isCaseSensitive, readUseFieldId, timestampNTZEnabled)
          // Check if the read schema is compatible with the file schema.
          checkSchemaCompat(clippedSchemaTmp, readDataSchema,
            (t: Type, d: DataType) => throwTypeIncompatibleError(t, d, file.filePath),
            isCaseSensitive, readUseFieldId)

          // ParquetSchemaClipShims.clipSchema does most of what we want, but it includes
          // everything in readDataSchema, even if it is not in fileSchema we want to remove those
          // for our own purposes
          val clippedSchema =
          GpuParquetPartitionReaderFactoryBase.filterClippedSchema(clippedSchemaTmp,
            fileSchema, isCaseSensitive, readUseFieldId)
          val columnPaths = clippedSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
          val clipped =
            ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala, isCaseSensitive)
          (clipped, clippedSchema)
        }

      ParquetFileInfoWithBlockMeta(filePath, clipped, file.partitionValues,
        clippedSchema, isCorrectedInt96RebaseForThisFile, isCorrectedRebaseForThisFile,
        hasInt96Timestamps)
    }
  }

  /**
   * Recursively check if the read schema is compatible with the file schema. The errorCallback
   * will be invoked to throw an exception once any incompatible type pairs are found.
   *
   * The function assumes all elements in read schema are included in file schema, so please
   * run this check after clipping read schema upon file schema.
   *
   * The function only accepts top-level schemas, which means structures of root columns. Based
   * on this assumption, it can infer root types from input schemas.
   *
   * @param fileType input file's Parquet schema
   * @param readType spark type read from Parquet file
   * @param errorCallback call back function to throw exception if type mismatch
   * @param rootFileType file type of each root column
   * @param rootReadType read type of each root column
   */
  private def checkSchemaCompat(fileType: Type,
                                readType: DataType,
                                errorCallback: (Type, DataType) => Unit,
                                isCaseSensitive: Boolean,
                                useFieldId: Boolean,
                                rootFileType: Option[Type] = None,
                                rootReadType: Option[DataType] = None): Unit = {
    readType match {
      case struct: StructType =>
        val fileFieldMap = fileType.asGroupType().getFields.asScala
          .map { f =>
            (if (isCaseSensitive) f.getName else f.getName.toLowerCase(Locale.ROOT)) -> f
          }.toMap

        val fieldIdToFieldMap = ParquetSchemaClipShims.fieldIdToFieldMap(useFieldId, fileType)

        def getParquetType(f: StructField): Type = {
          if(useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
            // use field ID and Spark schema specified field ID
            // Note: can always get a result from the map.
            // For unmatched field ID column `fileType` has a `_fake_name_UUID` column.
            fieldIdToFieldMap(ParquetSchemaClipShims.getFieldId(f))
          } else {
            fileFieldMap(if (isCaseSensitive) f.name else f.name.toLowerCase(Locale.ROOT))
          }
        }
        struct.fields.foreach { f =>
          val curFile = getParquetType(f)
          checkSchemaCompat(curFile,
            f.dataType,
            errorCallback,
            isCaseSensitive,
            useFieldId,
            // Record root types for each column, so as to throw a readable exception
            // over nested types.
            Some(rootFileType.getOrElse(curFile)),
            Some(rootReadType.getOrElse(f.dataType)))
        }
      case array: ArrayType =>
        val fileChild = fileType.asGroupType().getType(0)
          .asGroupType().getType(0)
        checkSchemaCompat(fileChild, array.elementType, errorCallback, isCaseSensitive, useFieldId,
          rootFileType, rootReadType)

      case map: MapType =>
        val parquetMap = fileType.asGroupType().getType(0).asGroupType()
        val parquetMapKey = parquetMap.getType(0)
        val parquetMapValue = parquetMap.getType(1)
        checkSchemaCompat(parquetMapKey, map.keyType, errorCallback, isCaseSensitive, useFieldId,
          rootFileType, rootReadType)
        checkSchemaCompat(parquetMapValue, map.valueType, errorCallback, isCaseSensitive,
          useFieldId, rootFileType, rootReadType)

      case dt =>
        checkPrimitiveCompat(fileType.asPrimitiveType(),
          dt,
          () => errorCallback(rootFileType.get, rootReadType.get))
    }
  }

  /**
   * Check the compatibility over primitive types. This function refers to the `getUpdater` method
   * of [[org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory]].
   *
   * To avoid unnecessary pattern matching, this function is designed to return or throw ASAP.
   *
   * This function uses some deprecated Parquet APIs, because Spark 3.1 is relied on parquet-mr
   * of an older version.
   */
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def checkPrimitiveCompat(pt: PrimitiveType,
                                   dt: DataType,
                                   errorCallback: () => Unit): Unit = {
    pt.getPrimitiveTypeName match {
      case PrimitiveTypeName.BOOLEAN if dt == DataTypes.BooleanType =>
        return

      case PrimitiveTypeName.INT32 =>
        if (dt == DataTypes.IntegerType || GpuTypeShims.isSupportedYearMonthType(dt)
            || canReadAsIntDecimal(pt, dt)) {
          // Year-month interval type is stored as int32 in parquet
          return
        }
        // TODO: After we deprecate Spark 3.1, replace OriginalType with LogicalTypeAnnotation
        if (dt == DataTypes.LongType && pt.getOriginalType == OriginalType.UINT_32) {
          return
        }
         if (dt == DataTypes.ByteType || dt == DataTypes.ShortType || dt == DataTypes.DateType) {
           return
         }

      case PrimitiveTypeName.INT64 =>
        if (dt == DataTypes.LongType || GpuTypeShims.isSupportedDayTimeType(dt) ||
            // Day-time interval type is stored as int64 in parquet
            canReadAsLongDecimal(pt, dt)) {
          return
        }
        // TODO: After we deprecate Spark 3.1, replace OriginalType with LogicalTypeAnnotation
        if (isLongDecimal(dt) && pt.getOriginalType == OriginalType.UINT_64) {
          return
        }
        if (pt.getOriginalType == OriginalType.TIMESTAMP_MICROS ||
          pt.getOriginalType == OriginalType.TIMESTAMP_MILLIS) {
          return
        }

      case PrimitiveTypeName.FLOAT if dt == DataTypes.FloatType =>
        return

      case PrimitiveTypeName.DOUBLE if dt == DataTypes.DoubleType =>
        return

      case PrimitiveTypeName.INT96 if dt == DataTypes.TimestampType =>
        return

      case PrimitiveTypeName.BINARY if dt == DataTypes.StringType ||
        dt == DataTypes.BinaryType || canReadAsBinaryDecimal(pt, dt) =>
        return

      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY if canReadAsIntDecimal(pt, dt) ||
        canReadAsLongDecimal(pt, dt) || canReadAsBinaryDecimal(pt, dt) =>
        return

      case _ =>
    }

    // If we get here, it means the combination of Spark and Parquet type is invalid or not
    // supported.
    errorCallback()
  }

  private def throwTypeIncompatibleError(parquetType: Type,
                                         sparkType: DataType,
                                         filePath: String): Unit = {
    val exception = new SchemaColumnConvertNotSupportedException(
      parquetType.getName,
      parquetType.toString,
      sparkType.catalogString)

    // A copy of QueryExecutionErrors.unsupportedSchemaColumnConvertError introduced in 3.2+
    // TODO: replace with unsupportedSchemaColumnConvertError after we deprecate Spark 3.1
    val message = "Parquet column cannot be converted in " +
      s"file $filePath. Column: ${parquetType.getName}, " +
      s"Expected: ${sparkType.catalogString}, Found: $parquetType"
    throw new QueryExecutionException(message, exception)
  }

  private def isLongDecimal(dt: DataType): Boolean =
    dt match {
      case d: DecimalType => d.precision == 20 && d.scale == 0
      case _ => false
    }

  // TODO: After we deprecate Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def canReadAsIntDecimal(pt: PrimitiveType, dt: DataType) = {
    DecimalType.is32BitDecimalType(dt) && isDecimalTypeMatched(pt.getDecimalMetadata, dt)
  }

  // TODO: After we deprecate Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def canReadAsLongDecimal(pt: PrimitiveType, dt: DataType): Boolean = {
    DecimalType.is64BitDecimalType(dt) && isDecimalTypeMatched(pt.getDecimalMetadata, dt)
  }

  // TODO: After we deprecate Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=method getDecimalMetadata in class PrimitiveType is deprecated")
  private def canReadAsBinaryDecimal(pt: PrimitiveType, dt: DataType): Boolean = {
    DecimalType.isByteArrayDecimalType(dt) && isDecimalTypeMatched(pt.getDecimalMetadata, dt)
  }

  // TODO: After we deprecate Spark 3.1, fetch decimal meta with DecimalLogicalTypeAnnotation
  @scala.annotation.nowarn("msg=class DecimalMetadata in package schema is deprecated")
  private def isDecimalTypeMatched(metadata: DecimalMetadata,
                                   sparkType: DataType): Boolean = {
    if (metadata == null) {
      false
    } else {
      val dt = sparkType.asInstanceOf[DecimalType]
      metadata.getPrecision <= dt.precision && metadata.getScale == dt.scale
    }
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
  private val numThreads = rapidsConf.multiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel
  private val footerReadType = rapidsConf.parquetReaderFooterType
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  private val filterHandler = GpuParquetFileFilterHandler(sqlConf)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isParquetCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

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
    new MultiFileCloudParquetPartitionReader(conf, footerReadType, files,
      isCaseSensitive, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics, partitionSchema,
      numThreads, maxNumFileProcessed, filterHandler, filters,
      ignoreMissingFiles, ignoreCorruptFiles, readUseFieldId)
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
      val singleFileInfo = try {
        filterHandler.filterBlocks(footerReadType, file, conf, filters, readDataSchema)
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.filePath}", e)
          ParquetFileInfoWithBlockMeta(new Path(new URI(file.filePath)), Seq.empty,
            file.partitionValues, null, false, false, false)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        // If ignoreMissingFiles=true, this case will never be reached. But it's ok
        // to leave this branch here.
        case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
          ParquetFileInfoWithBlockMeta(new Path(new URI(file.filePath)), Seq.empty,
            file.partitionValues, null, false, false, false)
      }
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
      partitionSchema, numThreads, ignoreMissingFiles, ignoreCorruptFiles,
      readUseFieldId)
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
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) with Arm with Logging {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val footerReadType = rapidsConf.parquetReaderFooterType

  private val filterHandler = GpuParquetFileFilterHandler(sqlConf)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)

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
    val singleFileInfo = filterHandler.filterBlocks(footerReadType, file, conf, filters,
      readDataSchema)
    new ParquetPartitionReader(conf, file, singleFileInfo.filePath, singleFileInfo.blocks,
      singleFileInfo.schema, isCaseSensitive, readDataSchema,
      debugDumpPrefix, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics, singleFileInfo.isCorrectedInt96RebaseMode,
      singleFileInfo.isCorrectedRebaseMode, singleFileInfo.hasInt96Timestamps, readUseFieldId)
  }
}

trait ParquetPartitionReaderBase extends Logging with Arm with ScanWithMetrics
    with MultiFileReaderFunctions {

  // Configuration
  def conf: Configuration
  def execMetrics: Map[String, GpuMetric]

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
    var readTime = 0L
    var writeTime = 0L
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    var bytesLeft = range.length
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      val start = System.nanoTime()
      in.readFully(copyBuffer, 0, readLength)
      val mid = System.nanoTime()
      out.write(copyBuffer, 0, readLength)
      val end = System.nanoTime()
      readTime += (mid - start)
      writeTime += (end - mid)
      bytesLeft -= readLength
    }
    execMetrics.get(READ_FS_TIME).foreach(_.add(readTime))
    execMetrics.get(WRITE_BUFFER_TIME).foreach(_.add(writeTime))
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

  /**
   * Check if field ID matches if `useFieldId` is true, or check if name matches
   */
  protected def areFieldIDsOrNamesEquiv(useFieldId: Boolean, groups: GroupType, index: Int,
      readField: StructField, isCaseSensitive: Boolean): Boolean = {
    if (useFieldId && ParquetSchemaClipShims.hasFieldId(readField)) {
      val actualId = groups.getFields.get(index).getId
      // check if field ID matches
      actualId != null && actualId.intValue() == ParquetSchemaClipShims.getFieldId(readField)
    } else {
      val otherName = readField.name
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
      useFieldId: Boolean,
      inputTable: Table,
      filePath: String,
      clippedSchema: MessageType): Table = {

    val precisions = getPrecisionsList(clippedSchema.asGroupType().getFields.asScala)
    // check if there are cols with precision that can be stored in an int
    val hasDecimalAsInt = precisions.exists(p => p <= Decimal.MAX_INT_DIGITS)
    val hasUnsignedType = existsUnsignedType(clippedSchema.asGroupType())
    val hasInt32Downcast = existsInt32Downcast(readDataSchema)
    val needExtraCast = hasDecimalAsInt || hasUnsignedType || hasInt32Downcast

    if (readDataSchema.length > inputTable.getNumberOfColumns || needExtraCast) {
      // Spark+Parquet schema evolution is relatively simple with only adding/removing columns
      // To type casting or anything like that
      val clippedGroups = clippedSchema.asGroupType()
      val newColumns = new Array[ColumnVector](readDataSchema.length)
      try {
        withResource(inputTable) { table =>
          var readAt = 0
          (0 until readDataSchema.length).foreach(writeAt => {
            val readField = readDataSchema(writeAt)
            if (areFieldIDsOrNamesEquiv(useFieldId, clippedGroups, readAt, readField,
              isSchemaCaseSensitive)) {
              val origCol = table.getColumn(readAt)
              val col: ColumnVector = if (needExtraCast) {
                ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(origCol, readField.dataType,
                  (dt, cv) => needDecimalCast(cv, dt) || needUnsignedToSignedCast(cv, dt) ||
                    needInt32Downcast(cv, dt),
                  (dt, cv) => evolveSchemaCasts(cv, dt))
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
          (t == OriginalType.UINT_8) || (t == OriginalType.UINT_16) ||
            (t == OriginalType.UINT_32) || (t == OriginalType.UINT_64)
        } else {
          existsUnsignedType(field.asGroupType)
        }
      }
    )
  }

  /**
   * ByteType/ShortType/DateType are physically stored as INT32 in parquet files. We need to do
   * extra conversion on these types, since cuDF reads these columns as INT32.
   */
  def existsInt32Downcast(dt: DataType): Boolean =
    dt match {
      case struct: StructType =>
        struct.fields.exists(f => existsInt32Downcast(f.dataType))
      case array: ArrayType =>
        existsInt32Downcast(array.elementType)
      case map: MapType =>
        existsInt32Downcast(map.keyType) || existsInt32Downcast(map.valueType)
      case ByteType | ShortType | DateType =>
        true
      case _ =>
        false
    }

  def needDecimalCast(cv: ColumnView, dt: DataType): Boolean = {
    // UINT64 is casted to Decimal(20,0) by Spark to accommodate
    // the largest possible values this type can take. Other Unsigned data types are converted to
    // basic types like LongType, this is analogous to that except we spill over to large
    // decimal/ints.
    cv.getType.isDecimalType && !GpuColumnVector.getNonNestedRapidsType(dt).equals(cv.getType()) ||
      cv.getType.equals(DType.UINT64)
  }

  def needUnsignedToSignedCast(cv: ColumnView, dt: DataType): Boolean = {
    (cv.getType.equals(DType.UINT8) && dt.isInstanceOf[ShortType]) ||
      (cv.getType.equals(DType.UINT16) && dt.isInstanceOf[IntegerType]) ||
      (cv.getType.equals(DType.UINT32) && dt.isInstanceOf[LongType])
  }

  def needInt32Downcast(cv: ColumnView, dt: DataType): Boolean = {
    cv.getType.equals(DType.INT32) && Seq(ByteType, ShortType, DateType).contains(dt)
  }

  // Wrap up all required casts for Parquet schema evolution
  //
  // Note: The behavior of unsigned to signed is decided by the Spark,
  // this means the parameter dt is from Spark meta module.
  // This implements the requested type behavior accordingly for GPU.
  // This is suitable for all Spark versions, no need to add to shim layer.
  private def evolveSchemaCasts(cv: ColumnView, dt: DataType): ColumnView = {
    if (needDecimalCast(cv, dt)) {
      cv.castTo(DecimalUtil.createCudfDecimal(dt.asInstanceOf[DecimalType]))
    } else if (needUnsignedToSignedCast(cv, dt)) {
      cv.castTo(DType.create(GpuColumnVector.getNonNestedRapidsType(dt).getTypeId))
    } else if (needInt32Downcast(cv, dt)) {
      cv.castTo(DType.create(GpuColumnVector.getNonNestedRapidsType(dt).getTypeId))
    } else {
      throw new IllegalStateException("Logical error: no valid casts are found")
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
   * Also clips the column names if `useFieldId` is true.
   *
   * @param readDataSchema Spark schema to read
   * @param fileSchema the schema of the dumped parquet-formatted buffer, already removed unmatched
   *
   * @param isCaseSensitive if it is case sensitive
   * @param useFieldId if enabled `spark.sql.parquet.fieldId.read.enabled`
   * @return a sequence of column names following the order of readDataSchema
   */
  protected def toCudfColumnNames(
      readDataSchema: StructType,
      fileSchema: MessageType,
      isCaseSensitive: Boolean,
      useFieldId: Boolean): Seq[String] = {

    // map from field ID to the parquet column name
    val fieldIdToNameMap = ParquetSchemaClipShims.fieldIdToNameMap(useFieldId, fileSchema)

    // if use field id, clip the unused reading column names
    // e.g.:  reading schema is:
    //  StructType(
    //    StructField("mapped_c1", IntegerType, metadata={'parquet.field.id': 1}),
    //    StructField("mapped_c2", IntegerType, metadata={'parquet.field.id': 55}),
    //    StructField("c3", IntegerType))
    //  File schema is:
    //    message spark_schema {
    //      optional int32 c1 = 1 (field is is 1),
    //      optional int32 c2 = 2 (field is is 2),
    //      optional int32 c3,
    //    }
    //  ID = 55 not matched, returns ["c1", "c3"]

    // excludes unmatched columns
    val clippedReadFields = readDataSchema.fields.filter(f => !(useFieldId &&
        ParquetSchemaClipShims.hasFieldId(f) &&
        !fieldIdToNameMap.contains(ParquetSchemaClipShims.getFieldId(f))))

    if (!isCaseSensitive) {
      val fields = fileSchema.asGroupType().getFields.asScala.map(_.getName).toSet
      val m = CaseInsensitiveMap(fields.zip(fields).toMap)
      // The schemas may be different among parquet files, so some column names may not be existing
      // in the CaseInsensitiveMap. In that case, we just use the field name of readDataSchema
      //
      // For hive special case, the readDataSchema is lower case, we need to do
      // the case insensitive conversion
      // See https://github.com/NVIDIA/spark-rapids/pull/3982#issue-770410779
      clippedReadFields.map { f =>
        if (useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
          // find the parquet column name
          fieldIdToNameMap(ParquetSchemaClipShims.getFieldId(f))
        } else {
          m.get(f.name).getOrElse(f.name)
        }
      }
    } else {
      clippedReadFields.map { f =>
        if (useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
          fieldIdToNameMap(ParquetSchemaClipShims.getFieldId(f))
        } else {
          f.name
        }
      }
    }
  }
}

// Parquet schema wrapper
private case class ParquetSchemaWrapper(schema: MessageType) extends SchemaBase {

  override def fieldNames: Array[String] = schema.getFields.asScala.map(_.getName).toArray
}

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
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
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
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean)
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
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.toString}", e)
          (Seq.empty, 0)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.toString}", e)
          // It leave the empty hole for the re-composed parquet file if we skip
          // the corrupted file. But it should be ok since there is no meta pointing to that "hole"
          (Seq.empty, 0)
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

  override def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long = {
    val allBlocks = batchContext.origChunkedBlocks.values.flatten.toSeq
    // Some Parquet versions sanity check the block metadata, and since the blocks could be from
    // multiple files they will not pass the checks as they are.
    val blockStartOffset = ParquetPartitionReader.PARQUET_MAGIC.length
    val updatedBlocks = computeBlockMetaData(allBlocks, blockStartOffset)
    calculateParquetOutputSize(updatedBlocks, batchContext.schema, true)
  }

  override def getBatchRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): Callable[(Seq[DataBlockBase], Long)] = {
    new ParquetCopyBlocksRunner(taskContext, file, outhmb, blocks, offset)
  }

  override final def getFileFormatShortName: String = "Parquet"

  override def readBufferToTable(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, extraInfo: ExtraInfo): Table = {

    // Dump parquet data into a file
    dumpDataToFile(dataBuffer, dataSize, splits, Option(debugDumpPrefix), Some("parquet"))

    val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
      isSchemaCaseSensitive, useFieldId)
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
      GpuParquetScan.throwIfNeeded(
        table,
        extraInfo.isCorrectedInt96RebaseMode,
        extraInfo.isCorrectedRebaseMode,
        extraInfo.hasInt96Timestamps)
    }
    evolveSchemaIfNeededAndClose(useFieldId, table, splits.mkString(","), clippedSchema)
  }

  override def writeFileHeader(buffer: HostMemoryBuffer, bContext: BatchContext): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      out.write(ParquetPartitionReader.PARQUET_MAGIC)
      out.getPos
    }
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: Seq[DataBlockBase], bContext: BatchContext): Long = {

    val actualFooterSize = calculateParquetFooterSize(blocks, bContext.schema)
    // 4 + 4 is for writing size and the ending PARQUET_MAGIC.
    footerOffset + actualFooterSize + 4 + 4
  }

  override def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], bContext: BatchContext): (HostMemoryBuffer, Long) = {

    val lenLeft = bufferSize - footerOffset

    val finalSize = closeOnExcept(buffer) { _ =>
      withResource(buffer.slice(footerOffset, lenLeft)) { finalizehmb =>
        withResource(new HostMemoryOutputStream(finalizehmb)) { footerOut =>
          writeFooter(footerOut, blocks, bContext.schema)
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
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 */
class MultiFileCloudParquetPartitionReader(
    override val conf: Configuration,
    footerReadType: ParquetFooterReaderType.Value,
    files: Array[PartitionedFile],
    override val isSchemaCaseSensitive: Boolean,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    maxNumFileProcessed: Int,
    filterHandler: GpuParquetFileFilterHandler,
    filters: Array[Filter],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, filters,
    execMetrics, ignoreCorruptFiles) with ParquetPartitionReaderBase {


  private case class HostMemoryEmptyMetaData(
    override val partitionedFile: PartitionedFile,
    bufferSize: Long,
    override val bytesRead: Long,
    isCorrectRebaseMode: Boolean,
    isCorrectInt96RebaseMode: Boolean,
    hasInt96Timestamps: Boolean,
    clippedSchema: MessageType,
    readSchema: StructType) extends HostMemoryBuffersWithMetaDataBase {

    override def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)] =
      Array(null.asInstanceOf[HostMemoryBuffer] -> bufferSize)
  }

  case class HostMemoryBuffersWithMetaData(
      override val partitionedFile: PartitionedFile,
      override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
      override val bytesRead: Long,
      isCorrectRebaseMode: Boolean,
      isCorrectInt96RebaseMode: Boolean,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      footerReadType: ParquetFooterReaderType.Value,
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
    override def call(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.filePath}", e)
          HostMemoryEmptyMetaData(file, 0, 0,  false, false, false, null, null)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
          HostMemoryEmptyMetaData(file, 0, 0,  false, false, false, null, null)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      try {
        val fileBlockMeta = filterHandler.filterBlocks(footerReadType, file, conf, filters,
          readDataSchema)
        if (fileBlockMeta.blocks.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          return HostMemoryEmptyMetaData(file, 0, bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, readDataSchema)
        }
        blockChunkIter = fileBlockMeta.blocks.iterator.buffered
        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryEmptyMetaData(file, 0, bytesRead,
            fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, readDataSchema)
        } else {
          if (fileBlockMeta.schema.getFieldCount == 0) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = fileBlockMeta.blocks.map(_.getRowCount).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryEmptyMetaData(file, numRows, bytesRead,
              fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
              fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, readDataSchema)
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
              HostMemoryEmptyMetaData(file, 0, bytesRead,
                fileBlockMeta.isCorrectedRebaseMode, fileBlockMeta.isCorrectedInt96RebaseMode,
                fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, readDataSchema)
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
    new ReadBatchRunner(footerReadType, tc, filterHandler, file, conf, filters)
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
      case meta: HostMemoryEmptyMetaData =>
        // Not reading any data, but add in partition data if needed
        val rows = meta.bufferSize.toInt
        val batch = if (rows == 0) {
          new ColumnarBatch(Array.empty, 0)
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = meta.readSchema.fields.safeMap(f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
          new ColumnarBatch(nullColumns, rows)
        }
        addPartitionValues(Some(batch), meta.partitionedFile.partitionValues, partitionSchema)

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
    val table = withResource(hostBuffer) { _ =>

      // Dump parquet data into a file
      dumpDataToFile(hostBuffer, dataSize, files, Option(debugDumpPrefix), Some("parquet"))

      val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
        isSchemaCaseSensitive, useFieldId)
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
        GpuParquetScan.throwIfNeeded(table, isCorrectInt96RebaseMode, isCorrectRebaseMode,
          hasInt96Timestamps)
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
        if (readDataSchema.length < table.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $fileName")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      Some(evolveSchemaIfNeededAndClose(useFieldId, table, fileName, clippedSchema))
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
    override val execMetrics: Map[String, GpuMetric],
    isCorrectedInt96RebaseMode: Boolean,
    isCorrectedRebaseMode: Boolean,
    hasInt96Timestamps: Boolean,
    useFieldId: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with ParquetPartitionReaderBase {

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

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("Parquet readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes)
      if (clippedParquetSchema.getFieldCount == 0) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.getRowCount).sum.toInt
        if (numRows == 0) {
          None
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = readDataSchema.safeMap(f =>
            GpuColumnVector.fromNull(numRows, f.dataType).asInstanceOf[SparkVector])
          Some(new ColumnarBatch(nullColumns.toArray, numRows))
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
          isSchemaCaseSensitive, useFieldId)
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
          GpuParquetScan.throwIfNeeded(table, isCorrectedInt96RebaseMode, isCorrectedRebaseMode,
            hasInt96Timestamps)
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $filePath")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(evolveSchemaIfNeededAndClose(useFieldId, table, filePath.toString,
          clippedParquetSchema))
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

