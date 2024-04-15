/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import java.io.{Closeable, EOFException, FileNotFoundException, IOException, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Collections, Locale}
import java.util.concurrent._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.ParquetPartitionReader.{CopyRange, LocalCopy}
import com.nvidia.spark.rapids.RapidsConf.ParquetFooterReaderType
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.filecache.FileCache
import com.nvidia.spark.rapids.jni.{DateTimeRebase, ParquetFooter}
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, GpuParquetCrypto, GpuTypeShims, ParquetLegacyNanoAsLongShims, ParquetSchemaClipShims, ParquetStringPredShims, ReaderUtils, ShimFilePartitionReaderFactory, SparkShimImpl}
import org.apache.commons.io.IOUtils
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
import org.apache.parquet.schema.{DecimalMetadata, GroupType, MessageType, OriginalType, PrimitiveType, Type}
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
  extends FileScan with GpuScan with Logging {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isParquetPerFileReadEnabled) {
      logInfo("Using the original per file parquet reader")
      GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        options.asScala.toMap, None)
    } else {
      GpuParquetMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        queryUsesInputFile, None)
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

  override def withInputFile(): GpuScan = copy(queryUsesInputFile = true)
}

object GpuParquetScan {
  def tagSupport(scanMeta: ScanMeta[ParquetScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(sparkSession: SparkSession, readSchema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    if (ParquetLegacyNanoAsLongShims.legacyParquetNanosAsLong) {
      meta.willNotWorkOnGpu("GPU does not support spark.sql.legacy.parquet.nanosAsLong")
    }

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetReadEnabled) {
      meta.willNotWorkOnGpu("Parquet input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_READ} to true")
    }

    FileFormatChecks.tag(meta, readSchema, ParquetFormatType, ReadFileOp)

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    val schemaHasTimestamps = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps && sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support int96 timestamp conversion")
    }

    if (ColumnDefaultValuesShims.hasExistenceDefaultValues(readSchema)) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support default values in schema")
    }

  }

  /**
   * This estimates the number of nodes in a parquet footer schema based off of the parquet spec
   * Specifically https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
   */
  private def numNodesEstimate(dt: DataType): Long = dt match {
    case StructType(fields) =>
      // A struct has a group node that holds the children
      1 + fields.map(f => numNodesEstimate(f.dataType)).sum
    case ArrayType(elementType, _) =>
      // A List/Array has one group node to tag it as a list and another one
      // that is marked as repeating.
      2 + numNodesEstimate(elementType)
    case MapType(keyType, valueType, _) =>
      // A Map has one group node to tag it as a map and another one
      // that is marked as repeating, but holds the key/value
      2 + numNodesEstimate(keyType) + numNodesEstimate(valueType)
    case _ =>
      // All the other types are just value types and are represented by a non-group node
      1
  }

  /**
   * Adjust the footer reader type based off of a heuristic.
   */
  def footerReaderHeuristic(
      inputValue: ParquetFooterReaderType.Value,
      data: StructType,
      read: StructType,
      useFieldId: Boolean): ParquetFooterReaderType.Value = {
    val canUseNative = !(useFieldId && ParquetSchemaClipShims.hasFieldIds(read))
    if (!canUseNative) {
      // Native does not support field IDs yet
      ParquetFooterReaderType.JAVA
    } else {
      inputValue match {
        case ParquetFooterReaderType.AUTO =>
          val dnc = numNodesEstimate(data)
          val rnc = numNodesEstimate(read)
          if (rnc.toDouble / dnc <= 0.5 && dnc - rnc > 10) {
            ParquetFooterReaderType.NATIVE
          } else {
            ParquetFooterReaderType.JAVA
          }
        case other => other
      }
    }
  }

  def throwIfRebaseNeededInExceptionMode(table: Table, dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode): Unit = {
    (0 until table.getNumberOfColumns).foreach { i =>
      val col = table.getColumn(i)
      if (dateRebaseMode == DateTimeRebaseException &&
        DateTimeRebaseUtils.isDateRebaseNeededInRead(col)) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
      } else if (timestampRebaseMode == DateTimeRebaseException &&
        DateTimeRebaseUtils.isTimeRebaseNeededInRead(col)) {
        throw DataSourceUtils.newRebaseExceptionInRead("Parquet")
      }
    }
  }

  def rebaseDateTime(table: Table, dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode): Table = {
    val dateRebaseNeeded = dateRebaseMode == DateTimeRebaseLegacy
    val timeRebaseNeeded = timestampRebaseMode == DateTimeRebaseLegacy

    lazy val tableHasDate = (0 until table.getNumberOfColumns).exists { i =>
      checkTypeRecursively(table.getColumn(i), { dt => dt == DType.TIMESTAMP_DAYS })
    }
    lazy val tableHasTimestamp = (0 until table.getNumberOfColumns).exists { i =>
      checkTypeRecursively(table.getColumn(i), { dt => dt == DType.TIMESTAMP_MICROSECONDS })
    }

    if ((dateRebaseNeeded && tableHasDate) || (timeRebaseNeeded && tableHasTimestamp)) {
      // Need to close the input table when returning a new table.
      withResource(table) { tmpTable =>
        val newColumns = (0 until tmpTable.getNumberOfColumns).map { i =>
          deepTransformRebaseDateTime(tmpTable.getColumn(i), dateRebaseNeeded, timeRebaseNeeded)
        }
        withResource(newColumns) { newCols =>
          new Table(newCols: _*)
        }
      }
    } else {
      table
    }
  }

  private def checkTypeRecursively(input: ColumnView, f: DType => Boolean): Boolean = {
    val dt = input.getType
    if (dt.isTimestampType && dt != DType.TIMESTAMP_DAYS && dt != DType.TIMESTAMP_MICROSECONDS) {
      // There should be something wrong here since timestamps other than DAYS should already
      // been converted into MICROSECONDS when reading Parquet files.
      throw new IllegalStateException(s"Unexpected date/time type: $dt " +
        "(expected TIMESTAMP_DAYS or TIMESTAMP_MICROSECONDS)")
    }
    dt match {
      case DType.LIST | DType.STRUCT => (0 until input.getNumChildren).exists(i =>
        withResource(input.getChildColumnView(i)) { child =>
          checkTypeRecursively(child, f)
        })
      case t: DType => f(t)
    }
  }

  private def deepTransformRebaseDateTime(cv: ColumnVector, dateRebaseNeeded: Boolean,
      timeRebaseNeeded: Boolean): ColumnVector = {
    ColumnCastUtil.deepTransform(cv) {
      case (cv, _) if cv.getType.isTimestampType =>
        // cv type is guaranteed to be either TIMESTAMP_DAYS or TIMESTAMP_MICROSECONDS,
        // since we already checked it in `checkTypeRecursively`.
        if ((cv.getType == DType.TIMESTAMP_DAYS && dateRebaseNeeded) ||
          cv.getType == DType.TIMESTAMP_MICROSECONDS && timeRebaseNeeded) {
          DateTimeRebase.rebaseJulianToGregorian(cv)
        } else {
          cv.copyToColumnVector()
        }
    }
  }
}

// contains meta about all the blocks in a file
case class ParquetFileInfoWithBlockMeta(filePath: Path, blocks: collection.Seq[BlockMetaData],
    partValues: InternalRow, schema: MessageType, readSchema: StructType,
    dateRebaseMode: DateTimeRebaseMode, timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean)

private case class BlockMetaWithPartFile(meta: ParquetFileInfoWithBlockMeta, file: PartitionedFile)

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

private case class GpuParquetFileFilterHandler(
    @transient sqlConf: SQLConf,
    metrics: Map[String, GpuMetric]) extends Logging {

  private val FOOTER_LENGTH_SIZE = 4
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  // From Spark 340, more string predicates are supported as push-down filters, so this
  // flag is renamed to 'xxxxStringPredicate' and specified by another config.
  private val pushDownStringPredicate = ParquetStringPredShims.pushDown(sqlConf)
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val datetimeRebaseMode = SparkShimImpl.parquetRebaseRead(sqlConf)
  private val int96RebaseMode = SparkShimImpl.int96ParquetRebaseRead(sqlConf)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  private val ignoreMissingParquetFieldId = ParquetSchemaClipShims.ignoreMissingIds(sqlConf)

  private val PARQUET_ENCRYPTION_CONFS = Seq("parquet.encryption.kms.client.class",
    "parquet.encryption.kms.client.class", "parquet.crypto.factory.class")
  private val PARQUET_MAGIC_ENCRYPTED = "PARE".getBytes(StandardCharsets.US_ASCII)

  private def isParquetTimeInInt96(parquetType: Type): Boolean = {
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

  private def convertToFooterSchema(schema: StructType): ParquetFooter.StructElement = {
    convertToParquetNative(schema).asInstanceOf[ParquetFooter.StructElement]
  }

  private def getFooterBuffer(
      filePath: Path,
      conf: Configuration,
      metrics: Map[String, GpuMetric]): HostMemoryBuffer = {
    val filePathString = filePath.toString
    FileCache.get.getFooter(filePathString, conf).map { hmb =>
      withResource(hmb) { _ =>
        metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS, NoopMetric) += 1
        metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS_SIZE, NoopMetric) += hmb.getLength
        // buffer includes header and trailing length and magic, stripped here
        hmb.slice(MAGIC.length, hmb.getLength - Integer.BYTES - MAGIC.length)
      }
    }.getOrElse {
      metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
      withResource(readFooterBuffer(filePath, conf)) { hmb =>
        metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES_SIZE, NoopMetric) += hmb.getLength
        // footer was not cached, so try to cache it
        // If we get a filecache token then we can complete the caching by providing the data.
        // If we do not get a token then we should not cache this data.
        val cacheToken = FileCache.get.startFooterCache(filePathString, conf)
        cacheToken.foreach { t =>
          t.complete(hmb.slice(0, hmb.getLength))
        }
        // buffer includes header and trailing length and magic, stripped here
        hmb.slice(MAGIC.length, hmb.getLength - Integer.BYTES - MAGIC.length)
      }
    }
  }

  private def readFooterBuffer(
      filePath: Path,
      conf: Configuration): HostMemoryBuffer = {
    PerfIO.readParquetFooterBuffer(filePath, conf, verifyParquetMagic)
      .getOrElse(readFooterBufUsingHadoop(filePath, conf))
  }

  private def readFooterBufUsingHadoop(filePath: Path, conf: Configuration): HostMemoryBuffer = {
    val fs = filePath.getFileSystem(conf)
    val stat = fs.getFileStatus(filePath)
    // Much of this code came from the parquet_mr projects ParquetFileReader, and was modified
    // to match our needs
    val fileLen = stat.getLen
    // MAGIC + data + footer + footerIndex + MAGIC
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) {
      throw new RuntimeException(s"$filePath is not a Parquet file (too small length: $fileLen )")
    }
    val footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length
    withResource(fs.open(filePath)) { inputStream =>
      withResource(new NvtxRange("ReadFooterBytes", NvtxColor.YELLOW)) { _ =>
        inputStream.seek(footerLengthIndex)
        val footerLength = readIntLittleEndian(inputStream)
        val magic = new Array[Byte](MAGIC.length)
        inputStream.readFully(magic)
        val footerIndex = footerLengthIndex - footerLength
        verifyParquetMagic(filePath, magic)
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
          throw new RuntimeException(s"corrupted file: the footer index is not within " +
            s"the file: $footerIndex")
        }
        val hmbLength = (fileLen - footerIndex).toInt
        closeOnExcept(HostMemoryBuffer.allocate(hmbLength + MAGIC.length, false)) { outBuffer =>
          val out = new HostMemoryOutputStream(outBuffer)
          out.write(MAGIC)
          inputStream.seek(footerIndex)
          // read the footer til the end of the file
          val tmpBuffer = new Array[Byte](4096)
          var bytesLeft = hmbLength
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
  }


  private def verifyParquetMagic(filePath: Path, magic: Array[Byte]): Unit = {
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
  }

  private def readAndFilterFooter(
      file: PartitionedFile,
      conf : Configuration,
      readDataSchema: StructType,
      filePath: Path): ParquetFooter = {
    val footerSchema = convertToFooterSchema(readDataSchema)
    val footerBuffer = getFooterBuffer(filePath, conf, metrics)
    withResource(footerBuffer) { footerBuffer =>
      withResource(new NvtxRange("Parse and filter footer by range", NvtxColor.RED)) { _ =>
        // In the future, if we know we're going to read the entire file,
        // i.e.: having file length from file stat == amount to read,
        // then sending -1 as length to the native footer parser allows it to
        // skip the row group filtering.
        val len = file.length

        ParquetFooter.readAndFilter(footerBuffer, file.start, len,
          footerSchema, !isCaseSensitive)
      }
    }
  }

  @scala.annotation.nowarn(
    "msg=method readFooter in class ParquetFileReader is deprecated"
  )
  private def readAndSimpleFilterFooter(
      file: PartitionedFile,
      conf : Configuration,
      filePath: Path): ParquetMetadata = {
    //noinspection ScalaDeprecation
    withResource(new NvtxRange("readFooter", NvtxColor.YELLOW)) { _ =>
      val filePathString = filePath.toString
      FileCache.get.getFooter(filePathString, conf).map { hmb =>
        withResource(hmb) { _ =>
          metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS, NoopMetric) += 1
          metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_HITS_SIZE, NoopMetric) += hmb.getLength
          ParquetFileReader.readFooter(new HMBInputFile(hmb),
            ParquetMetadataConverter.range(file.start, file.start + file.length))
        }
      }.getOrElse {
        metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES, NoopMetric) += 1
        // footer was not cached, so try to cache it
        // If we get a filecache token then we can complete the caching by providing the data.
        // If something goes wrong before completing the caching then the token must be canceled.
        // If we do not get a token then we should not cache this data.
        val cacheToken = FileCache.get.startFooterCache(filePathString, conf)
        cacheToken.map { token =>
          var needTokenCancel = true
          try {
            withResource(readFooterBuffer(filePath, conf)) { hmb =>
              metrics.getOrElse(GpuMetric.FILECACHE_FOOTER_MISSES_SIZE, NoopMetric) += hmb.getLength
              token.complete(hmb.slice(0, hmb.getLength))
              needTokenCancel = false
              ParquetFileReader.readFooter(new HMBInputFile(hmb),
                ParquetMetadataConverter.range(file.start, file.start + file.length))
            }
          } finally {
            if (needTokenCancel) {
              token.cancel()
            }
          }
        }.getOrElse {
          ParquetFileReader.readFooter(conf, filePath,
            ParquetMetadataConverter.range(file.start, file.start + file.length))
        }
      }
    }
  }

  @scala.annotation.nowarn
  def filterBlocks(
      footerReader: ParquetFooterReaderType.Value,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): ParquetFileInfoWithBlockMeta = {
    withResource(new NvtxRange("filterBlocks", NvtxColor.PURPLE)) { _ =>
      val filePath = new Path(new URI(file.filePath.toString()))
      // Make sure we aren't trying to read encrypted files. For now, remove the related
      // parquet confs from the hadoop configuration and try to catch the resulting
      // exception and print a useful message
      PARQUET_ENCRYPTION_CONFS.foreach { encryptConf =>
        if (conf.get(encryptConf) != null) {
          conf.unset(encryptConf)
        }
      }
      val fileHadoopConf =
        ReaderUtils.getHadoopConfForReaderThread(new Path(file.filePath.toString), conf)
      val footer: ParquetMetadata = try {
        footerReader match {
          case ParquetFooterReaderType.NATIVE =>
            val serialized = withResource(readAndFilterFooter(file, fileHadoopConf,
              readDataSchema, filePath)) { tableFooter =>
                if (tableFooter.getNumColumns <= 0) {
                  // Special case because java parquet reader does not like having 0 columns.
                  val numRows = tableFooter.getNumRows
                  val block = new BlockMetaData()
                  block.setRowCount(numRows)
                  val schema = new MessageType("root")
                  return ParquetFileInfoWithBlockMeta(filePath, Seq(block), file.partitionValues,
                    schema, readDataSchema, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
                    hasInt96Timestamps = false)
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
            readAndSimpleFilterFooter(file, fileHadoopConf, filePath)
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
          isCaseSensitive, footer.getFileMetaData.getKeyValueMetaData.get, datetimeRebaseMode)
        filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
      } else {
        None
      }

      val blocks = if (pushedFilters.isDefined) {
        withResource(new NvtxRange("getBlocksWithFilter", NvtxColor.CYAN)) { _ =>
          // Use the ParquetFileReader to perform dictionary-level filtering
          ParquetInputFormat.setFilterPredicate(fileHadoopConf, pushedFilters.get)
          //noinspection ScalaDeprecation
          withResource(new ParquetFileReader(fileHadoopConf, footer.getFileMetaData, filePath,
            footer.getBlocks, Collections.emptyList[ColumnDescriptor])) { parquetReader =>
            parquetReader.getRowGroups
          }
        }
      } else {
        footer.getBlocks
      }

      val (clipped, clippedSchema) =
        withResource(new NvtxRange("clipSchema", NvtxColor.DARK_GREEN)) { _ =>
          val clippedSchema = ParquetSchemaUtils.clipParquetSchema(
            fileSchema, readDataSchema, isCaseSensitive, readUseFieldId)
          // Check if the read schema is compatible with the file schema.
          checkSchemaCompat(clippedSchema, readDataSchema,
            (t: Type, d: DataType) => throwTypeIncompatibleError(t, d, file.filePath.toString()),
            isCaseSensitive, readUseFieldId)
          val clipped = GpuParquetUtils.clipBlocksToSchema(clippedSchema, blocks, isCaseSensitive)
          (clipped, clippedSchema)
        }
      val hasDateTimeInReadSchema = DataTypeUtils.hasDateOrTimestampType(readDataSchema)
      val dateRebaseModeForThisFile = DateTimeRebaseUtils.datetimeRebaseMode(
          footer.getFileMetaData.getKeyValueMetaData.get,
          datetimeRebaseMode,
          hasDateTimeInReadSchema)
      val hasInt96Timestamps = isParquetTimeInInt96(fileSchema)
      val timestampRebaseModeForThisFile = if (hasInt96Timestamps) {
        DateTimeRebaseUtils.int96RebaseMode(
          footer.getFileMetaData.getKeyValueMetaData.get, int96RebaseMode)
      } else {
        dateRebaseModeForThisFile
      }

      ParquetFileInfoWithBlockMeta(filePath, clipped, file.partitionValues,
        clippedSchema, readDataSchema, dateRebaseModeForThisFile,
        timestampRebaseModeForThisFile, hasInt96Timestamps)
    }
  }

  /**
   * Recursively check if the read schema is compatible with the file schema. The errorCallback
   * will be invoked to throw an exception once any incompatible type pairs are found.
   *
   * Any element in the read schema that are missing from the file schema are ignored.
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

        def getParquetType(f: StructField): Option[Type] = {
          if(useFieldId && ParquetSchemaClipShims.hasFieldId(f)) {
            // use field ID and Spark schema specified field ID
            fieldIdToFieldMap.get(ParquetSchemaClipShims.getFieldId(f))
          } else {
            fileFieldMap.get(if (isCaseSensitive) f.name else f.name.toLowerCase(Locale.ROOT))
          }
        }
        struct.fields.foreach { f =>
          getParquetType(f).foreach { fieldType =>
            checkSchemaCompat(fieldType,
              f.dataType,
              errorCallback,
              isCaseSensitive,
              useFieldId,
              // Record root types for each column, so as to throw a readable exception
              // over nested types.
              Some(rootFileType.getOrElse(fieldType)),
              Some(rootReadType.getOrElse(f.dataType)))
          }
        }
      case array: ArrayType =>
        if (fileType.isPrimitive) {
          if (fileType.getRepetition == Type.Repetition.REPEATED) {
            checkSchemaCompat(fileType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          } else {
            errorCallback(fileType, readType)
          }
        } else {
          val fileGroupType = fileType.asGroupType()
          if (fileGroupType.getFieldCount > 1 &&
              fileGroupType.isRepetition(Type.Repetition.REPEATED)) {
            // legacy array format where struct child is directly repeated under array type group
            checkSchemaCompat(fileGroupType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          } else {
            val repeatedType = fileGroupType.getType(0)
            val childType =
              if (isElementType(repeatedType, fileType.getName)) {
                // Legacy element, per Parquet LogicalType backward compatibility rules.
                // Retain the child as the element type.
                repeatedType
              }
              else {
                // Conforms to current Parquet LogicalType rules.
                // Unwrap child group layer, and use grandchild's element type.
                repeatedType.asGroupType().getType(0)
              }
            checkSchemaCompat(childType, array.elementType, errorCallback, isCaseSensitive,
              useFieldId, rootFileType, rootReadType)
          }
        }

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

  // scalastyle:off
  // Implement Parquet LIST backwards-compatibility rules.
  // See: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
  // Inspired by Apache Spark's ParquetSchemaConverter.isElementType():
  // https://github.com/apache/spark/blob/3a0e6bde2aaa11e1165f4fde040ff02e1743795e/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetSchemaConverter.scala#L413
  // scalastyle:on
  private def isElementType(repeatedType: Type, parentName: String): Boolean = {
    {
      // For legacy 2-level list types with primitive element type, e.g.:
      //
      //    // ARRAY<INT> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated int32 element;
      //    }
      //
      repeatedType.isPrimitive
    } || {
      // For legacy 2-level list types whose element type is a group type with 2 or more fields,
      // e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group element {
      //        required binary str (UTF8);
      //        required int32 num;
      //      };
      //    }
      //
      repeatedType.asGroupType().getFieldCount > 1
    } || {
      // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0), e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group array {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == "array"
    } || {
      // For Parquet data generated by parquet-thrift, e.g.:
      //
      //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
      //    optional group my_list (LIST) {
      //      repeated group my_list_tuple {
      //        required binary str (UTF8);
      //      };
      //    }
      //
      repeatedType.getName == s"${parentName}_tuple"
    }
  }

  /**
   * Check the compatibility over primitive types. This function refers to the `getUpdater` method
   * of org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory.
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

      case PrimitiveTypeName.BINARY if dt == DataTypes.StringType =>
        // PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY for StringType is not supported by parquet
        return

      case PrimitiveTypeName.BINARY | PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        if dt == DataTypes.BinaryType =>
        return

      case PrimitiveTypeName.BINARY | PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        if canReadAsIntDecimal(pt, dt) || canReadAsLongDecimal(pt, dt) ||
            canReadAsBinaryDecimal(pt, dt) =>
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
    queryUsesInputFile: Boolean,
    alluxioPathReplacementMap: Option[Map[String, String]])
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf,
    rapidsConf, alluxioPathReplacementMap) {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val useChunkedReader = rapidsConf.chunkedReaderEnabled
  private val useSubPageChunked = rapidsConf.chunkedSubPageReaderEnabled
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.parquetDebugDumpAlways
  private val numThreads = rapidsConf.multiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumParquetFilesParallel
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  private val filterHandler = GpuParquetFileFilterHandler(sqlConf, metrics)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  private val footerReadType = GpuParquetScan.footerReaderHeuristic(
    rapidsConf.parquetReaderFooterType, dataSchema, readDataSchema, readUseFieldId)
  private val numFilesFilterParallel = rapidsConf.numFilesFilterParallel
  private val combineThresholdSize =
    RapidsConf.PARQUET_MULTITHREADED_COMBINE_THRESHOLD.get(sqlConf)
      .map { deprecatedVal =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_COMBINE_THRESHOLD} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_COMBINE_THRESHOLD} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        deprecatedVal
      }.getOrElse(rapidsConf.getMultithreadedCombineThreshold)
  private val combineWaitTime =
    RapidsConf.PARQUET_MULTITHREADED_COMBINE_WAIT_TIME.get(sqlConf)
      .map { f =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_COMBINE_WAIT_TIME} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_COMBINE_WAIT_TIME} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        f.intValue()
      }.getOrElse(rapidsConf.getMultithreadedCombineWaitTime)
  private val keepReadsInOrderFromConf =
    RapidsConf.PARQUET_MULTITHREADED_READ_KEEP_ORDER.get(sqlConf)
      .map { deprecatedVal =>
        logWarning(s"${RapidsConf.PARQUET_MULTITHREADED_READ_KEEP_ORDER} is deprecated, " +
          s"use ${RapidsConf.READER_MULTITHREADED_READ_KEEP_ORDER} instead. But for now " +
          "the deprecated one will be honored if both are set.")
        deprecatedVal
      }.getOrElse(rapidsConf.getMultithreadedReaderKeepOrder)
  private val alluxioReplacementTaskTime =
    AlluxioCfgUtils.enabledAlluxioReplacementAlgoTaskTime(rapidsConf)

  // We can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row. If this changes we need to
  // make sure the Alluxio path replacement also handles setting the input file name to
  // the non-Alluxio path like the multi-threaded reader does.
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
    val filterFunc = (file: PartitionedFile) => {
      filterHandler.filterBlocks(footerReadType, file, conf,
        filters, readDataSchema)
    }
    val combineConf = CombineConf(combineThresholdSize, combineWaitTime)
    new MultiFileCloudParquetPartitionReader(conf, files, filterFunc, isCaseSensitive,
      debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader, subPageChunked, metrics,
      partitionSchema, numThreads, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles,
      readUseFieldId, alluxioPathReplacementMap.getOrElse(Map.empty), alluxioReplacementTaskTime,
      queryUsesInputFile, keepReadsInOrderFromConf, combineConf)
  }

  private def filterBlocksForCoalescingReader(
      footerReadType: ParquetFooterReaderType.Value,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType): BlockMetaWithPartFile = {
    try {
      logDebug(s"Filtering blocks for coalescing reader, file: ${file.filePath}")
      val meta = filterHandler.filterBlocks(footerReadType, file, conf, filters,
        readDataSchema)
      BlockMetaWithPartFile(meta, file)
    } catch {
      case e: FileNotFoundException if ignoreMissingFiles =>
        logWarning(s"Skipped missing file: ${file.filePath}", e)
        val meta = ParquetFileInfoWithBlockMeta(
          new Path(new URI(file.filePath.toString())), Seq.empty,
          file.partitionValues, null, null, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
          hasInt96Timestamps = false)
        BlockMetaWithPartFile(meta, file)
      // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
      case e: FileNotFoundException if !ignoreMissingFiles => throw e
      // If ignoreMissingFiles=true, this case will never be reached. But it's ok
      // to leave this branch here.
      case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(
          s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
        val meta = ParquetFileInfoWithBlockMeta(
          new Path(new URI(file.filePath.toString())), Seq.empty,
          file.partitionValues, null, null, DateTimeRebaseLegacy, DateTimeRebaseLegacy,
          hasInt96Timestamps = false)
        BlockMetaWithPartFile(meta, file)
    }
  }

  private class CoalescingFilterRunner(
      footerReadType: ParquetFooterReaderType.Value,
      taskContext: TaskContext,
      files: Array[PartitionedFile],
      conf: Configuration,
      filters: Array[Filter],
      readDataSchema: StructType) extends Callable[Array[BlockMetaWithPartFile]] with Logging {

    override def call(): Array[BlockMetaWithPartFile] = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        files.map { file =>
          filterBlocksForCoalescingReader(footerReadType, file, conf, filters, readDataSchema)
        }
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  override def buildBaseColumnarReaderForCoalescing(
      origFiles: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // update the file paths for Alluxio if needed, the coalescing reader doesn't support
    // input_file_name so no need to track what the non Alluxio file name is
    val files = if (alluxioReplacementTaskTime) {
      AlluxioUtils.updateFilesTaskTimeIfAlluxio(origFiles, alluxioPathReplacementMap).map(_.toRead)
    } else {
      // Since coalescing reader isn't supported if input_file_name is used, so won't
      // ever get here with that. So with convert time or no Alluxio just use the files as
      // passed in.
      origFiles
    }
    val clippedBlocks = ArrayBuffer[ParquetSingleDataBlockMeta]()
    val startTime = System.nanoTime()
    val metaAndFilesArr = if (numFilesFilterParallel > 0) {
      val tc = TaskContext.get()
      val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
      files.grouped(numFilesFilterParallel).map { fileGroup =>
        threadPool.submit(
          new CoalescingFilterRunner(footerReadType, tc, fileGroup, conf, filters, readDataSchema))
      }.toArray.flatMap(_.get())
    } else {
      files.map { file =>
        filterBlocksForCoalescingReader(footerReadType, file, conf, filters, readDataSchema)
      }
    }
    metaAndFilesArr.foreach { metaAndFile =>
      val singleFileInfo = metaAndFile.meta
      clippedBlocks ++= singleFileInfo.blocks.map(block =>
        ParquetSingleDataBlockMeta(
          singleFileInfo.filePath,
          ParquetDataBlock(block),
          metaAndFile.file.partitionValues,
          ParquetSchemaWrapper(singleFileInfo.schema),
          singleFileInfo.readSchema,
          new ParquetExtraInfo(singleFileInfo.dateRebaseMode,
            singleFileInfo.timestampRebaseMode,
            singleFileInfo.hasInt96Timestamps)))
    }
    val filterTime = System.nanoTime() - startTime
    metrics.get(FILTER_TIME).foreach {
      _ += filterTime
    }
    metrics.get("scanTime").foreach {
      _ += TimeUnit.NANOSECONDS.toMillis(filterTime)
    }
    new MultiFileParquetPartitionReader(conf, files, clippedBlocks.toSeq, isCaseSensitive,
      debugDumpPrefix, debugDumpAlways, useChunkedReader, useSubPageChunked, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, metrics, partitionSchema,
      numThreads, ignoreMissingFiles, ignoreCorruptFiles, readUseFieldId)
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
    @transient params: Map[String, String],
    alluxioPathReplacementMap: Option[Map[String, String]])
  extends ShimFilePartitionReaderFactory(params) with Logging {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val debugDumpAlways = rapidsConf.parquetDebugDumpAlways
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val targetSizeBytes = rapidsConf.gpuTargetBatchSizeBytes
  private val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes
  private val useChunkedReader = rapidsConf.chunkedReaderEnabled
  private val useSubPageChunked = rapidsConf.chunkedSubPageReaderEnabled
  private val filterHandler = GpuParquetFileFilterHandler(sqlConf, metrics)
  private val readUseFieldId = ParquetSchemaClipShims.useFieldId(sqlConf)
  private val footerReadType = GpuParquetScan.footerReaderHeuristic(
    rapidsConf.parquetReaderFooterType, dataSchema, readDataSchema, readUseFieldId)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val reader = new PartitionReaderWithBytesRead(buildBaseColumnarParquetReader(partitionedFile))
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }

  private def buildBaseColumnarParquetReader(
      file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val startTime = System.nanoTime()
    val singleFileInfo = filterHandler.filterBlocks(footerReadType, file, conf, filters,
      readDataSchema)
    metrics.get(FILTER_TIME).foreach {
      _ += (System.nanoTime() - startTime)
    }
    new ParquetPartitionReader(conf, file, singleFileInfo.filePath, singleFileInfo.blocks,
      singleFileInfo.schema, isCaseSensitive, readDataSchema, debugDumpPrefix, debugDumpAlways,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, targetSizeBytes,
      useChunkedReader, useSubPageChunked, metrics, singleFileInfo.dateRebaseMode,
      singleFileInfo.timestampRebaseMode, singleFileInfo.hasInt96Timestamps, readUseFieldId)
  }
}

trait ParquetPartitionReaderBase extends Logging with ScanWithMetrics
    with MultiFileReaderFunctions {
  // the size of Parquet magic (at start+end) and footer length values
  val PARQUET_META_SIZE: Long = 4 + 4 + 4

  // Configuration

  def conf: Configuration
  def execMetrics: Map[String, GpuMetric]

  def isSchemaCaseSensitive: Boolean

  val copyBufferSize = conf.getInt("parquet.read.allocation.size", 8 * 1024 * 1024)

  def checkIfNeedToSplitBlocks(currentDateRebaseMode: DateTimeRebaseMode,
      nextDateRebaseMode: DateTimeRebaseMode,
      currentTimestampRebaseMode: DateTimeRebaseMode,
      nextTimestampRebaseMode: DateTimeRebaseMode,
      currentSchema: SchemaBase,
      nextSchema: SchemaBase,
      currentFilePath: String,
      nextFilePath: String): Boolean = {
    // We need to ensure all files we are going to combine have the same datetime
    // rebase mode.
    if (currentDateRebaseMode != nextDateRebaseMode &&
      currentTimestampRebaseMode != nextTimestampRebaseMode) {
      logInfo(s"datetime rebase mode for the next file ${nextFilePath} is different " +
        s"then current file ${currentFilePath}, splitting into another batch.")
      return true
    }

    if (!currentSchema.equals(nextSchema)) {
      logInfo(s"File schema for the next file ${nextFilePath}" +
        s" schema ${nextSchema} doesn't match current ${currentFilePath}" +
        s" schema ${currentSchema}, splitting it into another batch!")
      return true
    }
    false
  }

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

  /**
   * Calculate an amount of extra memory if we are combining multiple files together.
   * We want to add extra memory because the ColumnChunks saved in the footer have 2 fields
   * file_offset and data_page_offset that get much larger when we are combining files.
   * Here we estimate that by taking the number of columns * number of blocks which should be
   * the number of column chunks and then saying there are 2 fields that could be larger and
   * assume max size of those would be 8 bytes worst case. So we probably allocate to much here
   * but it shouldn't be by a huge amount and its better then having to realloc and copy.
   *
   * @param numCols the number of columns
   * @param numBlocks the total number of blocks to be combined
   * @return amount of extra memory to allocate
   */
  def calculateExtraMemoryForParquetFooter(numCols: Int, numBlocks: Int): Int = {
    val numColumnChunks = numCols * numBlocks
    numColumnChunks * 2 * 8
  }

  protected def calculateParquetOutputSize(
      currentChunkedBlocks: Seq[BlockMetaData],
      schema: MessageType,
      handleCoalesceFiles: Boolean): Long = {
    // start with the size of Parquet magic (at start+end) and footer length values
    var size: Long = PARQUET_META_SIZE

    // Calculate the total amount of column data that will be copied
    // NOTE: Avoid using block.getTotalByteSize here as that is the
    //       uncompressed size rather than the size in the file.
    size += currentChunkedBlocks.flatMap(_.getColumns.asScala.map(_.getTotalSize)).sum

    val footerSize = calculateParquetFooterSize(currentChunkedBlocks, schema)
    val extraMemory = if (handleCoalesceFiles) {
      val numCols = currentChunkedBlocks.head.getColumns().size()
      calculateExtraMemoryForParquetFooter(numCols, currentChunkedBlocks.size)
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
      out: HostMemoryOutputStream,
      copyBuffer: Array[Byte]): Long = {
    var readTime = 0L
    var writeTime = 0L
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    out.seek(range.outputOffset)
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
    range.length
  }

  /**
   * Computes new block metadata to reflect where the blocks and columns will appear in the
   * computed Parquet file.
   *
   * @param blocks block metadata from the original file(s) that will appear in the computed file
   * @param realStartOffset starting file offset of the first block
   * @return updated block metadata
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  protected def computeBlockMetaData(
      blocks: Seq[BlockMetaData],
      realStartOffset: Long): Seq[BlockMetaData] = {
    val outputBlocks = new ArrayBuffer[BlockMetaData](blocks.length)
    var totalBytesToCopy = 0L
    blocks.foreach { block =>
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
        val columnSize = column.getTotalSize
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
          columnSize,
          column.getTotalUncompressedSize)
        totalBytesToCopy += columnSize
      }
      outputBlocks += GpuParquetUtils.newBlockMeta(block.getRowCount, outputColumns.toSeq)
    }
    outputBlocks.toSeq
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
      filePath: Path,
      out: HostMemoryOutputStream,
      blocks: Seq[BlockMetaData],
      realStartOffset: Long,
      metrics: Map[String, GpuMetric]): Seq[BlockMetaData] = {
    val startPos = out.getPos
    val filePathString: String = filePath.toString
    val remoteItems = new ArrayBuffer[CopyRange](blocks.length)
    var totalBytesToCopy = 0L
    val fileHadoopConf = ReaderUtils.getHadoopConfForReaderThread(filePath, conf)
    withResource(new ArrayBuffer[LocalCopy](blocks.length)) { localItems =>
      blocks.foreach { block =>
        block.getColumns.asScala.foreach { column =>
          val columnSize = column.getTotalSize
          val outputOffset = totalBytesToCopy + startPos
          val channel = FileCache.get.getDataRangeChannel(filePathString,
            column.getStartingPos, columnSize, fileHadoopConf)
          if (channel.isDefined) {
            localItems += LocalCopy(channel.get, columnSize, outputOffset)
          } else {
            remoteItems += CopyRange(column.getStartingPos, columnSize, outputOffset)
          }
          totalBytesToCopy += columnSize
        }
      }
      localItems.foreach { localItem =>
        copyLocal(localItem, out, metrics)
        localItem.close()
      }
    }
    copyRemoteBlocksData(remoteItems.toSeq, filePath,
      filePathString, out, metrics)
    // fixup output pos after blocks were copied possibly out of order
    out.seek(startPos + totalBytesToCopy)
    computeBlockMetaData(blocks, realStartOffset)
  }

  private def copyRemoteBlocksData(
      remoteCopies: Seq[CopyRange],
      filePath: Path,
      filePathString: String,
      out: HostMemoryOutputStream,
      metrics: Map[String, GpuMetric]): Long = {
    if (remoteCopies.isEmpty) {
      return 0L
    }

    val fileHadoopConf = ReaderUtils.getHadoopConfForReaderThread(filePath, conf)
    val coalescedRanges = coalesceReads(remoteCopies)

    val totalBytesCopied = PerfIO.readToHostMemory(
        fileHadoopConf, out.buffer, filePath.toUri,
        coalescedRanges.map(r => IntRangeWithOffset(r.offset, r.length, r.outputOffset))
      ).getOrElse {
        withResource(filePath.getFileSystem(fileHadoopConf).open(filePath)) { in =>
          val copyBuffer: Array[Byte] = new Array[Byte](copyBufferSize)
          coalescedRanges.foldLeft(0L) { (acc, blockCopy) =>
            acc + copyDataRange(blockCopy, in, out, copyBuffer)
          }
        }
      }
    // try to cache the remote ranges that were copied
    remoteCopies.foreach { range =>
      metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_MISSES, NoopMetric) += 1
      metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_MISSES_SIZE, NoopMetric) += range.length
      val cacheToken = FileCache.get.startDataRangeCache(
        filePathString, range.offset, range.length, fileHadoopConf)
      // If we get a filecache token then we can complete the caching by providing the data.
      // If we do not get a token then we should not cache this data.
      cacheToken.foreach { token =>
        token.complete(out.buffer.slice(range.outputOffset, range.length))
      }
    }
    totalBytesCopied
  }

  private def coalesceReads(ranges: Seq[CopyRange]): Seq[CopyRange] = {
    val coalesced = new ArrayBuffer[CopyRange](ranges.length)
    var currentRange: CopyRange = null
    var currentRangeEnd = 0L

    def addCurrentRange(): Unit = {
      if (currentRange != null) {
        val rangeLength = currentRangeEnd - currentRange.offset
        if (rangeLength == currentRange.length) {
          coalesced += currentRange
        } else {
          coalesced += currentRange.copy(length = rangeLength)
        }
        currentRange = null
        currentRangeEnd = 0L
      }
    }

    ranges.foreach { c =>
      if (c.offset == currentRangeEnd) {
        currentRangeEnd += c.length
      } else {
        addCurrentRange()
        currentRange = c
        currentRangeEnd = c.offset + c.length
      }
    }
    addCurrentRange()
    coalesced.toSeq
  }

  private def copyLocal(
      item: LocalCopy,
      out: HostMemoryOutputStream,
      metrics: Map[String, GpuMetric]): Long = {
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_HITS, NoopMetric) += 1
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_HITS_SIZE, NoopMetric) += item.length
    metrics.getOrElse(GpuMetric.FILECACHE_DATA_RANGE_READ_TIME, NoopMetric).ns {
      out.seek(item.outputOffset)
      out.copyFromChannel(item.channel, item.length)
    }
    item.length
  }

  protected def readPartFile(
      blocks: Seq[BlockMetaData],
      clippedSchema: MessageType,
      filePath: Path): (HostMemoryBuffer, Long, Seq[BlockMetaData]) = {
    withResource(new NvtxRange("Parquet buffer file split", NvtxColor.YELLOW)) { _ =>
      val estTotalSize = calculateParquetOutputSize(blocks, clippedSchema, false)
      closeOnExcept(HostMemoryBuffer.allocate(estTotalSize)) { hmb =>
        val out = new HostMemoryOutputStream(hmb)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        val outputBlocks = copyBlocksData(filePath, out, blocks, out.getPos, metrics)
        val footerPos = out.getPos
        writeFooter(out, outputBlocks, clippedSchema)
        BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        // check we didn't go over memory
        if (out.getPos > estTotalSize) {
          throw new QueryExecutionException(s"Calculated buffer size $estTotalSize is to " +
              s"small, actual written: ${out.getPos}")
        }
        (hmb, out.getPos, outputBlocks)
      }
    }
  }

  protected def populateCurrentBlockChunk(
      blockIter: BufferedIterator[BlockMetaData],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long,
      readDataSchema: StructType): Seq[BlockMetaData] = {
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
    currentChunk.toSeq
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
   * @return a sequence of tuple of column names following the order of readDataSchema
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
    //      optional int32 c1 = 1 (field ID is 1),
    //      optional int32 c2 = 2 (field ID is 2),
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

  def getParquetOptions(
      readDataSchema: StructType,
      clippedSchema: MessageType,
      useFieldId: Boolean): ParquetOptions = {
    val includeColumns = toCudfColumnNames(readDataSchema, clippedSchema,
      isSchemaCaseSensitive, useFieldId)
    ParquetOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .includeColumn(includeColumns : _*)
        .build()
  }

  /** conversions used by multithreaded reader and coalescing reader */
  implicit def toBlockMetaData(block: DataBlockBase): BlockMetaData =
    block.asInstanceOf[ParquetDataBlock].dataBlock

  implicit def toDataBlockBase(blocks: Seq[BlockMetaData]): Seq[DataBlockBase] =
    blocks.map(ParquetDataBlock)

  implicit def toBlockMetaDataSeq(blocks: Seq[DataBlockBase]): Seq[BlockMetaData] =
    blocks.map(_.asInstanceOf[ParquetDataBlock].dataBlock)
}

// Parquet schema wrapper
private case class ParquetSchemaWrapper(schema: MessageType) extends SchemaBase {
  override def isEmpty: Boolean = schema.getFields.isEmpty
}

// Parquet BlockMetaData wrapper
private case class ParquetDataBlock(dataBlock: BlockMetaData) extends DataBlockBase {
  override def getRowCount: Long = dataBlock.getRowCount
  override def getReadDataSize: Long = dataBlock.getTotalByteSize
  override def getBlockSize: Long = dataBlock.getColumns.asScala.map(_.getTotalSize).sum
}

/** Parquet extra information containing rebase modes and whether there is int96 timestamp */
class ParquetExtraInfo(val dateRebaseMode: DateTimeRebaseMode,
    val timestampRebaseMode: DateTimeRebaseMode,
    val hasInt96Timestamps: Boolean) extends ExtraInfo

// contains meta about a single block in a file
private case class ParquetSingleDataBlockMeta(
  filePath: Path,
  dataBlock: ParquetDataBlock,
  partitionValues: InternalRow,
  schema: ParquetSchemaWrapper,
  readSchema: StructType,
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
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data
 * @param debugDumpAlways whether to debug dump always or only on errors
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
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    useChunkedReader: Boolean,
    useSubPageChunked: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedBlocks,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
    numThreads, execMetrics)
  with ParquetPartitionReaderBase {

  // Some implicits to convert the base class to the sub-class and vice versa
  implicit def toMessageType(schema: SchemaBase): MessageType =
    schema.asInstanceOf[ParquetSchemaWrapper].schema

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
        val outputBlocks = withResource(outhmb) { _ =>
          withResource(new HostMemoryOutputStream(outhmb)) { out =>
            copyBlocksData(file, out, blocks.toSeq, offset, metrics)
          }
        }
        val bytesRead = fileSystemBytesRead() - startBytesRead
        (outputBlocks, bytesRead)
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
    checkIfNeedToSplitBlocks(currentBlockInfo.extraInfo.dateRebaseMode,
      nextBlockInfo.extraInfo.dateRebaseMode,
      currentBlockInfo.extraInfo.timestampRebaseMode,
      nextBlockInfo.extraInfo.timestampRebaseMode,
      currentBlockInfo.schema,
      nextBlockInfo.schema,
      currentBlockInfo.filePath.toString,
      nextBlockInfo.filePath.toString
    )
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

  private var currentTargetBatchSize = targetBatchSizeBytes

  override final def startNewBufferRetry(): Unit = {
    currentTargetBatchSize = targetBatchSizeBytes
  }

  override def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, readDataSchema: StructType,
      extraInfo: ExtraInfo): GpuDataProducer[Table] = {

    val parseOpts = getParquetOptions(readDataSchema, clippedSchema, useFieldId)

    // About to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    MakeParquetTableProducer(useChunkedReader, useSubPageChunked,
      conf, currentTargetBatchSize, parseOpts,
      dataBuffer, 0, dataSize, metrics,
      extraInfo.dateRebaseMode, extraInfo.timestampRebaseMode,
      extraInfo.hasInt96Timestamps, isSchemaCaseSensitive, useFieldId, readDataSchema,
      clippedSchema, splits, debugDumpPrefix, debugDumpAlways)
  }

  override def writeFileHeader(buffer: HostMemoryBuffer, bContext: BatchContext): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      out.write(ParquetPartitionReader.PARQUET_MAGIC)
      out.getPos
    }
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: collection.Seq[DataBlockBase], bContext: BatchContext): Long = {

    val actualFooterSize = calculateParquetFooterSize(blocks.toSeq, bContext.schema)
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
 * @param filterFunc a function to filter the necessary blocks from a given file
 * @param isSchemaCaseSensitive whether schema is case sensitive
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data
 * @param debugDumpAlways whether to debug dump always or only on errors
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param execMetrics metrics
 * @param partitionSchema Schema of partitions.
 * @param numThreads the size of the threadpool
 * @param maxNumFileProcessed the maximum number of files to read on the CPU side and waiting to be
 *                            processed on the GPU. This affects the amount of host memory used.
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 * @param useFieldId Whether to use field id for column matching
 * @param alluxioPathReplacementMap Map containing mapping of DFS scheme to Alluxio scheme
 * @param alluxioReplacementTaskTime Whether the Alluxio replacement algorithm is set to task time
 * @param queryUsesInputFile Whether the query requires the input file name functionality
 * @param keepReadsInOrder Whether to require the files to be read in the same order as Spark.
 *                         Defaults to true for formats that don't explicitly handle this.
 * @param combineConf configs relevant to combination
 */
class MultiFileCloudParquetPartitionReader(
    override val conf: Configuration,
    files: Array[PartitionedFile],
    filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
    override val isSchemaCaseSensitive: Boolean,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    useChunkedReader: Boolean,
    subPageChunked: Boolean,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    maxNumFileProcessed: Int,
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    useFieldId: Boolean,
    alluxioPathReplacementMap: Map[String, String],
    alluxioReplacementTaskTime: Boolean,
    queryUsesInputFile: Boolean,
    keepReadsInOrder: Boolean,
    combineConf: CombineConf)
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, null,
    execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes, ignoreCorruptFiles,
    alluxioPathReplacementMap, alluxioReplacementTaskTime, keepReadsInOrder, combineConf)
  with ParquetPartitionReaderBase {

  def checkIfNeedToSplit(current: HostMemoryBuffersWithMetaData,
      next: HostMemoryBuffersWithMetaData): Boolean = {

    checkIfNeedToSplitBlocks(current.dateRebaseMode,
      next.dateRebaseMode,
      current.timestampRebaseMode,
      next.timestampRebaseMode,
      ParquetSchemaWrapper(current.clippedSchema),
      ParquetSchemaWrapper(next.clippedSchema),
      current.partitionedFile.filePath.toString(),
      next.partitionedFile.filePath.toString()
    )
  }

  override def canUseCombine: Boolean = {
    if (queryUsesInputFile) {
      logDebug("Query uses input file name, can't use combine mode")
      false
    } else {
      combineConf.combineThresholdSize > 0
    }
  }

  private case class CombinedEmptyMeta(emptyNumRows: Long, emptyBufferSize: Long,
      emptyTotalBytesRead: Long, allEmpty: Boolean, metaForEmpty: HostMemoryEmptyMetaData)

  private case class CombinedMeta(allPartValues: Array[(Long, InternalRow)],
      toCombine: Array[HostMemoryBuffersWithMetaDataBase],
      firstNonEmpty: HostMemoryBuffersWithMetaData)

  // assumes all these are ok to combine and have the same metadata for schema
  // and *RebaseMode and timestamp type settings
  private def doCombineHMBs(combinedMeta: CombinedMeta): HostMemoryBuffersWithMetaDataBase = {
    val toCombineHmbs = combinedMeta.toCombine.filterNot(_.isInstanceOf[HostMemoryEmptyMetaData])
    val metaToUse = combinedMeta.firstNonEmpty
    logDebug(s"Using Combine mode and actually combining, num files ${toCombineHmbs.size} " +
      s"files: ${toCombineHmbs.map(_.partitionedFile.filePath).mkString(",")}")
    val startCombineTime = System.currentTimeMillis()
    // this size includes the written header and footer on each buffer so remove
    // the size of those to get data size
    val existingSizeUsed = toCombineHmbs.map { hbWithMeta =>
      hbWithMeta.memBuffersAndSizes.map(smb => Math.max(smb.bytes - PARQUET_META_SIZE, 0)).sum
    }.sum

    // since we know not all of them are empty and we know all these have the same schema since
    // we already separated, just use the clippedSchema from metadata
    val schemaToUse = metaToUse.clippedSchema
    val blocksAlreadyRead = toCombineHmbs.flatMap(_.memBuffersAndSizes.flatMap(_.blockMeta))
    val footerSize = calculateParquetFooterSize(blocksAlreadyRead.toSeq, schemaToUse)
    // all will have same schema so same number of columns
    val numCols = blocksAlreadyRead.head.getColumns().size()
    val extraMemory = calculateExtraMemoryForParquetFooter(numCols, blocksAlreadyRead.size)
    val initTotalSize = existingSizeUsed + footerSize + extraMemory
    val combined = closeOnExcept(HostMemoryBuffer.allocate(initTotalSize)) { combinedHmb =>
      var offset = withResource(new HostMemoryOutputStream(combinedHmb)) { out =>
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        out.getPos
      }
      val allOutputBlocks = new ArrayBuffer[BlockMetaData]()

      // copy the actual data
      toCombineHmbs.map { hbWithMeta =>
        hbWithMeta.memBuffersAndSizes.map { hmbInfo =>
          val copyAmount = hmbInfo.blockMeta.map { meta =>
            meta.getColumns.asScala.map(_.getTotalSize).sum
          }.sum
          if (copyAmount > 0 && hmbInfo.hmb != null) {
            combinedHmb.copyFromHostBuffer(offset, hmbInfo.hmb,
              ParquetPartitionReader.PARQUET_MAGIC.size, copyAmount)
          }
          val outputBlocks = computeBlockMetaData(hmbInfo.blockMeta, offset)
          allOutputBlocks ++= outputBlocks
          offset += copyAmount
          if (hmbInfo.hmb != null) {
            hmbInfo.hmb.close()
          }
        }
      }
      // using all of the actual combined output blocks meta calculate what the footer size
      // will really be
      val actualFooterSize = calculateParquetFooterSize(allOutputBlocks.toSeq, schemaToUse)
      var buf: HostMemoryBuffer = combinedHmb
      val totalBufferSize = if ((initTotalSize - offset) < actualFooterSize) {
        val newBufferSize = offset + actualFooterSize + 4 + 4
        logWarning(s"The original estimated size $initTotalSize is too small, " +
          s"reallocating and copying data to bigger buffer size: $newBufferSize")
        // Copy the old buffer to a new allocated bigger buffer and close the old buffer
        buf = withResource(combinedHmb) { _ =>
          withResource(new HostMemoryInputStream(combinedHmb, offset)) { in =>
            // realloc memory and copy
            closeOnExcept(HostMemoryBuffer.allocate(newBufferSize)) { newhmb =>
              withResource(new HostMemoryOutputStream(newhmb)) { out =>
                IOUtils.copy(in, out)
              }
              newhmb
            }
          }
        }
        newBufferSize
      } else {
        initTotalSize
      }

      withResource(buf.slice(offset, (totalBufferSize - offset))) { footerHmbSlice =>
        withResource(new HostMemoryOutputStream(footerHmbSlice)) { footerOut =>
          writeFooter(footerOut, allOutputBlocks.toSeq, schemaToUse)
          BytesUtils.writeIntLittleEndian(footerOut, footerOut.getPos.toInt)
          footerOut.write(ParquetPartitionReader.PARQUET_MAGIC)
          offset += footerOut.getPos
        }
      }
      val newHmbBufferInfo = SingleHMBAndMeta(buf, offset,
        combinedMeta.allPartValues.map(_._1).sum, Seq.empty)
      val newHmbMeta = HostMemoryBuffersWithMetaData(
        metaToUse.partitionedFile,
        metaToUse.origPartitionedFile, // this doesn't matter since already read
        Array(newHmbBufferInfo),
        offset,
        metaToUse.dateRebaseMode,
        metaToUse.timestampRebaseMode,
        metaToUse.hasInt96Timestamps,
        metaToUse.clippedSchema,
        metaToUse.readSchema,
        Some(combinedMeta.allPartValues))
      val filterTime = combinedMeta.toCombine.map(_.getFilterTime).sum
      val bufferTime = combinedMeta.toCombine.map(_.getBufferTime).sum
      newHmbMeta.setMetrics(filterTime, bufferTime)
      newHmbMeta
    }
    logDebug(s"Took ${(System.currentTimeMillis() - startCombineTime)} " +
      s"ms to do combine of ${toCombineHmbs.size} files, " +
      s"task id: ${TaskContext.get().taskAttemptId()}")
    combined
  }

  private def computeCombineHMBMeta(
      input: Array[HostMemoryBuffersWithMetaDataBase]): (CombinedMeta, CombinedEmptyMeta) = {
    val allPartValues = new ArrayBuffer[(Long, InternalRow)]()
    var allEmpty = true
    var metaForEmpty: HostMemoryEmptyMetaData = null
    var emptyNumRows = 0L
    var emptyBufferSize = 0L
    var emptyTotalBytesRead = 0L
    val toCombine = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    // this is only used when keepReadsInOrder is false
    val leftOversWhenNotKeepReadsInOrder = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    var firstNonEmpty: HostMemoryBuffersWithMetaData = null
    var numCombined: Int = 0
    var needsSplit = false
    var iterLoc = 0
    // iterating through this to handle the case we want to keep the files
    // in the same order as Spark
    while (!needsSplit && iterLoc < input.size) {
      val result = input(iterLoc)
      result match {
        case emptyHMData: HostMemoryEmptyMetaData =>
          if (metaForEmpty == null || emptyHMData.numRows > 0) {
            // we might have multiple EmptyMetaData results. If some are due to ignoring
            // missing files and others are row counts, we want to make sure we
            // take the metadata information from the ones with row counts because
            // the ones from ignoring missing files has less information with it.
            metaForEmpty = emptyHMData
          }
          val totalNumRows = result.memBuffersAndSizes.map(_.numRows).sum
          val partValues = result.partitionedFile.partitionValues
          allPartValues.append((totalNumRows, partValues))
          emptyBufferSize += emptyHMData.bufferSize
          emptyNumRows += emptyHMData.numRows
          emptyTotalBytesRead += emptyHMData.bytesRead
          toCombine += emptyHMData
          numCombined += 1
        case hmWithData: HostMemoryBuffersWithMetaData =>
          allEmpty = false
          if (firstNonEmpty != null && checkIfNeedToSplit(firstNonEmpty, hmWithData)) {
            // if we need to keep the same order as Spark we just stop here and put rest in
            // leftOverFiles, but if we don't then continue so we combine as much as possible
            if (keepReadsInOrder) {
              needsSplit = true
              combineLeftOverFiles = Some(input.drop(numCombined))
            } else {
              leftOversWhenNotKeepReadsInOrder += hmWithData
            }
          } else {
            if (firstNonEmpty == null) {
              firstNonEmpty = hmWithData
            }
            numCombined += 1
            toCombine += hmWithData
            val partValues = hmWithData.partitionedFile.partitionValues
            val totalNumRows = hmWithData.memBuffersAndSizes.map(_.numRows).sum
            allPartValues.append((totalNumRows, partValues))
          }
        case _ => throw new RuntimeException("Unknown HostMemoryBuffersWithMetaDataBase")
      }
      iterLoc += 1
    }
    if (!keepReadsInOrder && leftOversWhenNotKeepReadsInOrder.nonEmpty) {
      combineLeftOverFiles = Some(leftOversWhenNotKeepReadsInOrder.toArray)
    }
    val combinedMeta = CombinedMeta(allPartValues.toArray, toCombine.toArray, firstNonEmpty)
    val combinedEmptyMeta = CombinedEmptyMeta(emptyNumRows, emptyBufferSize, emptyTotalBytesRead,
      allEmpty, metaForEmpty)
    (combinedMeta, combinedEmptyMeta)
  }

  override def combineHMBs(
      input: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    if (input.size == 1) {
      input.head
    } else {
      val (combinedMeta, combinedEmptyMeta) = computeCombineHMBMeta(input)

      if (combinedEmptyMeta.allEmpty) {
        val metaForEmpty = combinedEmptyMeta.metaForEmpty
        HostMemoryEmptyMetaData(metaForEmpty.partitionedFile, // just pick one since not used
          metaForEmpty.origPartitionedFile,
          combinedEmptyMeta.emptyBufferSize,
          combinedEmptyMeta.emptyTotalBytesRead,
          metaForEmpty.dateRebaseMode, // these shouldn't matter since data is empty
          metaForEmpty.timestampRebaseMode, // these shouldn't matter since data is empty
          metaForEmpty.hasInt96Timestamps, // these shouldn't matter since data is empty
          metaForEmpty.clippedSchema,
          metaForEmpty.readSchema,
          combinedEmptyMeta.emptyNumRows,
          Some(combinedMeta.allPartValues)
        )
      } else {
        doCombineHMBs(combinedMeta)
      }
    }
  }

  private case class HostMemoryEmptyMetaData(
      override val partitionedFile: PartitionedFile,
      override val origPartitionedFile: Option[PartitionedFile],
      bufferSize: Long,
      override val bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      numRows: Long,
      override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
    extends HostMemoryBuffersWithMetaDataBase {
    override def memBuffersAndSizes: Array[SingleHMBAndMeta] =
      Array(SingleHMBAndMeta.empty(numRows))
  }

  case class HostMemoryBuffersWithMetaData(
      override val partitionedFile: PartitionedFile,
      override val origPartitionedFile: Option[PartitionedFile],
      override val memBuffersAndSizes: Array[SingleHMBAndMeta],
      override val bytesRead: Long,
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readSchema: StructType,
      override val allPartValues: Option[Array[(Long, InternalRow)]]
  ) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      file: PartitionedFile,
      origPartitionedFile: Option[PartitionedFile],
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      taskContext: TaskContext) extends Callable[HostMemoryBuffersWithMetaDataBase] with Logging {

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
          HostMemoryEmptyMetaData(file, origPartitionedFile, 0, 0,
            DateTimeRebaseLegacy, DateTimeRebaseLegacy,
            hasInt96Timestamps = false, null, null, 0)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
          HostMemoryEmptyMetaData(file, origPartitionedFile, 0, 0,
            DateTimeRebaseLegacy, DateTimeRebaseLegacy,
            hasInt96Timestamps = false, null, null, 0)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[SingleHMBAndMeta]
      var filterTime = 0L
      var bufferStartTime = 0L
      val result = try {
        val filterStartTime = System.nanoTime()
        val fileBlockMeta = filterFunc(file)
        filterTime = System.nanoTime() - filterStartTime
        bufferStartTime = System.nanoTime()
        if (fileBlockMeta.blocks.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          HostMemoryEmptyMetaData(file, origPartitionedFile, 0, bytesRead,
            fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
            fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema, 0)
        } else {
          blockChunkIter = fileBlockMeta.blocks.iterator.buffered
          if (isDone) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            // got close before finishing
            HostMemoryEmptyMetaData(file, origPartitionedFile, 0, bytesRead,
              fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
              fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema, 0)
          } else {
            if (fileBlockMeta.schema.getFieldCount == 0) {
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              val numRows = fileBlockMeta.blocks.map(_.getRowCount).sum.toInt
              HostMemoryEmptyMetaData(file, origPartitionedFile, 0, bytesRead,
                fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
                fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema, fileBlockMeta.readSchema,
                numRows)
            } else {
              val filePath = new Path(new URI(file.filePath.toString()))
              while (blockChunkIter.hasNext) {
                val blocksToRead = populateCurrentBlockChunk(blockChunkIter,
                  maxReadBatchSizeRows, maxReadBatchSizeBytes, fileBlockMeta.readSchema)
                val (dataBuffer, dataSize, blockMeta) =
                  readPartFile(blocksToRead, fileBlockMeta.schema, filePath)
                val numRows = blocksToRead.map(_.getRowCount).sum.toInt
                hostBuffers += SingleHMBAndMeta(dataBuffer, dataSize,
                  numRows, blockMeta)
              }
              val bytesRead = fileSystemBytesRead() - startingBytesRead
              if (isDone) {
                // got close before finishing
                hostBuffers.foreach(_.hmb.safeClose())
                HostMemoryEmptyMetaData(file, origPartitionedFile, 0, bytesRead,
                  fileBlockMeta.dateRebaseMode, fileBlockMeta.timestampRebaseMode,
                  fileBlockMeta.hasInt96Timestamps, fileBlockMeta.schema,
                  fileBlockMeta.readSchema, 0)
              } else {
                HostMemoryBuffersWithMetaData(file, origPartitionedFile, hostBuffers.toArray,
                  bytesRead, fileBlockMeta.dateRebaseMode,
                  fileBlockMeta.timestampRebaseMode, fileBlockMeta.hasInt96Timestamps,
                  fileBlockMeta.schema, fileBlockMeta.readSchema, None)
              }
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.foreach(_.hmb.safeClose())
          throw e
      }
      val bufferTime = System.nanoTime() - bufferStartTime
      result.setMetrics(filterTime, bufferTime)
      result
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
      origFile: Option[PartitionedFile],
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(file, origFile, filterFunc, tc)
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
  override def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
  Iterator[ColumnarBatch] = fileBufsAndMeta match {
    case meta: HostMemoryEmptyMetaData =>
      // Not reading any data, but add in partition data if needed
      val rows = meta.numRows.toInt
      val origBatch = if (meta.readSchema.isEmpty) {
        new ColumnarBatch(Array.empty, rows)
      } else {
        // Someone is going to process this data, even if it is just a row count
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val nullColumns = meta.readSchema.fields.safeMap(f =>
          GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
        new ColumnarBatch(nullColumns, rows)
      }

      // we have to add partition values here for this batch, we already verified that
      // its not different for all the blocks in this batch
      meta.allPartValues match {
        case Some(partRowsAndValues) =>
          val (rowsPerPart, partValues) = partRowsAndValues.unzip
          BatchWithPartitionDataUtils.addPartitionValuesToBatch(origBatch, rowsPerPart,
            partValues, partitionSchema, maxGpuColumnSizeBytes)
        case None =>
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(origBatch,
            meta.partitionedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
      }

    case buffer: HostMemoryBuffersWithMetaData =>
      val memBuffersAndSize = buffer.memBuffersAndSizes
      val hmbAndInfo = memBuffersAndSize.head
      val batchIter = readBufferToBatches(buffer.dateRebaseMode,
        buffer.timestampRebaseMode, buffer.hasInt96Timestamps, buffer.clippedSchema,
        buffer.readSchema, buffer.partitionedFile, hmbAndInfo.hmb, hmbAndInfo.bytes,
        buffer.allPartValues)
      if (memBuffersAndSize.length > 1) {
        val updatedBuffers = memBuffersAndSize.drop(1)
        currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
      } else {
        currentFileHostBuffers = None
      }
      batchIter
    case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
  }

  private def readBufferToBatches(
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      clippedSchema: MessageType,
      readDataSchema: StructType,
      partedFile: PartitionedFile,
      hostBuffer: HostMemoryBuffer,
      dataSize: Long,
      allPartValues: Option[Array[(Long, InternalRow)]]): Iterator[ColumnarBatch] = {

    val parseOpts = closeOnExcept(hostBuffer) { _ =>
      getParquetOptions(readDataSchema, clippedSchema, useFieldId)
    }
    val colTypes = readDataSchema.fields.map(f => f.dataType)

    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    RmmRapidsRetryIterator.withRetryNoSplit(hostBuffer) { _ =>
      // The MakeParquetTableProducer will close the input buffer, and that would be bad
      // because we don't want to close it until we know that we are done with it
      hostBuffer.incRefCount()
      val tableReader = MakeParquetTableProducer(useChunkedReader, subPageChunked,
        conf, targetBatchSizeBytes,
        parseOpts,
        hostBuffer, 0, dataSize, metrics,
        dateRebaseMode, timestampRebaseMode, hasInt96Timestamps,
        isSchemaCaseSensitive, useFieldId, readDataSchema, clippedSchema, files,
        debugDumpPrefix, debugDumpAlways)

      val batchIter = CachedGpuBatchIterator(tableReader, colTypes)

      if (allPartValues.isDefined) {
        val allPartInternalRows = allPartValues.get.map(_._2)
        val rowsPerPartition = allPartValues.get.map(_._1)
        new GpuColumnarBatchWithPartitionValuesIterator(batchIter, allPartInternalRows,
          rowsPerPartition, partitionSchema, maxGpuColumnSizeBytes)
      } else {
        // this is a bit weird, we don't have number of rows when allPartValues isn't
        // filled in so can't use GpuColumnarBatchWithPartitionValuesIterator
        batchIter.flatMap { batch =>
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(batch,
            partedFile.partitionValues, partitionSchema, maxGpuColumnSizeBytes)
        }
      }
    }
  }
}

object MakeParquetTableProducer extends Logging {
  def apply(
      useChunkedReader: Boolean,
      useSubPageChunked: Boolean,
      conf: Configuration,
      chunkSizeByteLimit: Long,
      opts: ParquetOptions,
      buffer: HostMemoryBuffer,
      offset: Long,
      len: Long,
      metrics : Map[String, GpuMetric],
      dateRebaseMode: DateTimeRebaseMode,
      timestampRebaseMode: DateTimeRebaseMode,
      hasInt96Timestamps: Boolean,
      isSchemaCaseSensitive: Boolean,
      useFieldId: Boolean,
      readDataSchema: StructType,
      clippedParquetSchema: MessageType,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean
  ): GpuDataProducer[Table] = {
    if (useChunkedReader) {
      val passReadLimit = if (useSubPageChunked) {
        4 * chunkSizeByteLimit
      } else {
        0L
      }
      ParquetTableReader(conf, chunkSizeByteLimit, passReadLimit, opts, buffer, offset,
        len, metrics, dateRebaseMode, timestampRebaseMode, hasInt96Timestamps,
        isSchemaCaseSensitive, useFieldId, readDataSchema, clippedParquetSchema,
        splits, debugDumpPrefix, debugDumpAlways)
    } else {
      val table = withResource(buffer) { _ =>
        try {
          RmmRapidsRetryIterator.withRetryNoSplit[Table] {
            withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
              metrics(GPU_DECODE_TIME))) { _ =>
              Table.readParquet(opts, buffer, offset, len)
            }
          }
        } catch {
          case e: Exception =>
            val dumpMsg = debugDumpPrefix.map { prefix =>
              val p = DumpUtils.dumpBuffer(conf, buffer, offset, len, prefix, ".parquet")
              s", data dumped to $p"
            }.getOrElse("")
            throw new IOException(s"Error when processing ${splits.mkString("; ")}$dumpMsg", e)
        }
      }
      closeOnExcept(table) { _ =>
        debugDumpPrefix.foreach { prefix =>
          if (debugDumpAlways) {
            val p = DumpUtils.dumpBuffer(conf, buffer, offset, len, prefix, ".parquet")
            logWarning(s"Wrote data for ${splits.mkString(", ")} to $p")
          }
        }
        GpuParquetScan.throwIfRebaseNeededInExceptionMode(table, dateRebaseMode,
          timestampRebaseMode)
        if (readDataSchema.length < table.getNumberOfColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read ${table.getNumberOfColumns} from ${splits.mkString("; ")}")
        }
      }
      metrics(NUM_OUTPUT_BATCHES) += 1
      val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(table,
        clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
      val outputTable = GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode,
        timestampRebaseMode)
      new SingleGpuDataProducer(outputTable)
    }
  }
}

case class ParquetTableReader(
    conf: Configuration,
    chunkSizeByteLimit: Long,
    passReadLimit: Long,
    opts: ParquetOptions,
    buffer: HostMemoryBuffer,
    offset: Long,
    len: Long,
    metrics : Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    isSchemaCaseSensitive: Boolean,
    useFieldId: Boolean,
    readDataSchema: StructType,
    clippedParquetSchema: MessageType,
    splits: Array[PartitionedFile],
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean) extends GpuDataProducer[Table] with Logging {
  private[this] val reader = new ParquetChunkedReader(chunkSizeByteLimit, passReadLimit, opts,
    buffer, offset, len)

  private[this] lazy val splitsString = splits.mkString("; ")

  override def hasNext: Boolean = reader.hasNext

  override def next: Table = {
    val table = withResource(new NvtxWithMetrics("Parquet decode", NvtxColor.DARK_GREEN,
      metrics(GPU_DECODE_TIME))) { _ =>
      try {
        reader.readChunk()
      } catch {
        case e: Exception =>
          val dumpMsg = debugDumpPrefix.map { prefix =>
            val p = DumpUtils.dumpBuffer(conf, buffer, offset, len, prefix, ".parquet")
            s", data dumped to $p"
          }.getOrElse("")
          throw new IOException(s"Error when processing $splitsString$dumpMsg", e)
      }
    }

    closeOnExcept(table) { _ =>
      GpuParquetScan.throwIfRebaseNeededInExceptionMode(table, dateRebaseMode, timestampRebaseMode)
      if (readDataSchema.length < table.getNumberOfColumns) {
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but read ${table.getNumberOfColumns} from $splitsString")
      }
    }
    metrics(NUM_OUTPUT_BATCHES) += 1
    val evolvedSchemaTable = ParquetSchemaUtils.evolveSchemaIfNeededAndClose(table,
      clippedParquetSchema, readDataSchema, isSchemaCaseSensitive, useFieldId)
    GpuParquetScan.rebaseDateTime(evolvedSchemaTable, dateRebaseMode, timestampRebaseMode)
  }

  override def close(): Unit = {
    debugDumpPrefix.foreach { prefix =>
      if (debugDumpAlways) {
        val p = DumpUtils.dumpBuffer(conf, buffer, offset, len, prefix, ".parquet")
        logWarning(s"Wrote data for $splitsString to $p")
      }
    }
    reader.close()
    buffer.close()
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
 * @param debugDumpAlways whether to debug dump always or only on errors
 */
class ParquetPartitionReader(
    override val conf: Configuration,
    split: PartitionedFile,
    filePath: Path,
    clippedBlocks: Iterable[BlockMetaData],
    clippedParquetSchema: MessageType,
    override val isSchemaCaseSensitive: Boolean,
    readDataSchema: StructType,
    debugDumpPrefix: Option[String],
    debugDumpAlways: Boolean,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    targetBatchSizeBytes: Long,
    useChunkedReader: Boolean,
    useSubPageChunked: Boolean,
    override val execMetrics: Map[String, GpuMetric],
    dateRebaseMode: DateTimeRebaseMode,
    timestampRebaseMode: DateTimeRebaseMode,
    hasInt96Timestamps: Boolean,
    useFieldId: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with ParquetPartitionReaderBase {

  private val blockIterator:  BufferedIterator[BlockMetaData] = clippedBlocks.iterator.buffered

  override def next(): Boolean = {
    if (batchIter.hasNext) {
      return true
    }
    batchIter = EmptyGpuColumnarBatchIterator
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
      } else {
        batchIter = readBatches()
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  private def readBatches(): Iterator[ColumnarBatch] = {
    withResource(new NvtxRange("Parquet readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes, readDataSchema)
      if (clippedParquetSchema.getFieldCount == 0) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.getRowCount).sum.toInt
        if (numRows == 0) {
          EmptyGpuColumnarBatchIterator
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val nullColumns = readDataSchema.safeMap(f =>
            GpuColumnVector.fromNull(numRows, f.dataType).asInstanceOf[SparkVector])
          new SingleGpuColumnarBatchIterator(new ColumnarBatch(nullColumns.toArray, numRows))
        }
      } else {
        val colTypes = readDataSchema.fields.map(f => f.dataType)
        val iter = if (currentChunkedBlocks.isEmpty) {
          CachedGpuBatchIterator(EmptyTableReader, colTypes)
        } else {
          val parseOpts = getParquetOptions(readDataSchema, clippedParquetSchema, useFieldId)
          val (dataBuffer, dataSize, _) = metrics(BUFFER_TIME).ns {
            readPartFile(currentChunkedBlocks, clippedParquetSchema, filePath)
          }
          if (dataSize == 0) {
            dataBuffer.close()
            CachedGpuBatchIterator(EmptyTableReader, colTypes)
          } else {
            // about to start using the GPU
            GpuSemaphore.acquireIfNecessary(TaskContext.get())

            RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
              // Inc the ref count because MakeParquetTableProducer will try to close the dataBuffer
              // which we don't want until we know that the retry is done with it.
              dataBuffer.incRefCount()
              val producer = MakeParquetTableProducer(useChunkedReader, useSubPageChunked, conf,
                targetBatchSizeBytes, parseOpts,
                dataBuffer, 0, dataSize, metrics,
                dateRebaseMode, timestampRebaseMode,
                hasInt96Timestamps, isSchemaCaseSensitive,
                useFieldId, readDataSchema,
                clippedParquetSchema, Array(split),
                debugDumpPrefix, debugDumpAlways)
              CachedGpuBatchIterator(producer, colTypes)
            }
          }
        }
        iter.map { batch =>
          logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          batch
        }
      }
    }
  }
}

object ParquetPartitionReader {
  private[rapids] val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private[rapids] val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private[rapids] val PARQUET_VERSION = 1

  private[rapids] trait CopyItem {
    val length: Long
  }

  private[rapids] case class LocalCopy(
      channel: SeekableByteChannel,
      length: Long,
      outputOffset: Long) extends CopyItem with Closeable {
    override def close(): Unit = {
      channel.close()
    }
  }

  private[rapids] case class CopyRange(
      offset: Long,
      length: Long,
      outputOffset: Long) extends CopyItem

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
}
