/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.math.max

import ai.rapids.cudf
import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, Table}
import com.nvidia.spark.rapids.GpuMetricNames._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration


case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends DataSourceV2ScanExecBase with GpuExec {

  @transient lazy val batch: Batch = scan.toBatch

  override def supportsColumnar = true

  override lazy val additionalMetrics = GpuMetricNames.buildGpuScanMetrics(sparkContext)

  scan match {
    case s: ScanWithMetrics => s.metrics = metrics ++ additionalMetrics
    case _ =>
  }

  override lazy val partitions: Seq[InputPartition] = batch.planInputPartitions()

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    new GpuDataSourceRDD(sparkContext, partitions, readerFactory)
  }

  override def doCanonicalize(): GpuBatchScanExec = {
    this.copy(output = output.map(QueryPlan.normalizeExpressions(_, output)))
  }
}

trait ScanWithMetrics {
  //this is initialized by the exec post creation
  var metrics : Map[String, SQLMetric] = Map.empty
}

object GpuCSVScan {
  private val supportedDateFormats = Set(
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "yyyy-MM",
    "yyyy/MM",
    "MM-yyyy",
    "MM/yyyy",
    "MM-dd-yyyy",
    "MM/dd/yyyy"
    // TODO "dd-MM-yyyy" and "dd/MM/yyyy" can also be supported, but only if we set
    // dayfirst to true in the parser config. This is not plumbed into the java cudf yet
    // and would need to coordinate with the timestamp format too, because both cannot
    // coexist
  )

  private val supportedTsPortionFormats = Set(
    "HH:mm:ss.SSSXXX",
    "HH:mm:ss[.SSS][XXX]",
    "HH:mm",
    "HH:mm:ss",
    "HH:mm[:ss]",
    "HH:mm:ss.SSS",
    "HH:mm:ss[.SSS]"
  )

  def tagSupport(scanMeta: ScanMeta[CSVScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession,
      scan.dataSchema,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      dataSchema: StructType,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {
    val parsedOptions: CSVOptions = new CSVOptions(
      options,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    if (!meta.conf.isCsvEnabled) {
      meta.willNotWorkOnGpu("CSV input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_CSV} to true")
    }

    if (!meta.conf.isCsvReadEnabled) {
      meta.willNotWorkOnGpu("CSV input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_CSV_READ} to true")
    }

    if (!parsedOptions.enforceSchema) {
      meta.willNotWorkOnGpu("GpuCSVScan always enforces schemas")
    }

    if (dataSchema == null || dataSchema.isEmpty) {
      meta.willNotWorkOnGpu("GpuCSVScan requires a specified data schema")
    }

    if (parsedOptions.delimiter.length > 1) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support multi-character delimiters")
    }

    if (parsedOptions.delimiter.codePointAt(0) > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII delimiters")
    }

    if (parsedOptions.quote > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII quote chars")
    }

    if (parsedOptions.comment > 127) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII comment chars")
    }

    if (parsedOptions.escape != '\\') {
      meta.willNotWorkOnGpu("GpuCSVScan does not support modified escape chars")
    }

    // TODO charToEscapeQuoteEscaping???

    if (StandardCharsets.UTF_8.name() != parsedOptions.charset &&
        StandardCharsets.US_ASCII.name() != parsedOptions.charset) {
      meta.willNotWorkOnGpu("GpuCSVScan only supports UTF8 encoded data")
    }

    if (parsedOptions.ignoreLeadingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      meta.willNotWorkOnGpu("GpuCSVScan does not support ignoring leading white space")
    }

    if (parsedOptions.ignoreTrailingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      meta.willNotWorkOnGpu("GpuCSVScan does not support ignoring trailing white space")
    }

    if (parsedOptions.multiLine) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support multi-line")
    }

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu("GpuCSVScan only supports \"\\n\" as a line separator")
    }

    if (parsedOptions.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu("GpuCSVScan only supports Permissive CSV parsing")
    }

    // TODO parsedOptions.nanValue This is here by default so we need to be able to support it
    // TODO parsedOptions.positiveInf This is here by default so we need to be able to support it
    // TODO parsedOptions.negativeInf This is here by default so we need to be able to support it

    if (readSchema.map(_.dataType).contains(DateType) &&
      !supportedDateFormats.contains(parsedOptions.dateFormat)) {
      meta.willNotWorkOnGpu(s"the date format '${parsedOptions.dateFormat}' is not supported'")
    }

    if (readSchema.map(_.dataType).contains(TimestampType)) {
      if (!meta.conf.isCsvTimestampEnabled) {
        meta.willNotWorkOnGpu("GpuCSVScan does not support parsing timestamp types. To " +
          s"enable it please set ${RapidsConf.ENABLE_CSV_TIMESTAMPS} to true.")
      }
      if (parsedOptions.zoneId.normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
        meta.willNotWorkOnGpu("Only UTC zone id is supported")
      }
      val tsFormat = parsedOptions.timestampFormat
      val parts = tsFormat.split("'T'", 2)
      if (parts.length == 0) {
        meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
      }
      if (parts.length > 0 && !supportedDateFormats.contains(parts(0))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
      }
      if (parts.length > 1 && !supportedTsPortionFormats.contains(parts(1))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
      }
    }
    // TODO parsedOptions.emptyValueInRead
  }
}

case class GpuCSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType, // original schema passed in by the user (all the data)
    readDataSchema: StructType, // schema for data being read (including dropped columns)
    readPartitionSchema: StructType, // schema for the parts that come from the file path
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long)
  extends TextBasedFileScan(sparkSession, options) with ScanWithMetrics {

  private lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def isSplitable(path: Path): Boolean = {
    CSVDataSource(parsedOptions).isSplitable && super.isSplitable(path)
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    if (!super.isSplitable(path)) {
      super.getFileUnSplittableReason(path)
    } else {
      "the csv datasource is set multiLine mode"
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    GpuCSVPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, maxReaderBatchSizeRows,
      maxReaderBatchSizeBytes, metrics)
  }

  override def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def equals(obj: Any): Boolean = obj match {
    case c: GpuCSVScan =>
      super.equals(c) && dataSchema == c.dataSchema && options == c.options &&
      maxReaderBatchSizeRows == c.maxReaderBatchSizeRows &&
      maxReaderBatchSizeBytes == c.maxReaderBatchSizeBytes
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

case class GpuCSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in.
                                 // These are values from the file name/path itself
    parsedOptions: CSVOptions,
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    metrics: Map[String, SQLMetric]) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(new CSVPartitionReader(conf, partFile, dataSchema,
      readDataSchema, parsedOptions, maxReaderBatchSizeRows, maxReaderBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}


class CSVPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    parsedOptions: CSVOptions,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, SQLMetric])
  extends PartitionReader[ColumnarBatch] with ScanWithMetrics with Arm {

  private var batch: Option[ColumnarBatch] = None
  private val lineReader = new HadoopFileLinesReader(partFile, parsedOptions.lineSeparatorInRead,
    conf)
  private var isFirstChunkForIterator: Boolean = true
  private var isExhausted: Boolean = false
  private var maxDeviceMemory: Long = 0

  metrics = execMetrics

  private lazy val estimatedHostBufferSize: Long = {
    val rawPath = new Path(partFile.filePath)
    val fs = rawPath.getFileSystem(conf)
    val path = fs.makeQualified(rawPath)
    val fileSize = fs.getFileStatus(path).getLen
    val codecFactory = new CompressionCodecFactory(conf)
    val codec = codecFactory.getCodec(path)
    if (codec != null) {
      // wild guess that compression is 2X or less
      partFile.length * 2
    } else if (partFile.start + partFile.length == fileSize) {
      // last split doesn't need to read an additional record
      partFile.length
    } else {
      // wild guess for extra space needed for the record after the split end offset
      partFile.length + 128 * 1024
    }
  }

  def buildCsvOptions(
      parsedOptions: CSVOptions,
      schema: StructType,
      hasHeader: Boolean): cudf.CSVOptions = {
    val builder = cudf.CSVOptions.builder()
    builder.withDelim(parsedOptions.delimiter.charAt(0))
    builder.hasHeader(hasHeader)
    // TODO parsedOptions.parseMode
    builder.withQuote(parsedOptions.quote)
    builder.withComment(parsedOptions.comment)
    builder.withNullValue(parsedOptions.nullValue)
    builder.includeColumn(schema.fields.map(_.name): _*)
    builder.build
  }

  /**
   * Grows a host buffer, returning a new buffer and closing the original
   * after copying the data into the new buffer.
   * @param original the original host memory buffer
   */
  private def growHostBuffer(original: HostMemoryBuffer, needed: Long): HostMemoryBuffer = {
    val newSize = Math.max(original.getLength * 2, needed)
    closeOnExcept(HostMemoryBuffer.allocate(newSize)) { result =>
      result.copyFromHostBuffer(0, original, 0, original.getLength)
      original.close()
      result
    }
  }

  private def readPartFile(): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
        metrics("bufferTime"))) { _ =>
      isFirstChunkForIterator = false
      val separator = parsedOptions.lineSeparatorInRead.getOrElse(Array('\n'.toByte))
      var succeeded = false
      var totalSize: Long = 0L
      var totalRows: Integer = 0
      var hmb = HostMemoryBuffer.allocate(estimatedHostBufferSize)
      try {
        while (lineReader.hasNext
          && totalRows != maxRowsPerChunk
          && totalSize <= maxBytesPerChunk /* soft limit and returns at least one row */) {
          val line = lineReader.next()
          val lineSize = line.getLength
          val newTotal = totalSize + lineSize + separator.length
          if (newTotal > hmb.getLength) {
            hmb = growHostBuffer(hmb, newTotal)
          }
          // Can have an empty line, do not write this to buffer but add the separator 
          // and totalRows
          if (lineSize != 0) {
            hmb.setBytes(totalSize, line.getBytes, 0, lineSize)
          }
          hmb.setBytes(totalSize + lineSize, separator, 0, separator.length)
          totalRows += 1
          totalSize = newTotal
        }
        //Indicate this is the last chunk
        isExhausted = !lineReader.hasNext
        succeeded = true
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
      (hmb, totalSize)
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("CSV readBatch", NvtxColor.GREEN, metrics(TOTAL_TIME))) { _ =>
      val hasHeader = partFile.start == 0 && isFirstChunkForIterator && parsedOptions.headerFlag
      val table = readToTable(hasHeader)
      try {
        if (readDataSchema.isEmpty) {
          table.map(t => new ColumnarBatch(Array.empty, t.getRowCount.toInt))
        } else {
          table.map(GpuColumnVector.from(_, readDataSchema.toArray.map(_.dataType)))
        }
      } finally {
        metrics(NUM_OUTPUT_BATCHES) += 1
        table.foreach(_.close())
      }
    }
  }

  private def readToTable(hasHeader: Boolean): Option[Table] = {
    val (dataBuffer, dataSize) = readPartFile()
    try {
      if (dataSize == 0) {
        None
      } else {
        val newReadDataSchema: StructType = if (readDataSchema.isEmpty) {
          val smallestField = 
              dataSchema.min(Ordering.by[StructField, Integer](_.dataType.defaultSize))
          StructType(Seq(smallestField))
        } else {
          readDataSchema
        }
        val cudfSchema = GpuColumnVector.from(dataSchema)
        val csvOpts = buildCsvOptions(parsedOptions, newReadDataSchema, hasHeader)
        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        // The buffer that is sent down
        val table = withResource(new NvtxWithMetrics("CSV decode", NvtxColor.DARK_GREEN,
            metrics(GPU_DECODE_TIME))) { _ =>
          Table.readCSV(cudfSchema, csvOpts, dataBuffer, 0, dataSize)
        }
        maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
        val numColumns = table.getNumberOfColumns
        if (newReadDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${newReadDataSchema.length} columns " +
            s"but only read ${table.getNumberOfColumns} from $partFile")
        }
        Some(table)
      }
    } finally {
      dataBuffer.close()
    }
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = if (isExhausted) {
      metrics("peakDevMemory").set(maxDeviceMemory)
      None
    } else {
      readBatch()
    }
    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    lineReader.close()
    batch.foreach(_.close())
    batch = None
    isExhausted = true
  }
}
