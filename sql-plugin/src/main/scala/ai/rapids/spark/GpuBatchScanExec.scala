/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.math.max

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, Table}
import ai.rapids.cudf
import ai.rapids.spark.GpuMetricNames._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceV2ScanExecBase, FilePartitionReaderFactory, TextBasedFileScan}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext


class GpuSerializableConfiguration(@transient var value: Configuration)
  extends Serializable with Logging {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends DataSourceV2ScanExecBase with GpuExec {

  @transient lazy val batch: Batch = scan.toBatch

  override def supportsColumnar = true

  override lazy val additionalMetrics = Map(
    "bufferTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "buffer time"),
    "peakDevMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak device memory")
  )

  scan match {
    case s: ScanWithMetrics => s.metrics = metrics ++ additionalMetrics
    case _ =>
  }

  override lazy val partitions: Seq[InputPartition] = batch.planInputPartitions()

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    new DataSourceRDD(sparkContext, partitions, readerFactory, supportsColumnar)
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
  def tagSupport(scanMeta: ScanMeta[CSVScan]) : Unit = {
    val scan = scanMeta.wrapped
    val options = scan.options
    val sparkSession = scan.sparkSession
    val parsedOptions: CSVOptions = new CSVOptions(
      options.asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    if (!parsedOptions.enforceSchema) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan always enforces schemas")
    }

    if (scan.dataSchema == null || scan.dataSchema.isEmpty) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan requires a specified data schema")
    }

    // TODO: Add an incompat override flag to specify no timezones appear in timestamp types?
    if (scan.dataSchema.map(_.dataType).contains(TimestampType)) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support parsing timestamp types")
    }

    if (parsedOptions.delimiter.length > 1) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support multi-character delimiters")
    }

    if (parsedOptions.delimiter.codePointAt(0) > 127) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII delimiters")
    }

    if (parsedOptions.quote > 127) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII quote chars")
    }

    if (parsedOptions.comment > 127) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support non-ASCII comment chars")
    }

    if (parsedOptions.escape != '\\') {
      // TODO need to fix this
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support modified escape chars")
    }

    // TODO charToEscapeQuoteEscaping???


    if (StandardCharsets.UTF_8.name() != parsedOptions.charset &&
      StandardCharsets.US_ASCII.name() != parsedOptions.charset) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan only supports UTF8 encoded data")
    }

    if (parsedOptions.ignoreLeadingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support ignoring leading white space")
    }

    if (parsedOptions.ignoreTrailingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support ignoring trailing white space")
    }

    if (parsedOptions.multiLine) {
      // TODO should we support this
      scanMeta.willNotWorkOnGpu("GpuCSVScan does not support multi-line")
    }

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      // TODO should we support this
      scanMeta.willNotWorkOnGpu("GpuCSVScan only supports \"\\n\" as a line separator")
    }

    if (parsedOptions.parseMode != PermissiveMode) {
      scanMeta.willNotWorkOnGpu("GpuCSVScan only supports Permissive CSV parsing")
    }
    // TODO parsedOptions.columnNameOfCorruptRecord
    // TODO parsedOptions.nanValue This is here by default so we need to be able to support it
    // TODO parsedOptions.positiveInf This is here by default so we need to be able to support it
    // TODO parsedOptions.negativeInf This is here by default so we need to be able to support it
    // TODO parsedOptions.zoneId This is here by default so we need to be able to support it
    // TODO parsedOptions.local This is here by default so we need to be able to support it
    // TODO parsedOptions.dateFormat This is here by default so we need to be able to support it
    // TODO parsedOptions.timestampFormat This is here by default so we need to be able to support it
    // TODO parsedOptions.emptyValueInRead
    // TODO ColumnPruning????
    // TODO sparkSession.sessionState.conf.caseSensitiveAnalysis on the column names
  }
}

case class GpuCSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType, // original schema passed in by the user (all the data)
    readDataSchema: StructType, // schema that is for the data being read in (including dropped columns)
    readPartitionSchema: StructType, // schema for the parts that come from the file path
    options: CaseInsensitiveStringMap,
    maxReaderBatchSize: Integer)
  extends TextBasedFileScan(sparkSession, fileIndex, readDataSchema, readPartitionSchema, options)
  with ScanWithMetrics {

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
      new GpuSerializableConfiguration(hadoopConf))

    GpuCSVPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, maxReaderBatchSize, metrics)
  }
}

case class GpuCSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[GpuSerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in. These are values from the file name/path itself
    parsedOptions: CSVOptions,
    maxReaderBatchSize: Integer,
    metrics: Map[String, SQLMetric]) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new CSVPartitionReader(conf, partFile, dataSchema, readDataSchema, parsedOptions,
      maxReaderBatchSize, metrics)
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
    execMetrics: Map[String, SQLMetric])
  extends PartitionReader[ColumnarBatch] with ScanWithMetrics {

  private var batch: Option[ColumnarBatch] = None
  private val lineReader = new HadoopFileLinesReader(partFile, parsedOptions.lineSeparatorInRead, conf)
  private var isFirstChunkForIterator: Boolean = true
  private var isExhausted: Boolean = false

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
    val result = HostMemoryBuffer.allocate(newSize)
    try {
      result.copyFromHostBuffer(0, original, 0, original.getLength)
      original.close()
    } catch {
      case e: Throwable =>
        result.close()
        throw e
    }
    result
  }

  private def readPartFile(): (HostMemoryBuffer, Long) = {
    val nvtxRange = new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))
    try {
      isFirstChunkForIterator = false
      val separator = parsedOptions.lineSeparatorInRead.getOrElse(Array('\n'.toByte))
      var succeeded = false
      var totalSize: Long = 0L
      var totalRows: Integer = 0
      var hmb = HostMemoryBuffer.allocate(estimatedHostBufferSize)
      try {
        while (lineReader.hasNext && totalRows != maxRowsPerChunk) {
          val line = lineReader.next()
          val lineSize = line.getLength
          val newTotal = totalSize + lineSize + separator.length
          if (newTotal > hmb.getLength) {
            hmb = growHostBuffer(hmb, newTotal)
          }
          // Can have an empty line, do not write this to buffer but add the separator and totalRows
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
    } finally {
      nvtxRange.close()
    }
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val nvtxRange = new NvtxWithMetrics("CSV readBatch", NvtxColor.GREEN, metrics(TOTAL_TIME))
    try {
      val hasHeader = partFile.start == 0 && isFirstChunkForIterator && parsedOptions.headerFlag
      val table = readToTable(hasHeader)
      try {
        if (readDataSchema.isEmpty) {
          table.map(t => new ColumnarBatch(Array.empty, t.getRowCount.toInt))
        } else {
          table.map(GpuColumnVector.from)
        }
      } finally {
        metrics(NUM_OUTPUT_BATCHES) += 1
        table.foreach(_.close())
      }
    } finally {
      nvtxRange.close()
    }
  }

  private def readToTable(hasHeader: Boolean): Option[Table] = {
    val (dataBuffer, dataSize) = readPartFile()
    var maxDeviceMemory:Long = 0
    try {
      if (dataSize == 0) {
        None
      } else {
        val csvSchemaBuilder = ai.rapids.cudf.Schema.builder
        dataSchema.foreach(f => csvSchemaBuilder.column(GpuColumnVector.getRapidsType(f.dataType), f.name))
        val newReadDataSchema: StructType = if (readDataSchema.isEmpty) {
          val smallestField = dataSchema.min(Ordering.by[StructField, Integer](_.dataType.defaultSize))
          StructType(Seq(smallestField))
        } else {
          readDataSchema
        }
        val csvOpts = buildCsvOptions(parsedOptions, newReadDataSchema, hasHeader)

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = Table.readCSV(csvSchemaBuilder.build(), csvOpts, dataBuffer, 0, dataSize)
        maxDeviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        val numColumns = table.getNumberOfColumns
        if (newReadDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${newReadDataSchema.length} columns " +
            s"but only read ${table.getNumberOfColumns} from $partFile")
        }
        Some(table)
      }
    } finally {
      metrics("peakDevMemory") += maxDeviceMemory
      dataBuffer.close()
    }
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = if (isExhausted) None else readBatch()
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
