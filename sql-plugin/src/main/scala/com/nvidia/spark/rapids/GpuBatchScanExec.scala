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

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import ai.rapids.cudf
import ai.rapids.cudf.{HostMemoryBuffer, Schema, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends DataSourceV2ScanExecBase with GpuExec {
  import GpuMetric._
  @transient lazy val batch: Batch = scan.toBatch

  override def supportsColumnar = true

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    GPU_DECODE_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY)) ++
      semaphoreMetrics

  scan match {
    case s: ScanWithMetrics => s.metrics = allMetrics ++ additionalMetrics
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
  var metrics : Map[String, GpuMetric] = Map.empty
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

    if (parsedOptions.charToEscapeQuoteEscaping.isDefined) {
      meta.willNotWorkOnGpu("GPU CSV Parsing does not support charToEscapeQuoteEscaping")
    }

    if (StandardCharsets.UTF_8.name() != parsedOptions.charset &&
        StandardCharsets.US_ASCII.name() != parsedOptions.charset) {
      meta.willNotWorkOnGpu("GpuCSVScan only supports UTF8 encoded data")
    }

    // TODO parsedOptions.ignoreLeadingWhiteSpaceInRead cudf always does this, but not for strings
    // TODO parsedOptions.ignoreTrailingWhiteSpaceInRead cudf always does this, but not for strings
    // TODO parsedOptions.multiLine cudf always does this, but it is not the default and it is not
    //  consistent

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu("GpuCSVScan only supports \"\\n\" as a line separator")
    }

    if (parsedOptions.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu("GpuCSVScan only supports Permissive CSV parsing")
    }

    // TODO parsedOptions.nanValue This is here by default so we should support it, but cudf
    // make it null https://github.com/NVIDIA/spark-rapids/issues/125
    parsedOptions.positiveInf.toLowerCase() match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
      case _ =>
        meta.willNotWorkOnGpu(s"the positive infinity value '${parsedOptions.positiveInf}'" +
            s" is not supported'")
    }
    parsedOptions.negativeInf.toLowerCase() match {
      case "-inf" | "-infinity" =>
      case _ =>
        meta.willNotWorkOnGpu(s"the positive infinity value '${parsedOptions.positiveInf}'" +
            s" is not supported'")
    }
    // parsedOptions.maxCharsPerColumn does not impact the final output it is a performance
    // improvement if you know the maximum size

    // parsedOptions.maxColumns was originally a performance optimization but is not used any more

    if (readSchema.map(_.dataType).contains(DateType)) {
      if (!meta.conf.isCsvDateReadEnabled) {
        meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading dates. " +
            s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DATES} to true.")
      }
      ShimLoader.getSparkShims.dateFormatInRead(parsedOptions).foreach { dateFormat =>
        if (!supportedDateFormats.contains(dateFormat)) {
          meta.willNotWorkOnGpu(s"the date format '${dateFormat}' is not supported'")
        }
      }
    }

    if (!meta.conf.isCsvBoolReadEnabled && readSchema.map(_.dataType).contains(BooleanType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading boolean. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_BOOLS} to true.")
    }

    if (!meta.conf.isCsvByteReadEnabled && readSchema.map(_.dataType).contains(ByteType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading bytes. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_BYTES} to true.")
    }

    if (!meta.conf.isCsvShortReadEnabled && readSchema.map(_.dataType).contains(ShortType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading shorts. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_SHORTS} to true.")
    }

    if (!meta.conf.isCsvIntReadEnabled && readSchema.map(_.dataType).contains(IntegerType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading integers. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_INTEGERS} to true.")
    }

    if (!meta.conf.isCsvLongReadEnabled && readSchema.map(_.dataType).contains(LongType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading longs. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_LONGS} to true.")
    }

    if (!meta.conf.isCsvFloatReadEnabled && readSchema.map(_.dataType).contains(FloatType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading floats. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_FLOATS} to true.")
    }

    if (!meta.conf.isCsvDoubleReadEnabled && readSchema.map(_.dataType).contains(DoubleType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading doubles. " +
          s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DOUBLES} to true.")
    }

    if (readSchema.map(_.dataType).contains(TimestampType)) {
      if (!meta.conf.isCsvTimestampReadEnabled) {
        meta.willNotWorkOnGpu("GpuCSVScan does not support parsing timestamp types. To " +
          s"enable it please set ${RapidsConf.ENABLE_CSV_TIMESTAMPS} to true.")
      }
      if (!TypeChecks.areTimestampsSupported(parsedOptions.zoneId)) {
        meta.willNotWorkOnGpu("Only UTC zone id is supported")
      }
      ShimLoader.getSparkShims.timestampFormatInRead(parsedOptions).foreach { tsFormat =>
        val parts = tsFormat.split("'T'", 2)
        if (parts.isEmpty) {
          meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
        }
        if (parts.headOption.exists(h => !supportedDateFormats.contains(h))) {
          meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
        }
        if (parts.length > 1 && !supportedTsPortionFormats.contains(parts(1))) {
          meta.willNotWorkOnGpu(s"the timestamp format '$tsFormat' is not supported")
        }
      }
    }
    // TODO parsedOptions.emptyValueInRead

    FileFormatChecks.tag(meta, readSchema, CsvFormatType, ReadFileOp)
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

  // overrides nothing in 330
  def withFilters(
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
    metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory {

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
    execMetrics: Map[String, GpuMetric]) extends
  GpuTextBasedPartitionReader(conf, partFile, dataSchema, readDataSchema,
    parsedOptions.lineSeparatorInRead, maxRowsPerChunk, maxBytesPerChunk, execMetrics) {

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
   * Read the host buffer to GPU table
   *
   * @param dataBuffer     host buffer to be read
   * @param dataSize       the size of host buffer
   * @param cudfSchema     the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param isFirstChunk   if it is the first chunk
   * @return table
   */
  override def readToTable(
      dataBuffer: HostMemoryBuffer,
      dataSize: Long, cudfSchema: Schema,
      readDataSchema: StructType,
      isFirstChunk: Boolean): Table = {
    val hasHeader = isFirstChunk && parsedOptions.headerFlag
    val csvOpts = buildCsvOptions(parsedOptions, readDataSchema, hasHeader)
    Table.readCSV(cudfSchema, csvOpts, dataBuffer, 0, dataSize)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "CSV"
}
