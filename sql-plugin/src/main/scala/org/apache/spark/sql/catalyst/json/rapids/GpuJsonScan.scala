/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.catalyst.json.rapids

import java.io.IOException
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import ai.rapids.cudf
import ai.rapids.cudf.{CaptureGroups, ColumnVector, DType, NvtxColor, RegexProgram, Scalar, Schema, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.{ColumnDefaultValuesShims, GpuJsonToStructsShim, LegacyBehaviorPolicyShim, ShimFilePartitionReaderFactory}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.json.{GpuJsonUtils, JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, DoubleType, FloatType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuJsonScan {

  def tagSupport(scanMeta: ScanMeta[JsonScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession,
      "JsonScan",
      scan.dataSchema,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupportOptions(opName: String,
                        options: JSONOptionsInRead,
                        meta: RapidsMeta[_, _, _]): Unit = {
    if (options.multiLine) {
      meta.willNotWorkOnGpu(s"$opName does not support multiLine")
    }

    // {"name": /* hello */ "Reynold Xin"} is not supported by CUDF
    if (options.allowComments) {
      meta.willNotWorkOnGpu(s"$opName does not support allowComments")
    }

    // {name: 'Reynold Xin'} is not supported by CUDF
    if (options.allowUnquotedFieldNames) {
      meta.willNotWorkOnGpu(s"$opName does not support allowUnquotedFieldNames")
    }

    // {'name': 'Reynold Xin'} turning single quotes off is not supported by CUDF
    if (!options.allowSingleQuotes) {
      meta.willNotWorkOnGpu(s"$opName does not support disabling allowSingleQuotes")
    }

    // {"name": "Cazen Lee", "price": "\$10"} is not supported by CUDF
    if (options.allowBackslashEscapingAnyCharacter) {
      meta.willNotWorkOnGpu(s"$opName does not support allowBackslashEscapingAnyCharacter")
    }

    // {"a":null, "b":1, "c":3.0}, Spark will drop column `a` if dropFieldIfAllNull is enabled.
    if (options.dropFieldIfAllNull) {
      meta.willNotWorkOnGpu(s"$opName does not support dropFieldIfAllNull")
    }

    if (options.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu(s"$opName only supports Permissive JSON parsing")
    }

    if (options.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu(opName + " only supports \"\\n\" as a line separator")
    }

    options.encoding.foreach(enc =>
      if (enc != StandardCharsets.UTF_8.name() && enc != StandardCharsets.US_ASCII.name()) {
      meta.willNotWorkOnGpu(s"$opName only supports UTF8 or US-ASCII encoded data")
    })
  }

  def tagJsonToStructsSupport(options:Map[String, String],
      dt: DataType,
      meta: RapidsMeta[_, _, _]): Unit = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      SQLConf.get.sessionLocalTimeZone,
      SQLConf.get.columnNameOfCorruptRecord)

    val hasDates = TrampolineUtil.dataTypeExistsRecursively(dt, _.isInstanceOf[DateType])
    if (hasDates) {
      GpuJsonToStructsShim.tagDateFormatSupport(meta,
        GpuJsonUtils.optionalDateFormatInRead(parsedOptions))
    }

    val hasTimestamps = TrampolineUtil.dataTypeExistsRecursively(dt, _.isInstanceOf[TimestampType])
    if (hasTimestamps) {
      GpuJsonToStructsShim.tagTimestampFormatSupport(meta,
        GpuJsonUtils.optionalTimestampFormatInRead(parsedOptions))

      GpuJsonUtils.optionalTimestampFormatInRead(parsedOptions) match {
        case None | Some("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]") =>
          // this is fine
        case timestampFormat =>
          meta.willNotWorkOnGpu(s"GpuJsonToStructs unsupported timestampFormat $timestampFormat")
      }
    }

    if (LegacyBehaviorPolicyShim.isLegacyTimeParserPolicy) {
      meta.willNotWorkOnGpu("LEGACY timeParserPolicy is not supported in GpuJsonToStructs")
    }

    tagSupportOptions("JsonToStructs", parsedOptions, meta)
  }

  def tagSupport(sparkSession: SparkSession,
                 opName: String,
                 dataSchema: StructType,
                 readSchema: StructType,
                 options: Map[String, String],
                 meta: RapidsMeta[_, _, _]): Unit = {
    val parsedOptions = new JSONOptionsInRead(
      options,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    if (!meta.conf.isJsonEnabled) {
      meta.willNotWorkOnGpu("JSON input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_JSON} to true")
    }

    if (!meta.conf.isJsonReadEnabled) {
      meta.willNotWorkOnGpu("JSON input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_JSON_READ} to true. Please note that, currently json reader does " +
        s"not support column prune, so user must specify the full schema or just let spark to " +
        s"infer the schema")
    }

    tagSupportOptions(opName, parsedOptions, meta)

    val types = readSchema.map(_.dataType)

    val hasDates = TrampolineUtil.dataTypeExistsRecursively(readSchema, _.isInstanceOf[DateType])
    if (hasDates) {

      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.dateFormatInRead(parsedOptions), parseString = true)

      GpuJsonToStructsShim.tagDateFormatSupportFromScan(meta,
        GpuJsonUtils.optionalDateFormatInRead(parsedOptions))

      // For date type, timezone needs to be checked also. This is because JVM timezone is used
      // to get days offset before rebasing Julian to Gregorian in Spark while not in Rapids.
      //
      // In details, for Json data format, Spark uses dateFormatter to parse string as date data
      // type which utilizes [[org.apache.spark.sql.catalyst.DateFormatter]]. For Json format, it
      // uses [[LegacyFastDateFormatter]] which is based on Apache Commons FastDateFormat. It parse
      // string into Java util.Date base on JVM default timezone. From Java util.Date, it's
      // converted into java.sql.Date type. By leveraging [[JavaDateTimeUtils]], it finally do
      // `rebaseJulianToGregorianDays` considering its offset to UTC timezone.
      if(!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)){
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }
    }

    if (types.contains(TimestampType) || types.contains(DateType)) {
      if (!GpuOverrides.isUTCTimezone(parsedOptions.zoneId)) {
        meta.willNotWorkOnGpu(s"Not supported timezone type ${parsedOptions.zoneId}.")
      }

      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.timestampFormatInRead(parsedOptions), parseString = true)
    }

    if(GpuJsonUtils.enableDateTimeParsingFallback(parsedOptions) &&
        (types.contains(DateType) || types.contains(TimestampType))) {
      meta.willNotWorkOnGpu(s"GpuJsonScan does not support enableDateTimeParsingFallback")
    }

    if (!meta.conf.isJsonFloatReadEnabled && types.contains(FloatType)) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading floats. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_FLOATS} to true.")
    }

    if (!meta.conf.isJsonDoubleReadEnabled && types.contains(DoubleType)) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading doubles. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_DOUBLES} to true.")
    }

    if (!meta.conf.isJsonDecimalReadEnabled && types.exists(_.isInstanceOf[DecimalType])) {
      meta.willNotWorkOnGpu("JSON reading is not 100% compatible when reading decimals. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_JSON_DECIMALS} to true.")
    }

    dataSchema.getFieldIndex(parsedOptions.columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = dataSchema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        // fallback to cpu to throw exception
        meta.willNotWorkOnGpu("GpuJsonScan does not support Corrupt Record which must " +
          "be string type and nullable")
      }
    }

    if (readSchema.length == 1 &&
      readSchema.head.name == parsedOptions.columnNameOfCorruptRecord) {
      // fallback to cpu to throw exception
      meta.willNotWorkOnGpu("GpuJsonScan does not support Corrupt Record")
    }

    if (ColumnDefaultValuesShims.hasExistenceDefaultValues(readSchema)) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support default values in schema")
    }

    FileFormatChecks.tag(meta, readSchema, JsonFormatType, ReadFileOp)
  }
}

case class GpuJsonScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType, // original schema passed in by the user (all the data)
    readDataSchema: StructType, // schema for data being read (including dropped columns)
    readPartitionSchema: StructType, // schema for the parts that come from the file path
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    mixedTypesAsStringEnabled: Boolean)
  extends TextBasedFileScan(sparkSession, options) with GpuScan {

  private lazy val parsedOptions: JSONOptions = new JSONOptions(
    options.asScala.toMap,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  // overrides nothing in 330
  def withFilters(partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    GpuJsonPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions, maxReaderBatchSizeRows,
      maxReaderBatchSizeBytes, maxGpuColumnSizeBytes, metrics, options.asScala.toMap,
      mixedTypesAsStringEnabled)
  }

  override def withInputFile(): GpuScan = this
}

case class GpuJsonPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in.
                                 // These are values from the file name/path itself
    parsedOptions: JSONOptions,
    maxReaderBatchSizeRows: Integer,
    maxReaderBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String],
    mixedTypesAsStringEnabled: Boolean) extends ShimFilePartitionReaderFactory(params) {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(new JsonPartitionReader(conf, partFile,
      dataSchema, readDataSchema, parsedOptions, maxReaderBatchSizeRows, maxReaderBatchSizeBytes,
      metrics, mixedTypesAsStringEnabled))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
  }
}

object JsonPartitionReader {
  def readToTable(
      dataBufferer: HostLineBufferer,
      cudfSchema: Schema,
      decodeTime: GpuMetric,
      jsonOpts:  cudf.JSONOptions,
      formatName: String,
      partFile: PartitionedFile): Table = {
    val dataSize = dataBufferer.getLength
    // cuDF does not yet support reading a subset of columns so we have
    // to apply the read schema projection here
    try {
      RmmRapidsRetryIterator.withRetryNoSplit(dataBufferer.getBufferAndRelease) { dataBuffer =>
        withResource(new NvtxWithMetrics(formatName + " decode",
          NvtxColor.DARK_GREEN, decodeTime)) { _ =>
          try {
            Table.readJSON(cudfSchema, jsonOpts, dataBuffer, 0, dataSize)
          } catch {
            case e: AssertionError if e.getMessage == "CudfColumns can't be null or empty" =>
              // this happens when every row in a JSON file is invalid (or we are
              // trying to read a non-JSON file format as JSON)
              throw new IOException(s"Error when processing file [$partFile]", e)
          }
        }
      }
    } catch {
      case e: Exception =>
        throw new IOException(s"Error when processing file [$partFile]", e)
    }
  }
}

class JsonPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    parsedOptions: JSONOptions,
    maxRowsPerChunk: Integer,
    maxBytesPerChunk: Long,
    execMetrics: Map[String, GpuMetric],
    enableMixedTypesAsString: Boolean)
  extends GpuTextBasedPartitionReader[HostLineBufferer, HostLineBuffererFactory.type](conf,
    partFile, dataSchema, readDataSchema, parsedOptions.lineSeparatorInRead, maxRowsPerChunk,
    maxBytesPerChunk, execMetrics, HostLineBuffererFactory) {

  def buildJsonOptions(parsedOptions: JSONOptions): cudf.JSONOptions = {
    cudf.JSONOptions.builder()
      .withRecoverWithNull(true)
      .withMixedTypesAsStrings(enableMixedTypesAsString)
      .withNormalizeSingleQuotes(parsedOptions.allowSingleQuotes)
      .build
  }

  /**
   * Read the host buffer to GPU table
   *
   * @param dataBuffer     host buffer to be read
   * @param dataSize       the size of host buffer
   * @param cudfSchema     the cudf schema of the data
   * @param readDataSchema the Spark schema describing what will be read
   * @param hasHeader      if it has header
   * @return table
   */
  override def readToTable(
      dataBufferer: HostLineBufferer,
      cudfSchema: Schema,
      readDataSchema: StructType,
      hasHeader: Boolean,
      decodeTime: GpuMetric): Table = {
    val jsonOpts = buildJsonOptions(parsedOptions)
    val jsonTbl = JsonPartitionReader.readToTable(dataBufferer, cudfSchema, decodeTime, jsonOpts,
      getFileFormatShortName, partFile)
    withResource(jsonTbl) { tbl =>
      val cudfColumnNames = cudfSchema.getColumnNames
      val columns = readDataSchema.map { field =>
        val i = cudfColumnNames.indexOf(field.name)
        if (i == -1) {
          throw new IllegalStateException(
            s"read schema contains field named '${field.name}' that is not in the data schema")
        }
        tbl.getColumn(i)
      }
      new Table(columns: _*)
    }
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "JSON"

  /**
   * Handle the table decoded by GPU
   *
   * @param readDataSchema the Spark schema describing what will be read
   * @param table          the table decoded by GPU
   * @return the new optional Table
   */
  override def handleResult(readDataSchema: StructType, table: Table): Option[Table] = {
    val tableCols = table.getNumberOfColumns

    // For the GPU resource handling convention, we should close input table and return a new
    // table just like below code. But for optimization, we just return the input table.
    // withResource(table) { _
    //  val cols = (0 until  table.getNumberOfColumns).map(i => table.getColumn(i))
    //  Some(new Table(cols: _*))
    // }
    if (readDataSchema.length == tableCols) {
      return Some(table)
    }

    // JSON will read all columns in dataSchema due to https://github.com/rapidsai/cudf/issues/9990,
    // but actually only the columns in readDataSchema are needed, we need to do the "column" prune,
    // Once the FEA is shipped in CUDF, we should remove this hack.
    withResource(table) { _ =>
      val prunedCols = readDataSchema.fieldNames.map { name =>
        val optionIndex = dataSchema.getFieldIndex(name)
        if (optionIndex.isEmpty || optionIndex.get >= tableCols) {
          throw new QueryExecutionException(s"Something wrong for $name columns in readDataSchema")
        }
        optionIndex.get
      }

      val prunedColumnVectors = prunedCols.map(i => table.getColumn(i))
      Some(new Table(prunedColumnVectors: _*))
    }
  }

  /**
   * JSON only supports unquoted lower-case "true" and "false" as valid boolean values.
   */
  override def castStringToBool(input: ColumnVector): ColumnVector = {
    withResource(Scalar.fromString(true.toString)) { t =>
      withResource(Scalar.fromNull(DType.BOOL8)) { nullBool =>
        withResource(ColumnVector.fromStrings(true.toString, false.toString)) { boolStrings =>
          withResource(input.contains(boolStrings)) { isValidBool =>
            withResource(input.equalTo(t)) {
              isValidBool.ifElse(_, nullBool)
            }
          }
        }
      }
    }
  }

  override def castStringToDate(input: ColumnVector, dt: DType): ColumnVector = {
    GpuJsonToStructsShim.castJsonStringToDateFromScan(input, dt, dateFormat)
  }

  /**
   * JSON has strict rules about valid numeric formats. See https://www.json.org/ for specification.
   *
   * Spark then has its own rules for supporting NaN and Infinity, which are not
   * valid numbers in JSON.
   */
  private def sanitizeNumbers(input: ColumnVector): ColumnVector = {
    // Note that this is not 100% consistent with Spark versions prior to Spark 3.3.0
    // due to https://issues.apache.org/jira/browse/SPARK-38060
    // cuDF `isFloat` supports some inputs that are not valid JSON numbers, such as `.1`, `1.`,
    // and `+1` so we use a regular expression to match valid JSON numbers instead
    val jsonNumberRegexp = "^-?[0-9]+(?:\\.[0-9]+)?(?:[eE][\\-\\+]?[0-9]+)?$"
    val prog = new RegexProgram(jsonNumberRegexp, CaptureGroups.NON_CAPTURE)
    val isValid = if (parsedOptions.allowNonNumericNumbers) {
      withResource(ColumnVector.fromStrings("NaN", "+INF", "-INF", "+Infinity",
        "Infinity", "-Infinity")) { nonNumeric =>
        withResource(input.matchesRe(prog)) { isJsonNumber =>
          withResource(input.contains(nonNumeric)) { nonNumeric =>
            isJsonNumber.or(nonNumeric)
          }
        }
      }
    } else {
      input.matchesRe(prog)
    }
    withResource(isValid) { _ =>
      withResource(Scalar.fromNull(DType.STRING)) { nullString =>
        isValid.ifElse(input, nullString)
      }
    }
  }

  override def castStringToFloat(input: ColumnVector, dt: DType): ColumnVector = {
    withResource(sanitizeNumbers(input)) { sanitizedInput =>
      super.castStringToFloat(sanitizedInput, dt)
    }
  }

  override def castStringToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {
    withResource(sanitizeNumbers(input)) { sanitizedInput =>
      super.castStringToDecimal(sanitizedInput, dt)
    }
  }

  override def dateFormat: Option[String] = GpuJsonUtils.optionalDateFormatInRead(parsedOptions)
  override def timestampFormat: String = GpuJsonUtils.timestampFormatInRead(parsedOptions)
}
