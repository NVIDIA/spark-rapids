/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, Scalar, Schema, Table}
import com.nvidia.spark.rapids._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.json.{JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan, TextBasedFileScan}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, DecimalType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuJsonScan {

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

  def tagSupport(scanMeta: ScanMeta[JsonScan]) : Unit = {
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

    if (parsedOptions.multiLine) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support multiLine")
    }

    // {"name": /* hello */ "Reynold Xin"} is not supported by CUDF
    if (parsedOptions.allowComments) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support allowComments")
    }

    // {name: 'Reynold Xin'} is not supported by CUDF
    if (parsedOptions.allowUnquotedFieldNames) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support allowUnquotedFieldNames")
    }

    // {'name': 'Reynold Xin'} is not supported by CUDF
    if (options.get("allowSingleQuotes").map(_.toBoolean).getOrElse(false)) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support allowSingleQuotes")
    }

    // {"name": "Cazen Lee", "price": "\$10"} is not supported by CUDF
    if (parsedOptions.allowBackslashEscapingAnyCharacter) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support allowBackslashEscapingAnyCharacter")
    }

    // {"a":null, "b":1, "c":3.0}, Spark will drop column `a` if dropFieldIfAllNull is enabled.
    if (parsedOptions.dropFieldIfAllNull) {
      meta.willNotWorkOnGpu("GpuJsonScan does not support dropFieldIfAllNull")
    }

    if (parsedOptions.parseMode != PermissiveMode) {
      meta.willNotWorkOnGpu("GpuJsonScan only supports Permissive JSON parsing")
    }

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu("GpuJsonScan only supports \"\\n\" as a line separator")
    }

    parsedOptions.encoding.foreach(enc =>
      if (enc != StandardCharsets.UTF_8.name() && enc != StandardCharsets.US_ASCII.name()) {
      meta.willNotWorkOnGpu("GpuJsonScan only supports UTF8 or US-ASCII encoded data")
    })

    if (readSchema.map(_.dataType).contains(DateType)) {
      ShimLoader.getSparkShims.dateFormatInRead(parsedOptions).foreach { dateFormat =>
        DateUtils.tagAndGetCudfFormat(meta, dateFormat, parseString = true)
      }
    }

    if (readSchema.map(_.dataType).contains(TimestampType)) {
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
    maxReaderBatchSizeBytes: Long)
  extends TextBasedFileScan(sparkSession, options) with ScanWithMetrics {

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
      maxReaderBatchSizeBytes, metrics)
  }
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
    metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(new JsonPartitionReader(conf, partFile,
      dataSchema, readDataSchema, parsedOptions, maxReaderBatchSizeRows, maxReaderBatchSizeBytes,
      metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
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
    execMetrics: Map[String, GpuMetric])
  extends GpuTextBasedPartitionReader(conf, partFile, dataSchema, readDataSchema,
    parsedOptions.lineSeparatorInRead, maxRowsPerChunk, maxBytesPerChunk, execMetrics) {

  def buildJsonOptions(parsedOptions: JSONOptions): cudf.JSONOptions = {
    val builder = cudf.JSONOptions.builder()
    builder.build
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
      dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      cudfSchema: Schema,
      readDataSchema: StructType,
      hasHeader: Boolean): Table = {

    val jsonOpts = buildJsonOptions(parsedOptions)
    // cuDF does not yet support reading a subset of columns so we have
    // to apply the read schema projection here
    withResource(Table.readJSON(cudfSchema, jsonOpts, dataBuffer, 0, dataSize)) { tbl =>
      val columns = new ListBuffer[ColumnVector]()
      closeOnExcept(columns) { _ =>
        for (name <- readDataSchema.fieldNames) {
          val i = cudfSchema.getColumnNames.indexOf(name)
          if (i == -1) {
            throw new IllegalStateException(
              s"read schema contains field named '$name' that is not in the data schema")
          }
          columns += tbl.getColumn(i)
        }
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
    withResource(Scalar.fromString("true")) { t =>
      withResource(Scalar.fromString("false")) { f =>
        withResource(input.equalTo(t)) { isTrue =>
          withResource(input.equalTo(f)) { isFalse =>
            withResource(isTrue.or(isFalse)) { isValidBool =>
              withResource(Scalar.fromNull(DType.BOOL8)) { nullBool =>
                isValidBool.ifElse(isTrue, nullBool)
              }
            }
          }
        }
      }
    }
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
    val isValid = if (parsedOptions.allowNonNumericNumbers) {
      withResource(ColumnVector.fromStrings("NaN", "+INF", "-INF", "+Infinity",
        "Infinity", "-Infinity")) { nonNumeric =>
        withResource(input.matchesRe(jsonNumberRegexp)) { isJsonNumber =>
          withResource(input.contains(nonNumeric)) { nonNumeric =>
            isJsonNumber.or(nonNumeric)
          }
        }
      }
    } else {
      input.matchesRe(jsonNumberRegexp)
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

  override def dateFormat: String = parsedOptions.dateFormat

}
