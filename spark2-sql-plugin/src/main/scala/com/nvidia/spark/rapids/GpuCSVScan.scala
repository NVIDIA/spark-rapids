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

package com.nvidia.spark.rapids

import java.nio.charset.StandardCharsets

import com.nvidia.spark.rapids._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.GpuCsvUtils
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.execution.datasources.csv.CSVOptions
import org.apache.spark.sql.rapids.LegacyTimeParserPolicy
import org.apache.spark.sql.types._

object GpuCSVScan {
  def tagSupport(
      sparkSession: SparkSession,
      dataSchema: StructType,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _]): Unit = {
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

    // 2.x only supports delimiter as char
    /*
    if (parsedOptions.delimiter.length > 1) {
      meta.willNotWorkOnGpu("GpuCSVScan does not support multi-character delimiters")
    }
    */

    // delimiter is char in 2.x
    if (parsedOptions.delimiter > 127) {
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

    // 2.x doesn't have linSeparator config
    // CSV text with '\n', '\r' and '\r\n' as line separators.
    // Since I have no way to check in 2.x we will just assume it works for explain until
    // they move to 3.x
    /*
    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      meta.willNotWorkOnGpu("GpuCSVScan only supports \"\\n\" as a line separator")
    }
    */

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

    val types = readSchema.map(_.dataType).toSet
    if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy &&
        (types.contains(DateType) ||
        types.contains(TimestampType))) {
      // Spark's CSV parser will parse the string "2020-50-16" to the date 2024/02/16 when
      // timeParserPolicy is set to LEGACY mode and we would reject this as an invalid date
      // so we fall back to CPU
      meta.willNotWorkOnGpu(s"GpuCSVScan does not support timeParserPolicy=LEGACY")
    }

    if (types.contains(DateType)) {
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuCsvUtils.dateFormatInRead(parsedOptions), parseString = true)
    }

    if (types.contains(TimestampType)) {
      // Spark 2.x doesn't have zoneId, so use timeZone and then to id
      meta.checkTimeZoneId(parsedOptions.timeZone.toZoneId)
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuCsvUtils.timestampFormatInRead(parsedOptions), parseString = true)
    }
    // TODO parsedOptions.emptyValueInRead

    if (!meta.conf.isCsvFloatReadEnabled && types.contains(FloatType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading floats. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_FLOATS} to true.")
    }

    if (!meta.conf.isCsvDoubleReadEnabled && types.contains(DoubleType)) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading doubles. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DOUBLES} to true.")
    }

    if (!meta.conf.isCsvDecimalReadEnabled && types.exists(_.isInstanceOf[DecimalType])) {
      meta.willNotWorkOnGpu("CSV reading is not 100% compatible when reading decimals. " +
        s"To enable it please set ${RapidsConf.ENABLE_READ_CSV_DECIMALS} to true.")
    }

    FileFormatChecks.tag(meta, readSchema, CsvFormatType, ReadFileOp)
  }
}
