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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.json.{JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.types._

object GpuJsonUtils {
  // spark 2.x uses FastDateFormat, use getPattern
  def dateFormatInRead(options: JSONOptions): String = options.dateFormat.getPattern
  def timestampFormatInRead(options: JSONOptions): String = options.timestampFormat.getPattern
}

object GpuJsonScan {

  def tagSupport(
      sparkSession: SparkSession,
      dataSchema: StructType,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _]): Unit = {

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

    val types = readSchema.map(_.dataType)
    if (types.contains(DateType)) {
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.dateFormatInRead(parsedOptions), parseString = true)
    }

   if (types.contains(TimestampType)) {
      // Spark 2.x doesn't have zoneId, so use timeZone and then to id
      meta.checkTimeZoneId(parsedOptions.timeZone.toZoneId)
      GpuTextBasedDateUtils.tagCudfFormat(meta,
        GpuJsonUtils.timestampFormatInRead(parsedOptions), parseString = true)
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

    FileFormatChecks.tag(meta, readSchema, JsonFormatType, ReadFileOp)
  }
}
