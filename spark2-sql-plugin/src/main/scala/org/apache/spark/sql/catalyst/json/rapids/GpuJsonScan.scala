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
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.json.{JSONOptions, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}

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

  def dateFormatInRead(fileOptions: Serializable): Option[String] = {
    fileOptions match {
      case jsonOpts: JSONOptions => Option(jsonOpts.dateFormat.getPattern)
      case _ => throw new RuntimeException("Wrong file options.")
    }
  }

  def timestampFormatInRead(fileOptions: Serializable): Option[String] = {
    fileOptions match {
      case jsonOpts: JSONOptions => Option(jsonOpts.timestampFormat.getPattern)
      case _ => throw new RuntimeException("Wrong file options.")
    }
  }

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

    if (readSchema.map(_.dataType).contains(DateType)) {
      dateFormatInRead(parsedOptions).foreach { dateFormat =>
        if (!supportedDateFormats.contains(dateFormat)) {
          meta.willNotWorkOnGpu(s"the date format '${dateFormat}' is not supported'")
        }
      }
    }

    if (readSchema.map(_.dataType).contains(TimestampType)) {
      // Spark 2.x doesn't have zoneId, so use timeZone and then to id
      if (!TypeChecks.areTimestampsSupported(parsedOptions.timeZone.toZoneId)) {
        meta.willNotWorkOnGpu("Only UTC zone id is supported")
      }
      timestampFormatInRead(parsedOptions).foreach { tsFormat =>
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
