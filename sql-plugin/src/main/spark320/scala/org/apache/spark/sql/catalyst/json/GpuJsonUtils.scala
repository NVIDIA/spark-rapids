/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.catalyst.json

import org.apache.spark.sql.internal.SQLConf

object GpuJsonUtils {

  def optionalDateFormatInRead(options: JSONOptions): Option[String] =
    Some(options.dateFormat)

  def optionalDateFormatInRead(options: Map[String, String]): Option[String] =
    optionalDateFormatInRead(parseJSONReadOptions(options))

  def dateFormatInRead(options: JSONOptions): String = options.dateFormat

  def dateFormatInRead(options: Map[String, String]): String =
    dateFormatInRead(parseJSONReadOptions(options))

  def optionalTimestampFormatInRead(options: JSONOptions): Option[String] =
    Some(options.timestampFormat)
  def optionalTimestampFormatInRead(options: Map[String, String]): Option[String] =
    optionalTimestampFormatInRead(parseJSONReadOptions(options))

  def timestampFormatInRead(options: JSONOptions): String = options.timestampFormat

  def dateFormatInWrite(options: JSONOptions): String =
    options.dateFormat

  def timestampFormatInWrite(options: JSONOptions): String =
    options.timestampFormat

  def enableDateTimeParsingFallback(options: JSONOptions): Boolean = false

  def parseJSONOptions(options: Map[String, String]) = {
    new JSONOptions(
      options,
      SQLConf.get.sessionLocalTimeZone,
      SQLConf.get.columnNameOfCorruptRecord)
  }

  def parseJSONReadOptions(options: Map[String, String]) = {
    new JSONOptionsInRead(
      options,
      SQLConf.get.sessionLocalTimeZone,
      SQLConf.get.columnNameOfCorruptRecord)
  }
}
