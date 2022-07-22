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

package org.apache.spark.sql.catalyst.util.rapids

import java.text.ParseException
import java.time.DateTimeException
import java.time.format.DateTimeParseException

// Copied from Spark: org/apache/spark/sql/catalyst/util/TimestampFormatter.scala
// for for https://github.com/NVIDIA/spark-rapids/issues/6026
// It can be removed when Spark3.3 is the least supported Spark version
sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local time.
   *
   * @param s - string with timestamp to parse
   * @param allowTimeZone - indicates strict parsing of timezone
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   * @throws IllegalStateException The formatter for timestamp without time zone should always
   *                               implement this method. The exception should never be hit.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  def parseWithoutTimeZone(s: String, allowTimeZone: Boolean): Long =
    throw new IllegalStateException(
      s"The method `parseWithoutTimeZone(s: String, allowTimeZone: Boolean)` should be " +
        "implemented in the formatter of timestamp without time zone")

  /**
   * Parses a timestamp in a string and converts it to microseconds since Unix Epoch in local time.
   * Zone-id and zone-offset components are ignored.
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  @throws(classOf[IllegalStateException])
  final def parseWithoutTimeZone(s: String): Long =
  // This is implemented to adhere to the original behaviour of `parseWithoutTimeZone` where we
  // did not fail if timestamp contained zone-id or zone-offset component and instead ignored it.
    parseWithoutTimeZone(s, true)
}
