/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.utils

import org.apache.spark.sql.rapids.suites.{RapidsCastSuite, RapidsDataFrameAggregateSuite, RapidsJsonFunctionsSuite, RapidsJsonSuite, RapidsMathFunctionsSuite, RapidsRegexpExpressionsSuite, RapidsStringExpressionsSuite, RapidsStringFunctionsSuite}

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class RapidsTestSettings extends BackendTestSettings {

  enableSuite[RapidsCastSuite]
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("SPARK-35112: Cast string to day-time interval", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("SPARK-35735: Take into account day-time interval fields in cast", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("casting to fixed-precision decimals", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
  enableSuite[RapidsDataFrameAggregateSuite]
    .exclude("collect functions", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10772"))
    .exclude("collect functions structs", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10772"))
    .exclude("collect functions should be able to cast to array type with no null values", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10772"))
    .exclude("SPARK-17641: collect functions should not collect null values", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10772"))
    .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10772"))
  enableSuite[RapidsJsonFunctionsSuite]
  enableSuite[RapidsJsonSuite]
    .exclude("Casting long as timestamp", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("Write timestamps correctly with timestampFormat option and timeZone option", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: json in UTF-16 with BOM", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: multi-line json in UTF-32BE with BOM", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: Use user's encoding in reading of multi-line json in UTF-16LE", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: Unsupported encoding name", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: checking that the encoding option is case agnostic", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23723: specified encoding is not matched to actual encoding", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-23724: lineSep should be set if encoding if different from UTF-8", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-31716: inferring should handle malformed input", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-24190: restrictions for JSONOptions in read", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("exception mode for parsing date/timestamp string", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
  enableSuite[RapidsMathFunctionsSuite]
  enableSuite[RapidsRegexpExpressionsSuite]
    .exclude("RegexReplace", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10774"))
    .exclude("RegexExtract", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10774"))
    .exclude("RegexExtractAll", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10774"))
    .exclude("SPLIT", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10774"))
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("SPARK-22498: Concat should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22549: ConcatWs should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("StringComparison", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("Substring", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("ascii for string", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("base64/unbase64 for string", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("encode/decode for string", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("LOCATE", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("LPAD/RPAD", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("REPEAT", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("length for string / binary", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("ParseUrl", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
  enableSuite[RapidsStringFunctionsSuite]
}
// scalastyle:on line.size.limit
