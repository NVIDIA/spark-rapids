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
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner", UNKNOWN_ISSUE)
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone", UNKNOWN_ISSUE)
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone", UNKNOWN_ISSUE)
    .exclude("SPARK-35112: Cast string to day-time interval", UNKNOWN_ISSUE)
    .exclude("SPARK-35735: Take into account day-time interval fields in cast", UNKNOWN_ISSUE)
    .exclude("casting to fixed-precision decimals", UNKNOWN_ISSUE)
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type", UNKNOWN_ISSUE)
  enableSuite[RapidsDataFrameAggregateSuite]
      .exclude("collect functions", UNKNOWN_ISSUE)
      .exclude("collect functions structs", UNKNOWN_ISSUE)
      .exclude("collect functions should be able to cast to array type with no null values", UNKNOWN_ISSUE)
      .exclude("SPARK-17641: collect functions should not collect null values", UNKNOWN_ISSUE)
      .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it", UNKNOWN_ISSUE)
  enableSuite[RapidsJsonFunctionsSuite]
  enableSuite[RapidsJsonSuite]
    .exclude("Casting long as timestamp", UNKNOWN_ISSUE)
    .exclude("Write timestamps correctly with timestampFormat option and timeZone option", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: json in UTF-16 with BOM", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: multi-line json in UTF-32BE with BOM", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: Use user's encoding in reading of multi-line json in UTF-16LE", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: Unsupported encoding name", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: checking that the encoding option is case agnostic", UNKNOWN_ISSUE)
    .exclude("SPARK-23723: specified encoding is not matched to actual encoding", UNKNOWN_ISSUE)
    .exclude("SPARK-23724: lineSep should be set if encoding if different from UTF-8", UNKNOWN_ISSUE)
    .exclude("SPARK-31716: inferring should handle malformed input", UNKNOWN_ISSUE)
    .exclude("SPARK-24190: restrictions for JSONOptions in read", UNKNOWN_ISSUE)
    .exclude("exception mode for parsing date/timestamp string", UNKNOWN_ISSUE)
    .exclude("SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ", UNKNOWN_ISSUE)
  enableSuite[RapidsMathFunctionsSuite]
  enableSuite[RapidsRegexpExpressionsSuite]
    .exclude("RegexReplace", UNKNOWN_ISSUE)
    .exclude("RegexExtract", UNKNOWN_ISSUE)
    .exclude("RegexExtractAll", UNKNOWN_ISSUE)
    .exclude("SPLIT", UNKNOWN_ISSUE)
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("SPARK-22498: Concat should not generate codes beyond 64KB", UNKNOWN_ISSUE)
    .exclude("SPARK-22549: ConcatWs should not generate codes beyond 64KB", UNKNOWN_ISSUE)
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB", UNKNOWN_ISSUE)
    .exclude("StringComparison", UNKNOWN_ISSUE)
    .exclude("Substring", UNKNOWN_ISSUE)
    .exclude("ascii for string", UNKNOWN_ISSUE)
    .exclude("base64/unbase64 for string", UNKNOWN_ISSUE)
    .exclude("encode/decode for string", UNKNOWN_ISSUE)
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB", UNKNOWN_ISSUE)
    .exclude("LOCATE", UNKNOWN_ISSUE)
    .exclude("LPAD/RPAD", UNKNOWN_ISSUE)
    .exclude("REPEAT", UNKNOWN_ISSUE)
    .exclude("length for string / binary", UNKNOWN_ISSUE)
    .exclude("ParseUrl", UNKNOWN_ISSUE)
  enableSuite[RapidsStringFunctionsSuite]
}
// scalastyle:on line.size.limit
