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

import org.apache.spark.sql.rapids.suites.{RapidsCastSuite, RapidsDataFrameAggregateSuite, RapidsJsonExpressionsSuite, RapidsJsonFunctionsSuite, RapidsJsonSuite, RapidsMathFunctionsSuite, RapidsRegexpExpressionsSuite, RapidsStringExpressionsSuite, RapidsStringFunctionsSuite}

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
    .exclude("SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10801"))
  enableSuite[RapidsJsonExpressionsSuite]
    .exclude("from_json - invalid data", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("from_json - input=empty array, schema=struct, output=single row with null", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("from_json - input=empty object, schema=struct, output=single row with null", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("SPARK-20549: from_json bad UTF-8", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("from_json with timestamp", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json - array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json - array with single empty row", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json - empty array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json with timestamp", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("SPARK-21513: to_json support map[string, struct] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("SPARK-21513: to_json support map[struct, struct] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("SPARK-21513: to_json support map[string, integer] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json - array with maps", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("to_json - array with single map", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
    .exclude("from_json missing fields", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10849"))
  enableSuite[RapidsJsonFunctionsSuite]
    .exclude("from_json invalid json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10852"))
    .exclude("to_json - array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10852"))
    .exclude("to_json - map", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10852"))
    .exclude("to_json - array of primitive types", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10852"))
    .exclude("SPARK-33134: return partial results only for root JSON objects", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10852"))
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
  enableSuite[RapidsMathFunctionsSuite]
  enableSuite[RapidsRegexpExpressionsSuite]
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("concat", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("string substring_index function", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22498: Concat should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22549: ConcatWs should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("ParseUrl", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
  enableSuite[RapidsStringFunctionsSuite]
}
// scalastyle:on line.size.limit
