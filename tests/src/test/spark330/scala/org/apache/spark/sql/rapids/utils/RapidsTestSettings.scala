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
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone", WONT_FIX_ISSUE("https://issues.apache.org/jira/browse/SPARK-40851"))
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone", WONT_FIX_ISSUE("https://issues.apache.org/jira/browse/SPARK-40851"))
    .exclude("SPARK-35112: Cast string to day-time interval", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10980"))
    .exclude("SPARK-35735: Take into account day-time interval fields in cast", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10980"))
    .exclude("casting to fixed-precision decimals", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11250"))
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type", WONT_FIX_ISSUE("User-defined types are not supported"))
    .exclude("cast string to timestamp", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/blob/main/docs/compatibility.md#string-to-timestamp"))
    .exclude("cast string to date", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10771"))
  enableSuite[RapidsDataFrameAggregateSuite]
    .exclude("collect functions", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("collect functions structs", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("collect functions should be able to cast to array type with no null values", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("SPARK-17641: collect functions should not collect null values", ADJUST_UT("order of elements in the array is non-deterministic in collect"))
    .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it", WONT_FIX_ISSUE("Codegen related UT, not applicable for GPU"))
    .exclude("SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10801"), (getJavaMajorVersion() >= 17))
  enableSuite[RapidsJsonExpressionsSuite]
    .exclude("from_json - invalid data", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10891"))
    .exclude("from_json - input=empty array, schema=struct, output=single row with null", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10907"))
    .exclude("from_json - input=empty object, schema=struct, output=single row with null", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10910"))
    .exclude("SPARK-20549: from_json bad UTF-8", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10911"))
    .exclude("to_json - array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10915"))
    .exclude("to_json - array with single empty row", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10923"))
    .exclude("to_json - empty array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10924"))
    .exclude("SPARK-21513: to_json support map[string, struct] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10916"))
    .exclude("SPARK-21513: to_json support map[struct, struct] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10918"))
    .exclude("SPARK-21513: to_json support map[string, integer] to json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10919"))
    .exclude("to_json - array with maps", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10920"))
    .exclude("to_json - array with single map", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10921"))
    .exclude("from_json missing fields", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10922"))
  enableSuite[RapidsJsonFunctionsSuite]
    .exclude("from_json invalid json", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10891"))
    .exclude("to_json - array", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10894"))
    .exclude("to_json - map", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10895"))
    .exclude("to_json - array of primitive types", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10896"))
    .exclude("SPARK-33134: return partial results only for root JSON objects", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10901"))
  enableSuite[RapidsJsonSuite]
    .exclude("SPARK-32810: JSON data source should be able to read files with escaped glob metacharacter in the paths", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-18352: Parse normal multi-line JSON files (uncompressed)", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("SPARK-18352: Parse normal multi-line JSON files (compressed)", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("Applying schemas", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("Loading a JSON dataset from a text file with SQL", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
    .exclude("Loading a JSON dataset from a text file", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10773"))
  enableSuite[RapidsMathFunctionsSuite]
    .exclude("SPARK-33428 conv function shouldn't raise error if input string is too big", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11142"))
    .exclude("SPARK-36229 conv should return result equal to -1 in base of toBase", KNOWN_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/11142"))
  enableSuite[RapidsRegexpExpressionsSuite]
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB", WONT_FIX_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB", WONT_FIX_ISSUE("https://github.com/NVIDIA/spark-rapids/issues/10775"))
  enableSuite[RapidsStringFunctionsSuite]
}
// scalastyle:on line.size.limit
