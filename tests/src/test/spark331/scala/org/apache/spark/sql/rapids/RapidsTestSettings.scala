/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "331"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import org.apache.spark.sql.{RapidsCastSuite, RapidsDataFrameAggregateSuite, RapidsJsonFunctionsSuite, RapidsJsonSuite, RapidsMathFunctionsSuite, RapidsRegexpExpressionsSuite, RapidsStringExpressionsSuite, RapidsStringFunctionsSuite}
import org.apache.spark.sql.rapids.utils.{BackendTestSettings, SQLQueryTestSettings}

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class RapidsTestSettings extends BackendTestSettings {

  enableSuite[RapidsCastSuite]
    .exclude("Process Infinity, -Infinity, NaN in case insensitive manner")
    .exclude("SPARK-35711: cast timestamp without time zone to timestamp with local time zone")
    .exclude("SPARK-35719: cast timestamp with local time zone to timestamp without timezone")
    .exclude("SPARK-35112: Cast string to day-time interval")
    .exclude("SPARK-35735: Take into account day-time interval fields in cast")
    .exclude("casting to fixed-precision decimals")
    .exclude("SPARK-32828: cast from a derived user-defined type to a base type")
  enableSuite[RapidsDataFrameAggregateSuite]
      .exclude("collect functions")
      .exclude("collect functions structs")
      .exclude("collect functions should be able to cast to array type with no null values")
      .exclude("SPARK-17641: collect functions should not collect null values")
      .exclude("SPARK-19471: AggregationIterator does not initialize the generated result projection before using it")
  enableSuite[RapidsJsonFunctionsSuite]
  enableSuite[RapidsJsonSuite]
    .exclude("Casting long as timestamp")
    .exclude("Write timestamps correctly with timestampFormat option and timeZone option")
    .exclude("SPARK-23723: json in UTF-16 with BOM")
    .exclude("SPARK-23723: multi-line json in UTF-32BE with BOM")
    .exclude("SPARK-23723: Use user's encoding in reading of multi-line json in UTF-16LE")
    .exclude("SPARK-23723: Unsupported encoding name")
    .exclude("SPARK-23723: checking that the encoding option is case agnostic")
    .exclude("SPARK-23723: specified encoding is not matched to actual encoding")
    .exclude("SPARK-23724: lineSep should be set if encoding if different from UTF-8")
    .exclude("SPARK-31716: inferring should handle malformed input")
    .exclude("SPARK-24190: restrictions for JSONOptions in read")
    .exclude("exception mode for parsing date/timestamp string")
    .exclude("SPARK-37360: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ")
  enableSuite[RapidsMathFunctionsSuite]
  enableSuite[RapidsRegexpExpressionsSuite]
    .exclude("RegexReplace")
    .exclude("RegexExtract")
    .exclude("RegexExtractAll")
    .exclude("SPLIT")
  enableSuite[RapidsStringExpressionsSuite]
    .exclude("SPARK-22498: Concat should not generate codes beyond 64KB")
    .exclude("SPARK-22549: ConcatWs should not generate codes beyond 64KB")
    .exclude("SPARK-22550: Elt should not generate codes beyond 64KB")
    .exclude("StringComparison")
    .exclude("Substring")
    .exclude("ascii for string")
    .exclude("base64/unbase64 for string")
    .exclude("encode/decode for string")
    .exclude("SPARK-22603: FormatString should not generate codes beyond 64KB")
    .exclude("LOCATE")
    .exclude("LPAD/RPAD")
    .exclude("REPEAT")
    .exclude("length for string / binary")
    .exclude("ParseUrl")
  enableSuite[RapidsStringFunctionsSuite]

   override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getSupportedSQLQueryTests: Set[String] = Set()

    override def getOverwriteSQLQueryTests: Set[String] = Set()
  }
}
// scalastyle:on line.size.limit
