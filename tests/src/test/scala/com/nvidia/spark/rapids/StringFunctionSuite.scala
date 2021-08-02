/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

class StringFunctionSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Test repeat string(s) with scalar string and column repeatTimes",
    intsDf) {
    frame => frame.selectExpr(
      "repeat(NULL, ints)",
      "repeat('abc123', ints)"
    )
  }

  testSparkResultsAreEqual("Test repeat string(s) with strings column and scalar repeatTimes",
    nullableStringsDf) {
    frame => frame.selectExpr(
      "repeat(strings, NULL)",
      "repeat(strings, -10)",
      "repeat(strings, 0)",
      "repeat(strings, 10)"
    )
  }

  testSparkResultsAreEqual("Test repeat string(s) with strings column and column repeatTimes",
    nullableStringsIntsDf) {
    frame => frame.selectExpr(
      "repeat(strings, ints)",
      "repeat(strings, -ints)"
    )
  }
}
