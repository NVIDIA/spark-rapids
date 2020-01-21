/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

class FilterExprSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("filter is not null", nullableFloatCsvDf) {
    frame => frame.filter("floats is not null")
  }

  testSparkResultsAreEqual("filter is null", nullableFloatCsvDf) {
    frame => frame.filter("floats is null")
  }

  testSparkResultsAreEqual("filter is null col1 OR is null col2", nullableFloatCsvDf) {
    frame => frame.filter("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("filter less than", floatCsvDf) {
    frame => frame.filter("floats < more_floats")
  }

  testSparkResultsAreEqual("filter greater than", floatCsvDf) {
    frame => frame.filter("floats > more_floats")
  }

  testSparkResultsAreEqual("filter less than or equal", floatCsvDf) {
    frame => frame.filter("floats <= more_floats")
  }

  testSparkResultsAreEqual("filter greater than or equal", floatCsvDf) {
    frame => frame.filter("floats >= more_floats")
  }

  testSparkResultsAreEqual("filter is null and greater than or equal", nullableFloatCsvDf) {
    frame => frame.filter("floats is null AND more_floats >= 3.0")
  }

  testSparkResultsAreEqual("filter is not null and greater than or equal", nullableFloatCsvDf) {
    frame => frame.filter("floats is not null AND more_floats >= 3.0")
  }

  // these are ALLOW_NON_GPU because you can't project a string
  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings are not null", nullableStringsFromCsv) {
    frame => frame.filter("strings is not null or more_strings is not null")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings equality", nullableStringsFromCsv) {
    frame => frame.filter("strings = 'Bar'")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings greater than", nullableStringsFromCsv) {
    frame => frame.filter("strings > 'Bar'")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings less than", nullableStringsFromCsv) {
    frame => frame.filter("strings < 'Bar'")
  }
}
