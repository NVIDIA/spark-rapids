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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  testSparkResultsAreEqual("project is not null", nullableFloatDf) {
    frame => frame.selectExpr("floats is not null")
  }

  testSparkResultsAreEqual("project is null", nullableFloatDf) {
    frame => frame.selectExpr("floats is null")
  }

  testSparkResultsAreEqual("project is null col1 OR is null col2", nullableFloatDf) {
    frame => frame.selectExpr("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("Test literal values in select", floatDf) {
    frame => frame.select(col("floats"), lit(100))
  }

  testSparkResultsAreEqual("IsNotNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is not null")
  }

  testSparkResultsAreEqual("IsNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is null")
  }

  testSparkResultsAreEqual("IsNull OR IsNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is null OR more_strings is null")
  }

  testSparkResultsAreEqual("equals strings", nullableStringsDf) {
    frame => frame.selectExpr("strings = \"500.0\"")
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.select("time")
  }

  testSparkResultsAreEqual("IsNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("time is null")
  }

  testSparkResultsAreEqual("IsNotNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("time is not null")
  }

  testSparkResultsAreEqual("year timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("year(time)")
  }

  testSparkResultsAreEqual("month timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("month(time)")
  }

  testSparkResultsAreEqual("day timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("day(time)")
  }
}
