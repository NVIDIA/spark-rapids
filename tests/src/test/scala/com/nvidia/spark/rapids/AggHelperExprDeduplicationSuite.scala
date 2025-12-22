/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims._

/**
 * Integration tests for AggHelper's expression deduplication logic.
 *
 * When avg(x) and count(x) are used together, they both need to count non-null values of x.
 * AggHelper should deduplicate the 'x' expression in preStep and let cudf deduplicate
 * the identical count aggregations on the same column.
 *
 * These tests verify the end-to-end correctness by comparing GPU results with CPU results.
 */
class AggHelperExprDeduplicationSuite extends SparkQueryCompareTestSuite {

  private def aggConf: SparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")

  // Test data with integers including nulls
  private def intDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[java.lang.Integer](
      1, 2, 3, null, 5, 6, null, 8, 9, 10
    ).toDF("x")
  }

  // Test data with doubles including nulls
  private def doubleDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[java.lang.Double](
      1.5, 2.5, 3.5, null, 5.5, 6.5, null, 8.5, 9.5, 10.5
    ).toDF("x")
  }

  // Test data with integers for group by
  private def intGroupByDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[(String, java.lang.Integer)](
      ("a", 1), ("a", 2), ("a", null),
      ("b", 4), ("b", 5), ("b", null),
      ("c", 7), ("c", 8), ("c", 9)
    ).toDF("key", "x")
  }

  // Test data with doubles for group by
  private def doubleGroupByDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[(String, java.lang.Double)](
      ("a", 1.5), ("a", 2.5), ("a", null),
      ("b", 4.5), ("b", 5.5), ("b", null),
      ("c", 7.5), ("c", 8.5), ("c", 9.5)
    ).toDF("key", "x")
  }

  // ==================== Reduction tests (no group by) ====================

  // avg at the end - int
  testSparkResultsAreEqual(
    "sum(x), count(x), avg(x) - int reduction",
    intDf,
    conf = aggConf) { df =>
    df.agg(sum("x"), count("x"), avg("x"))
  }

  // avg at the beginning - int
  testSparkResultsAreEqual(
    "avg(x), sum(x), count(x) - int reduction",
    intDf,
    conf = aggConf) { df =>
    df.agg(avg("x"), sum("x"), count("x"))
  }

  // avg at the end - double
  testSparkResultsAreEqual(
    "sum(x), count(x), avg(x) - double reduction",
    doubleDf,
    conf = aggConf,
    maxFloatDiff = 0.0001) { df =>
    df.agg(sum("x"), count("x"), avg("x"))
  }

  // avg at the beginning - double
  testSparkResultsAreEqual(
    "avg(x), sum(x), count(x) - double reduction",
    doubleDf,
    conf = aggConf,
    maxFloatDiff = 0.0001) { df =>
    df.agg(avg("x"), sum("x"), count("x"))
  }

  // ==================== Group by tests ====================

  // avg at the end - int
  IGNORE_ORDER_testSparkResultsAreEqual(
    "sum(x), count(x), avg(x) - int group by",
    intGroupByDf,
    conf = aggConf) { df =>
    df.groupBy("key").agg(sum("x"), count("x"), avg("x"))
  }

  // avg at the beginning - int
  IGNORE_ORDER_testSparkResultsAreEqual(
    "avg(x), sum(x), count(x) - int group by",
    intGroupByDf,
    conf = aggConf) { df =>
    df.groupBy("key").agg(avg("x"), sum("x"), count("x"))
  }

  // avg at the end - double
  testSparkResultsAreEqual(
    "sum(x), count(x), avg(x) - double group by",
    doubleGroupByDf,
    conf = aggConf,
    sort = true,
    maxFloatDiff = 0.0001) { df =>
    df.groupBy("key").agg(sum("x"), count("x"), avg("x"))
  }

  // avg at the beginning - double
  testSparkResultsAreEqual(
    "avg(x), sum(x), count(x) - double group by",
    doubleGroupByDf,
    conf = aggConf,
    sort = true,
    maxFloatDiff = 0.0001) { df =>
    df.groupBy("key").agg(avg("x"), sum("x"), count("x"))
  }

  // ==================== DISTINCT avg tests for LongType optimization ====================

  // Test data with Long values (larger range to potentially trigger overflow if using Long sum)
  private def longDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[java.lang.Long](
      1000000000000L, 2000000000000L, 3000000000000L, null,
      1000000000000L, 2000000000000L, // duplicates for DISTINCT
      5000000000000L, 6000000000000L, null, 8000000000000L
    ).toDF("x")
  }

  // Test data with very large Long values that will overflow if summed as Long
  // These values are close to Long.MAX_VALUE (9223372036854775807)
  private def overflowLongDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[java.lang.Long](
      5000000000000000000L,  // 5e18
      4000000000000000000L,  // 4e18
      3000000000000000000L,  // 3e18
      2000000000000000000L,  // 2e18
      1000000000000000000L   // 1e18
      // Sum = 15e18, which overflows Long.MAX_VALUE (9.2e18)
    ).toDF("x")
  }

  // Test data with Long for group by with duplicates
  private def longGroupByDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq[(String, java.lang.Long)](
      ("a", 1000000000000L), ("a", 2000000000000L), ("a", 1000000000000L), ("a", null),
      ("b", 4000000000000L), ("b", 5000000000000L), ("b", 4000000000000L), ("b", null),
      ("c", 7000000000000L), ("c", 8000000000000L), ("c", 9000000000000L)
    ).toDF("key", "x")
  }

  // avg(DISTINCT x) - Long reduction - this is the failing case
  testSparkResultsAreEqual(
    "avg(DISTINCT x) - Long reduction",
    longDf,
    conf = aggConf,
    maxFloatDiff = 0.0001) { df =>
    df.agg(avg(col("x")).as("avg_x"), countDistinct("x").as("cnt_distinct"))
  }

  // avg(DISTINCT x) - Long group by
  IGNORE_ORDER_testSparkResultsAreEqual(
    "avg(DISTINCT x) - Long group by",
    longGroupByDf,
    conf = aggConf) { df =>
    df.groupBy("key").agg(avg(col("x")).as("avg_x"), countDistinct("x").as("cnt_distinct"))
  }

  // The actual failing test case: avgDistinct on Long type
  testSparkResultsAreEqual(
    "avgDistinct(x) - Long reduction - reproducer",
    longDf,
    conf = aggConf,
    maxFloatDiff = 0.0001) { df =>
    // Use SQL to get avg(DISTINCT x) since DataFrame API doesn't have avgDistinct
    df.createOrReplaceTempView("long_data")
    df.sparkSession.sql("SELECT avg(DISTINCT x) as avg_distinct_x FROM long_data")
  }

  // Test with replaceMode=partial to simulate CPU fallback scenario
  // This is the configuration that causes integration test failures
  private def partialModeConf: SparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")
    .set(RapidsConf.HASH_AGG_REPLACE_MODE.key, "partial")
    .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
      "HashAggregateExec,AggregateExpression,Average,Cast,Alias,AttributeReference")

  // This test simulates the integration test failure scenario
  testSparkResultsAreEqual(
    "avgDistinct(x) - Long with partial replaceMode (CPU fallback)",
    longDf,
    conf = partialModeConf,
    maxFloatDiff = 0.0001) { df =>
    df.createOrReplaceTempView("long_data_partial")
    df.sparkSession.sql("SELECT avg(DISTINCT x) as avg_distinct_x FROM long_data_partial")
  }

  // This test uses values large enough to overflow Long when summed
  // Expected: avg = (5e18 + 4e18 + 3e18 + 2e18 + 1e18) / 5 = 15e18 / 5 = 3e18
  // If Long overflow occurs, sum would wrap around to a negative number
  testSparkResultsAreEqual(
    "avg(x) - Large Long values that overflow Long.MAX_VALUE when summed",
    overflowLongDf,
    conf = aggConf,
    maxFloatDiff = 0.0001) { df =>
    df.agg(avg("x").as("avg_x"), sum("x").as("sum_x"), count("x").as("cnt_x"))
  }
}
