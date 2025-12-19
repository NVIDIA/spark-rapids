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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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
}
