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

import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class HashAggregatesSuite extends FunSuite with BeforeAndAfterEach with SparkQueryCompareTestSuite {

  IGNORE_ORDER_testSparkResultsAreEqual("test hash agg with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("longs")).agg(sum(col("more_longs")))
  }

  /*
   * HASH AGGREGATE TESTS
   */
  IGNORE_ORDER_testSparkResultsAreEqual("short reduction aggs", shortsFromCsv) {
    frame => frame.agg(
      (max("shorts") - min("more_shorts")) * lit(5),
      sum("shorts"),
      count("*"),
      avg("shorts"),
      avg(col("more_shorts") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("reduction aggs", longsCsvDf) {
    frame => frame.agg(
      (max("longs") - min("more_longs")) * lit(5),
      sum("longs"),
      count("*"),
      avg("longs"),
      avg(col("more_longs") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test count, sum, max, min with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("more_longs")).agg(
      count("*"),
      sum("more_longs"),
      sum("longs") * lit(2),
      (max("more_longs") - min("more_longs")) * 3.0)
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("float basic aggregates group by floats", floatCsvDf) {
    frame => frame.groupBy("floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("float basic aggregates group by more_floats", floatCsvDf) {
    frame => frame.groupBy("more_floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("nullable float basic aggregates group by more_floats", nullableFloatCsvDf) {
    frame => frame.groupBy("more_floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("shorts basic aggregates group by more_shorts", shortsFromCsv) {
    frame => frame.groupBy("more_shorts").agg(
      lit(456),
      min(col("shorts")) + lit(123),
      sum(col("more_shorts") + lit(123.0)),
      max(col("shorts") * col("more_shorts")),
      max("shorts") - min("more_shorts"),
      max("more_shorts") - min("shorts"),
      sum("shorts"),
      avg("shorts"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by longs", longsCsvDf) {
    frame => frame.groupBy("longs").agg(
      lit(456f),
      min(col("longs")) + lit(123),
      sum(col("more_longs") + lit(123.0)),
      max(col("longs") * col("more_longs")),
      max("longs") - min("more_longs"),
      max("more_longs") - min("longs"),
      sum("longs") + sum("more_longs"),
      avg("longs"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by more_longs", longsCsvDf) {
    frame => frame.groupBy("more_longs").agg(
      lit(456f),
      min(col("longs")) + lit(123),
      sum(col("more_longs") + lit(123.0)),
      max(col("longs") * col("more_longs")),
      max("longs") - min("more_longs"),
      max("more_longs") - min("longs"),
      sum("longs") + sum("more_longs"),
      avg("longs"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by ints", intCsvDf) {
    frame => frame.groupBy("ints").agg(
      lit(456f),
      min(col("ints")) + lit(123),
      sum(col("more_ints") + lit(123.0)),
      max(col("ints") * col("more_ints")),
      max("ints") - min("more_ints"),
      max("more_ints") - min("ints"),
      sum("ints") + sum("more_ints"),
      avg("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by more_ints", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      lit(456f),
      min(col("ints")) + lit(123),
      sum(col("more_ints") + lit(123.0)),
      max(col("ints") * col("more_ints")),
      max("ints") - min("more_ints"),
      max("more_ints") - min("ints"),
      sum("ints") + sum("more_ints"),
      avg("ints"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("doubles basic aggregates group by doubles", doubleCsvDf) {
    frame => frame.groupBy("doubles").agg(
      lit(456f),
      min(col("doubles")) + lit(123),
      sum(col("more_doubles") + lit(123.0)),
      max(col("doubles") * col("more_doubles")),
      max("doubles") - min("more_doubles"),
      max("more_doubles") - min("doubles"),
      sum("doubles") + sum("more_doubles"),
      avg("doubles"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("doubles basic aggregates group by more_doubles", doubleCsvDf) {
    frame => frame.groupBy("more_doubles").agg(
      lit(456f),
      min(col("doubles")) + lit(123),
      sum(col("more_doubles") + lit(123.0)),
      max(col("doubles") * col("more_doubles")),
      max("doubles") - min("more_doubles"),
      max("more_doubles") - min("doubles"),
      sum("doubles") + sum("more_doubles"),
      avg("doubles"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("sum(longs) multi group by longs, more_longs", longsCsvDf) {
    frame => frame.groupBy("longs", "more_longs").agg(
      sum("longs"), count("*"))
  }

  // misc aggregation tests
  testSparkResultsAreEqual("sum(ints) group by literal", intCsvDf) {
    frame => frame.groupBy(lit(1)).agg(sum("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("sum(ints) group by dates", datesCsvDf) {
    frame => frame.groupBy("dates").sum("ints")
  }

  IGNORE_ORDER_testSparkResultsAreEqual("max(ints) group by month", datesCsvDf) {
    frame => frame.withColumn("monthval", month(col("dates")))
      .groupBy(col("monthval"))
      .agg(max("ints").as("max_ints_by_month"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("sum(floats) group by more_floats 2 partitions", floatCsvDf, repart = 2) {
    frame => frame.groupBy("more_floats").sum("floats")
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("avg(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame.groupBy("more_floats").avg("floats")
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("avg(floats),count(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame
      .groupBy("more_floats")
      .agg(avg("floats"), count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions", intCsvDf) {
    frame => frame.groupBy(col("more_ints") * 2).agg(
      lit(1000) +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions 2", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      min("ints") +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expression 3", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      min("ints"), avg("ints"),
      max(col("ints") + col("more_ints")), lit(1), min("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions", longsCsvDf) {
    frame => frame.groupBy(col("more_longs") + lit(10)).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions 2", longsCsvDf) {
    frame => frame.groupBy(col("more_longs") + col("longs")).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last aggregates", intCsvDf) {
    frame => frame.groupBy(col("more_ints")).agg(
      first("five", true), last("six", true))
  }

}
