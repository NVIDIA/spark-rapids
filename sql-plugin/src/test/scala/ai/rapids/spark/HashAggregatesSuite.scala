/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class HashAggregatesSuite extends SparkQueryCompareTestSuite {
  private val floatAggConf: SparkConf = new SparkConf().set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")
  def replaceHashAggMode(mode: String, conf: SparkConf = new SparkConf()): SparkConf = {
    // configures whether Plugin will replace certain aggregate exec nodes
    conf.set("spark.rapids.sql.hashAgg.replaceMode", mode)
  }

  def FLOAT_TEST_testSparkResultsAreEqual(testName: String,
                                          df: SparkSession => DataFrame,
                                          conf: SparkConf = new SparkConf(),
                                          batchSize: Int = 0,
                                          repart: Int = 1,
                                          allowNonGpu: Boolean = false)
                                         (fn: DataFrame => DataFrame) {
    if (batchSize > 0) {
      makeBatchedBytes(batchSize, conf)
    }
    conf.set(RapidsConf.HAS_NANS.key, "false")
    conf.set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")
    testSparkResultsAreEqual(testName, df,
      conf = conf, allowNonGpu = allowNonGpu, repart = repart,
      incompat = true, sort = true)(fn)
  }

  def firstDf(spark: SparkSession): DataFrame = {
    val options = FuzzerOptions(asciiStringsOnly = true, numbersAsStrings = false, maxStringLen = 4)
    val schema = FuzzerUtils.createSchema(Seq(DataTypes.StringType, DataTypes.IntegerType))
    FuzzerUtils.generateDataFrame(spark, schema, 100, options, seed = 0)
      .withColumn("c2", col("c1").mod(lit(10)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test sort agg with first and last string deterministic case", firstDf, repart = 2) {
    frame => frame
        .coalesce(1)
        .sort(col("c2").asc, col("c0").asc) // force deterministic use case
        .groupBy(col("c2"))
        .agg(first(col("c0"), ignoreNulls = true), last(col("c0"), ignoreNulls = true))
  }

  test("SortAggregateExec is translated correctly ENABLE_HASH_OPTIMIZE_SORT=false") {

    val conf = new SparkConf()
      .set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "false")

    withGpuSparkSession(spark => {
      val df = firstDf(spark)
        .coalesce(1)
        .sort(col("c2").asc, col("c0").asc) // force deterministic use case
        .groupBy(col("c2"))
        .agg(first(col("c0"), ignoreNulls = true), last(col("c0"), ignoreNulls = true))

      val cpuPlan = df.queryExecution.sparkPlan
      assert(cpuPlan.find(_.isInstanceOf[SortAggregateExec]).isDefined)

      val gpuPlan = df.queryExecution.executedPlan

      gpuPlan match {
        case WholeStageCodegenExec(GpuColumnarToRowExec(plan, _)) =>
          assert(plan.children.head.isInstanceOf[GpuHashAggregateExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case _ =>
          fail("Incorrect plan")
      }

    }, conf)
  }
  test("SortAggregateExec is translated correctly ENABLE_HASH_OPTIMIZE_SORT=true") {

    val conf = new SparkConf()
        .set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, "true")

    withGpuSparkSession(spark => {
      val df = firstDf(spark)
        .coalesce(1)
        .sort(col("c2").asc, col("c0").asc) // force deterministic use case
        .groupBy(col("c2"))
        .agg(first(col("c0"), ignoreNulls = true), last(col("c0"), ignoreNulls = true))

      val cpuPlan = df.queryExecution.sparkPlan
      assert(cpuPlan.find(_.isInstanceOf[SortAggregateExec]).isDefined)

      val gpuPlan = df.queryExecution.executedPlan

      gpuPlan match {
        case WholeStageCodegenExec(GpuColumnarToRowExec(plan, _)) =>
          assert(plan.children.head.isInstanceOf[GpuSortExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.find(_.isInstanceOf[GpuHashAggregateExec]).isDefined)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case _ =>
          fail("Incorrect plan")
      }

    }, conf)
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test hash agg with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("longs")).agg(sum(col("more_longs")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test hash agg with Single partitioning",
    longsFromCSVDf, repart = 2, conf = new SparkConf().set("spark.sql.shuffle.partitions", "1")) {
    frame => {
      frame.agg(count("*"))
    }
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test hash agg with Single partitioning with partition sort",
    longsFromCSVDf, repart = 2, conf = new SparkConf().set("spark.sql.shuffle.partitions", "1"), sortBeforeRepart = true) {
    frame => {
      frame.agg(count("*"))
    }
  }

  /*
   * HASH AGGREGATE TESTS
   */
  IGNORE_ORDER_testSparkResultsAreEqual("short reduction aggs", shortsFromCsv,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = floatAggConf) {
    frame => frame.agg(
      (max("shorts") - min("more_shorts")) * lit(5),
      sum("shorts"),
      count("*"),
      avg("shorts"),
      avg(col("more_shorts") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("reduction aggs", longsCsvDf,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
    frame => frame.agg(
      (max("longs") - min("more_longs")) * lit(5),
      sum("longs"),
      count("*"),
      avg("longs"),
      avg(col("more_longs") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("distinct", datesCsvDf) {
    frame => frame.distinct()
  }

  IGNORE_ORDER_testSparkResultsAreEqual("avg literals", longsFromCSVDf,
    conf = floatAggConf) {
    frame => frame.agg(avg(lit(1.toDouble)),avg(lit(2.toDouble)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("avg literals with nulls", longsFromCSVDf,
    conf = floatAggConf) {
    frame => frame.agg(avg(lit(null)),avg(lit(2.toDouble)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("avg literals with all nulls", longsFromCSVDf,
    conf = floatAggConf) {
    frame => frame.agg(avg(lit(null)),avg(lit(null)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("distinct should not reorder columns", intsFromCsv) {
    frame => frame.distinct()
  }

  IGNORE_ORDER_testSparkResultsAreEqual("group by string include nulls in count aggregate", nullableStringsIntsDf,
    conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"), 
      min("ints"), 
      avg("ints"), 
      sum("ints"), 
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("group by strings exclude nulls in count aggregate", nullableStringsIntsDf,
    conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("group by float with NaNs and null", intnullableFloatWithNullAndNanDf,
    conf = floatAggConf) {
    frame => frame.groupBy("ints").agg(
      count("floats"),
      count(lit(null)));
  }

  IGNORE_ORDER_testSparkResultsAreEqual("group by utf8 strings", utf8RepeatedDf,
    conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test count, sum, max, min with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("more_longs")).agg(
      count("*"),
      sum("more_longs"),
      sum("longs") * lit(2),
      (max("more_longs") - min("more_longs")) * 3.0)
  }

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by string literal", floatCsvDf) {
    frame => frame.groupBy(lit("2019-02-10")).agg(
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by float and string literal", floatCsvDf) {
    frame => {
      val feature_window_end_time = date_format(lit("2018-02-01"), "yyyy-MM-dd HH:mm:ss")
      val frameWithCol = frame.withColumn("timestamp_lit", feature_window_end_time)
      frameWithCol.groupBy("floats", "timestamp_lit").agg(
        when(col("timestamp_lit") > "2018-02-01 00:00:00", 1).otherwise(2),
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
  }

  IGNORE_ORDER_testSparkResultsAreEqual("aggregates with timestamp and string literal", timestampsDf) {
  frame => {
      val feature_window_end_time = date_format(lit("2018-02-01"), "yyyy-MM-dd HH:mm:ss")
      val frameWithCol = frame.withColumn("timestamp_lit", feature_window_end_time)
      frameWithCol.groupBy("more_timestamps", "timestamp_lit").agg(
        sum(when(col("timestamp_lit") > col("timestamps"), 1).otherwise(2))
      )
    }
  }

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by floats", floatCsvDf) {
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

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by more_floats", floatCsvDf) {
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

  FLOAT_TEST_testSparkResultsAreEqual("partial on gpu: float basic aggregates group by more_floats", floatCsvDf,
    conf = replaceHashAggMode("partial"),
    allowNonGpu = true) {
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

  FLOAT_TEST_testSparkResultsAreEqual("final on gpu: float basic aggregates group by more_floats", floatCsvDf,
    conf = replaceHashAggMode("final"),
    allowNonGpu = true) {
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

  FLOAT_TEST_testSparkResultsAreEqual("nullable float basic aggregates group by more_floats", nullableFloatCsvDf,
    conf = makeBatchedBytes(3)) {
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

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("shorts basic aggregates group by more_shorts", shortsFromCsv,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by longs", longsCsvDf,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by more_longs", longsCsvDf,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by ints", intCsvDf,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by more_ints", intCsvDf,
    // All the literals get turned into doubles, so we need to support avg in those cases
    conf = makeBatchedBytes(3).set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  FLOAT_TEST_testSparkResultsAreEqual("doubles basic aggregates group by doubles", doubleCsvDf,
    conf = makeBatchedBytes(3)) {
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

  FLOAT_TEST_testSparkResultsAreEqual("doubles basic aggregates group by more_doubles", doubleCsvDf,
    conf = makeBatchedBytes(3)) {
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

  FLOAT_TEST_testSparkResultsAreEqual("sum(floats) group by more_floats 2 partitions", floatCsvDf, repart = 2) {
    frame => frame.groupBy("more_floats").sum("floats")
  }

  FLOAT_TEST_testSparkResultsAreEqual("avg(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame.groupBy("more_floats").avg("floats")
  }

  FLOAT_TEST_testSparkResultsAreEqual("avg(floats),count(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame
      .groupBy("more_floats")
      .agg(avg("floats"), count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions", intCsvDf,
    // Avg can always have floating point issues
    conf = floatAggConf) {
    frame => frame.groupBy(col("more_ints") * 2).agg(
      lit(1000) +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions 2", intCsvDf,
    // Avg can always have floating point issues
    conf = floatAggConf) {
    frame => frame.groupBy("more_ints").agg(
      min("ints") +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expression 3", intCsvDf,
    // Avg can always have floating point issues
    conf = floatAggConf) {
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

  FLOAT_TEST_testSparkResultsAreEqual("empty df: reduction count", floatCsvDf) {
    frame => frame.filter("floats > 10000000.0").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: reduction aggs", floatCsvDf) {
    frame => frame.filter("floats > 10000000.0").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      first("floats", true),
      last("floats", true),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: grouped count", floatCsvDf) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("partial on gpu: empty df: grouped count", floatCsvDf,
    allowNonGpu = true,
    conf = replaceHashAggMode("partial")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("final on gpu: empty df: grouped count", floatCsvDf,
    allowNonGpu = true,
    conf = replaceHashAggMode("final")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: float basic aggregates group by floats", floatCsvDf) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      first("floats", true),
      last("floats", true),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("partial on gpu: empty df: float basic aggregates group by floats", floatCsvDf,
    allowNonGpu = true,
    conf = replaceHashAggMode("partial")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      first("floats", true),
      last("floats", true),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("final on gpu: empty df: float basic aggregates group by floats", floatCsvDf,
    allowNonGpu = true,
    conf = replaceHashAggMode("final")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      first("floats", true),
      last("floats", true),
      count("*"))
  }

  testSparkResultsAreEqual("Agg expression with filter", longsFromCSVDf,
    allowNonGpu = true, repart = 0) {
    frame => frame.selectExpr("count(1) filter (where longs > 20)")
  }
}
