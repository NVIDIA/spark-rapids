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
import org.apache.spark.sql.types.{DataType, DataTypes}

class HashAggregatesSuite extends SparkQueryCompareTestSuite {
  private val floatAggConf: SparkConf = new SparkConf().set(
    RapidsConf.ENABLE_FLOAT_AGG.key, "true").set(RapidsConf.HAS_NANS.key, "false")

  private val execsAllowedNonGpu: Seq[String] =
    Seq("HashAggregateExec", "AggregateExpression", "AttributeReference", "Alias")
  def replaceHashAggMode(mode: String, conf: SparkConf = new SparkConf()): SparkConf = {
    // configures whether Plugin will replace certain aggregate exec nodes
    conf.set("spark.rapids.sql.hashAgg.replaceMode", mode)
  }

  private def checkExecNode(result: DataFrame) = {
    if (result.sparkSession.sqlContext.getAllConfs("spark.app.name").contains("gpu")) {
      assert(result.queryExecution.executedPlan.find(_.isInstanceOf[GpuHashAggregateExec]).isDefined)
    }
  }

  def FLOAT_TEST_testSparkResultsAreEqual(testName: String,
    df: SparkSession => DataFrame,
    conf: SparkConf = new SparkConf(),
    execsAllowedNonGpu: Seq[String] = Seq.empty,
    batchSize: Int = 0,
    repart: Int = 1)
    (fn: DataFrame => DataFrame) {
    if (batchSize > 0) {
      makeBatchedBytes(batchSize, conf)
    }
    conf.set(RapidsConf.HAS_NANS.key, "false")
    conf.set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")
    testSparkResultsAreEqual(testName, df,
      conf = conf, repart = repart,
      execsAllowedNonGpu = execsAllowedNonGpu,
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

  testExpectedExceptionStartsWith("test unsorted agg with first and last no grouping",
    classOf[IllegalArgumentException],
    "Part of the plan is not columnar", firstDf, repart = 2) {
    frame => frame
      .coalesce(1)
      .agg(first(col("c0"), ignoreNulls = true), last(col("c0"), ignoreNulls = true))
  }

  testExpectedExceptionStartsWith("test sorted agg with first and last no grouping",
    classOf[IllegalArgumentException],
    "Part of the plan is not columnar", firstDf, repart = 2) {
    frame => frame
      .coalesce(1)
      .sort(col("c2").asc, col("c0").asc) // force deterministic use case
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

  IGNORE_ORDER_testSparkResultsAreEqual("group by string include nulls in count aggregate small batches",
    nullableStringsIntsDf, conf = floatAggConf.set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "10")) {
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
    execsAllowedNonGpu = execsAllowedNonGpu) {
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
    execsAllowedNonGpu = execsAllowedNonGpu) {
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

  testExpectedExceptionStartsWith("first without grouping",
    classOf[IllegalArgumentException],
    "Part of the plan is not columnar",
    intCsvDf) {
    frame => frame.agg(first("ints", false))
  }

  testExpectedExceptionStartsWith("last without grouping",
    classOf[IllegalArgumentException],
    "Part of the plan is not columnar",
    intCsvDf) {
    frame => frame.agg(first("ints", false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first ignoreNulls=false", intCsvDf) {
    frame => frame.groupBy(col("more_ints")).agg(first("ints", false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("last ignoreNulls=false", intCsvDf) {
    frame => frame.groupBy(col("more_ints")).agg(last("ints", false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last ints column", intCsvDf) {
    frame => frame.groupBy(col("more_ints")).agg(
      first("ints", ignoreNulls = false),
      last("ints", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first hand-picked longs ignoreNulls=true", firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = true))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first hand-picked longs ignoreNulls=false", firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last hand-picked longs ignoreNulls=false", firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last random ints ignoreNulls=false", randomDF(DataTypes.IntegerType)) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last random longs ignoreNulls=false", randomDF(DataTypes.LongType)) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first random strings ignoreNulls=false", randomDF(DataTypes.StringType)) {
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("last random strings ignoreNulls=false", randomDF(DataTypes.StringType)) {
    frame => frame.groupBy(col("c0")).agg(last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last random strings ignoreNulls=false", randomDF(DataTypes.StringType)) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  private def firstLastLongsDf(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq[(java.lang.String, java.lang.Long)](
      ("aa", null),
      ("aa", Long.MinValue),
      ("bb", Long.MaxValue),
      ("bb", Long.MinValue),
      ("cc", Long.MinValue),
      ("cc", Long.MaxValue),
      ("dd", -0L),
      ("dd", 0L),
      ("ee", 0L),
      ("ee", -0L),
      ("ff", null),
      ("ff", Long.MaxValue),
      ("ff", null)
    ).toDF("c0", "c1")
  }

  private def randomDF(dataType: DataType)(spark: SparkSession) : DataFrame = {
    val schema = FuzzerUtils.createSchema(Seq(DataTypes.StringType, dataType))
    FuzzerUtils.generateDataFrame(spark, schema, rowCount = 1000,
      options = FuzzerOptions(numbersAsStrings = false, asciiStringsOnly = true, maxStringLen = 2))
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
      // first/last are disabled on GPU for now since we need CuDF changes to support nth_element reductions
//      first("floats", true),
//      last("floats", true),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: grouped count", floatCsvDf) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("partial on gpu: empty df: grouped count", floatCsvDf,
    conf = replaceHashAggMode("partial"),
    execsAllowedNonGpu = execsAllowedNonGpu) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("final on gpu: empty df: grouped count", floatCsvDf,
    conf = replaceHashAggMode("final"),
    execsAllowedNonGpu = execsAllowedNonGpu) {
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
    conf = replaceHashAggMode("partial"),
    execsAllowedNonGpu = execsAllowedNonGpu) {
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
    conf = replaceHashAggMode("final"),
    execsAllowedNonGpu = execsAllowedNonGpu) {
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

  testSparkResultsAreEqual("Agg expression with filter fall back", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu) {
    frame => frame.selectExpr("count(1) filter (where longs > 20)")
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:sum", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"), sum("more_longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:avg", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"), avg("more_longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:all", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      avg("more_longs"),
      count("longs"),
      min("more_longs"),
      max("more_longs"),
      sum("longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:min", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"), min("more_longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:max", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"), max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:sum",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:avg",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:all", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"),
      count("longs"),
      min("more_longs"),
      max("more_longs"),
      sum("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:avg:countDistinct:max",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(avg("more_longs"),
      countDistinct("longs"), max("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:avg:max:countDistinct",
    longsFromCSVDf, repart = 2, conf = floatAggConf) {
    frame => val result = frame.groupBy("longs").agg(avg("more_longs"),
      max("longs"), countDistinct("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:last",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      last("more_longs", true))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:min",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy:countDistinct:max",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy_2:countDistinct:sum",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy_2:countDistinct:avg",
    longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy_2:countDistinct:min",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_testSparkResultsAreEqual("PartMerge:groupBy_2:countDistinct:max",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  private val partialOnlyConf = replaceHashAggMode("partial").set(
    RapidsConf.ENABLE_FLOAT_AGG.key, "true").set(RapidsConf.HAS_NANS.key, "false")
  private val finalOnlyConf = replaceHashAggMode("final").set(
    RapidsConf.ENABLE_FLOAT_AGG.key, "true").set(RapidsConf.HAS_NANS.key, "false")

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:sum:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:avg:partOnly",
    longsFromCSVDf, execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:min:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:max:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:sum:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:avg:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:min:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:countDistinct:max:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:sum:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:avg:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:min:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:max:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:sum:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:avg:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:min:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:max:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }
  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:sum:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:avg:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:min:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinct:max:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:sum:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:avg:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:min:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinct:max:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinctOnly:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result =frame.groupBy("more_longs").agg(countDistinct("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinctOnly:finOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy_2:countDistinctOnly:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("more_longs").agg(countDistinct("longs"))
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:groupBy:countDistinctOnly:partOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.groupBy("longs").agg(countDistinct("longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinctOnly", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinctOnly_2", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.agg(countDistinct("longs"),
      countDistinct("more_longs"))
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:countDistinct:count", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("count(distinct longs)", "count(longs)")
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:avgDistinct:count", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(longs)")
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:avgDistinct:count:2cols", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(more_longs)")
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:avgDistinct:avg:2cols", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","avg(more_longs)")
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:avgDistinct:count:PartOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(longs)")
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:avgDistinct:count:FinOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(longs)")
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:avgDistinct:count:2cols:PartOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(more_longs)")
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
    "PartMerge:avgDistinct:count:2cols:FinOnly", longsFromCSVDf,
    execsAllowedNonGpu = execsAllowedNonGpu, conf = finalOnlyConf,
    repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)","count(more_longs)")
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:avgDistinctOnly", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)")
      checkExecNode(result)
      result
  }

  testSparkResultsAreEqual("PartMerge:avgDistinctOnly_2", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => val result = frame.selectExpr("avg(distinct longs)", "avg(distinct more_longs)")
      checkExecNode(result)
      result
  }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual("PartMerge:reduction_avg_partOnly",
    intCsvDf, execsAllowedNonGpu = execsAllowedNonGpu, conf = partialOnlyConf, repart = 8) {
    frame => val result = frame.agg(avg("ints"))
      checkExecNode(result)
      result
  }
}
