/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.sql.Timestamp

import org.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes}

class HashAggregatesSuite extends SparkQueryCompareTestSuite {
  private val floatAggConf: SparkConf = enableCsvConf()
      .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")
      .set(RapidsConf.HAS_NANS.key, "false")

  def replaceHashAggMode(mode: String, conf: SparkConf = new SparkConf()): SparkConf = {
    // configures whether Plugin will replace certain aggregate exec nodes
    conf.set("spark.rapids.sql.hashAgg.replaceMode", mode)
  }

  private def checkExecPlan(plan: SparkPlan): Unit = {
    val executedPlan = ExecutionPlanCaptureCallback.extractExecutedPlan(Some(plan))
    if (executedPlan.conf.getAllConfs(RapidsConf.SQL_ENABLED.key).toBoolean) {
      val gpuAgg = executedPlan.find(_.isInstanceOf[GpuHashAggregateExec]) match {
        case Some(agg) => Some(agg)
        case _ => executedPlan.find(_.isInstanceOf[QueryStageExec]) match {
          case Some(s: BroadcastQueryStageExec) => s.plan.find(_.isInstanceOf[GpuHashAggregateExec])
          case Some(s: ShuffleQueryStageExec) => s.plan.find(_.isInstanceOf[GpuHashAggregateExec])
          case _ => None
        }
      }
      assert(gpuAgg.isDefined, s"as the GPU plan expected a GPU aggregate but did not find any! " +
          s"plan: $plan; executedPlan: $executedPlan")
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
    val options = FuzzerOptions(asciiStringsOnly = true, numbersAsStrings = false,
        maxStringLen = 4)
    val schema = FuzzerUtils.createSchema(Seq(DataTypes.StringType, DataTypes.IntegerType))
    FuzzerUtils.generateDataFrame(spark, schema, 100, options, seed = 0)
      .withColumn("c2", col("c1").mod(lit(10)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "test sort agg with first and last string deterministic case",
      firstDf,
      repart = 2,
      skipCanonicalizationCheck = true) {
    // skip canonicalization check because Spark uses SortAggregate, which does not have
    // deterministic canonicalization in this case, and we replace it with HashAggregate, which
    // does have deterministic canonicalization
    frame => frame
      .coalesce(1)
      .sort(col("c2").asc, col("c0").asc) // force deterministic use case
      .groupBy(col("c2"))
      .agg(first(col("c0"), ignoreNulls = true), last(col("c0"), ignoreNulls = true))
  }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "nullable aggregate with not null filter",
      firstDf,
      repart = 2) {
    frame => frame.coalesce(1)
        .sort(col("c2").asc, col("c0").asc) // force deterministic use case
        .groupBy(col("c2"))
        .agg(min(col("c0")).alias("mymin"),
          max(col("c0")).alias("mymax"))
        .filter(col("mymin").isNotNull
            .and(col("mymax").isNotNull))
  } { (_, gpuPlan) => {
    checkExecPlan(gpuPlan)

    // IsNotNull filter means that the aggregates should not be nullable
    val output = gpuPlan.output
    assert(!output(1).nullable)
    assert(!output(2).nullable)
  } }

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
      // execute the plan so that the final adaptive plan is available when AQE is on
      df.collect()

      gpuPlan match {
        case WholeStageCodegenExec(GpuColumnarToRowExec(plan, _)) =>
          assert(plan.children.head.isInstanceOf[GpuHashAggregateExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case GpuColumnarToRowExec(plan, _) => // Codegen disabled
          assert(plan.children.head.isInstanceOf[GpuHashAggregateExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case a: AdaptiveSparkPlanExec =>
          assert(a.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
          assert(a.executedPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(a.executedPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

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
      // execute the plan so that the final adaptive plan is available when AQE is on
      df.collect()

      gpuPlan match {
        case WholeStageCodegenExec(GpuColumnarToRowExec(plan, _)) =>
          assert(plan.children.head.isInstanceOf[GpuSortExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.find(_.isInstanceOf[GpuHashAggregateExec]).isDefined)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case GpuColumnarToRowExec(plan, _) => // codegen disabled
          assert(plan.isInstanceOf[GpuSortExec])
          assert(gpuPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(gpuPlan.find(_.isInstanceOf[GpuHashAggregateExec]).isDefined)
          assert(gpuPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case a: AdaptiveSparkPlanExec =>
          assert(a.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
          assert(a.executedPlan.find(_.isInstanceOf[GpuSortExec]).isDefined)
          assert(a.executedPlan.find(_.isInstanceOf[SortAggregateExec]).isEmpty)
          assert(a.executedPlan.children.forall(exec => exec.isInstanceOf[GpuExec]))

        case _ =>
          fail(s"Incorrect plan $gpuPlan")
      }

    }, conf)
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "test hash agg with shuffle",
      longsFromCSVDf,
      conf = enableCsvConf(),
      repart = 2) {
    frame => frame.groupBy(col("longs")).agg(sum(col("more_longs")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "test hash agg with Single partitioning",
      longsFromCSVDf,
      repart = 2,
      conf = enableCsvConf().set("spark.sql.shuffle.partitions", "1")) {
    frame => {
      frame.agg(count("*"))
    }
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "test hash agg with Single partitioning with partition sort",
      longsFromCSVDf,
      repart = 2,
      conf = enableCsvConf().set("spark.sql.shuffle.partitions", "1"), sortBeforeRepart = true) {
    frame => {
      frame.agg(count("*"))
    }
  }

  /*
   * HASH AGGREGATE TESTS
   */
  IGNORE_ORDER_testSparkResultsAreEqual(
      "short reduction aggs",
      shortsFromCsv,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = floatAggConf) {
    frame => frame.agg(
      (max("shorts") - min("more_shorts")) * lit(5),
      sum("shorts"),
      count("*"),
      avg("shorts"),
      avg(col("more_shorts") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "reduction aggs",
      longsCsvDf,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3, enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
    frame => frame.agg(
      (max("longs") - min("more_longs")) * lit(5),
      sum("longs"),
      count("*"),
      avg("longs"),
      avg(col("more_longs") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("distinct", datesCsvDf, conf = enableCsvConf()) {
    frame => frame.distinct()
  }

  IGNORE_ORDER_testSparkResultsAreEqual("avg literals", longsFromCSVDf, conf = floatAggConf) {
    frame => frame.agg(avg(lit(1.toDouble)),avg(lit(2.toDouble)))
  }

  INCOMPAT_testSparkResultsAreEqual(
      "avg literals dbl_max",
      longsFromCSVDf,
      maxFloatDiff = 0.0001,
      conf = floatAggConf.set(RapidsConf.INCOMPATIBLE_OPS.key, "true")) {
    frame => frame.agg(avg(lit(1.4718094e+19)),avg(lit(1.4718094e+19)))
  }

  INCOMPAT_testSparkResultsAreEqual(
      "avg literals long_max casted",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(1.4718094e+19.toLong)),avg(lit(1.4718094e+19.toLong)))
  }

  INCOMPAT_testSparkResultsAreEqual(
      "avg literals strings",
      longsFromCSVDf,
      conf = floatAggConf) {
    //returns (null, null) as strings are casted to double prior avg eval.
    frame => frame.agg(avg(lit("abc")),avg(lit("pqr")))
  }

  testExpectedException[AnalysisException](
      "avg literals bools fail",
      _.getMessage.startsWith("cannot resolve"),
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(true)),avg(lit(false)))
  }

  testSparkResultsAreEqual(
      "avg literals bytes",
      longsFromCSVDf,
      conf = floatAggConf) {
      frame => frame.agg(avg(lit(1.toByte)),avg(lit(2.toByte)))
  }

  testSparkResultsAreEqual(
      "avg literals shorts",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(1.toShort)),avg(lit(2.toShort)))
  }

  INCOMPAT_testSparkResultsAreEqual(
      "avg literals strings dbl",
      longsFromCSVDf,
      maxFloatDiff = 0.0001,
      conf = floatAggConf.set(RapidsConf.INCOMPATIBLE_OPS.key, "true")) {
    frame => frame.agg(avg(lit("1.4718094e+19")),avg(lit("1.4718094e+19")))
  }

  INCOMPAT_testSparkResultsAreEqual(
      "avg literals timestamps",
      timestampsDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(Timestamp.valueOf("0100-1-1 23:00:01"))))
  }

  testSparkResultsAreEqual(
      "avg literals with nulls",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(null)),avg(lit(2.toDouble)))
  }

  testSparkResultsAreEqual(
      "avg literals with all nulls",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.agg(avg(lit(null)),avg(lit(null)))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("distinct should not reorder columns",
    intsFromCsv, conf = enableCsvConf()) {
    frame => frame.distinct()
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "group by string include nulls in count aggregate",
      nullableStringsIntsDf,
      conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "group by string include nulls in count aggregate small batches",
      nullableStringsIntsDf,
      conf = floatAggConf.set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "10")) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "group by strings exclude nulls in count aggregate",
      nullableStringsIntsDf,
      conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "group by float with NaNs and null",
      intnullableFloatWithNullAndNanDf,
      conf = floatAggConf) {
    frame => frame.groupBy("ints").agg(
      count("floats"),
      count(lit(null)));
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "group by utf8 strings",
      utf8RepeatedDf,
      conf = floatAggConf) {
    frame => frame.groupBy("strings").agg(
      max("ints"),
      min("ints"),
      avg("ints"),
      sum("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "test count, sum, max, min with shuffle",
      longsFromCSVDf,
      conf = enableCsvConf(),
      repart = 2) {
    frame => frame.groupBy(col("more_longs")).agg(
      count("*"),
      sum("more_longs"),
      sum("longs") * lit(2),
      (max("more_longs") - min("more_longs")) * 3.0)
  }

  FLOAT_TEST_testSparkResultsAreEqual(
      "float basic aggregates group by string literal",
      floatCsvDf,
      conf = enableCsvConf()) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "float basic aggregates group by float and string literal",
      floatCsvDf,
      conf = enableCsvConf()) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "aggregates with timestamp and string literal",
      timestampsDf) {
    frame => {
      val feature_window_end_time = date_format(lit("2018-02-01"), "yyyy-MM-dd HH:mm:ss")
      val frameWithCol = frame.withColumn("timestamp_lit", feature_window_end_time)
      frameWithCol.groupBy("more_timestamps", "timestamp_lit").agg(
        sum(when(col("timestamp_lit") > col("timestamps"), 1).otherwise(2))
      )
    }
  }

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by floats", floatCsvDf,
    conf = enableCsvConf()) {
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

  FLOAT_TEST_testSparkResultsAreEqual("float basic aggregates group by more_floats", floatCsvDf,
    conf = enableCsvConf()) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "partial on gpu: float basic aggregates group by more_floats",
      floatCsvDf,
      conf = replaceHashAggMode("partial",  enableCsvConf()),
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Literal", "Min", "Sum", "Max", "Average", "Add", "Multiply", "Subtract",
          "Cast", "Count")) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "final on gpu: float basic aggregates group by more_floats",
      floatCsvDf,
      conf = replaceHashAggMode("final",  enableCsvConf()),
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Literal", "Min", "Sum", "Max", "Average", "Add", "Multiply", "Subtract",
          "Cast", "Count", "KnownFloatingPointNormalized", "NormalizeNaNAndZero")) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "nullable float basic aggregates group by more_floats",
      nullableFloatCsvDf,
      conf = makeBatchedBytes(3,  enableCsvConf())) {
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

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual(
      "shorts basic aggregates group by more_shorts",
      shortsFromCsv,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3,  enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "long basic aggregates group by longs",
      longsCsvDf,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3,  enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "long basic aggregates group by more_longs",
      longsCsvDf,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3,  enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "ints basic aggregates group by ints",
      intCsvDf,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3,  enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "ints basic aggregates group by more_ints",
      intCsvDf,
      // All the literals get turned into doubles, so we need to support avg in those cases
      conf = makeBatchedBytes(3, enableCsvConf())
          .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "doubles basic aggregates group by doubles",
      doubleCsvDf,
      conf = makeBatchedBytes(3, enableCsvConf())) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "doubles basic aggregates group by more_doubles",
      doubleCsvDf,
      conf = makeBatchedBytes(3, enableCsvConf())) {
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

  IGNORE_ORDER_testSparkResultsAreEqual(
      "sum(longs) multi group by longs, more_longs",
      longsCsvDf,
      conf = enableCsvConf()) {
    frame => frame.groupBy("longs", "more_longs").agg(
      sum("longs"), count("*"))
  }

  // misc aggregation tests
  testSparkResultsAreEqual("sum(ints) group by literal", intCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(lit(1)).agg(sum("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("sum(ints) group by dates", datesCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy("dates").sum("ints")
  }

  IGNORE_ORDER_testSparkResultsAreEqual("max(ints) group by month", datesCsvDf,
    conf = enableCsvConf()) {
    frame => frame.withColumn("monthval", month(col("dates")))
      .groupBy(col("monthval"))
      .agg(max("ints").as("max_ints_by_month"))
  }

  FLOAT_TEST_testSparkResultsAreEqual(
      "sum(floats) group by more_floats 2 partitions",
      floatCsvDf,
      conf = enableCsvConf(),
      repart = 2) {
    frame => frame.groupBy("more_floats").sum("floats")
  }

  FLOAT_TEST_testSparkResultsAreEqual(
      "avg(floats) group by more_floats 4 partitions",
      floatCsvDf,
      conf = enableCsvConf(),
      repart = 4) {
    frame => frame.groupBy("more_floats").avg("floats")
  }

  FLOAT_TEST_testSparkResultsAreEqual(
      "avg(floats),count(floats) group by more_floats 4 partitions",
      floatCsvDf,
      conf = enableCsvConf(),
      repart = 4) {
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

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions", longsCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(col("more_longs") + lit(10)).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions 2", longsCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(col("more_longs") + col("longs")).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first ignoreNulls=false", intCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(col("more_ints")).agg(first("ints", false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("last ignoreNulls=false", intCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(col("more_ints")).agg(last("ints", false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last ints column", intCsvDf,
    conf = enableCsvConf()) {
    frame => frame.groupBy(col("more_ints")).agg(
      first("ints", ignoreNulls = false),
      last("ints", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first hand-picked longs ignoreNulls=true",
      firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = true))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first hand-picked longs ignoreNulls=false",
      firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first/last hand-picked longs ignoreNulls=false",
      firstLastLongsDf) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first/last random ints ignoreNulls=false",
      randomDF(DataTypes.IntegerType)) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first/last random longs ignoreNulls=false",
      randomDF(DataTypes.LongType)) {
    frame => frame.groupBy(col("c0")).agg(
      first("c1", ignoreNulls = false),
      last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first random strings ignoreNulls=false",
      randomDF(DataTypes.StringType), skipCanonicalizationCheck=true) {
    // skip canonicalization check because Spark uses SortAggregate, which does not have
    // deterministic canonicalization in this case, and we replace it with HashAggregate, which
    // does have deterministic canonicalization
    frame => frame.groupBy(col("c0")).agg(first("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "last random strings ignoreNulls=false",
      randomDF(DataTypes.StringType), skipCanonicalizationCheck=true) {
    // skip canonicalization check because Spark uses SortAggregate, which does not have
    // deterministic canonicalization in this case, and we replace it with HashAggregate, which
    // does have deterministic canonicalization
    frame => frame.groupBy(col("c0")).agg(last("c1", ignoreNulls = false))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "first/last random strings ignoreNulls=false",
      randomDF(DataTypes.StringType), skipCanonicalizationCheck=true) {
    // skip canonicalization check because Spark uses SortAggregate, which does not have
    // deterministic canonicalization in this case, and we replace it with HashAggregate, which
    // does have deterministic canonicalization
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
      options = FuzzerOptions(numbersAsStrings = false, asciiStringsOnly = true,
          maxStringLen = 2))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: reduction count", floatCsvDf,
    conf = enableCsvConf()) {
    frame => frame.filter("floats > 10000000.0").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: reduction aggs", floatCsvDf,
    conf = enableCsvConf()) {
    frame => frame.filter("floats > 10000000.0").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      // first/last are disabled on GPU for now since we need CuDF changes to support
      // nth_element reductions
      //      first("floats", true),
      //      last("floats", true),
      count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("empty df: grouped count", floatCsvDf,
    conf = enableCsvConf()) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("partial on gpu: empty df: grouped count", floatCsvDf,
    conf = replaceHashAggMode("partial", enableCsvConf()),
    execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
      "Alias", "Count", "Literal")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual("final on gpu: empty df: grouped count", floatCsvDf,
    conf = replaceHashAggMode("final", enableCsvConf()),
    execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
      "Alias", "Count", "Literal", "KnownFloatingPointNormalized", "NormalizeNaNAndZero")) {
    frame => frame.filter("floats > 10000000.0").groupBy("floats").agg(count("*"))
  }

  FLOAT_TEST_testSparkResultsAreEqual(
      "empty df: float basic aggregates group by floats",
      floatCsvDf,
      conf = enableCsvConf()) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "partial on gpu: empty df: float basic aggregates group by floats",
      floatCsvDf,
      conf = replaceHashAggMode("partial", enableCsvConf()),
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Literal", "Min", "Sum", "Max", "Average", "Add", "Multiply", "Subtract",
          "Cast", "First", "Last", "Count")) {
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

  FLOAT_TEST_testSparkResultsAreEqual(
      "final on gpu: empty df: float basic aggregates group by floats",
      floatCsvDf,
      conf = replaceHashAggMode("final", enableCsvConf()),
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Literal", "Min", "Sum", "Max", "Average", "Add", "Multiply", "Subtract",
          "Cast", "First", "Last", "Count", "KnownFloatingPointNormalized",
          "NormalizeNaNAndZero")) {
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
    conf = enableCsvConf()) {
    frame => frame.selectExpr("count(1) filter (where longs > 20)")
  }

  testSparkResultsAreEqualWithCapture("PartMerge:countDistinct:sum", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => frame.agg(countDistinct("longs"), sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture("PartMerge:countDistinct:avg", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
      frame => frame.agg(countDistinct("longs"), avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture("PartMerge:countDistinct:all", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      avg("more_longs"),
      count("longs"),
      min("more_longs"),
      max("more_longs"),
      sum("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture("PartMerge:countDistinct:min", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => frame.agg(countDistinct("longs"), min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture("PartMerge:countDistinct:max", longsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => frame.agg(countDistinct("longs"), max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture("PartMerge:groupBy:countDistinct:sum",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture("PartMerge:groupBy:countDistinct:avg",
    longsFromCSVDf, conf = floatAggConf, repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:all",
      longsFromCSVDf,
      conf = floatAggConf, repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"),
      count("longs"),
      min("more_longs"),
      max("more_longs"),
      sum("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:avg:countDistinct:max",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(avg("more_longs"),
      countDistinct("longs"), max("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:avg:max:countDistinct",
      longsFromCSVDf,
      repart = 2,
      conf = floatAggConf) {
    frame => frame.groupBy("longs").agg(avg("more_longs"),
      max("longs"), countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:last",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      last("more_longs", true))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:min",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:max",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:sum",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:avg",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:min",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:max",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }
  
  private val partialOnlyConf = replaceHashAggMode("partial", enableCsvConf()).set(
    RapidsConf.ENABLE_FLOAT_AGG.key, "true").set(RapidsConf.HAS_NANS.key, "false")
  private val finalOnlyConf = replaceHashAggMode("final", enableCsvConf()).set(
    RapidsConf.ENABLE_FLOAT_AGG.key, "true").set(RapidsConf.HAS_NANS.key, "false")

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:sum:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:avg:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression",
          "AttributeReference", "Alias", "Average", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:min:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Count", "Min"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:max:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:sum:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:avg:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:min:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Min", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:max:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:sum:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:avg:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:min:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Min", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:max:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:sum:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:avg:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:min:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Min", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:max:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:sum:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:avg:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:min:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Min", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinct:max:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:sum:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Sum", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      sum("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:avg:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      avg("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:min:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Min", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      min("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinct:max:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Max", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"),
      max("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinctOnly:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinctOnly:finOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy_2:countDistinctOnly:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("more_longs").agg(countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:groupBy:countDistinctOnly:partOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.groupBy("longs").agg(countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinctOnly",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinctOnly_2",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.agg(countDistinct("longs"),
      countDistinct("more_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:countDistinct:count",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("count(distinct longs)", "count(longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count:2cols",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(more_longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:avg:2cols",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","avg(more_longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count:PartOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count:FinOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count:2cols:PartOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = partialOnlyConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(more_longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinct:count:2cols:FinOnly",
      longsFromCSVDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Count"),
      conf = finalOnlyConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)","count(more_longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinctOnly",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqualWithCapture(
      "PartMerge:avgDistinctOnly_2",
      longsFromCSVDf,
      conf = floatAggConf,
      repart = 2) {
    frame => frame.selectExpr("avg(distinct longs)", "avg(distinct more_longs)")
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      "PartMerge:reduction_avg_partOnly",
      intCsvDf,
      execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression", "AttributeReference",
          "Alias", "Average", "Cast"),
      conf = partialOnlyConf,
      repart = 8) {
    frame => frame.agg(avg("ints"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  testSparkResultsAreEqual("Avg with filter", longsFromCSVDf, conf = floatAggConf) {
    frame => val res = frame.selectExpr("avg(longs) filter (where longs < 5)")
      res
  }

  testSparkResultsAreEqual("Sum with filter", longsFromCSVDf, conf = enableCsvConf()) {
    frame => val res = frame.selectExpr("sum(longs) filter (where longs < 10)")
      res
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "Avg with filter grpBy",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   avg(longs) filter (where longs < 20)
           | FROM testTable
           |   group by more_longs
           |""".stripMargin)
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "Avg with 2 filter grpBy",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   avg(longs) filter (where longs < 20),
           |   avg(more_longs) filter (where more_longs < 30)
           | FROM testTable
           |   group by more_longs
           |""".stripMargin)
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
      "Sum with filter grpBy",
      longsFromCSVDf,
      conf = floatAggConf) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   sum(longs) filter (where longs < 20)
           | FROM testTable
           |   group by more_longs
           |""".stripMargin)
  }

  testSparkResultsAreEqual("Count with filter", longsFromCSVDf, conf = floatAggConf) {
    frame => val res = frame.selectExpr("count(longs) filter (where longs < 5)")
      res
  }

  testSparkResultsAreEqual("Max with filter", longsFromCSVDf, conf = enableCsvConf()) {
    frame => val res = frame.selectExpr("max(longs) filter (where longs < 10)")
      res
  }

  if (spark.SPARK_VERSION_SHORT < "3.1.0") {
    // A test that verifies that Distinct with Filter is not supported on the CPU or the GPU.
    testExpectedException[AnalysisException](
        "Avg Distinct with filter - unsupported on CPU and GPU",
        _.getMessage.startsWith(
          "DISTINCT and FILTER cannot be used in aggregate functions at the same time"),
        longsFromCSVDf, conf = floatAggConf) {
      frame => frame.selectExpr("avg(distinct longs) filter (where longs < 5)")
    }
  } else {
    testSparkResultsAreEqual("Avg Distinct with filter", longsFromCSVDf, conf = floatAggConf) {
      frame => frame.selectExpr("avg(distinct longs) filter (where longs < 5)")
    }
  }

  testSparkResultsAreEqualWithCapture("PartMerge:avg_overflow_cast_dbl", veryLargeLongsFromCSVDf,
    conf = floatAggConf, repart = 2) {
    frame => frame.groupBy("large_longs").agg(avg("large_longs"))
  } { (_, gpuPlan) => checkExecPlan(gpuPlan) }

  ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
    "max_with_nans_fall_back",
    nanDf,
    Seq("HashAggregateExec", "AggregateExpression",
      "AttributeReference", "Alias", "Max"),
    conf = enableCsvConf()) {
    frame => frame.agg(max("doubles"))
  } { (_, gpuPlan) => {
    // verify nothing ran on the gpu
    if (gpuPlan.conf.getAllConfs(RapidsConf.SQL_ENABLED.key).toBoolean) {
      val execNode = gpuPlan.find(_.isInstanceOf[GpuHashAggregateExec])
      assert(!execNode.isDefined)
    }
  }}

  ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
    "min_with_nans_fall_back",
    nanDf,
    Seq("HashAggregateExec", "AggregateExpression",
      "AttributeReference", "Alias", "Min"),
    conf = enableCsvConf()) {
    frame => frame.agg(min("doubles"))
  } { (_, gpuPlan) => {
    // verify nothing ran on the gpu
    if (gpuPlan.conf.getAllConfs(RapidsConf.SQL_ENABLED.key).toBoolean) {
      val execNode = gpuPlan.find(_.isInstanceOf[GpuHashAggregateExec])
      assert(!execNode.isDefined)
    }
  }}

  IGNORE_ORDER_testSparkResultsAreEqual(
    testName = "Test NormalizeNansAndZeros(Float)",
    floatWithDifferentKindsOfNansAndZeros,
    conf = enableCsvConf()
      .set(RapidsConf.HAS_NANS.key, "false")
      .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
    frame => frame.groupBy(col("float")).agg(sum(col("int")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual(
    testName = "Test NormalizeNansAndZeros(Double)",
    doubleWithDifferentKindsOfNansAndZeros,
    conf = enableCsvConf()
      .set(RapidsConf.HAS_NANS.key, "false")
      .set(RapidsConf.ENABLE_FLOAT_AGG.key, "true")) {
    frame => frame.groupBy(col("double")).agg(sum(col("int")))
  }

  testSparkResultsAreEqual("Agg expression with filter avg with nulls", nullDf, execsAllowedNonGpu =
    Seq("HashAggregateExec", "AggregateExpression", "AttributeReference", "Alias", "Average",
      "Count", "Cast"),
    conf = partialOnlyConf, repart = 2) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   avg(more_longs) filter (where more_longs > 2)
           | FROM testTable
           |   group by longs
           |""".stripMargin)
  }

  testSparkResultsAreEqual("Agg expression with filter count with nulls",
    nullDf, execsAllowedNonGpu = Seq("HashAggregateExec", "AggregateExpression",
      "AttributeReference", "Alias", "Count", "Cast"),
    conf = partialOnlyConf, repart = 2) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   count(more_longs) filter (where more_longs > 2)
           | FROM testTable
           |   group by longs
           |""".stripMargin)
  }

  testSparkResultsAreEqual("Agg expression with filter sum with nulls", nullDf, execsAllowedNonGpu =
    Seq("HashAggregateExec", "AggregateExpression", "AttributeReference", "Alias", "Sum", "Cast"),
    conf = partialOnlyConf, repart = 2) {
    frame => frame.createOrReplaceTempView("testTable")
      frame.sparkSession.sql(
        s"""
           | SELECT
           |   sum(more_longs) filter (where more_longs > 2)
           | FROM testTable
           |   group by longs
           |""".stripMargin)
  }
}
