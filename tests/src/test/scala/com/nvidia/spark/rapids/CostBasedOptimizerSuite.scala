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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{ProjectExec, SortExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.types.DataTypes

class CostBasedOptimizerSuite extends SparkQueryCompareTestSuite {

  test("Force section of plan back onto CPU, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark)
          .alias("df1")
          .orderBy("more_strings_1")
      val df2: DataFrame = createQuery(spark)
          .alias("df2")
          .orderBy("more_strings_2")
      val df = df1.join(df2, col("df1.more_strings_1").equalTo(col("df2.more_strings_2")))
          .orderBy("df2.more_strings_2")

      df.collect()
      println(df.queryExecution.executedPlan)

      val cpuSort = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
            _.isInstanceOf[SortExec])

      val gpuSort = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
            _.isInstanceOf[GpuSortExec])

      assert(cpuSort.nonEmpty)
      assert(gpuSort.isEmpty)

      df
    }, conf)

  }

  test("Force section of plan back onto CPU, AQE off") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark)
          .alias("df1")
          .orderBy("more_strings_1")
      val df2: DataFrame = createQuery(spark)
          .alias("df2")
          .orderBy("more_strings_2")
      val df = df1.join(df2, col("df1.more_strings_1").equalTo(col("df2.more_strings_2")))
          .orderBy("df2.more_strings_2")

     df.collect()
      println(df.queryExecution.executedPlan)

      val cpuSort = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
            _.isInstanceOf[SortExec])

      val gpuSort = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
            _.isInstanceOf[GpuSortExec])

      assert(cpuSort.nonEmpty)
      assert(gpuSort.isEmpty)

      //getCannotBeReplacedReasons(df.queryExecution.executedPlan).foreach(println)

      df
    }, conf)

  }

  test("Force last section of plan back onto CPU, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()
      //assert that the top-level sort stayed on the CPU
      df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
          .executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[SortExec]

      df
    }, conf)

  }

  test("Force last section of plan back onto CPU, AQE off") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()
       //assert that the top-level sort stayed on the CPU
      df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[SortExec]

      df
    }, conf)

  }

  test("Avoid move to GPU for trivial projection, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()
      // assert that the top-level projection stayed on the CPU
      df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
          .executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[ProjectExec]

      df
    }, conf)

  }

  test("Avoid move to GPU for trivial projection, AQE off") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
          "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      // assert that the top-level projection stayed on the CPU
      assert(df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.isInstanceOf[ProjectExec])

      df
    }, conf)
  }

  test("Avoid move to GPU for shuffle, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      val gpuExchanges = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
          _.isInstanceOf[GpuShuffleExchangeExecBase])
      assert(gpuExchanges.isEmpty)

      df
    }, conf)
  }

  test("Avoid move to GPU for shuffle, AQE off") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .set(RapidsConf.CBO_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
              "Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      val gpuExchanges = ShimLoader.getSparkShims
          .findOperators(df.queryExecution.executedPlan,
          _.isInstanceOf[GpuShuffleExchangeExecBase])
      assert(gpuExchanges.isEmpty)

      df
    }, conf)
  }

  private def createQuery(spark: SparkSession) = {
    val df1 = nullableStringsDf(spark)
        .repartition(2)
        .withColumnRenamed("more_strings", "more_strings_1")

    val df2 = nullableStringsDf(spark)
        .repartition(2)
        .withColumnRenamed("more_strings", "more_strings_2")

    val df = df1.join(df2, "strings")
        // filter on unsupported CAST to force operation onto CPU
        .filter(col("more_strings_2").cast(DataTypes.TimestampType)
            .lt(col("more_strings_1").cast(DataTypes.TimestampType)))
        // this projection just swaps the order of the attributes and we want CBO to keep
        // this on CPU
        .select("more_strings_2", "more_strings_1")
    df
  }

}
