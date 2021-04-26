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

import scala.collection.mutable.ListBuffer

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{ProjectExec, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase
import org.apache.spark.sql.types.DataTypes

class CostBasedOptimizerSuite extends SparkQueryCompareTestSuite with BeforeAndAfter {

  before {
    GpuOverrides.removeAllListeners()
  }

  after {
    GpuOverrides.removeAllListeners()
  }

  test("Force section of plan back onto CPU, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST.key, "0.15")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST.key, "0.15")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
              "Alias,Cast,LessThan")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
      optimizations += costOptimizations
    })

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

      // check that the expected optimization was applied
      val opt = optimizations.last.last.asInstanceOf[ReplaceSection[_]]
      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

      // check that the final plan has a CPU sort and no GPU sort
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
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST.key, "0.15")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST.key, "0.15")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.ENABLE_REPLACE_SORTMERGEJOIN.key, "false")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec,SortMergeJoinExec," +
              "Alias,Cast,LessThan")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

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

      // check that the expected optimization was applied
      assert(7 == optimizations.flatten
          .filter(_.isInstanceOf[ReplaceSection[_]])
          .map(_.asInstanceOf[ReplaceSection[_]])
          .count(_.plan.wrapped.isInstanceOf[SortExec]))

      // check that the final plan has a CPU sort and no GPU sort
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

  test("Force last section of plan back onto CPU, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST.key, "0.15")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST.key, "0.15")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
              "Alias,Cast,LessThan")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()

      // check that the expected optimization was applied
      val opt = optimizations.last.last.asInstanceOf[ReplaceSection[_]]
      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

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
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST.key, "0.15")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST.key, "0.15")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec,SortExec," +
              "Alias,Cast,LessThan")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
          .orderBy("more_strings_1")
      df.collect()

      // check that the expected optimization was applied
      val opt = optimizations.last.last.asInstanceOf[ReplaceSection[_]]
      assert(opt.totalGpuCost > opt.totalCpuCost)
      assert(opt.plan.wrapped.isInstanceOf[SortExec])

      //assert that the top-level sort stayed on the CPU
      df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.asInstanceOf[SortExec]

      df
    }, conf)

  }

  test("Avoid move to GPU for trivial projection, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
              "Alias,Cast,LessThan")

    val optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

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
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "false")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,BroadcastExchangeExec,BroadcastHashJoinExec," +
          "Alias,Cast,LessThan")

    var optimizations: ListBuffer[Seq[Optimization]] = new ListBuffer[Seq[Optimization]]()
    GpuOverrides.addListener(
      (_: SparkPlanMeta[SparkPlan],
       _: SparkPlan,
       costOptimizations: Seq[Optimization]) => {
        optimizations += costOptimizations
      })

    withGpuSparkSession(spark => {
      val df: DataFrame = createQuery(spark)
      df.collect()

      // check that the expected optimization was applied
      assert(3 == optimizations
          .flatten
          .filter(_.isInstanceOf[AvoidTransition[_]])
          .map(_.asInstanceOf[AvoidTransition[_]])
          .count(_.plan.wrapped.isInstanceOf[ProjectExec]))

      // check that the expected optimization was applied
      assert(3 == optimizations
          .flatten
          .filter(_.isInstanceOf[AvoidTransition[_]])
          .map(_.asInstanceOf[AvoidTransition[_]])
          .count(_.plan.wrapped.isInstanceOf[ProjectExec]))

      // assert that the top-level projection stayed on the CPU
      assert(df.queryExecution.executedPlan.asInstanceOf[WholeStageCodegenExec]
          .child.isInstanceOf[ProjectExec])

      df
    }, conf)
  }

  test("Avoid move to GPU for shuffle, AQE on") {

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
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
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
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


  test("keep CustomShuffleReaderExec on GPU") {

    // if we force a GPU CustomShuffleReaderExec back onto CPU due to cost then the query will
    // fail because the shuffle already happened on GPU and we end up with an invalid plan

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "1")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_EXPLAIN.key, "ALL")
        .set(RapidsConf.EXPLAIN.key, "ALL")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_CPU_COST.key, "0")
        .set(RapidsConf.OPTIMIZER_DEFAULT_TRANSITION_TO_GPU_COST.key, "0")
        .set("spark.rapids.sql.optimizer.exec.CustomShuffleReaderExec", "99999999")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan")

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()

      df
    }, conf)
  }

  test("Compute estimated row count nested joins no broadcast") {
    assumeSpark301orLater

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan")

    var plans: ListBuffer[SparkPlanMeta[SparkPlan]] =
      new ListBuffer[SparkPlanMeta[SparkPlan]]()
    GpuOverrides.addListener(
      (plan: SparkPlanMeta[SparkPlan],
          _: SparkPlan,
          _: Seq[Optimization]) => {
        plans += plan
      })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()
    }, conf)

    val accum = new ListBuffer[SparkPlanMeta[_]]()
    plans.foreach(collectPlansWithRowCount(_, accum))

    val summary = accum
        .map(plan => plan.wrapped.getClass.getSimpleName -> plan.estimatedOutputRows.get)
        .distinct
        .sorted

    // due to the concurrent nature of adaptive execution, the results are not deterministic
    // so we just check that we do see row counts for shuffle exchanges and sort-merge joins

    val shuffleExchanges = summary
        .filter(_._1 == "ShuffleExchangeExec")
    assert(shuffleExchanges.nonEmpty)
    assert(shuffleExchanges.forall(_._2.toLong > 0))

    val sortMergeJoins = summary
        .filter(_._1 == "SortMergeJoinExec")
    assert(sortMergeJoins.nonEmpty)
    assert(sortMergeJoins.forall(_._2.toLong > 0))
  }

  test("Compute estimated row count nested joins with broadcast") {
    assumeSpark301orLater

    val conf = new SparkConf()
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(RapidsConf.OPTIMIZER_ENABLED.key, "true")
        .set(RapidsConf.TEST_ALLOWED_NONGPU.key,
          "ProjectExec,SortMergeJoinExec,SortExec,Alias,Cast,LessThan")

    var plans: ListBuffer[SparkPlanMeta[SparkPlan]] =
      new ListBuffer[SparkPlanMeta[SparkPlan]]()
    GpuOverrides.addListener(
      (plan: SparkPlanMeta[SparkPlan],
          _: SparkPlan,
          _: Seq[Optimization]) => {
        plans += plan
      })

    withGpuSparkSession(spark => {
      val df1: DataFrame = createQuery(spark).alias("l")
      val df2: DataFrame = createQuery(spark).alias("r")
      val df = df1.join(df2,
        col("l.more_strings_1").equalTo(col("r.more_strings_2")))
      df.collect()
    }, conf)

    val accum = new ListBuffer[SparkPlanMeta[_]]()
    plans.foreach(collectPlansWithRowCount(_, accum))

    val summary = accum
        .map(plan => plan.wrapped.getClass.getSimpleName -> plan.estimatedOutputRows.get)
        .distinct
        .sorted

    // due to the concurrent nature of adaptive execution, the results are not deterministic
    // so we just check that we do see row counts for multiple broadcast exchanges

    val broadcastExchanges = summary
        .filter(_._1 == "BroadcastExchangeExec")

    assert(broadcastExchanges.nonEmpty)
    assert(broadcastExchanges.forall(_._2.toLong > 0))
  }

  private def collectPlansWithRowCount(
      plan: SparkPlanMeta[_],
      accum: ListBuffer[SparkPlanMeta[_]]): Unit = {
    if (plan.estimatedOutputRows.exists(_ > 0)) {
      accum += plan
    }
    plan.childPlans.foreach(collectPlansWithRowCount(_, accum))
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
