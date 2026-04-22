/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.TestUtils.findOperator

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuHashJoin}

class BroadcastHashJoinSuite extends SparkQueryCompareTestSuite {
  private def broadcastReuseConf: SparkConf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.rapids.sql.join.broadcastHashTable.reuse", "true")
    .set("spark.rapids.sql.batchSizeBytes", "1")

  private def streamedProbeDf(spark: SparkSession): DataFrame =
    spark.range(0, 128).selectExpr(
      "CAST(id % 8 AS INT) AS join_key",
      "CAST(id AS INT) AS probe_value")

  private def distinctBuildDf(spark: SparkSession): DataFrame =
    spark.range(0, 8).selectExpr(
      "CAST(id AS INT) AS join_key",
      "CAST(id * 10 AS INT) AS build_value")

  private def nonDistinctBuildDf(spark: SparkSession): DataFrame =
    spark.range(0, 16).selectExpr(
      "CAST(id % 4 AS INT) AS join_key",
      "CAST(id AS INT) AS build_value")

  test("broadcast hint isn't propagated after a join") {
    val conf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark)
      val df2 = nonZeroLongsDf(spark)

      val df3 = df1.join(broadcast(df2), Seq("longs"), "inner").drop(df2("longs"))
      val df4 = longsDf(spark)
      val df5 = df4.join(df3, Seq("longs"), "inner")

      // execute the plan so that the final adaptive plan is available when AQE is on
      df5.collect()
      val plan = df5.queryExecution.executedPlan

      val bhjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
      assert(bhjCount.size === 1)

      val shjCount = PlanUtils.findOperators(plan, _.isInstanceOf[GpuShuffledSymmetricHashJoinExec])
      assert(shjCount.size === 1)
    }, conf)
  }

  test("broadcast hint in SQL") {
    withGpuSparkSession(spark => {
      longsDf(spark).createOrReplaceTempView("t")
      longsDf(spark).createOrReplaceTempView("u")

      for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
        val plan1 = spark.sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.longs = u.longs")
        val plan2 = spark.sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.longs = u.longs")

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan1.collect()
        val finalPlan1 = findOperator(plan1.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan1.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildLeft)

        // execute the plan so that the final adaptive plan is available when AQE is on
        plan2.collect()
        val finalPlan2 = findOperator(plan2.queryExecution.executedPlan,
          _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(finalPlan2.get.asInstanceOf[GpuHashJoin].buildSide == GpuBuildRight)
      }
    })
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct inner build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct left outer build right",
    streamedProbeDf,
    distinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "left")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse distinct right outer build left",
    distinctBuildDf,
    streamedProbeDf,
    conf = broadcastReuseConf) {
    (build, probe) => broadcast(build).join(probe, Seq("join_key"), "right")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct inner build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct inner build left",
    nonDistinctBuildDf,
    streamedProbeDf,
    conf = broadcastReuseConf) {
    (build, probe) => broadcast(build).join(probe, Seq("join_key"), "inner")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct left semi build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "leftsemi")
  }

  IGNORE_ORDER_testSparkResultsAreEqual2(
    "broadcast hash join reuse non-distinct left anti build right",
    streamedProbeDf,
    nonDistinctBuildDf,
    conf = broadcastReuseConf) {
    (probe, build) => probe.join(broadcast(build), Seq("join_key"), "leftanti")
  }
}
