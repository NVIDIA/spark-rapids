/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExec

class BroadcastHashJoinSuite extends SparkQueryCompareTestSuite {

  test("broadcast hint isn't propagated after a join") {
    val conf = new SparkConf()
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")

    withGpuSparkSession(spark => {
      val df1 = longsDf(spark)
      val df2 = nonZeroLongsDf(spark)

      val df3 = df1.join(broadcast(df2), Seq("longs"), "inner").drop(df2("longs"))
      val df4 = longsDf(spark)
      val df5 = df4.join(df3, Seq("longs"), "inner")

      val plan = df5.queryExecution.executedPlan

      assert(plan.collect { case p: GpuBroadcastHashJoinExec => p }.size === 1)
      assert(plan.collect { case p: GpuShuffledHashJoinExec => p }.size === 1)
    }, conf)
  }

  test("broadcast hint in SQL") {
    withGpuSparkSession(spark => {
      longsDf(spark).createOrReplaceTempView("t")
      longsDf(spark).createOrReplaceTempView("u")

      for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
        val plan1 = spark.sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.longs = u.longs")
          .queryExecution.executedPlan
        val plan2 = spark.sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.longs = u.longs")
          .queryExecution.executedPlan

        val res1 = plan1.find(_.isInstanceOf[GpuBroadcastHashJoinExec])
        val res2 = plan2.find(_.isInstanceOf[GpuBroadcastHashJoinExec])

        assert(res1.get.asInstanceOf[GpuBroadcastHashJoinExec].buildSide.toString
          .equals("BuildLeft"))
        assert(res2.get.asInstanceOf[GpuBroadcastHashJoinExec].buildSide.toString
          .equals("BuildRight"))
      }
    })
  }
}
