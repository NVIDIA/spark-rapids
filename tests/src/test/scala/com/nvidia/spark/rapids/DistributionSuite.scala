/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, ClusteredDistribution, UnspecifiedDistribution}

class DistributionSuite extends SparkQueryCompareTestSuite {

  val seq1 = Seq((10, 20), (10, 30), (20, 20), (20, 30), (30, 20))
  val seq2 = Seq((10, 20), (20, 300), (30, 400), (10, 400))

  test("BroadcastDistribution and UnknownDistribution") {
    val conf = new SparkConf()
    withGpuSparkSession(spark => {
      import spark.implicits._
      val df1 = seq1.toDF("a", "b")
      val df2 = seq2.toDF("aa", "bb")
      val df = df1.join(df2, df1("a") === df2("aa"), "inner")
      df.collect()

      val distributions = df.queryExecution.sparkPlan.requiredChildDistribution

      assert(distributions(0).isInstanceOf[UnspecifiedDistribution.type], true)
      assert(distributions(1).isInstanceOf[BroadcastDistribution], true)
    }, conf)
  }

  test("ClusteredDistribution") {
    val conf = new SparkConf()
    withGpuSparkSession(spark => {
      import spark.implicits._
      val df1 = seq1.toDF("a", "b")
      val df = df1.groupBy("a").sum("b")
      df.collect()
      val distributions = df.queryExecution.sparkPlan.requiredChildDistribution

      assert(distributions(0).isInstanceOf[ClusteredDistribution], true)
    }, conf)
  }
}
