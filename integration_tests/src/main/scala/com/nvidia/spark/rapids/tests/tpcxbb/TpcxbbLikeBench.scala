/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpcxbb

import com.nvidia.spark.rapids.tests.common.BenchUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TpcxbbLikeBench extends Logging {

  /**
   * This method can be called from Spark shell using the following syntax:
   *
   * TpcxbbLikeBench.runBench(spark, "q5")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param action Optional action to perform after creating the DataFrame, with default
   *               behavior of calling df.collect() but user could provide function to
   *               save results to CSV or Parquet instead.
   * @param numColdRuns The number of cold runs.
   * @param numHotRuns The number of hot runs.
   */
  def runBench(
      spark: SparkSession,
      query: String,
      action: Option[DataFrame => Unit] = None,
      numColdRuns: Int = 1,
      numHotRuns: Int = 3): Unit = {
    BenchUtils.runBench(
      spark,
      getQuery(query),
      action,
      query,
      s"tpcxbb-$query",
      numColdRuns,
      numHotRuns)
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)

    val spark = SparkSession.builder.appName("TPCxBB Bench").getOrCreate()
    TpcxbbLikeSpark.setupAllParquet(spark, input)

    args.drop(1).foreach(query => {
      println(s"*** RUNNING TPCx-BB QUERY $query")
      runBench(spark, query)
    })
  }

  def getQuery(query: String): SparkSession => DataFrame = {

    val queryIndex = if (query.startsWith("q")) {
      query.substring(1).toInt
    } else {
      query.toInt
    }

    queryIndex match {
      case 1 => Q1Like.apply
      case 2 => Q2Like.apply
      case 3 => Q3Like.apply
      case 4 => Q4Like.apply
      case 5 => Q5Like.apply
      case 6 => Q6Like.apply
      case 7 => Q7Like.apply
      case 8 => Q8Like.apply
      case 9 => Q9Like.apply
      case 10 => Q10Like.apply
      case 11 => Q11Like.apply
      case 12 => Q12Like.apply
      case 13 => Q13Like.apply
      case 14 => Q14Like.apply
      case 15 => Q15Like.apply
      case 16 => Q16Like.apply
      case 17 => Q17Like.apply
      case 18 => Q18Like.apply
      case 19 => Q19Like.apply
      case 20 => Q20Like.apply
      case 21 => Q21Like.apply
      case 22 => Q22Like.apply
      case 23 => Q23Like.apply
      case 24 => Q24Like.apply
      case 25 => Q25Like.apply
      case 26 => Q26Like.apply
      case 27 => Q27Like.apply
      case 28 => Q28Like.apply
      case 29 => Q29Like.apply
      case 30 => Q30Like.apply
      case _ => throw new IllegalArgumentException(s"Unknown TPCx-BB query number: $queryIndex")
    }

  }
}
