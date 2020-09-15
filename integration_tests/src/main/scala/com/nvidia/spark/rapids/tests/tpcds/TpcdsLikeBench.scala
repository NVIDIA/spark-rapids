/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpcds

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.tests.common.BenchUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TpcdsLikeBench extends Logging {

  /**
   * This method can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.runBench(spark, "q5")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param numColdRuns The number of cold runs.
   * @param numHotRuns The number of hot runs.
   * @param maxResultsToRecord The maximum number of result rows to collect and store in the
   *                           output JSON file. This can be used as a sanity check to compare
   *                           to previous runs but is not intended to be used for full results
   *                           verification. Setting this to `None` will capture all result rows,
   *                           and setting this to `Some(0)` will skip recording results.
   */
  def runBench(
      spark: SparkSession,
      query: String,
      numColdRuns: Int = 1,
      numHotRuns: Int = 3,
      maxResultsToRecord: Option[Int] = Some(0)): Unit = {
    BenchUtils.runBench(
      spark,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      s"tpcds-$query",
      numColdRuns,
      numHotRuns,
      maxResultsToRecord)
  }

  /**
   * The main method can be invoked by using spark-submit.
   */
  def main(args: Array[String]): Unit = {
    val input = args(0)

    val spark = SparkSession.builder.appName("TPC-DS Like Bench").getOrCreate()
    TpcdsLikeSpark.setupAllParquet(spark, input)

    args.drop(1).foreach(query => {
      println(s"*** RUNNING TPC-DS QUERY $query")
      runBench(spark, query)
    })

  }
}
