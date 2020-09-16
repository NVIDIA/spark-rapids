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

import com.nvidia.spark.rapids.tests.common.BenchUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TpcdsLikeBench extends Logging {

  /**
   * This method performs a benchmark of executing a query and collecting the results to the
   * driver and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.collect(spark, "q5", 3)
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param iterations The number of times to run the query.
   */
  def collect(
      spark: SparkSession,
      query: String,
      iterations: Int = 3,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.collect(
      spark,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      s"tpcds-$query-collect",
      iterations,
      gcBetweenRuns)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to CSV files
   * and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.writeCsv(spark, "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param iterations The number of times to run the query.
   */
  def writeCsv(
      spark: SparkSession,
      query: String,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.writeCsv(
      spark,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      s"tpcds-$query-csv",
      iterations,
      gcBetweenRuns,
      path,
      mode,
      writeOptions)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to Parquet files
   * and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.writeParquet(spark, "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param iterations The number of times to run the query.
   */
  def writeParquet(
      spark: SparkSession,
      query: String,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.writeParquet(
      spark,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      s"tpcds-$query-parquet",
      iterations,
      gcBetweenRuns,
      path,
      mode,
      writeOptions)
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
      collect(spark, query)
    })

  }
}
