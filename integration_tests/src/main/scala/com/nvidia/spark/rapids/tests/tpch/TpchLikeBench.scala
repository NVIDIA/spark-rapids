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

package com.nvidia.spark.rapids.tests.tpch

import com.nvidia.spark.rapids.tests.common.BenchUtils
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SaveMode, SparkSession}

object TpchLikeBench {

  /**
   * This method performs a benchmark of executing a query and collecting the results to the
   * driver and can be called from Spark shell using the following syntax:
   *
   * TpchLikeBench.collect(spark, "q5", 3)
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def collect(
      spark: SparkSession,
      query: String,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.collect(
      spark,
      spark => getQuery(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpch-$query-collect"),
      iterations,
      gcBetweenRuns)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to CSV files
   * and can be called from Spark shell using the following syntax:
   *
   * TpchLikeBench.writeCsv(spark, "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param path The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def writeCsv(
      spark: SparkSession,
      query: String,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.writeCsv(
      spark,
      spark => getQuery(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpch-$query-csv"),
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
   * TpchLikeBench.writeParquet(spark, "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param path The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def writeParquet(
      spark: SparkSession,
      query: String,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    BenchUtils.writeParquet(
      spark,
      spark => getQuery(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpch-$query-parquet"),
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
    val conf = new Conf(args)

    val spark = SparkSession.builder.appName("TPC-H Like Bench").getOrCreate()
    conf.inputFormat().toLowerCase match {
      case "parquet" => TpchLikeSpark.setupAllParquet(spark, conf.input())
      case "csv" => TpchLikeSpark.setupAllCSV(spark, conf.input())
      case other =>
        println(s"Invalid input format: $other")
        System.exit(-1)
    }

    println(s"*** RUNNING TPC-H QUERY ${conf.query()}")
    conf.output.toOption match {
      case Some(path) => conf.outputFormat().toLowerCase match {
        case "parquet" =>
          writeParquet(
            spark,
            conf.query(),
            path,
            iterations = conf.iterations(),
            summaryFilePrefix = conf.summaryFilePrefix.toOption)
        case "csv" =>
          writeCsv(
            spark,
            conf.query(),
            path,
            iterations = conf.iterations(),
            summaryFilePrefix = conf.summaryFilePrefix.toOption)
        case _ =>
          println("Invalid or unspecified output format")
          System.exit(-1)
      }
      case _ =>
        collect(
          spark,
          conf.query(),
          conf.iterations(),
          summaryFilePrefix = conf.summaryFilePrefix.toOption)
    }
  }

  private def getQuery(query: String)(spark: SparkSession) = {
    query match {
      case "q1" => Q1Like(spark)
      case "q2" => Q2Like(spark)
      case "q3" => Q3Like(spark)
      case "q4" => Q4Like(spark)
      case "q5" => Q5Like(spark)
      case "q6" => Q6Like(spark)
      case "q7" => Q7Like(spark)
      case "q8" => Q8Like(spark)
      case "q9" => Q9Like(spark)
      case "q10" => Q10Like(spark)
      case "q11" => Q11Like(spark)
      case "q12" => Q12Like(spark)
      case "q13" => Q13Like(spark)
      case "q14" => Q14Like(spark)
      case "q15" => Q15Like(spark)
      case "q16" => Q16Like(spark)
      case "q17" => Q17Like(spark)
      case "q18" => Q18Like(spark)
      case "q19" => Q19Like(spark)
      case "q20" => Q20Like(spark)
      case "q21" => Q21Like(spark)
      case "q22" => Q22Like(spark)
    }
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input = opt[String](required = true)
  val inputFormat = opt[String](required = true)
  val query = opt[String](required = true)
  val iterations = opt[Int](default = Some(3))
  val output = opt[String](required = false)
  val outputFormat = opt[String](required = false)
  val summaryFilePrefix = opt[String](required = false)
  verify()
}