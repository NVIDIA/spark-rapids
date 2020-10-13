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

import com.nvidia.spark.rapids.tests.common.{BenchUtils, InputSpecification}
import com.twitter.chill.Input
import org.rogach.scallop.ScallopConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object TpcdsLikeBench extends Logging {

  /**
   * This method performs a benchmark of executing a query and collecting the results to the
   * driver and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.collect(spark, "/path/to/read", "parquet", "q5", 3)
   *
   * @param spark The Spark session
   * @param inputPath The path to the source data set
   * @param inputFormat The source data format (parquet, csv, or orc)
   * @param query The name of the query to run e.g. "q5"
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def collect(
      spark: SparkSession,
      inputPath: String,
      inputFormat: String,
      query: String,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    // register the source data
    val input = setupAll(spark, inputPath, inputFormat)
    // run the benchmark
    BenchUtils.collect(
      spark,
      input,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpcds-$query-collect"),
      iterations,
      gcBetweenRuns)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to CSV files
   * and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.writeCsv(spark, "/path/to/read", "parquet", "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param inputPath The path to the source data set
   * @param inputFormat The source data format (parquet, csv, or orc)
   * @param query The name of the query to run e.g. "q5"
   * @param outputPath The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def writeCsv(
      spark: SparkSession,
      inputPath: String,
      inputFormat: String,
      query: String,
      outputPath: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    // register the source data
    val input = setupAll(spark, inputPath, inputFormat)
    // run the benchmark
    BenchUtils.writeCsv(
      spark,
      input,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpcds-$query-csv"),
      iterations,
      gcBetweenRuns,
      outputPath,
      mode,
      writeOptions)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to ORC files
   * and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.writeOrc(spark, "/path/to/read", "parquet", "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param inputPath The path to the source data set
   * @param inputFormat The source data format (parquet, csv, or orc)
   * @param query The name of the query to run e.g. "q5"
   * @param outputPath The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query.
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def writeOrc(
      spark: SparkSession,
      inputPath: String,
      inputFormat: String,
      query: String,
      outputPath: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    // register the source data
    val input = setupAll(spark, inputPath, inputFormat)
    // run the benchmark
    BenchUtils.writeOrc(
      spark,
      input,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpcds-$query-csv"),
      iterations,
      gcBetweenRuns,
      outputPath,
      mode,
      writeOptions)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to Parquet files
   * and can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.writeParquet(spark, "/path/to/read", "parquet", "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param inputPath The path to the source data set
   * @param inputFormat The source data format (parquet, csv, or orc)
   * @param query The name of the query to run e.g. "q5"
   * @param outputPath The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query
   * @param summaryFilePrefix Optional prefix for the generated JSON summary file.
   * @param gcBetweenRuns Whether to call `System.gc` between iterations to cause Spark to
   *                      call `unregisterShuffle`
   */
  def writeParquet(
      spark: SparkSession,
      inputPath: String,
      inputFormat: String,
      query: String,
      outputPath: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): Unit = {
    // register the source data
    val input = setupAll(spark, inputPath, inputFormat)
    // run the benchmark
    BenchUtils.writeParquet(
      spark,
      input,
      spark => TpcdsLikeSpark.query(query)(spark),
      query,
      summaryFilePrefix.getOrElse(s"tpcds-$query-parquet"),
      iterations,
      gcBetweenRuns,
      outputPath,
      mode,
      writeOptions)
  }

  def setupAll(spark: SparkSession, path: String, format: String): InputSpecification = {
    format match {
      case "parquet" => TpcdsLikeSpark.setupAllParquet(spark, path)
      case "csv" => TpcdsLikeSpark.setupAllCSV(spark, path)
      case "orc" => TpcdsLikeSpark.setupAllOrc(spark, path)
      case other =>
        throw new RuntimeException(s"Invalid input format: $other")
    }
    val partitions: Map[String, Seq[String]] = TpcdsLikeSpark.tables
        .map(_.name)
        .map(name => name -> spark.table(name).inputFiles.toSeq)
        .toMap
    InputSpecification(
      path,
      format,
      partitions)
  }

  /**
   * The main method can be invoked by using spark-submit.
   */
  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val spark = SparkSession.builder.appName("TPC-DS Like Bench").getOrCreate()

    println(s"*** RUNNING TPC-DS QUERY ${conf.query()}")
    conf.output.toOption match {
      case Some(output) => conf.outputFormat().toLowerCase match {
        case "parquet" =>
          writeParquet(
            spark,
            conf.input(),
            conf.inputFormat(),
            conf.query(),
            output,
            iterations = conf.iterations(),
            summaryFilePrefix = conf.summaryFilePrefix.toOption)
        case "csv" =>
          writeCsv(
            spark,
            conf.input(),
            conf.inputFormat(),
            conf.query(),
            output,
            iterations = conf.iterations(),
            summaryFilePrefix = conf.summaryFilePrefix.toOption)
        case "orc" =>
          writeOrc(
            spark,
            conf.input(),
            conf.inputFormat(),
            conf.query(),
            output,
            iterations = conf.iterations(),
            summaryFilePrefix = conf.summaryFilePrefix.toOption)
        case _ =>
          println("Invalid or unspecified output format")
          System.exit(-1)
      }
      case _ =>
        collect(
          spark,
          conf.input(),
          conf.inputFormat(),
          conf.query(),
          conf.iterations(),
          summaryFilePrefix = conf.summaryFilePrefix.toOption)
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

