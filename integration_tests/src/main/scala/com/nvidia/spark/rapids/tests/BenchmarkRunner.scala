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
package com.nvidia.spark.rapids.tests

import java.net.URI

import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.tests.common.{BenchmarkReport, BenchmarkSuite, BenchUtils}
import com.nvidia.spark.rapids.tests.tpcds.TpcdsLikeBench
import com.nvidia.spark.rapids.tests.tpch.TpchLikeBench
import com.nvidia.spark.rapids.tests.tpcxbb.TpcxbbLikeBench
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * The BenchmarkRunner can be submitted using spark-submit to run any of the TPC-* benchmarks.
 */
object BenchmarkRunner {

  def main(args: Array[String]): Unit = {
    val conf = new BenchmarkConf(args)

    if (conf.appendDat() && !conf.benchmark().equalsIgnoreCase("tpcds")) {
      System.err.println(
        s"The --append-dat flag is not supported for benchmark ${conf.benchmark()}")
      System.exit(-1)
    }

    val benchmarks = Map(
      "tpcds" -> new TpcdsLikeBench(conf.appendDat()),
      "tpch" -> TpchLikeBench,
      "tpcxbb" -> TpcxbbLikeBench
    )

    benchmarks.get(conf.benchmark().toLowerCase) match {
      case Some(bench) =>
        val appName = s"${bench.name()} Like Bench ${conf.query()}"
        val spark = SparkSession.builder.appName(appName).getOrCreate()
        spark.sparkContext.setJobDescription("Register input tables")
        conf.inputFormat().toLowerCase match {
          case "parquet" => bench.setupAllParquet(spark, conf.input())
          case "csv" => bench.setupAllCSV(spark, conf.input())
          case "orc" => bench.setupAllOrc(spark, conf.input())
          case other =>
            System.err.println(s"Invalid input format: $other")
            System.exit(-1)
        }

        val runner = new BenchmarkRunner(bench)
        println(s"*** RUNNING ${bench.name()} QUERY ${conf.query()}")
        val report = Try(conf.output.toOption match {
          case Some(path) => conf.outputFormat().toLowerCase match {
            case "parquet" =>
              runner.writeParquet(
                spark,
                conf.query(),
                path,
                iterations = conf.iterations(),
                summaryFilePrefix = conf.summaryFilePrefix.toOption,
                gcBetweenRuns = conf.gcBetweenRuns())
            case "csv" =>
              runner.writeCsv(
                spark,
                conf.query(),
                path,
                iterations = conf.iterations(),
                summaryFilePrefix = conf.summaryFilePrefix.toOption,
                gcBetweenRuns = conf.gcBetweenRuns())
            case "orc" =>
              runner.writeOrc(
                spark,
                conf.query(),
                path,
                iterations = conf.iterations(),
                summaryFilePrefix = conf.summaryFilePrefix.toOption,
                gcBetweenRuns = conf.gcBetweenRuns())
            case other =>
              throw new IllegalArgumentException(s"Invalid or unspecified output format: $other")
          }
          case _ =>
            runner.collect(
              spark,
              conf.query(),
              conf.iterations(),
              summaryFilePrefix = conf.summaryFilePrefix.toOption,
              gcBetweenRuns = conf.gcBetweenRuns())
        })

        report match {
          case Success(report) =>
            if (conf.uploadUri.isSupplied) {
              println(s"Uploading ${report.filename} to " +
                  s"${conf.uploadUri()}/${report.filename}")

              val hadoopConf = spark.sparkContext.hadoopConfiguration
              val fs = FileSystem.newInstance(new URI(conf.uploadUri()), hadoopConf)
              fs.copyFromLocalFile(
                new Path(report.filename),
                new Path(conf.uploadUri(), report.filename))
            }

          case Failure(e) =>
            System.err.println(e.getMessage)
            System.exit(-1)
        }
      case _ =>
        System.err.println(s"Invalid benchmark name: ${conf.benchmark()}. Supported benchmarks " +
            s"are ${benchmarks.keys.mkString(",")}")
        System.exit(-1)
    }
  }
}

/**
 * This is a wrapper for a specific benchmark suite that provides methods for executing queries
 * and collecting the results, or writing the results to one of the supported output formats.
 *
 * @param bench Benchmark suite (TpcdsLikeBench, TpcxbbLikeBench, or TpchLikeBench).
 */
class BenchmarkRunner(val bench: BenchmarkSuite) {

  /**
   * This method performs a benchmark by executing a query and collecting the results to the
   * driver and can be called from Spark shell using the following syntax:
   *
   * val benchmark = new BenchmarkRunner(TpcdsLikeBench)
   * benchmark.collect(spark, "q5", 3)
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
      gcBetweenRuns: Boolean = false): BenchmarkReport = {
    BenchUtils.collect(
      spark,
      spark => bench.createDataFrame(spark, query),
      query,
      summaryFilePrefix.getOrElse(s"${bench.shortName()}-$query-collect"),
      iterations,
      gcBetweenRuns)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to CSV files
   * and can be called from Spark shell using the following syntax:
   *
   * val benchmark = new BenchmarkRunner(TpcdsLikeBench)
   * benchmark.writeCsv(spark, "q5", 3, "/path/to/write")
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
      gcBetweenRuns: Boolean = false): BenchmarkReport = {
    BenchUtils.writeCsv(
      spark,
      spark => bench.createDataFrame(spark, query),
      query,
      summaryFilePrefix.getOrElse(s"${bench.shortName()}-$query-csv"),
      iterations,
      gcBetweenRuns,
      path,
      mode,
      writeOptions)
  }

  /**
   * This method performs a benchmark of executing a query and writing the results to ORC files
   * and can be called from Spark shell using the following syntax:
   *
   * val benchmark = new BenchmarkRunner(TpcdsLikeBench)
   * benchmark.writeOrc(spark, "q5", 3, "/path/to/write")
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
  def writeOrc(
      spark: SparkSession,
      query: String,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty,
      iterations: Int = 3,
      summaryFilePrefix: Option[String] = None,
      gcBetweenRuns: Boolean = false): BenchmarkReport = {
    BenchUtils.writeOrc(
      spark,
      spark => bench.createDataFrame(spark, query),
      query,
      summaryFilePrefix.getOrElse(s"${bench.shortName()}-$query-csv"),
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
   * val benchmark = new BenchmarkRunner(TpcdsLikeBench)
   * benchmark.writeParquet(spark, "q5", 3, "/path/to/write")
   *
   * @param spark The Spark session
   * @param query The name of the query to run e.g. "q5"
   * @param path The path to write the results to
   * @param mode The SaveMode to use when writing the results
   * @param writeOptions Write options
   * @param iterations The number of times to run the query
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
      gcBetweenRuns: Boolean = false): BenchmarkReport = {
    BenchUtils.writeParquet(
      spark,
      spark => bench.createDataFrame(spark, query),
      query,
      summaryFilePrefix.getOrElse(s"${bench.shortName()}-$query-parquet"),
      iterations,
      gcBetweenRuns,
      path,
      mode,
      writeOptions)
  }
}

class BenchmarkConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val benchmark = opt[String](required = true)
  val input = opt[String](required = true)
  val inputFormat = opt[String](required = true)
  val appendDat = opt[Boolean](required = false, default = Some(false))
  val query = opt[String](required = true)
  val iterations = opt[Int](default = Some(3))
  val output = opt[String](required = false)
  val outputFormat = opt[String](required = false)
  val summaryFilePrefix = opt[String](required = false)
  val gcBetweenRuns = opt[Boolean](required = false, default = Some(false))
  val uploadUri = opt[String](required = false)
  verify()
}
