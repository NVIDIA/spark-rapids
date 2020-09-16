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
package com.nvidia.spark.rapids.tests.common

import java.io.{File, FileOutputStream}
import java.time.Instant
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.writePretty

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BenchUtils {

  /** Perform benchmark of calling collect */
  def collect(
      spark: SparkSession,
      createDataFrame: SparkSession => DataFrame,
      queryDescription: String,
      filenameStub: String,
      iterations: Int,
      gcBetweenRuns: Boolean
  ): Unit = {
    runBench(
      spark,
      createDataFrame,
      Collect(),
      queryDescription,
      filenameStub,
      iterations,
      gcBetweenRuns)
  }

  /** Perform benchmark of writing results to CSV */
  def writeCsv(
      spark: SparkSession,
      createDataFrame: SparkSession => DataFrame,
      queryDescription: String,
      filenameStub: String,
      iterations: Int,
      gcBetweenRuns: Boolean,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty): Unit = {
    runBench(
      spark,
      createDataFrame,
      WriteParquet(path, mode, writeOptions),
      queryDescription,
      filenameStub,
      iterations,
      gcBetweenRuns)
  }

  /** Perform benchmark of writing results to Parquet */
  def writeParquet(
      spark: SparkSession,
      createDataFrame: SparkSession => DataFrame,
      queryDescription: String,
      filenameStub: String,
      iterations: Int,
      gcBetweenRuns: Boolean,
      path: String,
      mode: SaveMode = SaveMode.Overwrite,
      writeOptions: Map[String, String] = Map.empty): Unit = {
    runBench(
      spark,
      createDataFrame,
      WriteParquet(path, mode, writeOptions),
      queryDescription,
      filenameStub,
      iterations,
      gcBetweenRuns)
  }

  /**
   * Run the specified number of cold and hot runs and record the timings and summary of the
   * query and results to file, including all Spark configuration options and environment
   * variables.
   *
   * @param spark The Spark session
   * @param createDataFrame Function to create a DataFrame from the Spark session.
   * @param resultsAction Optional action to perform after creating the DataFrame, with default
   *                      behavior of calling df.collect() but user could provide function to
   *                      save results to CSV or Parquet instead.
   * @param filenameStub The prefix for the output file. The current timestamp will be appended
   *                     to ensure that filenames are unique and that results are not inadvertently
   *                     overwritten.
   * @param iterations The number of times to run the query.
   */
  def runBench(
      spark: SparkSession,
      createDataFrame: SparkSession => DataFrame,
      resultsAction: ResultsAction,
      queryDescription: String,
      filenameStub: String,
      iterations: Int,
      gcBetweenRuns: Boolean
    ): Unit = {

    assert(iterations>0)

    val queryStartTime = Instant.now()

    var df: DataFrame = null
    val queryTimes = new ListBuffer[Long]()
    for (i <- 0 until iterations) {
      println(s"*** Start iteration $i:")
      val start = System.nanoTime()
      df = createDataFrame(spark)

      resultsAction match {
        case Collect() => df.collect()
        case WriteCsv(path, mode, options) =>
          df.write.mode(mode).options(options).csv(path)
        case WriteParquet(path, mode, options) =>
          df.write.mode(mode).options(options).parquet(path)
      }

      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      queryTimes.append(elapsed)
      println(s"*** Iteration $i took $elapsed msec.")

      // cause Spark to call unregisterShuffle
      if (gcBetweenRuns) {
        System.gc()
        System.gc()
      }
    }

    // summarize all query times
    for (i <- 0 until iterations) {
      println(s"Iteration $i took ${queryTimes(i)} msec.")
    }

    // for multiple runs, summarize cold/hot timings
    if (iterations > 1) {
      println(s"Cold run: ${queryTimes(0)} msec.")
      val hotRuns = queryTimes.drop(1)
      val numHotRuns = hotRuns.length
      println(s"Best of $numHotRuns hot run(s): ${hotRuns.min} msec.")
      println(s"Worst of $numHotRuns hot run(s): ${hotRuns.max} msec.")
      println(s"Average of $numHotRuns hot run(s): " +
          s"${hotRuns.sum.toDouble/numHotRuns} msec.")
    }

    // write results to file
    val filename = s"$filenameStub-${queryStartTime.toEpochMilli}.json"
    println(s"Saving benchmark report to $filename")

    // try not to leak secrets
    val redacted = Seq("TOKEN", "SECRET", "PASSWORD")
    val envVars: Map[String, String] = sys.env
        .filterNot(entry => redacted.exists(entry._1.toUpperCase.contains))

    val testConfiguration = TestConfiguration(
      gcBetweenRuns
    )

    val environment = Environment(
      envVars,
      sparkConf = df.sparkSession.conf.getAll,
      getSparkVersion)

    val queryPlan = QueryPlan(
      df.queryExecution.logical.toString(),
      df.queryExecution.executedPlan.toString()
    )

    val report = resultsAction match {
      case Collect() => BenchmarkReport(
        filename,
        queryStartTime.toEpochMilli,
        environment,
        testConfiguration,
        "collect",
        Map.empty,
        queryDescription,
        queryPlan,
        queryTimes)

      case w: WriteCsv => BenchmarkReport(
        filename,
        queryStartTime.toEpochMilli,
        environment,
        testConfiguration,
        "csv",
        w.writeOptions,
        queryDescription,
        queryPlan,
        queryTimes)

      case w: WriteParquet => BenchmarkReport(
        filename,
        queryStartTime.toEpochMilli,
        environment,
        testConfiguration,
        "parquet",
        w.writeOptions,
        queryDescription,
        queryPlan,
        queryTimes)
    }

    writeReport(report, filename)
  }

  def readReport(file: File): BenchmarkReport = {
    implicit val formats = DefaultFormats
    val json = parse(file)
    json.extract[BenchmarkReport]
  }

  def writeReport(report: BenchmarkReport, filename: String): Unit = {
    implicit val formats = DefaultFormats
    val os = new FileOutputStream(filename)
    os.write(writePretty(report).getBytes)
    os.close()
  }

  def getSparkVersion: String = {
    // hack for databricks, try to find something more reliable?
    if (SPARK_BUILD_USER.equals("Databricks")) {
      SPARK_VERSION + "-databricks"
    } else {
      SPARK_VERSION
    }
  }

}

/** Top level benchmark report class */
case class BenchmarkReport(
    filename: String,
    startTime: Long,
    env: Environment,
    testConfiguration: TestConfiguration,
    action: String,
    writeOptions: Map[String, String],
    query: String,
    queryPlan: QueryPlan,
    queryTimes: Seq[Long])

/** Configuration options that affect how the tests are run */
case class TestConfiguration(
    gcBetweenRuns: Boolean
)

/** Details about the query plan */
case class QueryPlan(
    logical: String,
    executedPlan: String)

/** Details about the environment where the benchmark ran */
case class Environment(
    envVars: Map[String, String],
    sparkConf: Map[String, String],
    sparkVersion: String)

sealed trait ResultsAction

case class Collect() extends ResultsAction

case class WriteCsv(
    path: String,
    mode: SaveMode,
    writeOptions: Map[String, String]) extends ResultsAction

case class WriteParquet(
    path: String,
    mode: SaveMode,
    writeOptions: Map[String, String]) extends ResultsAction
