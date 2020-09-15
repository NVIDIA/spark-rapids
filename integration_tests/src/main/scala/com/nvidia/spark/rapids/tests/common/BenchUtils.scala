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
import org.apache.spark.sql.{DataFrame, SparkSession}

object BenchUtils {

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
   * @param filenameStub The prefix the for the output file. The current timestamp will be appended
   *                     to ensure that filenames are unique and that results are not inadvertently
   *                     overwritten.
   * @param numColdRuns The number of cold runs.
   * @param numHotRuns The number of hot runs.
   */
  def runBench(
      spark: SparkSession,
      createDataFrame: SparkSession => DataFrame,
      resultsAction: Option[DataFrame => Unit],
      queryDescription: String,
      filenameStub: String,
      numColdRuns: Int,
      numHotRuns: Int,
    ): Unit = {

    val queryStartTime = Instant.now()

    val action: DataFrame => Unit = resultsAction.getOrElse(_.collect())

    var df: DataFrame = null
    val coldRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numColdRuns) {
      println(s"*** Start cold run $i:")
      val start = System.nanoTime()
      df = createDataFrame(spark)
      action(df)
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      coldRunElapsed.append(elapsed)
      println(s"*** Cold run $i took $elapsed msec.")
    }

    val hotRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numHotRuns) {
      println(s"*** Start hot run $i:")
      val start = System.nanoTime()
      df = createDataFrame(spark)
      action(df)
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      hotRunElapsed.append(elapsed)
      println(s"*** Hot run $i took $elapsed msec.")
    }

    for (i <- 0 until numColdRuns) {
      println(s"Cold run $i took ${coldRunElapsed(i)} msec.")
    }
    println(s"Average cold run took ${coldRunElapsed.sum.toDouble/numColdRuns} msec.")

    for (i <- 0 until numHotRuns) {
      println(s"Hot run $i took ${hotRunElapsed(i)} msec.")
    }
    println(s"Average hot run took ${hotRunElapsed.sum.toDouble/numHotRuns} msec.")

    // write results to file
    val filename = s"$filenameStub-${queryStartTime.toEpochMilli}.json"
    println(s"Saving benchmark report to $filename")

    // try not to leak secrets
    val redacted = Seq("TOKEN", "SECRET", "PASSWORD")
    val envVars: Map[String, String] = sys.env
        .filterNot(entry => redacted.exists(entry._1.toUpperCase.contains))

    val environment = Environment(
      envVars,
      sparkConf = df.sparkSession.conf.getAll,
      getSparkVersion)

    val queryPlan = QueryPlan(
      df.queryExecution.logical.toString(),
      df.queryExecution.executedPlan.toString()
    )

    val report = BenchmarkReport(
      filename,
      queryStartTime.toEpochMilli,
      environment,
      queryDescription,
      queryPlan,
      coldRunElapsed,
      hotRunElapsed)

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
    query: String,
    queryPlan: QueryPlan,
    coldRun: Seq[Long],
    hotRun: Seq[Long])

/** Details about the query plan */
case class QueryPlan(
    logical: String,
    executedPlan: String)

/** Details about the environment where the benchmark ran */
case class Environment(
    envVars: Map[String, String],
    sparkConf: Map[String, String],
    sparkVersion: String)
