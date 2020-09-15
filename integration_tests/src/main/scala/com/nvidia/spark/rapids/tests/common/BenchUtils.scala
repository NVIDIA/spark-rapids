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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BenchUtils {

  /**
   * Run the specified number of cold and hot runs and record the timings and summary of the
   * query and results to file, including all Spark configuration options and environment
   * variables.
   */
  def runBench(
      spark: SparkSession,
      queryRunner: SparkSession => DataFrame,
      queryDescription: String,
      filenameStub: String,
      numColdRuns: Int = 1,
      numHotRuns: Int = 3,
      maxResultsToRecord: Int = 100): Unit = {

    val now = Instant.now()

    var df: DataFrame = null
    var results: Seq[Row] = null
    val coldRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numColdRuns) {
      println(s"*** Start cold run $i:")
      val start = System.nanoTime()
      df = queryRunner(spark)
      results = df.collect
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      coldRunElapsed.append(elapsed)
      println(s"*** Cold run $i took $elapsed msec.")
    }

    val hotRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numHotRuns) {
      println(s"*** Start hot run $i:")
      val start = System.nanoTime()
      df = queryRunner(spark)
      results = df.collect
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
    val filename = s"$filenameStub-${now.toEpochMilli}.json"
    println(s"Saving results to $filename")

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

    // record the first N rows for later verification
    val resultSummary = ResultSummary(
      results.length,
      maxResultsToRecord,
      // note that this sorting isn't entirely deterministic due to rounding differences between
      // CPU and GPU but it works well enough for the TPC-DS and TPCxBB benchmarks
      results.sortBy(_.mkString(",")).take(maxResultsToRecord).map(row => row.toSeq)
    )

    val report = BenchmarkReport(
      filename,
      now.toEpochMilli,
      environment,
      queryDescription,
      queryPlan,
      resultSummary,
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
    results: ResultSummary,
    coldRun: Seq[Long],
    hotRun: Seq[Long])

/** Summary about the data returned by the query, including first N rows */
case class ResultSummary(
    rowCount: Long,
    partialResultLimit: Int,
    partialResults: Seq[Seq[Any]])

/** Details about the query plan */
case class QueryPlan(
    logical: String,
    executedPlan: String)

/** Details about the environment where the benchmark ran */
case class Environment(
    envVars: Map[String, String],
    sparkConf: Map[String, String],
    sparkVersion: String)
