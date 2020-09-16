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

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter}
import java.time.Instant
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.github.difflib.DiffUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.writePretty

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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

  /**
   * Perform a diff of the results collected from two DataFrames, allowing for differences in
   * precision.
   *
   * This is only suitable for data sets that can fit in the driver's memory.

   * @param df1 DataFrame to compare.
   * @param df2 DataFrame to compare.
   * @param ignoreOrdering Sort the data collected from the DataFrames before comparing them.
   * @param epsilon Allow for differences in precision when comparing floating point values.
   */
  def compareResults(
      df1: DataFrame,
      df2: DataFrame,
      ignoreOrdering: Boolean,
      epsilon: Double = 0.00001): Unit = {

    val result1: Seq[Seq[Any]] = collectResults(df1, ignoreOrdering)
    val result2: Seq[Seq[Any]] = collectResults(df2, ignoreOrdering)

    val allMatch = result1.length == result2.length && result1.zip(result2).forall {
      case (l, r) => rowEqual(l, r, epsilon)
    }

    if (allMatch) {
      println("Results match (allowing for differences in precision).")
    } else {
      val filename = s"diff-${System.currentTimeMillis()}.txt"
      println(s"Results do not match. Writing diff to $filename")

      val w = new BufferedWriter(new FileWriter(filename))

      val deltas = DiffUtils.diff(
        result1.map(_.mkString("\t")).asJava,
        result2.map(_.mkString("\t")).asJava,
        false).getDeltas.asScala

      deltas.foreach { delta =>
        w.write(delta.getType.toString + "\n")
        w.write(delta.getSource.toString + "\n")
        w.write(delta.getTarget.toString + "\n")
      }

      w.close()
    }

  }

  private def collectResults(df: DataFrame, ignoreOrdering: Boolean): Seq[Seq[Any]] = {
    val results = df.collect().map(_.toSeq)
    if (ignoreOrdering) {
      // note that CPU and GPU results could, at least theoretically, end up being sorted
      // differently because precision of floating-point values can vary but this seems to work
      // well enough so far when comparing output from the TPC-* queries
      results.sortBy(_.mkString(","))
    } else {
      results
    }
  }

  private def rowEqual(row1: Seq[Any], row2: Seq[Any], epsilon: Double): Boolean = {
    row1.zip(row2).forall {
      case (l, r) => compare(l, r, epsilon)
    }
  }

  // this is copied from SparkQueryCompareTestSuite
  private def compare(expected: Any, actual: Any, epsilon: Double = 0.0): Boolean = {
    def doublesAreEqualWithinPercentage(expected: Double, actual: Double): (String, Boolean) = {
      if (!compare(expected, actual)) {
        if (expected != 0) {
          val v = Math.abs((expected - actual) / expected)
          (s"\n\nABS($expected - $actual) / ABS($actual) == $v is not <= $epsilon ",  v <= epsilon)
        } else {
          val v = Math.abs(expected - actual)
          (s"\n\nABS($expected - $actual) == $v is not <= $epsilon ",  v <= epsilon)
        }
      } else {
        ("SUCCESS", true)
      }
    }
    (expected, actual) match {
      case (a: Float, b: Float) if a.isNaN && b.isNaN => true
      case (a: Double, b: Double) if a.isNaN && b.isNaN => true
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r, epsilon) }
      case (a: Map[_, _], b: Map[_, _]) =>
        a.size == b.size && a.keys.forall { aKey =>
          b.keys.find(bKey => compare(aKey, bKey))
              .exists(bKey => compare(a(aKey), b(bKey)))
        }
      case (a: Iterable[_], b: Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r, epsilon) }
      case (a: Product, b: Product) =>
        compare(a.productIterator.toSeq, b.productIterator.toSeq, epsilon)
      case (a: Row, b: Row) =>
        compare(a.toSeq, b.toSeq, epsilon)
      // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
      case (a: Double, b: Double) if epsilon <= 0 =>
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      case (a: Double, b: Double) if epsilon > 0 =>
        val ret = doublesAreEqualWithinPercentage(a, b)
        if (!ret._2) {
          System.err.println(ret._1 + " (double)")
        }
        ret._2
      case (a: Float, b: Float) if epsilon <= 0 =>
        java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
      case (a: Float, b: Float) if epsilon > 0 =>
        val ret = doublesAreEqualWithinPercentage(a, b)
        if (!ret._2) {
          System.err.println(ret._1 + " (float)")
        }
        ret._2
      case (a, b) => a == b
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
