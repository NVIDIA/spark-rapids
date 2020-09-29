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

import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.collection.mutable.ListBuffer

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.writePretty

import org.apache.spark.{SPARK_BUILD_USER, SPARK_VERSION}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

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
   * @param spark           The Spark session
   * @param createDataFrame Function to create a DataFrame from the Spark session.
   * @param resultsAction   Optional action to perform after creating the DataFrame, with default
   *                        behavior of calling df.collect() but user could provide function to
   *                        save results to CSV or Parquet instead.
   * @param filenameStub    The prefix for the output file. The current timestamp will be appended
   *                        to ensure that filenames are unique and that results are not
   *                        inadvertently overwritten.
   * @param iterations      The number of times to run the query.
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

    assert(iterations > 0)

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
          s"${hotRuns.sum.toDouble / numHotRuns} msec.")
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
   * The intended usage is to run timed benchmarks that write results to file and then separately
   * use this utility to compare those result sets. This code performs a sort and a collect and
   * is only suitable for data sets that can fit in the driver's memory. For larger datasets,
   * a better approach would be to convert the results to single files, download them locally
   * and adapt this Scala code to read those files directly (without using Spark).
   *
   * Example usage:
   *
   * <pre>
   * scala> val cpu = spark.read.parquet("/data/q5-cpu")
   * scala> val gpu = spark.read.parquet("/data/q5-gpu")
   * scala> import com.nvidia.spark.rapids.tests.common._
   * scala> BenchUtils.compareResults(cpu, gpu, ignoreOrdering=true, epsilon=0.0)
   * Collecting rows from DataFrame
   * Collected 989754 rows in 7.701 seconds
   * Collecting rows from DataFrame
   * Collected 989754 rows in 2.325 seconds
   * Results match
   * </pre>
   *
   * @param df1            DataFrame to compare.
   * @param df2            DataFrame to compare.
   * @param ignoreOrdering Sort the data collected from the DataFrames before comparing them.
   * @param useIterator    When set to true, use `toLocalIterator` to load one partition at a time
   *                       into driver memory, reducing memory usage at the cost of performance
   *                       because processing will be single-threaded.
   * @param maxErrors      Maximum number of differences to report.
   * @param epsilon        Allow for differences in precision when comparing floating point values.
   */
  def compareResults(
      df1: DataFrame,
      df2: DataFrame,
      ignoreOrdering: Boolean,
      useIterator: Boolean = false,
      maxErrors: Int = 10,
      epsilon: Double = 0.00001): Unit = {

    val count1 = df1.count()
    val count2 = df2.count()

    if (count1 == count2) {
      println(s"Both DataFrames contain $count1 rows")
      val result1 = collectResults(df1, ignoreOrdering, useIterator)
      val result2 = collectResults(df2, ignoreOrdering, useIterator)

      var errors = 0
      var i = 0
      while (i < count1 && errors < maxErrors) {
        val l = result1.next()
        val r = result2.next()
        if (!rowEqual(l, r, epsilon)) {
          println(s"Row $i:\n${l.mkString(",")}\n${r.mkString(",")}\n")
          errors += 1
        }
        i += 1
      }
      println(s"Processed $i rows")

      if (errors == maxErrors) {
        println(s"Aborting comparison after reaching maximum of $maxErrors errors")
      } else if (errors == 0) {
        println(s"Results match")
      } else {
        println(s"There were $errors errors")
      }
    } else {
      println(s"DataFrame row counts do not match: $count1 != $count2")
    }
  }

  private def collectResults(
      df: DataFrame,
      ignoreOrdering: Boolean,
      useIterator: Boolean): Iterator[Seq[Any]] = {

    // apply sorting if specified
    val resultDf = if (ignoreOrdering) {
      // let Spark do the sorting
      df.sort(df.columns.map(col): _*)
    } else {
      df
    }

    val it: Iterator[Row] = if (useIterator) {
      resultDf.toLocalIterator()
    } else {
      println("Collecting rows from DataFrame")
      val t1 = System.currentTimeMillis()
      val rows = resultDf.collect()
      val t2 = System.currentTimeMillis()
      println(s"Collected ${rows.length} rows in ${(t2-t1)/1000.0} seconds")
      rows.toIterator
    }

    // map Iterator[Row] to Iterator[Seq[Any]]
    it.map(_.toSeq)
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
