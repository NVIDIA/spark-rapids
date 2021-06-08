/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.apache.hadoop.io.IOUtils
import org.scalatest.FunSuite

import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.types._

class AnalysisSuite extends FunSuite {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test sqlMetricsAggregation simple") {
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog"),
      "rapids_join_eventlog_sqlmetricsagg_expectation.csv",
      "rapids_join_eventlog_jobandstagemetrics_expectation.csv")
  }

  test("test sqlMetricsAggregation second single app") {
    testSqlMetricsAggregation(Array(s"$logDir/rapids_join_eventlog2"),
      "rapids_join_eventlog_sqlmetricsagg2_expectation.csv",
      "rapids_join_eventlog_jobandstagemetrics2_expectation.csv")
  }

  test("test sqlMetricsAggregation 2 combined") {
    testSqlMetricsAggregation(
      Array(s"$logDir/rapids_join_eventlog", s"$logDir/rapids_join_eventlog2"),
      "rapids_join_eventlog_sqlmetricsaggmulti_expectation.csv",
      "rapids_join_eventlog_jobandstagemetricsmulti_expectation.csv")
  }

  private def testSqlMetricsAggregation(logs: Array[String], expectFile: String,
      expectFileJS: String): Unit = {
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == logs.size)
    val analysis = new Analysis(apps, None)

    val actualDf = analysis.sqlMetricsAggregation()
    val resultExpectation = new File(expRoot,expectFile)
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)

    val actualDfJS = analysis.jobAndStageMetricsAggregation()
    val resultExpectationJS = new File(expRoot, expectFileJS)
    val dfExpectJS = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectationJS.getPath())
    ToolTestUtils.compareDataFrames(actualDfJS, dfExpectJS)
  }

  test("test sqlMetrics duration and execute cpu time") {
    testSqlMetricsDurationAndCpuTime()
  }

  test("zstd: test sqlMetrics duration and execute cpu time") {
    testSqlMetricsDurationAndCpuTime(Option("zstd"))
  }

  test("snappy: test sqlMetrics duration and execute cpu time") {
    testSqlMetricsDurationAndCpuTime(Option("snappy"))
  }

  test("lzf: test sqlMetrics duration and execute cpu time") {
    testSqlMetricsDurationAndCpuTime(Option("lz4"))
  }

  test("lz4: test sqlMetrics duration and execute cpu time") {
    testSqlMetricsDurationAndCpuTime(Option("lzf"))
  }

  private def testSqlMetricsDurationAndCpuTime(compressionNameOpt: Option[String] = None) = {
    val rawLog = s"$logDir/rp_sql_eventlog"
    compressionNameOpt.foreach { compressionName =>
      val codec = TrampolineUtil.createCodec(sparkSession.sparkContext.getConf,
        compressionName)
      TrampolineUtil.withTempDir { tempDir =>
        // copy and close streams
        IOUtils.copyBytes(Files.newInputStream(Paths.get(rawLog)),
          codec.compressedOutputStream(Files.newOutputStream(new File(tempDir,
            "rp_sql_eventlog." + compressionName).toPath, StandardOpenOption.CREATE)),
          4096, true)
        runTestSqlMetricsDurationAndCpuTime(Array(tempDir.toString))
      }
    }

    if (compressionNameOpt.isEmpty) {
      runTestSqlMetricsDurationAndCpuTime(Array(rawLog))
    }
  }

  private def runTestSqlMetricsDurationAndCpuTime(logs: Array[String]) = {
    val expectFile = "rapids_duration_and_cpu_expectation.csv"

    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    val analysis = new Analysis(apps, None)
    val sqlAggMetricsDF = analysis.sqlMetricsAggregation()
    sqlAggMetricsDF.createOrReplaceTempView("sqlAggMetricsDF")
    val actualDf = analysis.sqlMetricsAggregationDurationAndCpuTime()
    val resultExpectation = new File(expRoot, expectFile)
    val schema = new StructType()
      .add("appIndex",IntegerType,true)
      .add("appID",StringType,true)
      .add("sqlID",LongType,true)
      .add("sqlDuration",LongType,true)
      .add("containsDataset",BooleanType,true)
      .add("appDuration",LongType,true)
      .add("problematic",StringType,true)
      .add("executorCpuTime",DoubleType,true)

    val dfExpect = sparkSession.read.option("header", "true").option("nullValue", "-")
      .schema(schema).csv(resultExpectation.getPath())

    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test shuffleSkewCheck empty") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.shuffleSkewCheckSingleApp(apps.head)
    assert(actualDf.count() == 0)
  }
}
