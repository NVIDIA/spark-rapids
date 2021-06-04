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

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession

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

  test("test shuffleSkewCheck empty") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.shuffleSkewCheckSingleApp(apps.head)
    assert(actualDf.count() == 0)
  }
}
