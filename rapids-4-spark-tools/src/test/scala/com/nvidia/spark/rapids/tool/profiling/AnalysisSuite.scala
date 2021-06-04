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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class AnalysisSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test jobAndStageMetricsAggregation simple") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.jobAndStageMetricsAggregation()
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_jobandstagemetrics_expectation.csv")
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test sqlMetricsAggregation simple") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.sqlMetricsAggregation()
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_sqlmetricsagg_expectation.csv")
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }

  test("test shuffleSkewCheck simple") {
    val apps =
      ToolTestUtils.processProfileApps(Array(s"$logDir/rapids_join_eventlog"), sparkSession)
    assert(apps.size == 1)

    val analysis = new Analysis(apps, None)
    val actualDf = analysis.sqlMetricsAggregation()
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_shuffleskewcheck_expectation.csv")
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(actualDf, dfExpect)
  }
}
