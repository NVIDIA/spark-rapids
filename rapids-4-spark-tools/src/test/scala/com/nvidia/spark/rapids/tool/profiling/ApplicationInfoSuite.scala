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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.rapids.tool.profiling._

class ApplicationInfoSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test single event") {
    var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_minimal_events"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path)(0), index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("3.1.1"))
    assert(apps.head.gpuMode.equals(true))
    assert(apps.head.jobStart(apps.head.index).jobID.equals(1))
    assert(apps.head.stageSubmitted(apps.head.index).numTasks.equals(1))
    assert(apps.head.stageSubmitted(2).stageId.equals(2))
    assert(apps.head.taskEnd(apps.head.index).successful.equals(true))
    assert(apps.head.taskEnd(apps.head.index).endReason.equals("Success"))
  }

  test("test rapids jar") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir//rapids_join_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path)(0), index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("3.0.1"))
    assert(apps.head.gpuMode.equals(true))
    val rapidsJar =
      apps.head.classpathEntries.filterKeys(_ matches ".*rapids-4-spark_2.12-0.5.0.jar.*")
    val cuDFJar = apps.head.classpathEntries.filterKeys(_ matches ".*cudf-0.19.2-cuda11.jar.*")
    assert(rapidsJar.size == 1, "Rapids jar check")
    assert(cuDFJar.size == 1, "CUDF jar check")
  }

  test("test printSQLPlanMetrics") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path)(0), index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps){
      val accums = app.runQuery(app.generateSQLAccums, fileWriter = None)
      val resultExpectation =
        new File(expRoot, "rapids_join_eventlog_sqlmetrics_expectation.csv")
      val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
      ToolTestUtils.compareDataFrames(accums, dfExpect)
    }
  }

}
