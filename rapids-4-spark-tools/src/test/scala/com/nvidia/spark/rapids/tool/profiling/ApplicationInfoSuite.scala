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

import com.nvidia.spark.rapids.tool.qualification.QualificationTestUtils
import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._

class ApplicationInfoSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  // TODO - move test utils
  private val expRoot = QualificationTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = QualificationTestUtils.getTestResourcePath("spark-events-profiling")

  test("test single event") {
    var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array("src/test/resources/eventlog_minimal_events"))
    val tempFile = File.createTempFile("tempOutputFile", null)
    try {
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
    } finally {
      tempFile.deleteOnExit()
    }
  }

  test("test rapids jar") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array("src/test/resources/spark-events-profiling/rapids_join_eventlog"))
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
    val rapidsJar = apps.head.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
    val cuDFJar = apps.head.classpathEntries.filterKeys(_ matches ".*cudf.*jar")
    assert(rapidsJar.equals("rapids-4-spark_2.12-0.5.0.jar"), "Rapids jar check")
    assert(cuDFJar.equals("cudf-0.19.2-cuda11.jar"), "CUDF jar check")
  }

  test("test printSQLPlanMetrics") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array("src/test/resources/spark-events-profiling/rapids_join_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path)(0), index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps){
      // TODO - test with missing tables
      val accums = app.runQuery(app.generateSQLAccums, fileWriter = None)

      val resultExpectation =
        new File(expRoot, "ProfilingExpectations/rapids_join_eventlog_sqlmetrics_expectation.csv")
      val dfExpect = sparkSession.read.option("header", "true").
        option("nullValue", "-").csv(resultExpectation.getPath)
      val diffCount = accums.except(dfExpect).union(dfExpect.except(dfExpect)).count
      // print for easier debugging
      if (diffCount != 0) {
        logWarning("Diff:")
        dfExpect.show()
        accums.show()
      }
      assert(diffCount == 0)
    }

  }

}
