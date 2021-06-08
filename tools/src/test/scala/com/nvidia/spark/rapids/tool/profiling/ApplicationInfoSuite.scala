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
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
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
        ProfileUtils.stringToPath(path).head._1, index)
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
    assert(apps.head.executors.head.totalCores.equals(8))
    assert(apps.head.resourceProfiles.head.exec_mem.equals(1024L))
  }

  test("test rapids jar") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir//rapids_join_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
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

  test("test sql and resourceprofile eventlog") {
    val eventLog = s"$logDir/rp_sql_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(sparkSession, appArgs)
      assert(exit == 0)
    }
  }

  test("malformed json eventlog") {
    val eventLog = s"$logDir/malformed_json_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(sparkSession, appArgs)
      assert(exit == 1)
    }
  }

  test("test no sql eventlog") {
    val eventLog = s"$logDir/rp_nosql_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(sparkSession, appArgs)
      assert(exit == 0)
    }
  }

  test("test printSQLPlanMetrics") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps) {
      val accums = app.runQuery(app.generateSQLAccums, fileWriter = None)
      val resultExpectation =
        new File(expRoot, "rapids_join_eventlog_sqlmetrics_expectation.csv")
      val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
      ToolTestUtils.compareDataFrames(accums, dfExpect)
    }
  }

  test("test printSQLPlans") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/rapids_join_eventlog"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
          ProfileUtils.stringToPath(path).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps, None)
      collect.printSQLPlans(tempOutputDir.getAbsolutePath)
      val dotDirs = ToolTestUtils.listFilesMatching(tempOutputDir,
        _.startsWith("planDescriptions-"))
      assert(dotDirs.length === 1)
    }
  }

  test("test printJobInfo") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps) {
      val rows = app.runQuery(query = app.jobtoStagesSQL, fileWriter = None).collect()
      assert(rows.size == 2)
      val firstRow = rows.head
      assert(firstRow.getInt(firstRow.schema.fieldIndex("jobID")) === 0)
      assert(firstRow.getList(firstRow.schema.fieldIndex("stageIds")).size == 1)
      assert(firstRow.isNullAt(firstRow.schema.fieldIndex("sqlID")))

      val secondRow = rows(1)
      assert(secondRow.getInt(secondRow.schema.fieldIndex("jobID")) === 1)
      assert(secondRow.getList(secondRow.schema.fieldIndex("stageIds")).size == 4)
      assert(secondRow.getLong(secondRow.schema.fieldIndex("sqlID")) == 0)
    }
  }

  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      "src/test/resources/spark-events-qualification/udf_func_eventlog",
      "src/test/resources/spark-events-qualification/udf_dataset_eventlog",
      "src/test/resources/spark-events-qualification/dataset_eventlog"
    ))

    val result = ToolUtils.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
  }

  test("test filter file newest") {
    val tempFile1 = File.createTempFile("tempOutputFile1", null)
    val tempFile2 = File.createTempFile("tempOutputFile2", null)
    val tempFile3 = File.createTempFile("tempOutputFile3", null)
    val tempFile4 = File.createTempFile("tempOutputFile3", null)
    tempFile1.deleteOnExit()
    tempFile2.deleteOnExit()
    tempFile3.deleteOnExit()

    tempFile1.setLastModified(98765432)  // newest file
    tempFile2.setLastModified(12324567)  // oldest file
    tempFile3.setLastModified(34567891)  // second newest file
    tempFile4.setLastModified(23456789)
    val filterNew = "2-newest"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterNew,
      tempFile1.toString,
      tempFile2.toString,
      tempFile3.toString
    ))

    val result = ToolUtils.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 2)
    // Validate 2 newest files
    assert(result(0).getName.equals(tempFile1.getName))
    assert(result(1).getName.equals(tempFile3.getName))
  }

  test("test filter file oldest and file name match") {

    val tempFile1 = File.createTempFile("tempOutputFile1", null)
    val tempFile2 = File.createTempFile("tempOutputFile2", null)
    val tempFile3 = File.createTempFile("tempOutputFile3", null)
    val tempFile4 = File.createTempFile("tempOutputFile3", null)
    tempFile1.deleteOnExit()
    tempFile2.deleteOnExit()
    tempFile3.deleteOnExit()
    tempFile4.deleteOnExit()

    tempFile1.setLastModified(98765432)  // newest file
    tempFile2.setLastModified(12324567)  // oldest file
    tempFile3.setLastModified(34567891)  // second newest file
    tempFile4.setLastModified(23456789)

    val filterOld = "3-oldest"
    val matchFileName = "temp"
    val appArgs = new ProfileArgs(Array(
      "--filter-criteria",
      filterOld,
      "--match-event-logs",
      matchFileName,
      tempFile1.toString,
      tempFile2.toString,
      tempFile3.toString,
      tempFile4.toString
    ))

    val result = ToolUtils.processAllPaths(appArgs.filterCriteria,
      appArgs.matchEventLogs, appArgs.eventlog())
    assert(result.length == 3)
    // Validate 3 oldest files
    assert(result(0).getName.equals(tempFile2.getName))
    assert(result(1).getName.equals(tempFile4.getName))
    assert(result(2).getName.equals(tempFile3.getName))
  }
}
