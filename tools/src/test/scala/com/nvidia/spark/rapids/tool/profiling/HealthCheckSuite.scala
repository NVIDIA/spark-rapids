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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling._

class HealthCheckSuite extends FunSuite {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test task-stage-job-failures") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/task_job_failure_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps) {
      val taskAccums = app.runQuery(app.getFailedTasks, fileWriter = None)
      val tasksResultExpectation =
        new File(expRoot, "tasks_failure_eventlog_expectation.csv")
      val tasksDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, tasksResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(taskAccums, tasksDfExpect)

      val stageAccums = app.runQuery(app.getFailedStages, fileWriter = None)
      val stagesResultExpectation =
        new File(expRoot, "stages_failure_eventlog_expectation.csv")
      val stagesDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, stagesResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(stageAccums, stagesDfExpect)

      val jobsAccums = app.runQuery(app.getFailedJobs, fileWriter = None)
      val jobsResultExpectation =
        new File(expRoot, "jobs_failure_eventlog_expectation.csv")
      val jobsDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, jobsResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(jobsAccums, jobsDfExpect)
    }
  }

  test("test blockManager_executors_failures") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/executors_removed_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    for (app <- apps) {
      val blockManagersAccums = app.runQuery(app.getblockManagersRemoved, fileWriter = None)
      val blockManagersResultExpectation =
        new File(expRoot, "removed_blockManagers_eventlog_expectation.csv")
      val blockManagersDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, blockManagersResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(blockManagersAccums, blockManagersDfExpect)


      val executorRemovedAccums = app.runQuery(app.getExecutorsRemoved, fileWriter = None)
      val executorRemovedResultExpectation =
        new File(expRoot, "executors_removed_eventlog_expectation.csv")
      val executorsRemovedDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, executorRemovedResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(executorRemovedAccums, executorsRemovedDfExpect)
    }
  }

  test("test unSupportedSQLPlan") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val qualificationlogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val appArgs =
      new ProfileArgs(Array(s"$qualificationlogDir/dataset_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        ProfileUtils.stringToPath(path).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps) {
      val unsupportedPlanAccums = app.runQuery(app.unsupportedSQLPlan, fileWriter = None)
      val unSupportedPlanExpectation =
        new File(expRoot, "unsupported_sql_eventlog_expectation.csv")
      val unSupportedPlanDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, unSupportedPlanExpectation.getPath())
      ToolTestUtils.compareDataFrames(unsupportedPlanAccums, unSupportedPlanDfExpect)
    }
  }
}
