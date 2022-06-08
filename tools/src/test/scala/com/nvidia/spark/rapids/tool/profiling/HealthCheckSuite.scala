/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.scalatest.FunSuite

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

  lazy val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  test("test task-stage-job-failures") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val healthCheck = new HealthCheck(apps)
    for (app <- apps) {
      val failedTasks = healthCheck.getFailedTasks
      // the end reason gets the delimiter changed when writing to CSV so to compare properly
      // change it to be the same here
      val failedWithDelimiter = failedTasks.map { t =>
        val delimited = ProfileUtils.replaceDelimiter(t.endReason, ProfileOutputWriter.CSVDelimiter)
        t.copy(endReason = delimited)
      }
      import sparkSession.implicits._
      val taskAccums = failedWithDelimiter.toDF
      val tasksResultExpectation =
        new File(expRoot, "tasks_failure_eventlog_expectation.csv")
      val tasksDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, tasksResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(taskAccums, tasksDfExpect)

      val failedStages = healthCheck.getFailedStages
      val stageAccums = failedStages.toDF
      val stagesResultExpectation =
        new File(expRoot, "stages_failure_eventlog_expectation.csv")
      val stagesDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, stagesResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(stageAccums, stagesDfExpect)

      val failedJobs = healthCheck.getFailedJobs
      val jobsAccums = failedJobs.toDF
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
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val healthCheck = new HealthCheck(apps)
    for (app <- apps) {
      val removedBMs = healthCheck.getRemovedBlockManager
      import sparkSession.implicits._
      val blockManagersAccums = removedBMs.toDF
      val blockManagersResultExpectation =
        new File(expRoot, "removed_blockManagers_eventlog_expectation.csv")
      val blockManagersDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, blockManagersResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(blockManagersAccums, blockManagersDfExpect)

      val removedExecs = healthCheck.getRemovedExecutors
      val executorRemovedAccums = removedExecs.toDF
      val executorRemovedResultExpectation =
        new File(expRoot, "executors_removed_eventlog_expectation.csv")
      val executorsRemovedDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, executorRemovedResultExpectation.getPath())
      ToolTestUtils.compareDataFrames(executorRemovedAccums, executorsRemovedDfExpect)
    }
  }

  test("test unSupportedSQLPlan") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/dataset_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val healthCheck = new HealthCheck(apps)
    for (app <- apps) {
      val unsupported = healthCheck.getPossibleUnsupportedSQLPlan
      import sparkSession.implicits._
      val unsupportedPlanAccums = unsupported.toDF
      val unSupportedPlanExpectation =
        new File(expRoot, "unsupported_sql_eventlog_expectation.csv")
      val unSupportedPlanDfExpect =
        ToolTestUtils.readExpectationCSV(sparkSession, unSupportedPlanExpectation.getPath())
      ToolTestUtils.compareDataFrames(unsupportedPlanAccums, unSupportedPlanDfExpect)
    }
  }
}
