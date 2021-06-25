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

package org.apache.spark.sql.rapids.tool.qualification

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.rapids.tool.{EventProcessorBase, ToolUtils}

class QualEventProcessor() extends EventProcessorBase {

  type T = QualAppInfo

  override def doSparkListenerEnvironmentUpdate(
      app: QualAppInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sparkProperties = event.environmentDetails("Spark Properties").toMap
    if (ToolUtils.isPluginEnabled(sparkProperties)) {
      app.isPluginEnabled = true
    }
  }

  override def doSparkListenerApplicationStart(
      app: QualAppInfo,
      event: SparkListenerApplicationStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisAppInfo = QualApplicationInfo(
      event.appName,
      event.appId,
      event.time,
      event.sparkUser,
      None,
      None,
      endDurationEstimated = false
    )
    app.appInfo = Some(thisAppInfo)
    app.appId = event.appId.getOrElse("")
  }

  override def doSparkListenerTaskEnd(
      app: QualAppInfo,
      event: SparkListenerTaskEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    // Adds in everything (including failures)
    app.stageIdToSqlID.get(event.stageId).foreach { sqlID =>
      val taskSum = app.sqlIDToTaskEndSum.getOrElseUpdate(sqlID, {
        new StageTaskQualificationSummary(event.stageId, event.stageAttemptId, 0, 0)
      })
      taskSum.executorRunTime += event.taskMetrics.executorRunTime
      taskSum.executorCPUTime += NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime)
    }
  }

  override def doSparkListenerSQLExecutionStart(
      app: QualAppInfo,
      event: SparkListenerSQLExecutionStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlExecution = QualSQLExecutionInfo(
      event.executionId,
      event.time,
      None,
      None,
      "",
      None,
      false,
      ""
    )
    app.sqlStart += (event.executionId -> sqlExecution)
    // app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.processSQLPlan(event.executionId, event.sparkPlanInfo)
    // -1 to indicate that it started but not complete
    app.sqlDurationTime += (event.executionId -> -1)
  }

  override def doSparkListenerSQLExecutionEnd(
      app: QualAppInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.lastSQLEndTime = Some(event.time)
    // app.sqlEndTime += (event.executionId -> event.time)
    val sqlInfo = app.sqlStart.get(event.executionId)
    // only include duration if it contains no jobs that failed
    val failedJobs = app.sqlIDtoJobFailures.get(event.executionId)
    if (event.executionFailure.isDefined || failedJobs.isDefined) {
      logWarning(s"SQL execution id ${event.executionId} had failures, skipping")
      // zero out the cpu and run times since failed
      app.sqlIDToTaskEndSum.get(event.executionId).foreach { sum =>
        sum.executorRunTime = 0
        sum.executorCPUTime = 0
      }
      app.sqlDurationTime += (event.executionId -> 0)
    } else {
      // if start time not there, use 0 for duration
      val startTime = sqlInfo.map(_.startTime).getOrElse(0L)
      val sqlDuration = event.time - startTime
      app.sqlDurationTime += (event.executionId -> sqlDuration)
    }
  }

  override def doSparkListenerJobStart(
      app: QualAppInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    ProfileUtils.stringToLong(sqlIDString).foreach { sqlID =>
      event.stageIds.foreach { stageId =>
        app.stageIdToSqlID.getOrElseUpdate(stageId, sqlID)
      }
      app.jobIdToSqlID(event.jobId) = sqlID
    }

  }

  override def doSparkListenerJobEnd(
      app: QualAppInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.lastJobEndTime = Some(event.time)
    // TODO - verify job failures show up in sql failures
    // do we want to track separately for any failures?
    if (event.jobResult != JobSucceeded) {
      val sqlID = app.jobIdToSqlID(event.jobId)
      logWarning(s"job failed: ${event.jobId}")
      // zero out the cpu and run times since failed
      app.sqlIDToTaskEndSum.get(sqlID).foreach { sum =>
        sum.executorRunTime = 0
        sum.executorCPUTime = 0
      }
      val failedJobs = app.sqlIDtoJobFailures.getOrElseUpdate(sqlID, ArrayBuffer.empty[Int])
      failedJobs += event.jobId
    }
  }

  override def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: QualAppInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    // app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.processSQLPlan(event.executionId, event.sparkPlanInfo)
  }
}
