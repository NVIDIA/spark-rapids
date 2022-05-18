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

package org.apache.spark.sql.rapids.tool.qualification

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._
import org.apache.spark.sql.rapids.tool.{EventProcessorBase, GpuEventLogException, ToolUtils}

class QualificationEventProcessor(app: QualificationAppInfo)
  extends EventProcessorBase[QualificationAppInfo](app) {

  type T = QualificationAppInfo

  override def doSparkListenerEnvironmentUpdate(
      app: QualificationAppInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sparkProperties = event.environmentDetails("Spark Properties").toMap
    if (ToolUtils.isPluginEnabled(sparkProperties)) {
      throw GpuEventLogException(s"Eventlog is from GPU run. Skipping ...")
    }
  }

  override def doSparkListenerApplicationStart(
      app: QualificationAppInfo,
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
      app: QualificationAppInfo,
      event: SparkListenerTaskEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerTaskEnd(app, event)
    // keep all stage task times to see for nonsql duration
    val taskSum = app.stageIdToTaskEndSum.getOrElseUpdate(event.stageId, {
      new StageTaskQualificationSummary(event.stageId, event.stageAttemptId, 0, 0, 0)
    })
    taskSum.executorRunTime += event.taskMetrics.executorRunTime
    taskSum.executorCPUTime += NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime)
    taskSum.totalTaskDuration += event.taskInfo.duration

    // Adds in everything (including failures)
    app.stageIdToSqlID.get(event.stageId).foreach { sqlID =>
      val taskSum = app.sqlIDToTaskEndSum.getOrElseUpdate(sqlID, {
        new StageTaskQualificationSummary(event.stageId, event.stageAttemptId, 0, 0, 0)
      })
      taskSum.executorRunTime += event.taskMetrics.executorRunTime
      taskSum.executorCPUTime += NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime)
      taskSum.totalTaskDuration += event.taskInfo.duration
    }
  }

  override def doSparkListenerSQLExecutionStart(
      app: QualificationAppInfo,
      event: SparkListenerSQLExecutionStart): Unit = {
    super.doSparkListenerSQLExecutionStart(app, event)
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
    app.processSQLPlan(event.executionId, event.sparkPlanInfo)
    app.sqlPlans += (event.executionId -> event.sparkPlanInfo)
  }

  override def doSparkListenerSQLExecutionEnd(
      app: QualificationAppInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    super.doSparkListenerSQLExecutionEnd(app, event)
    logDebug("Processing event: " + event.getClass)
    app.lastSQLEndTime = Some(event.time)
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
    }
  }

  override def doSparkListenerJobStart(
      app: QualificationAppInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobStart(app, event)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    ProfileUtils.stringToLong(sqlIDString).foreach { sqlID =>
      event.stageIds.foreach { stageId =>
        app.stageIdToSqlID.getOrElseUpdate(stageId, sqlID)
      }
      app.jobIdToSqlID(event.jobId) = sqlID
    }
  }

  override def doSparkListenerJobEnd(
      app: QualificationAppInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerJobEnd(app, event)
    app.lastJobEndTime = Some(event.time)
    if (event.jobResult != JobSucceeded) {
      app.jobIdToSqlID.get(event.jobId) match {
        case Some(id) =>
          // zero out the cpu and run times since failed
          app.sqlIDToTaskEndSum.get(id).foreach { sum =>
            sum.executorRunTime = 0
            sum.executorCPUTime = 0
          }
          val failedJobs = app.sqlIDtoJobFailures.getOrElseUpdate(id, ArrayBuffer.empty[Int])
          failedJobs += event.jobId
        case None =>
      }
    }
  }

  override def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: QualificationAppInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    app.sqlPlans += (event.executionId -> event.sparkPlanInfo)
    app.processSQLPlan(event.executionId, event.sparkPlanInfo)
  }
}
