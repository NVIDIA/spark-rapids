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

package org.apache.spark.sql.rapids.tool

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling.{DriverAccumCase, JobInfoClass, ProfileUtils, SQLExecutionInfoClass, TaskStageAccumCase}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

abstract class EventProcessorBase[T <: AppBase](app: T) extends SparkListener with Logging {

  def processAnyEvent(event: SparkListenerEvent): Unit = {
    event match {
      case _: SparkListenerLogStart =>
        doSparkListenerLogStart(app, event.asInstanceOf[SparkListenerLogStart])
      case _: SparkListenerBlockManagerAdded =>
        doSparkListenerBlockManagerAdded(app,
          event.asInstanceOf[SparkListenerBlockManagerAdded])
      case _: SparkListenerBlockManagerRemoved =>
        doSparkListenerBlockManagerRemoved(app,
          event.asInstanceOf[SparkListenerBlockManagerRemoved])
      case _: SparkListenerEnvironmentUpdate =>
        doSparkListenerEnvironmentUpdate(app,
          event.asInstanceOf[SparkListenerEnvironmentUpdate])
      case _: SparkListenerApplicationStart =>
        doSparkListenerApplicationStart(app,
          event.asInstanceOf[SparkListenerApplicationStart])
      case _: SparkListenerApplicationEnd =>
        doSparkListenerApplicationEnd(app,
          event.asInstanceOf[SparkListenerApplicationEnd])
      case _: SparkListenerExecutorAdded =>
        doSparkListenerExecutorAdded(app,
          event.asInstanceOf[SparkListenerExecutorAdded])
      case _: SparkListenerExecutorRemoved =>
        doSparkListenerExecutorRemoved(app,
          event.asInstanceOf[SparkListenerExecutorRemoved])
      case _: SparkListenerTaskStart =>
        doSparkListenerTaskStart(app,
          event.asInstanceOf[SparkListenerTaskStart])
      case _: SparkListenerTaskEnd =>
        doSparkListenerTaskEnd(app,
          event.asInstanceOf[SparkListenerTaskEnd])
      case _: SparkListenerSQLExecutionStart =>
        doSparkListenerSQLExecutionStart(app,
          event.asInstanceOf[SparkListenerSQLExecutionStart])
      case _: SparkListenerSQLExecutionEnd =>
        doSparkListenerSQLExecutionEnd(app,
          event.asInstanceOf[SparkListenerSQLExecutionEnd])
      case _: SparkListenerDriverAccumUpdates =>
        doSparkListenerDriverAccumUpdates(app,
          event.asInstanceOf[SparkListenerDriverAccumUpdates])
      case _: SparkListenerJobStart =>
        doSparkListenerJobStart(app,
          event.asInstanceOf[SparkListenerJobStart])
      case _: SparkListenerJobEnd =>
        doSparkListenerJobEnd(app,
          event.asInstanceOf[SparkListenerJobEnd])
      case _: SparkListenerStageSubmitted =>
        doSparkListenerStageSubmitted(app,
          event.asInstanceOf[SparkListenerStageSubmitted])
      case _: SparkListenerStageCompleted =>
        doSparkListenerStageCompleted(app,
          event.asInstanceOf[SparkListenerStageCompleted])
      case _: SparkListenerTaskGettingResult =>
        doSparkListenerTaskGettingResult(app,
          event.asInstanceOf[SparkListenerTaskGettingResult])
      case _: SparkListenerSQLAdaptiveExecutionUpdate =>
        doSparkListenerSQLAdaptiveExecutionUpdate(app,
          event.asInstanceOf[SparkListenerSQLAdaptiveExecutionUpdate])
      case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
        doSparkListenerSQLAdaptiveSQLMetricUpdates(app,
          event.asInstanceOf[SparkListenerSQLAdaptiveSQLMetricUpdates])
      case _ =>
        val wasResourceProfileAddedEvent = doSparkListenerResourceProfileAddedReflect(app, event)
        if (!wasResourceProfileAddedEvent) doOtherEvent(app, event)
    }
  }

  def doSparkListenerResourceProfileAddedReflect(
      app: T,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case rpAdded: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app, rpAdded)
            true
          case _ => false
        }
      } catch {
        case _: ClassNotFoundException =>
          logWarning("Error trying to parse SparkListenerResourceProfileAdded, Spark" +
            " version likely older than 3.1.X, unable to parse it properly.")
          false
      }
    } else {
      false
    }
  }

  def doSparkListenerLogStart(
      app: T,
      event: SparkListenerLogStart): Unit  = {
    app.sparkVersion = event.sparkVersion
  }

  def doSparkListenerSQLExecutionStart(
      app: T,
      event: SparkListenerSQLExecutionStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlExecution = new SQLExecutionInfoClass(
      event.executionId,
      event.description,
      event.details,
      event.time,
      None,
      None,
      hasDatasetOrRDD = false,
      ""
    )
    app.sqlIdToInfo.put(event.executionId, sqlExecution)
  }

  def doSparkListenerSQLExecutionEnd(
      app: T,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sqlIdToInfo.get(event.executionId).foreach { sql =>
      sql.endTime = Some(event.time)
      sql.duration = ProfileUtils.OptionLongMinusLong(sql.endTime, sql.startTime)
    }
  }

  def doSparkListenerDriverAccumUpdates(
      app: T,
      event: SparkListenerDriverAccumUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)

    val SparkListenerDriverAccumUpdates(sqlID, accumUpdates) = event
    accumUpdates.foreach { accum =>
      val driverAccum = DriverAccumCase(sqlID, accum._1, accum._2)
      val arrBuf =  app.driverAccumMap.getOrElseUpdate(accum._1,
        ArrayBuffer[DriverAccumCase]())
      arrBuf += driverAccum
    }
  }

  def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: T,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {}

  def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: T,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      doSparkListenerSQLExecutionStart(app, e)
    case e: SparkListenerSQLAdaptiveExecutionUpdate =>
      doSparkListenerSQLAdaptiveExecutionUpdate(app, e)
    case e: SparkListenerSQLAdaptiveSQLMetricUpdates =>
      doSparkListenerSQLAdaptiveSQLMetricUpdates(app, e)
    case e: SparkListenerSQLExecutionEnd =>
      doSparkListenerSQLExecutionEnd(app, e)
    case e: SparkListenerDriverAccumUpdates =>
      doSparkListenerDriverAccumUpdates(app, e)
    case SparkListenerLogStart(sparkVersion) =>
      logInfo("on other event called")
      app.sparkVersion = sparkVersion
    case _ =>
      val wasResourceProfileAddedEvent = doSparkListenerResourceProfileAddedReflect(app, event)
      if (!wasResourceProfileAddedEvent) doOtherEvent(app, event)
  }

  def doSparkListenerResourceProfileAdded(
      app: T,
      event: SparkListenerResourceProfileAdded): Unit = {}

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    doSparkListenerResourceProfileAdded(app, event)
  }

  def doSparkListenerBlockManagerAdded(
      app: T,
      event: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    doSparkListenerBlockManagerAdded(app, blockManagerAdded)
  }

  def doSparkListenerBlockManagerRemoved(
      app: T,
      event: SparkListenerBlockManagerRemoved): Unit = {}

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    doSparkListenerBlockManagerRemoved(app, blockManagerRemoved)
  }

  def doSparkListenerEnvironmentUpdate(
      app: T,
      event: SparkListenerEnvironmentUpdate): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    doSparkListenerEnvironmentUpdate(app, environmentUpdate)
  }

  def doSparkListenerApplicationStart(
      app: T,
      event: SparkListenerApplicationStart): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    doSparkListenerApplicationStart(app, applicationStart)
  }

  def doSparkListenerApplicationEnd(
      app: T,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    doSparkListenerApplicationEnd(app, applicationEnd)
  }

  def doSparkListenerExecutorAdded(
      app: T,
      event: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    doSparkListenerExecutorAdded(app, executorAdded)
  }

  def doSparkListenerExecutorRemoved(
      app: T,
      event: SparkListenerExecutorRemoved): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    doSparkListenerExecutorRemoved(app, executorRemoved)
  }

  def doSparkListenerTaskStart(
      app: T,
      event: SparkListenerTaskStart): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    doSparkListenerTaskStart(app, taskStart)
  }

  def doSparkListenerTaskEnd(
      app: T,
      event: SparkListenerTaskEnd): Unit = {
    // Parse task accumulables
    for (res <- event.taskInfo.accumulables) {
      try {
        val value = res.value.map(_.toString.toLong)
        val update = res.update.map(_.toString.toLong)
        val thisMetric = TaskStageAccumCase(
          event.stageId, event.stageAttemptId, Some(event.taskInfo.taskId),
          res.id, res.name, value, update, res.internal)
        val arrBuf =  app.taskStageAccumMap.getOrElseUpdate(res.id,
          ArrayBuffer[TaskStageAccumCase]())
        arrBuf += thisMetric
      } catch {
        case NonFatal(e) =>
          logWarning("Exception when parsing accumulables for task "
            + "stageID=" + event.stageId + ",taskId=" + event.taskInfo.taskId
            + ": ")
          logWarning(e.toString)
          logWarning("The problematic accumulable is: name="
            + res.name + ",value=" + res.value + ",update=" + res.update)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    doSparkListenerTaskEnd(app, taskEnd)
  }

  def doSparkListenerJobStart(
      app: T,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = ProfileUtils.stringToLong(sqlIDString)
    // add jobInfoClass
    val thisJob = new JobInfoClass(
      event.jobId,
      event.stageIds,
      sqlID,
      event.properties.asScala,
      event.time,
      None,
      None,
      None,
      None,
      ProfileUtils.isPluginEnabled(event.properties.asScala) || app.gpuMode
    )
    app.jobIdToInfo.put(event.jobId, thisJob)
    sqlID.foreach(app.jobIdToSqlID(event.jobId) = _)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    doSparkListenerJobStart(app, jobStart)
  }

  def doSparkListenerJobEnd(
      app: T,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)

    def jobResult(res: JobResult): String = {
      res match {
        case JobSucceeded => "JobSucceeded"
        case _: JobFailed => "JobFailed"
        case _ => "Unknown"
      }
    }

    def failedReason(res: JobResult): String = {
      res match {
        case JobSucceeded => ""
        case jobFailed: JobFailed => jobFailed.exception.toString
        case _ => ""
      }
    }

    app.jobIdToInfo.get(event.jobId) match {
      case Some(j) =>
        j.endTime = Some(event.time)
        j.duration = ProfileUtils.OptionLongMinusLong(j.endTime, j.startTime)
        val thisJobResult = jobResult(event.jobResult)
        j.jobResult = Some(thisJobResult)
        val thisFailedReason = failedReason(event.jobResult)
        j.failedReason = Some(thisFailedReason)
      case None =>
        val thisJobResult = jobResult(event.jobResult)
        val thisFailedReason = failedReason(event.jobResult)
        val thisJob = new JobInfoClass(
          event.jobId,
          Seq.empty,
          None,
          Map.empty,
          event.time,  // put end time as start time
          Some(event.time),
          Some(thisJobResult),
          Some(thisFailedReason),
          None,
          app.gpuMode
        )
        app.jobIdToInfo.put(event.jobId, thisJob)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    doSparkListenerJobEnd(app, jobEnd)
  }

  def doSparkListenerStageSubmitted(
      app: T,
      event: SparkListenerStageSubmitted): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.getOrCreateStage(event.stageInfo)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    doSparkListenerStageSubmitted(app, stageSubmitted)
  }

  def doSparkListenerStageCompleted(
      app: T,
      event: SparkListenerStageCompleted): Unit = {
    logDebug("Processing event: " + event.getClass)
    val stage = app.getOrCreateStage(event.stageInfo)
    stage.completionTime = event.stageInfo.completionTime
    stage.failureReason = event.stageInfo.failureReason
    stage.duration = ProfileUtils.optionLongMinusOptionLong(stage.completionTime,
      stage.info.submissionTime)
    val stageAccumulatorIds = event.stageInfo.accumulables.values.map { m => m.id }.toSeq
    stageAccumulatorIds.foreach { accumId =>
      app.accumulatorToStage.put(accumId, event.stageInfo.stageId)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    doSparkListenerStageCompleted(app, stageCompleted)
  }

  def doSparkListenerTaskGettingResult(
      app: T,
      event: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    doSparkListenerTaskGettingResult(app, taskGettingResult)
  }

  // To process all other unknown events
  def doOtherEvent(
      app: T,
      event: SparkListenerEvent): Unit = {}
}
