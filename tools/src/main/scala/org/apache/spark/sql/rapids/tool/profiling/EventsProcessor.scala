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

package org.apache.spark.sql.rapids.tool.profiling

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.TaskFailedReason
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.rapids.tool.EventProcessorBase

/**
 * This class is to process all events and do validation in the end.
 */
class EventsProcessor(app: ApplicationInfo) extends EventProcessorBase[ApplicationInfo](app)
  with Logging {

  override def doSparkListenerResourceProfileAddedReflect(
      app: ApplicationInfo,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case _: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app,
              event.asInstanceOf[SparkListenerResourceProfileAdded])
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

  override def doSparkListenerLogStart(app: ApplicationInfo, event: SparkListenerLogStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sparkVersion = event.sparkVersion
  }

  override def doSparkListenerResourceProfileAdded(
      app: ApplicationInfo,
      event: SparkListenerResourceProfileAdded): Unit = {

    logDebug("Processing event: " + event.getClass)
    // leave off maxTasks for now
    val rp = ResourceProfileInfoCase(event.resourceProfile.id,
      event.resourceProfile.executorResources, event.resourceProfile.taskResources)
    app.resourceProfIdToInfo(event.resourceProfile.id) = rp
  }

  override def doSparkListenerBlockManagerAdded(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerAdded): Unit = {
    logDebug("Processing event: " + event.getClass)
    val execExists = app.executorIdToInfo.get(event.blockManagerId.executorId)
    if (event.blockManagerId.executorId == "driver" && !execExists.isDefined) {
      // means its not in local mode, skip counting as executor
    } else {
      // note that one block manager is for driver as well
      val exec = app.getOrCreateExecutor(event.blockManagerId.executorId, event.time)
      exec.hostPort = event.blockManagerId.hostPort
      event.maxOnHeapMem.foreach { mem =>
        exec.totalOnHeap = mem
      }
      event.maxOffHeapMem.foreach { offHeap =>
        exec.totalOffHeap = offHeap
      }
      exec.isActive = true
      exec.maxMemory = event.maxMem
    }
  }

  override def doSparkListenerBlockManagerRemoved(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisBlockManagerRemoved = BlockManagerRemovedCase(
      event.blockManagerId.executorId,
      event.blockManagerId.host,
      event.blockManagerId.port,
      event.time
    )
    app.blockManagersRemoved += thisBlockManagerRemoved
  }

  override def doSparkListenerEnvironmentUpdate(
      app: ApplicationInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sparkProperties = event.environmentDetails("Spark Properties").toMap
    app.classpathEntries = event.environmentDetails("Classpath Entries").toMap

    //Decide if this application is on GPU Mode
    if (ProfileUtils.isPluginEnabled(collection.mutable.Map() ++= app.sparkProperties)) {
      app.gpuMode = true
      logDebug("App's GPU Mode = TRUE")
    } else {
      logDebug("App's GPU Mode = FALSE")
    }
  }

  override def doSparkListenerApplicationStart(
      app: ApplicationInfo,
      event: SparkListenerApplicationStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisAppStart = ApplicationCase(
      event.appName,
      event.appId,
      event.sparkUser,
      event.time,
      None,
      None,
      "",
      "",
      pluginEnabled = false
    )
    app.appInfo = thisAppStart
    app.appId = event.appId.getOrElse("")
  }

  override def doSparkListenerApplicationEnd(
      app: ApplicationInfo,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  override def doSparkListenerExecutorAdded(
      app: ApplicationInfo,
      event: SparkListenerExecutorAdded): Unit = {
    logDebug("Processing event: " + event.getClass)
    val exec = app.getOrCreateExecutor(event.executorId, event.time)
    exec.host = event.executorInfo.executorHost
    exec.isActive = true
    exec.totalCores = event.executorInfo.totalCores
    val rpId = event.executorInfo.resourceProfileId
    exec.resources = event.executorInfo.resourcesInfo
    exec.resourceProfileId = rpId
  }

  override def doSparkListenerExecutorRemoved(
      app: ApplicationInfo,
      event: SparkListenerExecutorRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)
    val exec = app.getOrCreateExecutor(event.executorId, event.time)
    exec.isActive = false
    exec.removeTime = event.time
    exec.removeReason = event.reason
  }

  override def doSparkListenerTaskStart(
      app: ApplicationInfo,
      event: SparkListenerTaskStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    // currently not used
    // app.taskStart += event
  }

  override def doSparkListenerTaskEnd(
      app: ApplicationInfo,
      event: SparkListenerTaskEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    super.doSparkListenerTaskEnd(app, event)
    val reason = event.reason match {
      case failed: TaskFailedReason =>
        failed.toErrorString
      case _ =>
        event.reason.toString
    }

    val thisTask = TaskCase(
      event.stageId,
      event.stageAttemptId,
      event.taskType,
      reason,
      event.taskInfo.taskId,
      event.taskInfo.attemptNumber,
      event.taskInfo.launchTime,
      event.taskInfo.finishTime,
      event.taskInfo.duration,
      event.taskInfo.successful,
      event.taskInfo.executorId,
      event.taskInfo.host,
      event.taskInfo.taskLocality.toString,
      event.taskInfo.speculative,
      event.taskInfo.gettingResultTime,
      event.taskMetrics.executorDeserializeTime,
      NANOSECONDS.toMillis(event.taskMetrics.executorDeserializeCpuTime),
      event.taskMetrics.executorRunTime,
      NANOSECONDS.toMillis(event.taskMetrics.executorCpuTime),
      event.taskMetrics.peakExecutionMemory,
      event.taskMetrics.resultSize,
      event.taskMetrics.jvmGCTime,
      event.taskMetrics.resultSerializationTime,
      event.taskMetrics.memoryBytesSpilled,
      event.taskMetrics.diskBytesSpilled,
      event.taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.localBlocksFetched,
      event.taskMetrics.shuffleReadMetrics.fetchWaitTime,
      event.taskMetrics.shuffleReadMetrics.remoteBytesRead,
      event.taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      event.taskMetrics.shuffleReadMetrics.localBytesRead,
      event.taskMetrics.shuffleReadMetrics.totalBytesRead,
      event.taskMetrics.shuffleWriteMetrics.bytesWritten,
      NANOSECONDS.toMillis(event.taskMetrics.shuffleWriteMetrics.writeTime),
      event.taskMetrics.shuffleWriteMetrics.recordsWritten,
      event.taskMetrics.inputMetrics.bytesRead,
      event.taskMetrics.inputMetrics.recordsRead,
      event.taskMetrics.outputMetrics.bytesWritten,
      event.taskMetrics.outputMetrics.recordsWritten
    )
    app.taskEnd += thisTask
  }

  override def doSparkListenerSQLExecutionStart(
      app: ApplicationInfo,
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
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerSQLExecutionEnd(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sqlIdToInfo.get(event.executionId).foreach { sql =>
      sql.endTime = Some(event.time)
      sql.duration = ProfileUtils.OptionLongMinusLong(sql.endTime, sql.startTime)
    }
  }

  override def doSparkListenerDriverAccumUpdates(
      app: ApplicationInfo,
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

  override def doSparkListenerJobStart(
      app: ApplicationInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = ProfileUtils.stringToLong(sqlIDString)
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
  }

  override def doSparkListenerJobEnd(
      app: ApplicationInfo,
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

  override def doSparkListenerStageCompleted(
      app: ApplicationInfo,
      event: SparkListenerStageCompleted): Unit = {
    logDebug("Processing event: " + event.getClass)

    // Parse stage accumulables
    for (res <- event.stageInfo.accumulables) {
      try {
        val value = res._2.value.map(_.toString.toLong)
        val update = res._2.update.map(_.toString.toLong)
        val thisMetric = TaskStageAccumCase(
          event.stageInfo.stageId, event.stageInfo.attemptNumber(),
          None, res._2.id, res._2.name, value, update, res._2.internal)
        val arrBuf =  app.taskStageAccumMap.getOrElseUpdate(res._2.id,
          ArrayBuffer[TaskStageAccumCase]())
        app.accumIdToStageId.put(res._2.id, event.stageInfo.stageId)
        arrBuf += thisMetric
      } catch {
        case NonFatal(e) =>
          logWarning("Exception when parsing accumulables for task " +
              "stageID=" + event.stageInfo.stageId + ": ")
          logWarning(e.toString)
          logWarning("The problematic accumulable is: name="
              + res._2.name + ",value=" + res._2.value + ",update=" + res._2.update)
      }
    }
  }

  override def doSparkListenerTaskGettingResult(
      app: ApplicationInfo,
      event: SparkListenerTaskGettingResult): Unit = {
    logDebug("Processing event: " + event.getClass)
  }

  override def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)
    val SparkListenerSQLAdaptiveSQLMetricUpdates(sqlID, sqlPlanMetrics) = event
    val metrics = sqlPlanMetrics.map { metric =>
      SQLPlanMetricsCase(sqlID, metric.name,
        metric.accumulatorId, metric.metricType)
    }
    app.sqlPlanMetricsAdaptive ++= metrics
  }

  // To process all other unknown events
  override def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    logDebug("Skipping unhandled event: " + event.getClass)
    // not used
  }
}
