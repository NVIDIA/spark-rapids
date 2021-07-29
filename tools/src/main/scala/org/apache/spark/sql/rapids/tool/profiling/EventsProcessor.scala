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

package org.apache.spark.sql.rapids.tool.profiling

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile.CPUS
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.rapids.tool.EventProcessorBase
import org.apache.spark.ui.UIUtils

/**
 * This class is to process all events and do validation in the end.
 */
class EventsProcessor() extends EventProcessorBase with  Logging {

  type T = ApplicationInfo

  override def processAnyEvent(app: T, event: SparkListenerEvent): Unit = {
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
    app.resourceProfiles(event.resourceProfile.id) = rp
  }

  override def doSparkListenerBlockManagerAdded(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerAdded): Unit = {
    logDebug("Processing event: " + event.getClass)

    val exec = app.getOrCreateExecutor(event.blockManagerId.executorId, event.time)
    exec.hostPort = event.blockManagerId.hostPort
    event.maxOnHeapMem.foreach { _ =>
      exec.totalOnHeap = event.maxOnHeapMem.get
      exec.totalOffHeap = event.maxOffHeapMem.get
    }
    exec.isActive = true
    exec.maxMemory = event.maxMem
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

    // Parse task accumulables
    for (res <- event.taskInfo.accumulables) {
      try {
        val value = res.value.getOrElse("").toString.toLong
        val thisMetric = TaskStageAccumCase(
          event.stageId, event.stageAttemptId, Some(event.taskInfo.taskId),
          res.id, res.name, Some(value), res.internal)
        val arrBuf =  app.taskStageAccumMap.getOrElseUpdate(res.id,
          ArrayBuffer[TaskStageAccumCase]())
        // TODO - what about attempt?
        app.accumIdToStageId.put(res.id, event.stageId)
        arrBuf += thisMetric
      } catch {
        case e: ClassCastException =>
          logWarning("ClassCastException when parsing accumulables for task "
            + "stageID=" + event.stageId + ",taskId=" + event.taskInfo.taskId
            + ": ")
          logWarning(e.toString)
          logWarning("The problematic accumulable is: name="
            + res.name + ",value=" + res.value)
      }
    }

    val thisTask = TaskCase(
      event.stageId,
      event.stageAttemptId,
      event.taskType,
      event.reason.toString,
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
      "",
      None,
      false
    )
    app.sqls.put(event.executionId, sqlExecution)
    // app.sqlStart += sqlExecution
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerSQLExecutionEnd(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sqls.get(event.executionId).foreach { sql =>
      sql.endTime = Some(event.time)
      sql.duration = ProfileUtils.OptionLongMinusLong(sql.endTime, sql.startTime)
      sql.hasDataset = app.datasetSQL.exists(_.sqlID == event.executionId)
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
    app.jobs.put(event.jobId, thisJob)
  }

  override def doSparkListenerJobEnd(
      app: ApplicationInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.jobs.get(event.jobId).foreach { j =>
      j.endTime = Some(event.time)
      j.duration = ProfileUtils.OptionLongMinusLong(j.endTime, j.startTime)
      val thisJobResult = event.jobResult match {
        case JobSucceeded => "JobSucceeded"
        case _: JobFailed => "JobFailed"
        case _ => "Unknown"
      }
      j.jobResult = Some(thisJobResult)
      val thisFailedReason = event.jobResult match {
        case JobSucceeded => ""
        case jobFailed: JobFailed => jobFailed.exception.toString
        case _ => ""
      }
      j.failedReason = Some(thisFailedReason)
    }
  }

  override def doSparkListenerStageSubmitted(
      app: ApplicationInfo,
      event: SparkListenerStageSubmitted): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.getOrCreateStage(event.stageInfo)
  }

  override def doSparkListenerStageCompleted(
      app: ApplicationInfo,
      event: SparkListenerStageCompleted): Unit = {
    logDebug("Processing event: " + event.getClass)
    val stage = app.getOrCreateStage(event.stageInfo)
    stage.completionTime = event.stageInfo.completionTime
    stage.failureReason = event.stageInfo.failureReason

    stage.duration = ProfileUtils.optionLongMinusOptionLong(stage.completionTime,
      stage.info.submissionTime)

    // Parse stage accumulables
    // TODO - do this better!
    for (res <- event.stageInfo.accumulables) {
      try {
        val value = res._2.value.getOrElse("").toString.toLong
        val thisMetric = TaskStageAccumCase(
          event.stageInfo.stageId, event.stageInfo.attemptNumber(),
          None, res._2.id, res._2.name, Some(value), res._2.internal)
        val arrBuf =  app.taskStageAccumMap.getOrElseUpdate(res._2.id,
          ArrayBuffer[TaskStageAccumCase]())
        // TODO - what about attempt?
        app.accumIdToStageId.put(res._2.id, event.stageInfo.stageId)
        arrBuf += thisMetric
      } catch {
        case e: ClassCastException =>
          logWarning("ClassCastException when parsing accumulables for task " +
              "stageID=" + event.stageInfo.stageId + ": ")
          logWarning(e.toString)
          logWarning("The problematic accumulable is: name="
              + res._2.name + ",value=" + res._2.value)
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

  // TODO - need tests - do we need this?
  override def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)
    val SparkListenerSQLAdaptiveSQLMetricUpdates(sqlID, sqlPlanMetrics) = event
    val metrics = sqlPlanMetrics.map { metric =>
      // TODO - how get node info?
      /*SQLMetricInfoCase(sqlID, metric.name,
        metric.accumulatorId, metric.metricType, node.id,
        node.name, node.desc)
        */

      SQLPlanMetricsCase(sqlID, metric.name,
        metric.accumulatorId, metric.metricType)

    }
    app.sqlPlanMetricsAdaptive ++= metrics

  }

  // To process all other unknown events
  override def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    logInfo("Processing other event: " + event.getClass)
    // not used
   //  app.otherEvents += event
  }
}
