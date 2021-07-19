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

import java.util.Date
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.tool.profiling._
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
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
    val liveRP = new LiveResourceProfile(event.resourceProfile.id,
      event.resourceProfile.executorResources, event.resourceProfile.taskResources, None)
    app.resourceProfiles(event.resourceProfile.id) = liveRP
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
      // SPARK-30594: whenever(first time or re-register) a BlockManager added, all blocks
      // from this BlockManager will be reported to driver later. So, we should clean up
      // used memory to avoid overlapped count.
      exec.usedOnHeap = 0
      exec.usedOffHeap = 0
    }
    exec.isActive = true
    exec.maxMemory = event.maxMem
  }

  override def doSparkListenerBlockManagerRemoved(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)
    // TODO - do we really need, spark doesn't use because executor removed
    // handles this.
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
    app.hadoopProperties = event.environmentDetails("Hadoop Properties").toMap
    app.systemProperties = event.environmentDetails("System Properties").toMap
    app.jvmInfo = event.environmentDetails("JVM Information").toMap
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
      event.time,
      event.sparkUser,
      None,
      None,
      "",
      "",
      gpuMode = false
    )
    logWarning("found app start" + thisAppStart)
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
    val liveRP = app.resourceProfiles.get(rpId)
    // TODO - default cpus per task could be in sparkProperties
    val cpusPerTask = liveRP.flatMap(_.taskResources.get(CPUS))
      .map(_.amount.toInt).getOrElse(1)
    val maxTasksPerExec = liveRP.flatMap(_.maxTasksPerExecutor)
    exec.maxTasks = maxTasksPerExec.getOrElse(event.executorInfo.totalCores / cpusPerTask)
    exec.resources = event.executorInfo.resourcesInfo
    exec.attributes = event.executorInfo.attributes
    exec.resourceProfileId = rpId
  }

  override def doSparkListenerExecutorRemoved(
      app: ApplicationInfo,
      event: SparkListenerExecutorRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)

    val exec = app.getOrCreateExecutor(event.executorId, event.time)
    exec.isActive = false
    exec.removeTime = new Date(event.time)
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
        app.taskStageAccum += thisMetric
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
    val sqlExecution = new SQLExecutionCaseInfo(
      event.executionId,
      event.description,
      event.details,
      event.time,
      None,
      None,
      "",
      None,
      false,
      ""
    )
    app.liveSQL.put(event.executionId, sqlExecution)
    // app.sqlStart += sqlExecution
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  override def doSparkListenerSQLExecutionEnd(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.liveSQL.get(event.executionId).foreach { sql =>
      sql.endTime = Some(event.time)
      sql.duration = ProfileUtils.OptionLongMinusLong(sql.endTime, sql.startTime)
      sql.durationStr = sql.duration match {
        case Some(i) => UIUtils.formatDuration(i)
        case None => ""
      }
      val (containsDataset, sqlQDuration) =
        if (app.datasetSQL.exists(_.sqlID == event.executionId)) {
          (true, Some(0L))
        } else {
          (false, sql.duration)
        }
      sql.hasDataset = containsDataset
      sql.sqlQualDuration = sqlQDuration
      val potProbs = app.problematicSQL.filter { p =>
        p.sqlID == event.executionId && p.reason.nonEmpty
      }.map(_.reason).mkString(",")
      val finalPotProbs = if (potProbs.isEmpty) {
        null
      } else {
        potProbs
      }
      sql.problematic = finalPotProbs
    }

  }

  override def doSparkListenerDriverAccumUpdates(
      app: ApplicationInfo,
      event: SparkListenerDriverAccumUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)

    val SparkListenerDriverAccumUpdates(sqlID, accumUpdates) = event
    accumUpdates.foreach(accum =>
      app.driverAccum += DriverAccumCase(sqlID, accum._1, accum._2)
    )
    logDebug("Current driverAccum has " + app.driverAccum.size + " accums.")
  }

  override def doSparkListenerJobStart(
      app: ApplicationInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = ProfileUtils.stringToLong(sqlIDString)
    val thisJob = new JobCaseInfo(
      event.jobId,
      event.stageIds,
      sqlID,
      event.properties.asScala,
      event.time,
      None,
      None,
      None,
      None,
      "",
      ProfileUtils.isPluginEnabled(event.properties.asScala) || app.gpuMode
    )
    app.liveJobs.put(event.jobId, thisJob)
  }

  override def doSparkListenerJobEnd(
      app: ApplicationInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.liveJobs.get(event.jobId).foreach { j =>
      j.endTime = Some(event.time)
      j.duration = ProfileUtils.OptionLongMinusLong(j.endTime, j.startTime)
      j.durationStr = j.duration match {
        case Some(i) => UIUtils.formatDuration(i)
        case None => ""
      }
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
    val stage = app.getOrCreateStage(event.stageInfo)
    stage.properties = event.properties.asScala
    stage.gpuMode = ProfileUtils.isPluginEnabled(event.properties.asScala) || app.gpuMode

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
    stage.durationStr = stage.duration match {
      case Some(i) => UIUtils.formatDuration(i)
      case None => ""
    }


    // app.stageCompletionTime += (event.stageInfo.stageId -> event.stageInfo.completionTime)
   //  app.stageFailureReason += (event.stageInfo.stageId -> event.stageInfo.failureReason)

    // Parse stage accumulables
    // TODO - do this better!
    for (res <- event.stageInfo.accumulables) {
      try {
        val value = res._2.value.getOrElse("").toString.toLong
        val thisMetric = TaskStageAccumCase(
          event.stageInfo.stageId, event.stageInfo.attemptNumber(),
          None, res._2.id, res._2.name, Some(value), res._2.internal)
        app.taskStageAccum += thisMetric
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
    logInfo("Processing other event: " + event.getClass)
    // not used
   //  app.otherEvents += event
  }
}
