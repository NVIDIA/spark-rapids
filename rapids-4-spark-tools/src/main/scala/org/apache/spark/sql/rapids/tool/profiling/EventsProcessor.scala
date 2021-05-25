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

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

/**
 * This object is to process all events and do validation in the end.
 */
object EventsProcessor extends Logging {

  def processAnyEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    event match {
      case _: SparkListenerLogStart =>
        doSparkListenerLogStart(app, event.asInstanceOf[SparkListenerLogStart])
      case _: SparkListenerResourceProfileAdded =>
        doSparkListenerResourceProfileAdded(app,
          event.asInstanceOf[SparkListenerResourceProfileAdded])
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
      case _ => doOtherEvent(app, event)
    }
  }

  def doSparkListenerLogStart(app: ApplicationInfo, event: SparkListenerLogStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sparkVersion = event.sparkVersion
  }

  def doSparkListenerResourceProfileAdded(
      app: ApplicationInfo,
      event: SparkListenerResourceProfileAdded): Unit = {

    logDebug("Processing event: " + event.getClass)
    val res = event.resourceProfile
    val thisResourceProfile = ResourceProfileCase(
      res.id,
      res.getExecutorCores.getOrElse(0),
      res.executorResources.get(ResourceProfile.MEMORY).map(_.amount.toLong).getOrElse(0),
      res.executorResources.get("gpu").map(_.amount.toInt).getOrElse(0),
      res.executorResources.get(ResourceProfile.OVERHEAD_MEM).map(_.amount.toLong).getOrElse(0),
      res.getTaskCpus.getOrElse(0),
      res.taskResources.get("gpu").map(_.amount.toDouble).getOrElse(0)
    )
    app.resourceProfiles += thisResourceProfile
  }

  def doSparkListenerBlockManagerAdded(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerAdded): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisBlockManager = BlockManagerCase(
      event.blockManagerId.executorId,
      event.blockManagerId.host,
      event.blockManagerId.port,
      event.maxMem,
      event.maxOnHeapMem.getOrElse(0),
      event.maxOffHeapMem.getOrElse(0)
    )
    app.blockManagers += thisBlockManager
  }

  def doSparkListenerBlockManagerRemoved(
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

  def doSparkListenerEnvironmentUpdate(
      app: ApplicationInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sparkProperties = event.environmentDetails("Spark Properties").toMap
    app.hadoopProperties = event.environmentDetails("Hadoop Properties").toMap
    app.systemProperties = event.environmentDetails("System Properties").toMap
    app.jvmInfo = event.environmentDetails("JVM Information").toMap
    app.classpathEntries = event.environmentDetails("Classpath Entries").toMap

    //Decide if this application is on GPU Mode
    if (ProfileUtils.isGPUMode(collection.mutable.Map() ++= app.sparkProperties)) {
      app.gpuMode = true
      logDebug("App's GPU Mode = TRUE")
    } else {
      logDebug("App's GPU Mode = FALSE")
    }
  }

  def doSparkListenerApplicationStart(
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
    app.appStart += thisAppStart
    app.appId = event.appId.getOrElse("")
  }

  def doSparkListenerApplicationEnd(
      app: ApplicationInfo,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  def doSparkListenerExecutorAdded(
      app: ApplicationInfo,
      event: SparkListenerExecutorAdded): Unit = {
    logDebug("Processing event: " + event.getClass)
    val executor = ExecutorCase(
      event.executorId,
      event.executorInfo.executorHost,
      event.executorInfo.totalCores,
      event.executorInfo.resourceProfileId
    )
    app.executors += executor
  }

  def doSparkListenerExecutorRemoved(
      app: ApplicationInfo,
      event: SparkListenerExecutorRemoved): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisExecutorRemoved = ExecutorRemovedCase(
      event.executorId,
      event.reason,
      event.time
    )
    app.executorsRemoved += thisExecutorRemoved
  }

  def doSparkListenerTaskStart(
      app: ApplicationInfo,
      event: SparkListenerTaskStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.taskStart += event
  }

  def doSparkListenerTaskEnd(
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
      event.taskMetrics.executorDeserializeCpuTime / 1000000,
      event.taskMetrics.executorRunTime,
      event.taskMetrics.executorCpuTime / 1000000,
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
      event.taskMetrics.shuffleWriteMetrics.writeTime / 1000000,
      event.taskMetrics.shuffleWriteMetrics.recordsWritten,
      event.taskMetrics.inputMetrics.bytesRead,
      event.taskMetrics.inputMetrics.recordsRead,
      event.taskMetrics.outputMetrics.bytesWritten,
      event.taskMetrics.outputMetrics.recordsWritten
    )
    app.taskEnd += thisTask
  }

  def doSparkListenerSQLExecutionStart(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlExecution = SQLExecutionCase(
      event.executionId,
      event.description,
      event.details,
      event.time,
      None,
      None,
      ""
    )
    app.sqlStart += sqlExecution
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  def doSparkListenerSQLExecutionEnd(
      app: ApplicationInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.sqlEndTime += (event.executionId -> event.time)
  }

  def doSparkListenerDriverAccumUpdates(
      app: ApplicationInfo,
      event: SparkListenerDriverAccumUpdates): Unit = {
    logDebug("Processing event: " + event.getClass)

    val SparkListenerDriverAccumUpdates(sqlID, accumUpdates) = event
    accumUpdates.foreach(accum =>
      app.driverAccum += DriverAccumCase(sqlID, accum._1, accum._2)
    )
    logDebug("Current driverAccum has " + app.driverAccum.size + " accums.")
  }

  def doSparkListenerJobStart(
      app: ApplicationInfo,
      event: SparkListenerJobStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sqlIDString = event.properties.getProperty("spark.sql.execution.id")
    val sqlID = ProfileUtils.stringToLong(sqlIDString)
    val thisJob = JobCase(
      event.jobId,
      event.stageIds,
      sqlID,
      event.properties.asScala,
      event.time,
      None,
      None,
      "",
      None,
      "",
      ProfileUtils.isGPUMode(event.properties.asScala) || app.gpuMode
    )
    app.jobStart += thisJob
  }

  def doSparkListenerJobEnd(
      app: ApplicationInfo,
      event: SparkListenerJobEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.jobEndTime += (event.jobId -> event.time)

    // Parse jobResult
    val thisJobResult = event.jobResult match {
      case JobSucceeded => "JobSucceeded"
      case _: JobFailed => "JobFailed"
      case _ => "Unknown"
    }
    app.jobEndResult += (event.jobId -> thisJobResult)

    val thisFailedReason = event.jobResult match {
      case JobSucceeded => ""
      case jobFailed: JobFailed => jobFailed.exception.toString
      case _ => ""
    }
    app.jobFailedReason += (event.jobId -> thisFailedReason)
  }

  def doSparkListenerStageSubmitted(
      app: ApplicationInfo,
      event: SparkListenerStageSubmitted): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisStage = StageCase(
      event.stageInfo.stageId,
      event.stageInfo.attemptNumber(),
      event.stageInfo.name,
      event.stageInfo.numTasks,
      event.stageInfo.rddInfos.size,
      event.stageInfo.parentIds,
      event.stageInfo.details,
      event.properties.asScala,
      event.stageInfo.submissionTime,
      None,
      None,
      None,
      "",
      ProfileUtils.isGPUMode(event.properties.asScala) || app.gpuMode
    )
    app.stageSubmitted += thisStage
  }

  def doSparkListenerStageCompleted(
      app: ApplicationInfo,
      event: SparkListenerStageCompleted): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.stageCompletionTime += (event.stageInfo.stageId -> event.stageInfo.completionTime)
    app.stageFailureReason += (event.stageInfo.stageId -> event.stageInfo.failureReason)

    // Parse stage accumulables
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

  def doSparkListenerTaskGettingResult(
      app: ApplicationInfo,
      event: SparkListenerTaskGettingResult): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.taskGettingResult += event
  }

  def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: ApplicationInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    // AQE plan can override the ones got from SparkListenerSQLExecutionStart
    app.sqlPlan += (event.executionId -> event.sparkPlanInfo)
    app.physicalPlanDescription += (event.executionId -> event.physicalPlanDescription)
  }

  def doSparkListenerSQLAdaptiveSQLMetricUpdates(
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
  def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    logInfo("Processing other event: " + event.getClass)
    app.otherEvents += event
  }
}
