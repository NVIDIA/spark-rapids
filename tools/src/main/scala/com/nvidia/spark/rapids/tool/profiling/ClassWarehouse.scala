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

import java.util.Date

import scala.collection.Map

import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler.StageInfo

/**
 * This is a warehouse to store all Classes
 * used for profiling and qualification.
 */

trait ProfileResult {
  val outputHeaders: Seq[String]
  def convertToSeq: Seq[String]
}

case class DriverInfo(val executorId: String, maxMemory: Long, totalOnHeap: Long,
    totalOffHeap: Long)

class ExecutorInfoClass(val executorId: String, _addTime: Long) {
  var hostPort: String = null
  var host: String = null
  var isActive = true
  var totalCores = 0

  val addTime = new Date(_addTime)
  var removeTime: Long = 0L
  var removeReason: String = null
  var maxMemory = 0L

  var resources = Map[String, ResourceInformation]()

  // Memory metrics. They may not be recorded (e.g. old event logs) so if totalOnHeap is not
  // initialized, the store will not contain this information.
  var totalOnHeap = -1L
  var totalOffHeap = 0L

  var resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
}

case class ExecutorInfoProfileResult(appIndex: Int, resourceProfileId: Int,
    numExecutors: Int, executorCores: Int, maxMem: Long, maxOnHeapMem: Long,
    maxOffHeapMem: Long, executorMemory: Option[Long], numGpusPerExecutor: Option[Long],
    executorOffHeap: Option[Long], taskCpu: Option[Double],
    taskGpu: Option[Double]) extends ProfileResult {

  override val outputHeaders: Seq[String] = {
    Seq("appIndex", "resourceProfileId", "numExecutors", "executorCores",
      "maxMem", "maxOnHeapMem", "maxOffHeapMem", "executorMemory", "numGpusPerExecutor",
      "executorOffHeap", "taskCpu", "taskGpu")
  }
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, resourceProfileId.toString, numExecutors.toString,
      executorCores.toString, maxMem.toString, maxOnHeapMem.toString,
      maxOffHeapMem.toString, executorMemory.map(_.toString).getOrElse(null),
      numGpusPerExecutor.map(_.toString).getOrElse(null),
      executorOffHeap.map(_.toString).getOrElse(null), taskCpu.map(_.toString).getOrElse(null),
      taskGpu.map(_.toString).getOrElse(null))
  }
}

class JobInfoClass(val jobID: Int,
    val stageIds: Seq[Int],
    val sqlID: Option[Long],
    val properties: scala.collection.Map[String, String],
    val startTime: Long,
    var endTime: Option[Long],
    var jobResult: Option[String],
    var failedReason: Option[String],
    var duration: Option[Long],
    var gpuMode: Boolean)

case class JobInfoProfileResult(
    appIndex: Int,
    jobID: Int,
    stageIds: Seq[Int],
    sqlID: Option[Long],
    startTime: Long,
    endTime: Option[Long]) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "jobID", "stageIds", "sqlID", "startTime", "endTime")
  override def convertToSeq: Seq[String] = {
    val stageIdStr = s"[${stageIds.mkString(",")}]"
    Seq(appIndex.toString, jobID.toString, stageIdStr, sqlID.map(_.toString).getOrElse(null),
      startTime.toString, endTime.map(_.toString).getOrElse(null))
  }
}

case class SQLStageInfoProfileResult(
    appIndex: Int,
    sqlID: Long,
    jobID: Int,
    stageId: Int,
    stageAttemptId: Int,
    duration: Option[Long],
    nodeNames: Seq[String]) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "jobID", "stageId",
    "stageAttemptId", "Stage Duration", "SQL Nodes(IDs)")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, jobID.toString, stageId.toString,
      stageAttemptId.toString, duration.map(_.toString).getOrElse(null),
      nodeNames.mkString(","))
  }
}

case class RapidsJarProfileResult(appIndex: Int, jar: String)  extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "Rapids4Spark jars")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, jar)
  }
}

case class DataSourceProfileResult(appIndex: Int, sqlID: Long, format: String,
    location: String, pushedFilters: String, schema: String) extends ProfileResult {
  override val outputHeaders =
    Seq("appIndex", "sqlID", "format", "location", "pushedFilters", "schema")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, format, location, pushedFilters, schema)
  }
}

class StageInfoClass(val info: StageInfo) {
  var completionTime: Option[Long] = None
  var failureReason: Option[String] = None
  var duration: Option[Long] = None
}

// note that some things might not be set until after sqlMetricsAggregation called
class SQLExecutionInfoClass(
    val sqlID: Long,
    val description: String,
    val details: String,
    val startTime: Long,
    var endTime: Option[Long],
    var duration: Option[Long],
    var hasDatasetOrRDD: Boolean,
    var problematic: String = "",
    var sqlCpuTimePercent: Double = -1)

case class SQLAccumProfileResults(appIndex: Int, sqlID: Long, nodeID: Long,
    nodeName: String, accumulatorId: Long, name: String, max_value: Long,
    metricType: String, stageIds: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "nodeName", "accumulatorId",
    "name", "max_value", "metricType", "stageIds")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, nodeID.toString, nodeName, accumulatorId.toString,
      name, max_value.toString, metricType, stageIds)
  }
}

case class ResourceProfileInfoCase(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest])

case class BlockManagerRemovedCase(
    executorID: String, host: String, port: Int, time: Long)

case class BlockManagerRemovedProfileResult(appIndex: Int,
    executorID: String, time: Long) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "executorID", "time")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, executorID, time.toString)
  }
}

case class ExecutorsRemovedProfileResult(appIndex: Int,
    executorID: String, time: Long, reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "executorId", "time", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, executorID, time.toString, reason)
  }
}

case class UnsupportedOpsProfileResult(appIndex: Int,
    sqlID: Long, nodeID: Long, nodeName: String, nodeDescription: String,
    reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "nodeName",
    "nodeDescription", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, nodeID.toString, nodeName,
      nodeDescription, reason)
  }
}

case class AppInfoProfileResults(appIndex: Int, appName: String,
    appId: Option[String], sparkUser: String,
    startTime: Long, endTime: Option[Long], duration: Option[Long],
    durationStr: String, sparkVersion: String,
    pluginEnabled: Boolean)  extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "appName", "appId",
    "sparkUser", "startTime", "endTime", "duration", "durationStr",
    "sparkVersion", "pluginEnabled")

  def endTimeToStr: String = {
    endTime match {
      case Some(t) => t.toString
      case None => ""
    }
  }

  def durToStr: String = {
    duration match {
      case Some(t) => t.toString
      case None => ""
    }
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, appName, appId.getOrElse(""),
      sparkUser,  startTime.toString, endTimeToStr, durToStr,
      durationStr, sparkVersion, pluginEnabled.toString)
  }
}

case class ApplicationCase(
    appName: String, appId: Option[String], sparkUser: String,
    startTime: Long, endTime: Option[Long], duration: Option[Long],
    durationStr: String, sparkVersion: String, pluginEnabled: Boolean)

case class SQLPlanMetricsCase(
    sqlID: Long,
    name: String,
    accumulatorId: Long,
    metricType: String)

case class PlanNodeAccumCase(
    sqlID: Long,
    nodeID: Long,
    nodeName: String,
    nodeDesc: String,
    accumulatorId: Long)

case class SQLMetricInfoCase(
    sqlID: Long,
    name: String,
    accumulatorId: Long,
    metricType: String,
    nodeID: Long,
    nodeName: String,
    nodeDesc: String,
    stageIds: Seq[Int])

case class DriverAccumCase(
    sqlID: Long,
    accumulatorId: Long,
    value: Long)

case class TaskStageAccumCase(
    stageId: Int,
    attemptId: Int,
    taskId: Option[Long],
    accumulatorId: Long,
    name: Option[String],
    // The total accumulated so far for all tasks
    value: Option[Long],
    // The amount for this particular task/update
    update: Option[Long],
    isInternal: Boolean)

// Note: sr = Shuffle Read; sw = Shuffle Write
case class TaskCase(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    endReason: String,
    taskId: Long,
    attempt: Int,
    launchTime: Long,
    finishTime: Long,
    duration: Long,
    successful: Boolean,
    executorId: String,
    host: String,
    taskLocality: String,
    speculative: Boolean,
    gettingResultTime: Long,
    executorDeserializeTime: Long,
    executorDeserializeCPUTime: Long,
    executorRunTime: Long,
    executorCPUTime: Long,
    peakExecutionMemory: Long,
    resultSize: Long,
    jvmGCTime: Long,
    resultSerializationTime: Long,
    memoryBytesSpilled: Long,
    diskBytesSpilled: Long,
    sr_remoteBlocksFetched: Long,
    sr_localBlocksFetched: Long,
    sr_fetchWaitTime: Long,
    sr_remoteBytesRead: Long,
    sr_remoteBytesReadToDisk: Long,
    sr_localBytesRead: Long,
    sr_totalBytesRead: Long,
    sw_bytesWritten: Long,
    sw_writeTime: Long,
    sw_recordsWritten: Long,
    input_bytesRead: Long,
    input_recordsRead: Long,
    output_bytesWritten: Long,
    output_recordsWritten: Long)

case class UnsupportedSQLPlan(sqlID: Long, nodeID: Long, nodeName: String,
    nodeDesc: String, reason: String)

case class DataSourceCase(
    sqlID: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String)

case class FailedTaskProfileResults(appIndex: Int, stageId: Int, stageAttemptId: Int,
    taskId: Long, taskAttemptId: Int, endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
    "attempt", "failureReason")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString,
      taskId.toString, taskAttemptId.toString, ProfileUtils.truncateFailureStr(endReason))
  }
}

case class FailedStagesProfileResults(appIndex: Int, stageId: Int, stageAttemptId: Int,
    name: String, numTasks: Int, endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "attemptId", "name",
    "numTasks", "failureReason")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString,
      name, numTasks.toString, ProfileUtils.truncateFailureStr(endReason))
  }
}

case class FailedJobsProfileResults(appIndex: Int, jobId: Int,
    jobResult: String, endReason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "jobID", "jobResult", "failureReason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, jobId.toString,
      jobResult, ProfileUtils.truncateFailureStr(endReason))
  }
}

case class JobStageAggTaskMetricsProfileResult(
    appIndex: Int,
    id: String,
    numTasks: Int,
    duration: Option[Long],
    diskBytesSpilledSum: Long,
    durationSum: Long,
    durationMax: Long,
    durationMin: Long,
    durationAvg: Double,
    executorCPUTimeSum: Long,
    executorDeserializeCpuTimeSum: Long,
    executorDeserializeTimeSum: Long,
    executorRunTimeSum: Long,
    inputBytesReadSum: Long,
    inputRecordsReadSum: Long,
    jvmGCTimeSum: Long,
    memoryBytesSpilledSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    peakExecutionMemoryMax: Long,
    resultSerializationTimeSum: Long,
    resultSizeMax: Long,
    srFetchWaitTimeSum: Long,
    srLocalBlocksFetchedSum: Long,
    srcLocalBytesReadSum: Long,
    srRemoteBlocksFetchSum: Long,
    srRemoteBytesReadSum: Long,
    srRemoteBytesReadToDiskSum: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenSum: Long,
    swRecordsWrittenSum: Long,
    swWriteTimeSum: Long) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "ID", "numTasks", "Duration", "diskBytesSpilled_sum",
    "duration_sum", "duration_max", "duration_min",
    "duration_avg", "executorCPUTime_sum", "executorDeserializeCPUTime_sum",
    "executorDeserializeTime_sum", "executorRunTime_sum", "input_bytesRead_sum",
    "input_recordsRead_sum", "jvmGCTime_sum", "memoryBytesSpilled_sum",
    "output_bytesWritten_sum", "output_recordsWritten_sum", "peakExecutionMemory_max",
    "resultSerializationTime_sum", "resultSize_max", "sr_fetchWaitTime_sum",
    "sr_localBlocksFetched_sum", "sr_localBytesRead_sum", "sr_remoteBlocksFetched_sum",
    "sr_remoteBytesRead_sum", "sr_remoteBytesReadToDisk_sum", "sr_totalBytesRead_sum",
    "sw_bytesWritten_sum", "sw_recordsWritten_sum", "sw_writeTime_sum")

  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => "null"
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      id,
      numTasks.toString,
      durStr,
      diskBytesSpilledSum.toString,
      durationSum.toString,
      durationMax.toString,
      durationMin.toString,
      durationAvg.toString,
      executorCPUTimeSum.toString,
      executorDeserializeCpuTimeSum.toString,
      executorDeserializeTimeSum.toString,
      executorRunTimeSum.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      jvmGCTimeSum.toString,
      memoryBytesSpilledSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      peakExecutionMemoryMax.toString,
      resultSerializationTimeSum.toString,
      resultSizeMax.toString,
      srFetchWaitTimeSum.toString,
      srLocalBlocksFetchedSum.toString,
      srcLocalBytesReadSum.toString,
      srRemoteBlocksFetchSum.toString,
      srRemoteBytesReadSum.toString,
      srRemoteBytesReadToDiskSum.toString,
      srTotalBytesReadSum.toString,
      swBytesWrittenSum.toString,
      swRecordsWrittenSum.toString,
      swWriteTimeSum.toString)
  }
}

case class SQLTaskAggMetricsProfileResult(
    appIndex: Int,
    appId: String,
    sqlId: Long,
    description: String,
    numTasks: Int,
    duration: Option[Long],
    executorCpuTime: Long,
    executorRunTime: Long,
    executorCpuRatio: Double,
    diskBytesSpilledSum: Long,
    durationSum: Long,
    durationMax: Long,
    durationMin: Long,
    durationAvg: Double,
    executorCPUTimeSum: Long,
    executorDeserializeCpuTimeSum: Long,
    executorDeserializeTimeSum: Long,
    executorRunTimeSum: Long,
    inputBytesReadSum: Long,
    inputRecordsReadSum: Long,
    jvmGCTimeSum: Long,
    memoryBytesSpilledSum: Long,
    outputBytesWrittenSum: Long,
    outputRecordsWrittenSum: Long,
    peakExecutionMemoryMax: Long,
    resultSerializationTimeSum: Long,
    resultSizeMax: Long,
    srFetchWaitTimeSum: Long,
    srLocalBlocksFetchedSum: Long,
    srcLocalBytesReadSum: Long,
    srRemoteBlocksFetchSum: Long,
    srRemoteBytesReadSum: Long,
    srRemoteBytesReadToDiskSum: Long,
    srTotalBytesReadSum: Long,
    swBytesWrittenSum: Long,
    swRecordsWrittenSum: Long,
    swWriteTimeSum: Long) extends ProfileResult {

  override val outputHeaders = Seq("appIndex", "appID", "sqlID", "description", "numTasks",
    "Duration", "executorCPUTime", "executorRunTime", "executorCPURatio",
    "diskBytesSpilled_sum", "duration_sum", "duration_max", "duration_min",
    "duration_avg", "executorCPUTime_sum", "executorDeserializeCPUTime_sum",
    "executorDeserializeTime_sum", "executorRunTime_sum", "input_bytesRead_sum",
    "input_recordsRead_sum", "jvmGCTime_sum", "memoryBytesSpilled_sum",
    "output_bytesWritten_sum", "output_recordsWritten_sum", "peakExecutionMemory_max",
    "resultSerializationTime_sum", "resultSize_max", "sr_fetchWaitTime_sum",
    "sr_localBlocksFetched_sum", "sr_localBytesRead_sum", "sr_remoteBlocksFetched_sum",
    "sr_remoteBytesRead_sum", "sr_remoteBytesReadToDisk_sum", "sr_totalBytesRead_sum",
    "sw_bytesWritten_sum", "sw_recordsWritten_sum", "sw_writeTime_sum")

  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => ""
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      appId,
      sqlId.toString,
      description,
      numTasks.toString,
      durStr,
      executorCpuTime.toString,
      executorRunTime.toString,
      executorCpuRatio.toString,
      diskBytesSpilledSum.toString,
      durationSum.toString,
      durationMax.toString,
      durationMin.toString,
      durationAvg.toString,
      executorCPUTimeSum.toString,
      executorDeserializeCpuTimeSum.toString,
      executorDeserializeTimeSum.toString,
      executorRunTimeSum.toString,
      inputBytesReadSum.toString,
      inputRecordsReadSum.toString,
      jvmGCTimeSum.toString,
      memoryBytesSpilledSum.toString,
      outputBytesWrittenSum.toString,
      outputRecordsWrittenSum.toString,
      peakExecutionMemoryMax.toString,
      resultSerializationTimeSum.toString,
      resultSizeMax.toString,
      srFetchWaitTimeSum.toString,
      srLocalBlocksFetchedSum.toString,
      srcLocalBytesReadSum.toString,
      srRemoteBlocksFetchSum.toString,
      srRemoteBytesReadSum.toString,
      srRemoteBytesReadToDiskSum.toString,
      srTotalBytesReadSum.toString,
      swBytesWrittenSum.toString,
      swRecordsWrittenSum.toString,
      swWriteTimeSum.toString)
  }
}

case class SQLDurationExecutorTimeProfileResult(appIndex: Int, appId: String, sqlID: Long,
    duration: Option[Long], containsDataset: Boolean, appDuration: Option[Long],
    potentialProbs: String, executorCpuRatio: Double) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "App ID", "sqlID", "SQL Duration",
    "Contains Dataset or RDD Op", "App Duration", "Potential Problems", "Executor CPU Time Percent")
  val durStr = duration match {
    case Some(dur) => dur.toString
    case None => ""
  }
  val appDurStr = appDuration match {
    case Some(dur) => dur.toString
    case None => ""
  }
  val execCpuTimePercent = if (executorCpuRatio == -1.0) {
    "null"
  } else {
    executorCpuRatio.toString
  }
  val potentialStr = if (potentialProbs.isEmpty) {
    "null"
  } else {
    potentialProbs
  }

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, appId, sqlID.toString, durStr, containsDataset.toString,
      appDurStr, potentialStr, execCpuTimePercent)
  }
}

case class ShuffleSkewProfileResult(appIndex: Int, stageId: Long, stageAttemptId: Long,
    taskId: Long, taskAttemptId: Long, taskDuration: Long, avgDuration: Double,
    taskShuffleReadMB: Long, avgShuffleReadMB: Double, taskPeakMemoryMB: Long,
    successful: Boolean, reason: String) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId", "attempt",
    "taskDurationSec", "avgDurationSec", "taskShuffleReadMB", "avgShuffleReadMB",
    "taskPeakMemoryMB", "successful", "reason")

  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      stageId.toString,
      stageAttemptId.toString,
      taskId.toString,
      taskAttemptId.toString,
      f"${taskDuration.toDouble / 1000}%1.2f",
      f"${avgDuration / 1000}%1.1f",
      f"${taskShuffleReadMB.toDouble / 1024 / 1024}%1.2f",
      f"${avgShuffleReadMB / 1024 / 1024}%1.2f",
      f"${taskPeakMemoryMB.toDouble / 1024 / 1024}%1.2f",
      successful.toString,
      ProfileUtils.truncateFailureStr(reason))
  }
}

case class RapidsPropertyProfileResult(key: String, outputHeadersIn: Seq[String],
    rows: Seq[String]) extends ProfileResult {

  override val outputHeaders: Seq[String] = outputHeadersIn
  override def convertToSeq: Seq[String] = rows
}

case class CompareProfileResults(outputHeadersIn: Seq[String],
    rows: Seq[String]) extends ProfileResult {

  override val outputHeaders: Seq[String] = outputHeadersIn
  override def convertToSeq: Seq[String] = rows
}

case class WholeStageCodeGenResults(
    appIndex: Int,
    sqlID: Long,
    nodeID: Long,
    parent: String,
    child: String
) extends ProfileResult {
  override val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "SQL Node", "Child Node(ID)")
  override def convertToSeq: Seq[String] = {
    Seq(appIndex.toString,
      sqlID.toString,
      nodeID.toString,
      parent,
      child)
  }
}
