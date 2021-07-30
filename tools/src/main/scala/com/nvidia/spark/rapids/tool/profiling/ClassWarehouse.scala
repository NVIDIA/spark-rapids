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

import java.util.Date

import scala.collection.Map

import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler.StageInfo

/**
 * This is a warehouse to store all Classes
 * used for profiling and qualification.
 */

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
    executorOffHeap: Option[Long], taskCpu: Option[Double], taskGpu: Option[Double]) {

  val outputHeaders: Seq[String] = {
    Seq("appIndex", "resourceProfileId", "numExecutors", "executorCores",
      "maxMem", "maxOnHeapMem", "maxOffHeapMem", "executorMemory", "numGpusPerExecutor",
      "executorOffHeap", "taskCpu", "taskGpu")
  }
  def convertToSeq: Seq[String] = {
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
    sqlID: Option[Long]) {
  val outputHeaders = Seq("appIndex", "jobID", "stageIds", "sqlID")
  def convertToSeq: Seq[String] = {
    val stageIdStr = s"[${stageIds.mkString(",")}]"
    Seq(appIndex.toString, jobID.toString, stageIdStr, sqlID.map(_.toString).getOrElse(null))
  }
}

case class RapidsJarProfileResult(appIndex: Int, jar: String) {
  val outputHeaders = Seq("appIndex", "Rapids4Spark jars")
  def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, jar)
  }
}

case class DataSourceProfileResult(appIndex: Int, sqlID: Long, format: String,
    location: String, pushedFilters: String, schema: String) {
  val outputHeaders = Seq("appIndex", "sqlID", "format", "location", "pushedFilters", "schema")
  def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, format, location, pushedFilters, schema)
  }
}

class StageInfoClass(val info: StageInfo) {
  var completionTime: Option[Long] = None
  var failureReason: Option[String] = None
  var duration: Option[Long] = None
}

class SQLExecutionInfoClass(
    val sqlID: Long,
    val description: String,
    val details: String,
    val startTime: Long,
    var endTime: Option[Long],
    var duration: Option[Long],
    var hasDataset: Boolean,
    var problematic: String = "",
    var sqlCpuTimePercent: Double = -1)

case class SQLAccumProfileResults(appIndex: Int, sqlID: Long, nodeID: Long,
    nodeName: String, accumulatorId: Long,
    name: String, max_value: Long, metricType: String) {
  val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "nodeName", "accumulatorId",
    "name", "max_value", "metricType")
  def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, sqlID.toString, nodeID.toString, nodeName, accumulatorId.toString,
      name, max_value.toString, metricType)
  }
}

case class ResourceProfileInfoCase(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest])

case class BlockManagerRemovedCase(
    executorID: String, host: String, port: Int, time: Long)

case class AppInfoProfileResults(appIndex: Int, appName: String,
    appId: Option[String], sparkUser: String,
    startTime: Long, endTime: Option[Long], duration: Option[Long],
    durationStr: String, sparkVersion: String, pluginEnabled: Boolean) {
  val outputHeaders: Seq[String] = {
    ProfileUtils.getMethods[AppInfoProfileResults]
  }

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

  def convertToSeq: Seq[String] = {
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
    nodeDesc: String)

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
    value: Option[Long],
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

case class DatasetSQLCase(sqlID: Long)

case class ProblematicSQLCase(sqlID: Long, reason: String)

case class UnsupportedSQLPlan(sqlID: Long, nodeID: Long, nodeName: String,
    nodeDesc: String, reason: String)

case class DataSourceCase(
    sqlID: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String)

case class FailedTaskProfileResults(appIndex: Int, stageId: Int, stageAttemptId: Int,
    taskId: Long, taskAttemptId: Int, endReason: String) {
  val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
    "attempt", "failureReason")
  def convertToSeq: Seq[String] = {
    Seq(appIndex.toString, stageId.toString, stageAttemptId.toString,
      taskId.toString, taskAttemptId.toString, ProfileUtils.truncateFailureStr(endReason))
  }
}