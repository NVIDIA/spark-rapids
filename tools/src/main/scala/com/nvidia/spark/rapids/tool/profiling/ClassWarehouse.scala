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

import org.apache.spark.scheduler.StageInfo

/**
 * This is a warehouse to store all Case Classes
 * used to create Spark DataFrame.
 */

case class ResourceProfileCase(
    id: Int, executorCores: Int, executorMemory: Long, numGpusPerExecutor: Int,
    executorOffHeap: Long, taskCpu: Int, taskGpu: Double)

case class BlockManagerCase(
    executorID: String, host: String, port: Int,
    maxMem: Long, maxOnHeapMem: Long, maxOffHeapMem: Long)

case class BlockManagerRemovedCase(
    executorID: String, host: String, port: Int, time: Long)

case class PropertiesCase(
    source: String, key: String, value: String)

case class ApplicationCase(
    appName: String, appId: Option[String], sparkUser: String,
    startTime: Long, endTime: Option[Long], duration: Option[Long],
    durationStr: String, sparkVersion: String, pluginEnabled: Boolean) {

  val outputHeaders: Seq[String] = {
    Seq("appIndex") ++ ProfileUtils.getMethods[ApplicationCase]
  }

  def fieldsToPrint(index: Int): Seq[String] = {
    val endTimeStr = endTime match {
      case Some(t) => t.toString
      case None => ""
    }
    val durStr = duration match {
      case Some(t) => t.toString
      case None => ""
    }
    Seq(index.toString, appName, appId.getOrElse(""), sparkUser,
      startTime.toString, endTimeStr, durStr, durationStr, sparkVersion,
      pluginEnabled.toString)
  }
}

case class ExecutorCase(
    executorID: String, host: String, totalCores: Int, resourceProfileId: Int)

case class ExecutorRemovedCase(
    executorID: String,
    reason: String,
    time: Long)

class SQLExecutionCaseInfo(
    val sqlID: Long,
    val description: String,
    val details: String,
    val startTime: Long,
    var endTime: Option[Long],
    var duration: Option[Long],
    var durationStr: String,
    var sqlQualDuration: Option[Long],
    var hasDataset: Boolean,
    var problematic: String = "")

case class SQLExecutionCase(
    sqlID: Long,
    description: String,
    details: String,
    startTime: Long,
    endTime: Option[Long],
    duration: Option[Long],
    durationStr: String,
    sqlQualDuration: Option[Long],
    hasDataset: Boolean,
    problematic: String = "")

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

class JobCaseInfo(val jobID: Int,
    val stageIds: Seq[Int],
    val sqlID: Option[Long],
    val properties: scala.collection.Map[String, String],
    val startTime: Long,
    var endTime: Option[Long],
    var jobResult: Option[String],
    var failedReason: Option[String],
    var duration: Option[Long],
    var durationStr: String,
    var gpuMode: Boolean)

case class JobCase(
    jobID: Int,
    stageIds: Seq[Int],
    sqlID: Option[Long],
    properties: scala.collection.Map[String, String],
    startTime: Long,
    endTime: Option[Long],
    jobResult: Option[String],
    failedReason: Option[String],
    duration: Option[Long],
    durationStr: String,
    gpuMode: Boolean)

class StageCaseInfo(val info: StageInfo) {
  var completionTime: Option[Long] = None
  var failureReason: Option[String] = None
  var duration: Option[Long] = None
  var durationStr: String = ""
  var gpuMode: Boolean = false
  var properties: scala.collection.Map[String, String] = Map.empty[String, String]
}

case class StageCase(
    stageId: Int,
    attemptId: Int,
    name: String,
    numTasks: Int,
    numRDD: Int,
    parentIds: Seq[Int],
    details: String,
    properties: scala.collection.Map[String, String],
    submissionTime: Option[Long],
    completionTime: Option[Long],
    failureReason: Option[String],
    duration: Option[Long],
    durationStr: String,
    gpuMode: Boolean)

// Note: sr = Shuffle Read; sw = Shuffle Write
// Totally 39 columns
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

case class UnsupportedSQLPlan(sqlID: Long, nodeID: Long, nodeName: String, nodeDesc: String)

case class DataSourceCase(
    sqlID: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String)

case class DataSourceCompareCase(
    appIndex: Int,
    appId: String,
    sqlID: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String)