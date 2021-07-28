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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.{immutable, mutable, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.{EventLogInfo, EventLogPathProcessor}
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils


class SparkPlanInfoWithStage(
    nodeName: String,
    simpleString: String,
    override val children: Seq[SparkPlanInfoWithStage],
    metadata: scala.Predef.Map[String, String],
    metrics: Seq[SQLMetricInfo],
    val stageId: Option[Int]) extends SparkPlanInfo(nodeName, simpleString, children,
  metadata, metrics) {

  import SparkPlanInfoWithStage._

  def debugEquals(other: Any, depth: Int = 0): Boolean = {
    System.err.println(s"${" " * depth}DOES $this == $other?")
    other match {
      case o: SparkPlanInfo =>
        if (nodeName != o.nodeName) {
          System.err.println(s"${" " * depth}" +
              s"$this != $o (names different) ${this.nodeName} != ${o.nodeName}")
          return false
        }
        if (simpleString != o.simpleString) {
          System.err.println(s"${" " * depth}$this != $o (simpleString different) " +
              s"${this.simpleString} != ${o.simpleString}")
          return false
        }
        if (children.length != o.children.length) {
          System.err.println(s"${" " * depth}$this != $o (different num children) " +
              s"${this.children.length} ${o.children.length}")
          return false
        }
        children.zip(o.children).zipWithIndex.forall {
          case ((t, o), index) =>
            System.err.println(s"${" " * depth}COMPARING CHILD $index")
            t.debugEquals(o, depth = depth + 1)
        }
      case _ =>
        System.err.println(s"${" " * depth}NOT EQUAL WRONG TYPE")
        false
    }
  }

  override def toString: String = {
    s"PLAN NODE: '$nodeName' '$simpleString' $stageId"
  }

  /**
   * Create a new SparkPlanInfoWithStage that is normalized in a way so that CPU and GPU
   * plans look the same. This should really only be used when matching stages/jobs between
   * different plans.
   */
  def normalizeForStageComparison: SparkPlanInfoWithStage = {
    nodeName match {
      case "GpuColumnarToRow" | "GpuRowToColumnar" | "ColumnarToRow" | "RowToColumnar" |
           "AdaptiveSparkPlan" | "AvoidAdaptiveTransitionToRow" |
           "HostColumnarToGpu" | "GpuBringBackToHost" |
           "GpuCoalesceBatches" | "GpuShuffleCoalesce" |
           "InputAdapter" | "Subquery" | "ReusedSubquery" |
           "CustomShuffleReader" | "GpuCustomShuffleReader" | "ShuffleQueryStage" |
           "BroadcastQueryStage" |
           "Sort" | "GpuSort" =>
        // Remove data format, grouping changes because they don't impact the computation
        // Remove CodeGen stages (not important to actual query execution)
        // Remove AQE fix-up parts.
        // Remove sort because in a lot of cases (like sort merge join) it is not part of the
        // actual computation and it is hard to tell which sort is important to the query result
        // and which is not
        children.head.normalizeForStageComparison
      case name if name.startsWith("WholeStageCodegen") =>
        // Remove whole stage codegen (It includes an ID afterwards that we should ignore too)
        children.head.normalizeForStageComparison
      case name if name.contains("Exchange") =>
        // Drop all exchanges, a broadcast could become a shuffle/etc
        children.head.normalizeForStageComparison
      case "GpuTopN" if isShuffledTopN(this) =>
        val coalesce = this.children.head
        val shuffle = coalesce.children.head
        val firstTopN = shuffle.children.head
        firstTopN.normalizeForStageComparison
      case name =>
        val newName = normalizedNameRemapping.getOrElse(name,
          if (name.startsWith("Gpu")) {
            name.substring(3)
          } else {
            name
          })

        val normalizedChildren = children.map(_.normalizeForStageComparison)
        // We are going to ignore the simple string because it can contain things in it that
        // are specific to a given run
        new SparkPlanInfoWithStage(newName, newName, normalizedChildren, metadata,
          metrics, stageId)
    }
  }

  /**
   * Walk the tree depth first and get all of the stages for each node in the tree
   */
  def depthFirstStages: Seq[Option[Int]] =
    Seq(stageId) ++ children.flatMap(_.depthFirstStages)
}

object SparkPlanInfoWithStage {
  def apply(plan: SparkPlanInfo, accumIdToStageId: Map[Long, Int]): SparkPlanInfoWithStage = {
    // In some cases Spark will do a shuffle in the middle of an operation,
    // like TakeOrderedAndProject. In those cases the node is associated with the
    // min stage ID, just to remove ambiguity

    val newChildren = plan.children.map(SparkPlanInfoWithStage(_, accumIdToStageId))

    val stageId = plan.metrics.flatMap { m =>
      accumIdToStageId.get(m.accumulatorId)
    }.reduceOption((l, r) => Math.min(l, r))

    new SparkPlanInfoWithStage(plan.nodeName, plan.simpleString, newChildren, plan.metadata,
      plan.metrics, stageId)
  }

  private val normalizedNameRemapping: Map[String, String] = Map(
    "Execute GpuInsertIntoHadoopFsRelationCommand" -> "Execute InsertIntoHadoopFsRelationCommand",
    "GpuTopN" -> "TakeOrderedAndProject",
    "SortMergeJoin" -> "Join",
    "ShuffledHashJoin" -> "Join",
    "GpuShuffledHashJoin" -> "Join",
    "BroadcastHashJoin" -> "Join",
    "GpuBroadcastHashJoin" -> "Join",
    "HashAggregate" -> "Aggregate",
    "SortAggregate" -> "Aggregate",
    "GpuHashAggregate" -> "Aggregate",
    "RunningWindow" -> "Window", //GpuWindow and Window are already covered
    "GpuRunningWindow" -> "Window")

  private def isShuffledTopN(info: SparkPlanInfoWithStage): Boolean = {
    if (info.children.length == 1 && // shuffle coalesce
        info.children.head.children.length == 1 && // shuffle
        info.children.head.children.head.children.length == 1) { // first top n
      val coalesce = info.children.head
      val shuffle = coalesce.children.head
      val firstTopN = shuffle.children.head
      info.nodeName == "GpuTopN" &&
          (coalesce.nodeName == "GpuShuffleCoalesce" ||
              coalesce.nodeName == "GpuCoalesceBatches") &&
          shuffle.nodeName == "GpuColumnarExchange" && firstTopN.nodeName == "GpuTopN"
    } else {
      false
    }
  }
}

class LiveResourceProfile(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest],
    val maxTasksPerExecutor: Option[Int])

class LiveExecutor(val executorId: String, _addTime: Long) {
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
  var usedOnHeap = 0L
  var usedOffHeap = 0L

  var resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

  // peak values for executor level metrics
  val peakExecutorMetrics = new ExecutorMetrics()

  def hostname: String = if (host != null) host else Utils.parseHostPort(hostPort)._1
}

/**
 * ApplicationInfo class saves all parsed events for future use.
 * Used only for Profiling.
 */
class ApplicationInfo(
    numRows: Int,
    hadoopConf: Configuration,
    eLogInfo: EventLogInfo,
    val index: Int)
  extends AppBase(numRows, eLogInfo, hadoopConf) with Logging {

  val executors = new HashMap[String, LiveExecutor]()
  val resourceProfiles = new HashMap[Int, LiveResourceProfile]()

  val liveStages = new HashMap[(Int, Int), StageCaseInfo]()
  val liveJobs = new HashMap[Int, JobCaseInfo]()
  val liveSQL = new HashMap[Long, SQLExecutionCaseInfo]()

  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
     ArrayBuffer[BlockManagerRemovedCase]()

  // From SparkListenerEnvironmentUpdate
  var sparkProperties = Map.empty[String, String]
  var hadoopProperties = Map.empty[String, String]
  var systemProperties = Map.empty[String, String]
  var jvmInfo = Map.empty[String, String]
  var classpathEntries = Map.empty[String, String]
  var gpuMode = false

  // From SparkListenerApplicationStart and SparkListenerApplicationEnd
  var appInfo: ApplicationCase = null
  var appId: String = ""

  // From SparkListenerSQLExecutionStart and SparkListenerSQLExecutionEnd
  var sqlStart: ArrayBuffer[SQLExecutionCase] = ArrayBuffer[SQLExecutionCase]()
  val sqlEndTime: mutable.HashMap[Long, Long] = mutable.HashMap.empty[Long, Long]

  // From SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate
  // sqlPlan stores HashMap (sqlID <-> SparkPlanInfo)
  var sqlPlan: mutable.HashMap[Long, SparkPlanInfo] = mutable.HashMap.empty[Long, SparkPlanInfo]

  // physicalPlanDescription stores HashMap (sqlID <-> physicalPlanDescription)
  var physicalPlanDescription: mutable.HashMap[Long, String] = mutable.HashMap.empty[Long, String]

  // From SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate
  var sqlPlanMetrics: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()
  var planNodeAccum: ArrayBuffer[PlanNodeAccumCase] = ArrayBuffer[PlanNodeAccumCase]()
  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()

  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  // From SparkListenerDriverAccumUpdates
  var driverAccum: ArrayBuffer[DriverAccumCase] = ArrayBuffer[DriverAccumCase]()
  var driverAccumMap: mutable.HashMap[Long, ArrayBuffer[DriverAccumCase]] =
    mutable.HashMap[Long, ArrayBuffer[DriverAccumCase]]()

  // From SparkListenerTaskEnd and SparkListenerTaskEnd
  var taskStageAccum: ArrayBuffer[TaskStageAccumCase] = ArrayBuffer[TaskStageAccumCase]()
  var taskStageAccumMap: mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]] =
    mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]]()


  lazy val accumIdToStageId: immutable.Map[Long, Int] =
    taskStageAccum.map(accum => (accum.accumulatorId, accum.stageId)).toMap

  // From SparkListenerJobStart and SparkListenerJobEnd
  // JobStart contains mapping relationship for JobID -> StageID(s)
  var jobStart: ArrayBuffer[JobCase] = ArrayBuffer[JobCase]()
  val jobEndTime: mutable.HashMap[Int, Long] = mutable.HashMap.empty[Int, Long]

  // From SparkListenerTaskStart & SparkListenerTaskEnd
  // taskStart was not used so comment out for now
  // var taskStart: ArrayBuffer[SparkListenerTaskStart] = ArrayBuffer[SparkListenerTaskStart]()
  // taskEnd contains task level metrics - only used for profiling
  var taskEnd: ArrayBuffer[TaskCase] = ArrayBuffer[TaskCase]()

  // Unsupported SQL plan
  var unsupportedSQLplan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()

  // By looping through SQL Plan nodes to find out the problematic SQLs. Currently we define
  // problematic SQL's as those which have RowToColumnar, ColumnarToRow transitions and Lambda's in
  // the Spark plan.
  var problematicSQL: ArrayBuffer[ProblematicSQLCase] = ArrayBuffer[ProblematicSQLCase]()

  // SQL containing any Dataset operation
  var datasetSQL: ArrayBuffer[DatasetSQLCase] = ArrayBuffer[DatasetSQLCase]()

  private lazy val eventProcessor =  new EventsProcessor()

  // Process all events
  processEvents()
  // Process SQL Plan Metrics after all events are processed
  processSQLPlanMetrics()
  aggregateAppInfo

  override def processEvent(event: SparkListenerEvent) = {
    eventProcessor.processAnyEvent(this, event)
    false
  }

  def getOrCreateExecutor(executorId: String, addTime: Long): LiveExecutor = {
    executors.getOrElseUpdate(executorId, {
      new LiveExecutor(executorId, addTime)
    })
  }

  def getOrCreateStage(info: StageInfo): StageCaseInfo = {
    val stage = liveStages.getOrElseUpdate((info.stageId, info.attemptNumber),
      new StageCaseInfo(info))
    stage
  }

  /**
   * Function to process SQL Plan Metrics after all events are processed
   */
  def processSQLPlanMetrics(): Unit = {
    for ((sqlID, planInfo) <- sqlPlan){
      checkMetadataForReadSchema(sqlID, planInfo)
      val planGraph = SparkPlanGraph(planInfo)
      // SQLPlanMetric is a case Class of
      // (name: String,accumulatorId: Long,metricType: String)
      val allnodes = planGraph.allNodes
      for (node <- allnodes) {
        checkGraphNodeForBatchScan(sqlID, node)
        if (isDataSetPlan(node.desc)) {
          datasetSQL += DatasetSQLCase(sqlID)
          if (gpuMode) {
            // TODO - add in what problem is
            val thisPlan = UnsupportedSQLPlan(sqlID, node.id, node.name, node.desc)
            unsupportedSQLplan += thisPlan
          }
        }
        // Then process SQL plan metric type
        for (metric <- node.metrics) {
          val thisMetric = SQLPlanMetricsCase(sqlID, metric.name,
            metric.accumulatorId, metric.metricType)
          sqlPlanMetrics += thisMetric
          val thisNode = PlanNodeAccumCase(sqlID, node.id,
            node.name, node.desc, metric.accumulatorId)
          planNodeAccum += thisNode

          val allMetric = SQLMetricInfoCase(sqlID, metric.name,
            metric.accumulatorId, metric.metricType, node.id,
            node.name, node.desc)
          allSQLMetrics += allMetric
        }
      }
    }
    if (this.sqlPlanMetricsAdaptive.nonEmpty){
      logInfo(s"Merging ${sqlPlanMetricsAdaptive.size} SQL Metrics(Adaptive) for appID=$appId")
      sqlPlanMetrics = sqlPlanMetrics.union(sqlPlanMetricsAdaptive).distinct
    }
  }

  private def aggregateAppInfo: Unit = {
    if (this.appInfo != null) {
      val appStartNew: ArrayBuffer[ApplicationCase] = ArrayBuffer[ApplicationCase]()
      val res = this.appInfo

      val estimatedResult = this.appEndTime match {
        case Some(t) => this.appEndTime
        case None =>
          if (this.sqlEndTime.isEmpty && this.jobEndTime.isEmpty) {
            None
          } else {
            // TODO - need to fix
            logWarning("Application End Time is unknown, estimating based on" +
              " job and sql end times!")
            // estimate the app end with job or sql end times
            val sqlEndTime = if (this.sqlEndTime.isEmpty) 0L else this.sqlEndTime.values.max
            val jobEndTime = if (this.jobEndTime.isEmpty) 0L else this.jobEndTime.values.max
            val maxEndTime = math.max(sqlEndTime, jobEndTime)
            if (maxEndTime == 0) None else Some(maxEndTime)
          }
      }

      val durationResult = ProfileUtils.OptionLongMinusLong(estimatedResult, res.startTime)
      val durationString = durationResult match {
        case Some(i) => UIUtils.formatDuration(i.toLong)
        case None => ""
      }

      val newApp = res.copy(endTime = this.appEndTime, duration = durationResult,
        durationStr = durationString, sparkVersion = this.sparkVersion,
        pluginEnabled = this.gpuMode)
      logWarning("aggregate app start: " + newApp)
      appInfo = newApp
    }
  }

}

object ApplicationInfo extends Logging {


  def createApp(path: EventLogInfo, numRows: Int, index: Int,
      hadoopConf: Configuration): (Option[ApplicationInfo], Int) = {
    var errorCode = 0
    val app = try {
      // This apps only contains 1 app in each loop.
      val startTime = System.currentTimeMillis()
      val app = new ApplicationInfo(numRows, hadoopConf, path, index)
      EventLogPathProcessor.logApplicationInfo(app)
      val endTime = System.currentTimeMillis()
      logInfo(s"Took ${endTime - startTime}ms to process ${path.eventLog.toString}")
      Some(app)
    } catch {
      case json: com.fasterxml.jackson.core.JsonParseException =>
        logWarning(s"Error parsing JSON: $path")
        errorCode = 1
        None
      case il: IllegalArgumentException =>
        logWarning(s"Error parsing file: $path", il)
        errorCode = 2
        None
      case e: Exception =>
        // catch all exceptions and skip that file
        logWarning(s"Got unexpected exception processing file: $path", e)
        errorCode = 3
        None
    }
    (app, errorCode)
  }
}
