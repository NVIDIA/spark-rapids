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

import scala.collection.{mutable, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.ui.UIUtils


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

  // executorId to executor info
  val executorIdToInfo = new HashMap[String, ExecutorInfoClass]()
  // resourceprofile id to resource profile info
  val resourceProfIdToInfo = new HashMap[Int, ResourceProfileInfoCase]()

  // stageid, stageAttemptId to stage info
  val stageIdToInfo = new HashMap[(Int, Int), StageInfoClass]()
  // jobId to job info
  val jobIdToInfo = new HashMap[Int, JobInfoClass]()
  // sqlId to sql info
  val sqlIdToInfo = new HashMap[Long, SQLExecutionInfoClass]()

  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
     ArrayBuffer[BlockManagerRemovedCase]()

  // From SparkListenerEnvironmentUpdate
  var sparkProperties = Map.empty[String, String]
  var classpathEntries = Map.empty[String, String]
  var gpuMode = false

  var appInfo: ApplicationCase = null
  var appId: String = ""

  // sqlPlan stores HashMap (sqlID <-> SparkPlanInfo)
  var sqlPlan: mutable.HashMap[Long, SparkPlanInfo] = mutable.HashMap.empty[Long, SparkPlanInfo]

  // physicalPlanDescription stores HashMap (sqlID <-> physicalPlanDescription)
  var physicalPlanDescription: mutable.HashMap[Long, String] = mutable.HashMap.empty[Long, String]

  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()
  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  var driverAccumMap: mutable.HashMap[Long, ArrayBuffer[DriverAccumCase]] =
    mutable.HashMap[Long, ArrayBuffer[DriverAccumCase]]()

  // accum id to task stage accum info
  var taskStageAccumMap: mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]] =
    mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]]()

  val accumIdToStageId: mutable.HashMap[Long, Int] = new mutable.HashMap[Long, Int]()
  var taskEnd: ArrayBuffer[TaskCase] = ArrayBuffer[TaskCase]()
  var unsupportedSQLplan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()

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

  def getOrCreateExecutor(executorId: String, addTime: Long): ExecutorInfoClass = {
    executorIdToInfo.getOrElseUpdate(executorId, {
      new ExecutorInfoClass(executorId, addTime)
    })
  }

  def getOrCreateStage(info: StageInfo): StageInfoClass = {
    val stage = stageIdToInfo.getOrElseUpdate((info.stageId, info.attemptNumber),
      new StageInfoClass(info))
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
          sqlIdToInfo.get(sqlID).foreach { sql =>
            sql.hasDataset = true
          }
          if (gpuMode) {
            val thisPlan = UnsupportedSQLPlan(sqlID, node.id, node.name, node.desc,
              "Contains Dataset")
            unsupportedSQLplan += thisPlan
          }
        }
        // Then process SQL plan metric type
        for (metric <- node.metrics) {
          val allMetric = SQLMetricInfoCase(sqlID, metric.name,
            metric.accumulatorId, metric.metricType, node.id,
            node.name, node.desc)

          allSQLMetrics += allMetric
          if (this.sqlPlanMetricsAdaptive.nonEmpty) {
            val adaptive = sqlPlanMetricsAdaptive.filter { adaptiveMetric =>
              adaptiveMetric.sqlID == sqlID && adaptiveMetric.accumulatorId == metric.accumulatorId
            }
            adaptive.foreach { adaptiveMetric =>
              val allMetric = SQLMetricInfoCase(sqlID, adaptiveMetric.name,
                adaptiveMetric.accumulatorId, adaptiveMetric.metricType, node.id,
                node.name, node.desc)
              // could make this more efficient but seems ok for now
              val exists = allSQLMetrics.filter { a =>
                ((a.accumulatorId == adaptiveMetric.accumulatorId) && (a.sqlID == sqlID)
                  && (a.nodeID == node.id && adaptiveMetric.metricType == a.metricType))
              }
              if (exists.isEmpty) {
                allSQLMetrics += allMetric
              }
            }
          }
        }
      }
    }
  }

  private def aggregateAppInfo: Unit = {
    if (this.appInfo != null) {
      val res = this.appInfo

      val estimatedResult = this.appEndTime match {
        case Some(t) => this.appEndTime
        case None =>
          val jobEndTimes = jobIdToInfo.map { case (_, jc) => jc.endTime }.filter(_.isDefined)
          val sqlEndTimes = sqlIdToInfo.map { case (_, sc) => sc.endTime }.filter(_.isDefined)

          if (sqlEndTimes.size == 0 && jobEndTimes.size == 0) {
            None
          } else {
            logWarning("Application End Time is unknown, estimating based on" +
              " job and sql end times!")
            // estimate the app end with job or sql end times
            val sqlEndTime = if (sqlEndTimes.size == 0) 0L else sqlEndTimes.map(_.get).max
            val jobEndTime = if (jobEndTimes.size == 0) 0L else jobEndTimes.map(_.get).max
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
      appInfo = newApp
    }
  }
}
