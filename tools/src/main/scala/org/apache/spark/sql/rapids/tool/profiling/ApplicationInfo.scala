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

import scala.collection.{mutable, Map}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.planparser.SQLPlanParser
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
    hadoopConf: Configuration,
    eLogInfo: EventLogInfo,
    val index: Int)
  extends AppBase(Some(eLogInfo), Some(hadoopConf)) with Logging {

  // executorId to executor info
  val executorIdToInfo = new HashMap[String, ExecutorInfoClass]()
  // resourceprofile id to resource profile info
  val resourceProfIdToInfo = new HashMap[Int, ResourceProfileInfoCase]()

  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
     ArrayBuffer[BlockManagerRemovedCase]()

  // From SparkListenerEnvironmentUpdate
  var sparkProperties = Map.empty[String, String]
  var classpathEntries = Map.empty[String, String]

  var appInfo: ApplicationCase = null
  var appId: String = ""

  // sqlPlan stores HashMap (sqlID <-> SparkPlanInfo)
  var sqlPlan: mutable.HashMap[Long, SparkPlanInfo] = mutable.HashMap.empty[Long, SparkPlanInfo]

  // physicalPlanDescription stores HashMap (sqlID <-> physicalPlanDescription)
  var physicalPlanDescription: mutable.HashMap[Long, String] = mutable.HashMap.empty[Long, String]

  var allSQLMetrics: ArrayBuffer[SQLMetricInfoCase] = ArrayBuffer[SQLMetricInfoCase]()
  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  val accumIdToStageId: mutable.HashMap[Long, Int] = new mutable.HashMap[Long, Int]()
  var taskEnd: ArrayBuffer[TaskCase] = ArrayBuffer[TaskCase]()
  var unsupportedSQLplan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()
  var wholeStage: ArrayBuffer[WholeStageCodeGenResults] = ArrayBuffer[WholeStageCodeGenResults]()
  val sqlPlanNodeIdToStageIds: mutable.HashMap[(Long, Long), Seq[Int]] =
    mutable.HashMap.empty[(Long, Long), Seq[Int]]

  private lazy val eventProcessor =  new EventsProcessor(this)

  // Process all events
  processEvents()
  // Process SQL Plan Metrics after all events are processed
  processSQLPlanMetrics()
  aggregateAppInfo

  override def processEvent(event: SparkListenerEvent) = {
    eventProcessor.processAnyEvent(event)
    false
  }

  def getOrCreateExecutor(executorId: String, addTime: Long): ExecutorInfoClass = {
    executorIdToInfo.getOrElseUpdate(executorId, {
      new ExecutorInfoClass(executorId, addTime)
    })
  }

  // Connects Operators to Stages using AccumulatorIDs
  def connectOperatorToStage(): Unit = {
    for ((sqlId, planInfo) <- sqlPlan) {
      val planGraph = SparkPlanGraph(planInfo)
      // Maps stages to operators by checking for non-zero intersection
      // between nodeMetrics and stageAccumulateIDs
      val nodeIdToStage = planGraph.allNodes.map { node =>
        val mappedStages = SQLPlanParser.getStagesInSQLNode(node, this)
        ((sqlId, node.id), mappedStages)
      }.toMap
      sqlPlanNodeIdToStageIds ++= nodeIdToStage
    }
  }

  /**
   * Function to process SQL Plan Metrics after all events are processed
   */
  def processSQLPlanMetrics(): Unit = {
    connectOperatorToStage
    for ((sqlID, planInfo) <- sqlPlan) {
      checkMetadataForReadSchema(sqlID, planInfo)
      val planGraph = SparkPlanGraph(planInfo)
      // SQLPlanMetric is a case Class of
      // (name: String,accumulatorId: Long,metricType: String)
      val allnodes = planGraph.allNodes
      planGraph.nodes.foreach { n =>
        if (n.isInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster]) {
          val ch = n.asInstanceOf[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster].nodes
          ch.foreach { c =>
            wholeStage += WholeStageCodeGenResults(index, sqlID, n.id, n.name, c.name)
          }
        }
      }
      for (node <- allnodes) {
        checkGraphNodeForReads(sqlID, node)
        if (isDataSetOrRDDPlan(node.desc)) {
          sqlIdToInfo.get(sqlID).foreach { sql =>
            sqlIDToDataSetOrRDDCase += sqlID
            sql.hasDatasetOrRDD = true
          }
          if (gpuMode) {
            val thisPlan = UnsupportedSQLPlan(sqlID, node.id, node.name, node.desc,
              "Contains Dataset or RDD")
            unsupportedSQLplan += thisPlan
          }
        }

        // find potential problems
        val issues = findPotentialIssues(node.desc)
        if (issues.nonEmpty) {
          val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
          sqlIDtoProblematic(sqlID) = existingIssues ++ issues
        }
        val (_, nestedComplexTypes) = reportComplexTypes
        val potentialProbs = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)
        sqlIdToInfo.get(sqlID).foreach { sql =>
          sql.problematic = potentialProbs
        }

        // Then process SQL plan metric type
        for (metric <- node.metrics) {
          val stages = sqlPlanNodeIdToStageIds.get((sqlID, node.id)).getOrElse(Seq.empty)
          val allMetric = SQLMetricInfoCase(sqlID, metric.name,
            metric.accumulatorId, metric.metricType, node.id,
            node.name, node.desc, stages)

          allSQLMetrics += allMetric
          if (this.sqlPlanMetricsAdaptive.nonEmpty) {
            val adaptive = sqlPlanMetricsAdaptive.filter { adaptiveMetric =>
              adaptiveMetric.sqlID == sqlID && adaptiveMetric.accumulatorId == metric.accumulatorId
            }
            adaptive.foreach { adaptiveMetric =>
              val allMetric = SQLMetricInfoCase(sqlID, adaptiveMetric.name,
                adaptiveMetric.accumulatorId, adaptiveMetric.metricType, node.id,
                node.name, node.desc, stages)
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

  def aggregateSQLStageInfo: Seq[SQLStageInfoProfileResult] = {
    val jobsWithSQL = jobIdToInfo.filter { case (id, j) =>
      j.sqlID.nonEmpty
    }
    val sqlToStages = jobsWithSQL.flatMap { case (jobId, j) =>
      val stages = j.stageIds
      val stagesInJob = stageIdToInfo.filterKeys { case (sid, _) =>
        stages.contains(sid)
      }
      stagesInJob.map { case ((s,sa), info) =>
        val nodeIds = sqlPlanNodeIdToStageIds.filter { case (_, v) =>
          v.contains(s)
        }.keys.toSeq
        val nodeNames = sqlPlan.get(j.sqlID.get).map { planInfo =>
          val nodes = SparkPlanGraph(planInfo).allNodes
          val validNodes = nodes.filter { n =>
            nodeIds.contains((j.sqlID.get, n.id))
          }
          validNodes.map(n => s"${n.name}(${n.id.toString})")
        }.getOrElse(null)
        SQLStageInfoProfileResult(index, j.sqlID.get, jobId, s, sa, info.duration, nodeNames)
      }
    }
    sqlToStages.toSeq
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
