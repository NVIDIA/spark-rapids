/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}

class ExecInfo(
    val sqlID: Long,
    val exec: String,
    val expr: String,
    val speedupFactor: Double,
    val duration: Option[Long],
    val nodeId: Long,
    val isSupported: Boolean,
    val children: Option[Seq[ExecInfo]], // only one level deep
    val stages: Seq[Int] = Seq.empty,
    val shouldRemove: Boolean = false) {
  private def childrenToString = {
    val str = children.map { c =>
      c.map("       " + _.toString).mkString("\n")
    }.getOrElse("")
    if (str.nonEmpty) {
      "\n" + str
    } else {
      str
    }
  }
  override def toString: String = {
    s"exec: $exec, expr: $expr, sqlID: $sqlID , speedupFactor: $speedupFactor, " +
      s"duration: $duration, nodeId: $nodeId, " +
      s"isSupported: $isSupported, children: " +
      s"${childrenToString}, stages: ${stages.mkString(",")}, " +
      s"shouldRemove: $shouldRemove"
  }
}

case class PlanInfo(
    appID: String,
    sqlID: Long,
    execInfo: Seq[ExecInfo],
    stageIdsNotInExecs: Set[Int]
)

object SQLPlanParser extends Logging {

  def parseSQLPlan(
      appID: String,
      planInfo: SparkPlanInfo,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase): PlanInfo = {
    val planGraph = SparkPlanGraph(planInfo)
    // we want the sub-graph nodes to be inside of the wholeStageCodeGen so use nodes
    // vs allNodes
    val execInfos = planGraph.nodes.flatMap { node =>
      parsePlanNode(node, sqlID, checker, app)
    }
    // also get the job ids associated with the SQLId
    val jobsIdsInSQLQuery = app.jobIdToSqlID.filter { case (_, sqlIdForJob) =>
      sqlIdForJob == sqlID
    }.keys.toSeq
    val allStagesBasedOnJobs = jobsIdsInSQLQuery.flatMap { jId =>
      app.jobIdToInfo(jId).stageIds
    }
    val allStagesInExecs = execInfos.flatMap(_.stages)
    // all stages should be in both, just double check
    val stagesInBoth = allStagesBasedOnJobs.toSet.intersect(allStagesInExecs.toSet)
    if (stagesInBoth.size != allStagesInExecs.size) {
      logError("Something is wrong, all the stages from execs are not in the jobs stages")
    }
    val stagesNotInExecs = allStagesBasedOnJobs.toSet.diff(allStagesInExecs.toSet)
    logWarning(s"Stages not in execs are: $stagesNotInExecs")
    PlanInfo(appID, sqlID, execInfos, stagesNotInExecs)
  }

  def getStagesInSQLNode(node: SparkPlanGraphNode, app: AppBase): Seq[Int] = {
    val nodeAccums = node.metrics.map(_.accumulatorId)
    app.stageAccumulators.flatMap { case (stageId, stageAccums) =>
      if (nodeAccums.intersect(stageAccums).nonEmpty) {
        Some(stageId)
      } else {
        None
      }
    }.toSeq
  }

  private val skipUDFCheckExecs = Seq("ArrowEvalPython", "AggregateInPandas",
    "FlatMapGroupsInPandas", "MapInPandas", "WindowInPandas")

  def parsePlanNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase
  ): Seq[ExecInfo] = {
    if (node.name.contains("WholeStageCodegen")) {
      // this is special because it is a SparkPlanGraphCluster vs SparkPlanGraphNode
      WholeStageExecParser(node.asInstanceOf[SparkPlanGraphCluster], checker, sqlID, app).parse
    } else {
      val execInfos = node.name match {
        case "AggregateInPandas" =>
          AggregateInPandasExecParser(node, checker, sqlID).parse
        case "ArrowEvalPython" =>
          ArrowEvalPythonExecParser(node, checker, sqlID).parse
        case "BatchScan" =>
          BatchScanExecParser(node, checker, sqlID, app).parse
        case "BroadcastExchange" =>
          BroadcastExchangeExecParser(node, checker, sqlID, app).parse
        case "BroadcastHashJoin" =>
          BroadcastHashJoinExecParser(node, checker, sqlID).parse
        case "BroadcastNestedLoopJoin" =>
          BroadcastNestedLoopJoinExecParser(node, checker, sqlID).parse
        case "CartesianProduct" =>
          CartesianProductExecParser(node, checker, sqlID).parse
        case "Coalesce" =>
          CoalesceExecParser(node, checker, sqlID).parse
        case "CollectLimit" =>
          CollectLimitExecParser(node, checker, sqlID).parse
        case "ColumnarToRow" =>
          // ignore ColumnarToRow to row for now as assume everything is columnar
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None, Seq.empty, shouldRemove=true)
        case c if (c.contains("CreateDataSourceTableAsSelectCommand")) =>
          // create data source table doesn't show the format so we can't determine
          // if we support it
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None)
        case "CustomShuffleReader" | "AQEShuffleRead" =>
          CustomShuffleReaderExecParser(node, checker, sqlID).parse
        case "Exchange" =>
          ShuffleExchangeExecParser(node, checker, sqlID, app).parse
        case "Expand" =>
          ExpandExecParser(node, checker, sqlID).parse
        case "Filter" =>
          FilterExecParser(node, checker, sqlID).parse
        case "FlatMapGroupsInPandas" =>
          FlatMapGroupsInPandasExecParser(node, checker, sqlID).parse
        case "Generate" =>
          GenerateExecParser(node, checker, sqlID).parse
        case "GlobalLimit" =>
          GlobalLimitExecParser(node, checker, sqlID).parse
        case "HashAggregate" =>
          HashAggregateExecParser(node, checker, sqlID, app).parse
        case "LocalLimit" =>
          LocalLimitExecParser(node, checker, sqlID).parse
        case "InMemoryTableScan" =>
          InMemoryTableScanExecParser(node, checker, sqlID).parse
        case i if (i.contains("InsertIntoHadoopFsRelationCommand") ||
          i == "DataWritingCommandExec") =>
          DataWritingCommandExecParser(node, checker, sqlID).parse
        case "MapInPandas" =>
          MapInPandasExecParser(node, checker, sqlID).parse
        case "ObjectHashAggregate" =>
          ObjectHashAggregateExecParser(node, checker, sqlID, app).parse
        case "Project" =>
          ProjectExecParser(node, checker, sqlID).parse
        case "Range" =>
          RangeExecParser(node, checker, sqlID).parse
        case "Sample" =>
          SampleExecParser(node, checker, sqlID).parse
        case "ShuffledHashJoin" =>
          ShuffledHashJoinExecParser(node, checker, sqlID, app).parse
        case "Sort" =>
          SortExecParser(node, checker, sqlID).parse
        case s if (s.startsWith("Scan")) =>
          FileSourceScanExecParser(node, checker, sqlID, app).parse
        case "SortAggregate" =>
          SortAggregateExecParser(node, checker, sqlID).parse
        case "SortMergeJoin" =>
          SortMergeJoinExecParser(node, checker, sqlID).parse
        case "SubqueryBroadcast" =>
          SubqueryBroadcastExecParser(node, checker, sqlID, app).parse
        case "TakeOrderedAndProject" =>
          TakeOrderedAndProjectExecParser(node, checker, sqlID).parse
        case "Union" =>
          UnionExecParser(node, checker, sqlID).parse
        case "Window" =>
          WindowExecParser(node, checker, sqlID).parse
        case "WindowInPandas" =>
          WindowInPandasExecParser(node, checker, sqlID).parse
        case _ =>
          new ExecInfo(sqlID, node.name, expr = "", 1, duration = None, node.id,
            isSupported = false, None)
      }
      // check is the node has a dataset operations and if so change to not supported
      val ds = app.isDataSetOrRDDPlan(node.desc)
      // we don't want to mark the *InPandas and ArrowEvalPythonExec as unsupported with UDF
      val containsUDF = if (skipUDFCheckExecs.contains(node.name)) {
        false
      } else {
        app.containsUDF(node.desc)
      }
      val stagesInNode = getStagesInSQLNode(node, app)
      val supported = execInfos.isSupported && !ds && !containsUDF
      Seq(new ExecInfo(execInfos.sqlID, execInfos.exec, execInfos.expr, execInfos.speedupFactor,
        execInfos.duration, execInfos.nodeId, supported, execInfos.children,
        stagesInNode, execInfos.shouldRemove))
    }
  }

  /**
   * This function is used to calculate an average speedup factor. The input
   * is assumed to an array of doubles where each element is >= 1. If the input array
   * is empty we return 1 because we assume we don't slow things down. Generally
   * the array shouldn't be empty, but if there is some weird case we don't want to
   * blow up, just say we don't speed it up.
   */
  def averageSpeedup(arr: Seq[Double]): Double = {
    if (arr.isEmpty) {
      1.0
    } else {
      val sum = arr.sum
      ToolUtils.calculateAverage(sum, arr.size, 2)
    }
  }

  /**
   * Get the total duration by finding the accumulator with the largest value.
   * This is because each accumulator has a value and an update. As tasks end
   * they just update the value = value + update, so the largest value will be
   * the duration.
   */
  def getTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val taskForAccum = accumId.flatMap(id => app.taskStageAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = taskForAccum.map(_.value.getOrElse(0L))
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

  def getDriverTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val accums = accumId.flatMap(id => app.driverAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = accums.map(_.value)
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

}
