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
import org.apache.spark.sql.rapids.tool.AppBase

case class ExecInfo(
    sqlID: Long,
    exec: String,
    expr: String,
    speedupFactor: Int,
    duration: Option[Long],
    nodeId: Long,
    wholeStageId: Option[Long],
    isSupported: Boolean)

case class PlanInfo(
    sqlID: Long,
    execInfo: Seq[ExecInfo]
)

object SQLPlanParser extends Logging {

  def parseSQLPlan(
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
    PlanInfo(sqlID, execInfos)
  }

  def parsePlanNode(
      node: SparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase
  ): Seq[ExecInfo] = {
    node match {
      case w if (w.name.contains("WholeStageCodegen")) =>
        WholeStageExecParser(w.asInstanceOf[SparkPlanGraphCluster], checker, sqlID, app).parse
      case f if (f.name == "Filter") =>
        FilterExecParser(f, checker, sqlID, app).parse
      case p if (p.name == "Project") =>
        ProjectExecParser(p, checker, sqlID, app).parse
      case o =>
        logDebug(s"other graph node ${node.name} desc: ${node.desc} id: ${node.id}")
        ArrayBuffer(ExecInfo(sqlID, o.name, expr = "", 1, duration = Some(0), o.id,
          wholeStageId = None, isSupported = false))
    }
  }

  def average(arr: ArrayBuffer[Int]): Int = if (arr.isEmpty) 0 else arr.sum/arr.size

  // We get the total duration by finding the accumulator with the largest value.
  // This is because each accumulator has a value and an update. As tasks end
  // they just update the value = value + update, so the largest value will be
  // the duration.
  def getTotalDuration(accumId: Option[Long], app: AppBase): Option[Long] = {
    val taskForAccum = accumId.flatMap(id => app.taskStageAccumMap.get(id))
      .getOrElse(ArrayBuffer.empty)
    val accumValues = taskForAccum.map(_.value.getOrElse(0L))
    taskForAccum.map(_.value.getOrElse(0L))
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

}
