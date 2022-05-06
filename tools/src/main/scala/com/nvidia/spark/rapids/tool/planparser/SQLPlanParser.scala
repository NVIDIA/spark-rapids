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
    isSupported: Boolean,
    children: Option[Seq[ExecInfo]]) {
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
      s"${childrenToString}"
  }
}

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
      case c if (c.name == "CartesianProduct") =>
        CartesianProductExecParser(c, checker, sqlID).parse
      case c if (c.name == "Coalesce") =>
        CoalesceExecParser(c, checker, sqlID).parse
      case c if (c.name == "CollectLimit") =>
        CollectLimitExecParser(c, checker, sqlID).parse
      case e if (e.name == "Expand") =>
        ExpandExecParser(e, checker, sqlID).parse
      case f if (f.name == "Filter") =>
        FilterExecParser(f, checker, sqlID).parse
      case g if (g.name == "Generate") =>
        GenerateExecParser(g, checker, sqlID).parse
      case g if (g.name == "GlobalLimit") =>
        GlobalLimitExecParser(g, checker, sqlID).parse
      case l if (l.name == "LocalLimit") =>
        LocalLimitExecParser(l, checker, sqlID).parse
      case p if (p.name == "Project") =>
        ProjectExecParser(p, checker, sqlID).parse
      case r if (r.name == "Range") =>
        RangeExecParser(r, checker, sqlID).parse
      case s if (s.name == "Sample") =>
        SampleExecParser(s, checker, sqlID).parse
      case s if (s.name == "Sort") =>
        SortExecParser(s, checker, sqlID).parse
      case t if (t.name == "TakeOrderedAndProject") =>
        TakeOrderedAndProjectExecParser(t, checker, sqlID).parse
      case u if (u.name == "Union") =>
        UnionExecParser(u, checker, sqlID).parse
      case w if (w.name.contains("WholeStageCodegen")) =>
        WholeStageExecParser(w.asInstanceOf[SparkPlanGraphCluster], checker, sqlID, app).parse
      case o =>
        logDebug(s"other graph node ${node.name} desc: ${node.desc} id: ${node.id}")
        ArrayBuffer(ExecInfo(sqlID, o.name, expr = "", 1, duration = None, o.id,
          isSupported = false, None))
    }
  }

  /**
   * This function is used to calculate an average speedup factor. The input
   * is assumed to an array of ints where each element is >= 1. If the input array
   * is empty we return 1 because we assume we don't slow things down. Generally
   * the array shouldn't be empty, but if there is some weird case we don't want to
   * blow up, just say we don't speed it up.
   */
  def averageSpeedup(arr: ArrayBuffer[Int]): Int = if (arr.isEmpty) 1 else arr.sum / arr.size

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
    taskForAccum.map(_.value.getOrElse(0L))
    val maxDuration = if (accumValues.isEmpty) {
      None
    } else {
      Some(accumValues.max)
    }
    maxDuration
  }

}
