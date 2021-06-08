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

import java.io.{File, FileWriter}
import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo

/**
 * Generate a DOT graph for one query plan, or showing differences between two query plans.
 *
 * Diff mode is intended for comparing query plans that are expected to have the same
 * structure, such as two different runs of the same query but with different tuning options.
 *
 * When running in diff mode, any differences in SQL metrics are shown. Also, if the plan
 * starts to deviate then the graph will show where the plans deviate and will not recurse
 * further.
 *
 * Graphviz and other tools can be used to generate images from DOT files.
 *
 * See https://graphviz.org/pdf/dotguide.pdf for a description of DOT files.
 */
object GenerateDot {
  private val GPU_COLOR = "#76b900" // NVIDIA Green
  private val CPU_COLOR = "#0071c5"
  private val TRANSITION_COLOR = "red"

  /**
   * Generate a query plan visualization in dot format.
   *
   * @param plan First query plan and metrics
   * @param comparisonPlan Optional second query plan and metrics
   * @param filename Filename to write dot graph to
   */
  def generateDotGraph(
      plan: QueryPlanWithMetrics,
      comparisonPlan: Option[QueryPlanWithMetrics],
      fileWriter: ToolTextFileWriter,
      filename: String): Unit = {

    var nextId = 1

    def isGpuPlan(plan: SparkPlanInfo): Boolean = {
      plan.nodeName match {
        case name if name contains "QueryStage" =>
          plan.children.isEmpty || isGpuPlan(plan.children.head)
        case name if name == "ReusedExchange" =>
          plan.children.isEmpty || isGpuPlan(plan.children.head)
        case name =>
          name.startsWith("Gpu")
      }
    }

    def formatMetric(m: SQLMetricInfo, value: Long): String = {
      val formatter = java.text.NumberFormat.getIntegerInstance
      m.metricType match {
        case "timing" =>
          val ms = value
          s"${formatter.format(ms)} ms"
        case "nsTiming" =>
          val ms = TimeUnit.NANOSECONDS.toMillis(value)
          s"${formatter.format(ms)} ms"
        case _ =>
          s"${formatter.format(value)}"
      }
    }

    /** Recursively graph the operator nodes in the spark plan */
    def writeGraph(
        w: ToolTextFileWriter,
        node: QueryPlanWithMetrics,
        comparisonNode: QueryPlanWithMetrics,
        id: Int = 0): Unit = {

      val nodePlan = node.plan
      val comparisonPlan = comparisonNode.plan
      if (nodePlan.nodeName == comparisonPlan.nodeName &&
        nodePlan.children.length == comparisonPlan.children.length) {

        val metricNames = (nodePlan.metrics.map(_.name) ++
          comparisonPlan.metrics.map(_.name)).distinct.sorted

        val metrics = metricNames.flatMap(name => {
          val l = nodePlan.metrics.find(_.name == name)
          val r = comparisonPlan.metrics.find(_.name == name)
          (l, r) match {
            case (Some(metric1), Some(metric2)) =>
              (node.metrics.get(metric1.accumulatorId),
                comparisonNode.metrics.get(metric1.accumulatorId)) match {
                case (Some(value1), Some(value2)) =>
                  if (value1 == value2) {
                    Some(s"$name: ${formatMetric(metric1, value1)}")
                  } else {
                    metric1.metricType match {
                      case "nsTiming" | "timing" =>
                        val pctStr = createPercentDiffString(value1, value2)
                        Some(s"$name: ${formatMetric(metric1, value1)} / " +
                          s"${formatMetric(metric2, value2)} ($pctStr %)")
                      case _ =>
                        Some(s"$name: ${formatMetric(metric1, value1)} / " +
                          s"${formatMetric(metric2, value2)}")
                    }
                  }
                case _ => None
              }
            case _ => None
          }
        }).mkString("\n")

        val color = if (isGpuPlan(nodePlan)) { GPU_COLOR } else { CPU_COLOR }

        val label = if (nodePlan.nodeName.contains("QueryStage")) {
          nodePlan.simpleString
        } else {
          nodePlan.nodeName
        }

        val nodeText =
          s"""node$id [shape=box,color="$color",style="filled",
             |label = "$label\n
             |$metrics"];
             |""".stripMargin

        w.write(nodeText)
        nodePlan.children.indices.foreach(i => {
          val childId = nextId
          nextId += 1
          writeGraph(
            w,
            QueryPlanWithMetrics(nodePlan.children(i), node.metrics),
            QueryPlanWithMetrics(comparisonPlan.children(i), comparisonNode.metrics),
            childId);

          val style = (isGpuPlan(nodePlan), isGpuPlan(nodePlan.children(i))) match {
            case (true, true) => s"""color="$GPU_COLOR""""
            case (false, false) => s"""color="$CPU_COLOR""""
            case _ =>
              // show emphasis on transitions between CPU and GPU
              s"color=$TRANSITION_COLOR, style=bold"
          }
          w.write(s"node$childId -> node$id [$style];\n")
        })
      } else {
        // plans have diverged - cannot recurse further
        w.write(
          s"""node$id [shape=box, color=red,
             |label = "plans diverge here:
             |${nodePlan.nodeName} vs ${comparisonPlan.nodeName}"];\n""".stripMargin)
      }
    }

    // write the dot graph to a file
    fileWriter.write("digraph G {\n")
    writeGraph(fileWriter, plan, comparisonPlan.getOrElse(plan), 0)
    fileWriter.write("}\n")
  }

  private def createPercentDiffString(n1: Long, n2: Long) = {
    val pct = (n2 - n1) * 100.0 / n1
    val pctStr = if (pct < 0) {
      f"$pct%.1f"
    } else {
      f"+$pct%.1f"
    }
    pctStr
  }
}

/**
 * Query plan with metrics.
 *
 * @param plan Query plan.
 * @param metrics Map of accumulatorId to metric.
 */
case class QueryPlanWithMetrics(plan: SparkPlanInfo, metrics: Map[Long, Long])