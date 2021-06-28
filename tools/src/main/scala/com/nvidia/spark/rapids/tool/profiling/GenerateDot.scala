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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.execution.{SparkPlanInfo, WholeStageCodegenExec}
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.tool.profiling.{ApplicationInfo, SparkPlanInfoWithStage}

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
  val GPU_COLOR = "#76b900" // NVIDIA Green
  val CPU_COLOR = "#0071c5"
  val TRANSITION_COLOR = "red"

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

  /**
   * Generate a query plan visualization in dot format.
   *
   * @param plan First query plan and metrics
   * @param physicalPlanString The physical plan as a String
   * @param stageIdToStageMetrics metrics for teh stages.
   * @param sqlId id of the SQL query for the dot graph
   * @param appId Spark application Id
   */
  def writeDotGraph(plan: QueryPlanWithMetrics,
      physicalPlanString: String,
      stageIdToStageMetrics: Map[Int, StageMetrics],
      fileWriter: ToolTextFileWriter,
      sqlId: Long,
      appId: String): Unit = {
    val graph = SparkPlanGraph(plan.plan, appId, sqlId.toString, physicalPlanString,
      stageIdToStageMetrics)
    val str = graph.makeDotFile(plan.metrics)
    fileWriter.write(str)
  }

  def apply(app: ApplicationInfo, outputDirectory: String): Unit = {
    val accums = app.runQuery(app.generateSQLAccums)

    val accumIdToStageId = app.accumIdToStageId

    val formatter = java.text.NumberFormat.getIntegerInstance

    val stageIdToStageMetrics = app.taskEnd.groupBy(task => task.stageId).mapValues { tasks =>
      val durations = tasks.map(_.duration)
      val numTasks = durations.length
      val minDur = durations.min
      val maxDur = durations.max
      val meanDur = durations.sum/numTasks.toDouble
      StageMetrics(numTasks,
        s"MIN: ${formatter.format(minDur)} ms " +
            s"MAX: ${formatter.format(maxDur)} ms " +
            s"AVG: ${formatter.format(meanDur)} ms")
    }

    val accumSummary = accums
        .select(col("sqlId"), col("accumulatorId"), col("max_value"))
        .collect()
    val sqlIdToMaxMetric = new mutable.HashMap[Long, ArrayBuffer[(Long,Long)]]()
    for (row <- accumSummary) {
      val list = sqlIdToMaxMetric.getOrElseUpdate(row.getLong(0),
        new ArrayBuffer[(Long, Long)]())
      list += row.getLong(1) -> row.getLong(2)
    }

    val sqlPlansMap = app.sqlPlan.map { case (sqlId, sparkPlanInfo) =>
      sqlId -> ((sparkPlanInfo, app.physicalPlanDescription(sqlId)))
    }
    for ((sqlID,  (planInfo, physicalPlan)) <- sqlPlansMap) {
      val dotFileWriter = new ToolTextFileWriter(outputDirectory,
        s"${app.appId}-query-$sqlID.dot")
      try {
        val metrics = sqlIdToMaxMetric.getOrElse(sqlID, Seq.empty).toMap
        GenerateDot.writeDotGraph(
          QueryPlanWithMetrics(SparkPlanInfoWithStage(planInfo, accumIdToStageId), metrics),
          physicalPlan, stageIdToStageMetrics, dotFileWriter, sqlID, app.appId)
      } finally {
        dotFileWriter.close()
      }
    }
  }
}

/**
 * Query plan with metrics.
 *
 * @param plan Query plan.
 * @param metrics Map of accumulatorId to metric.
 */
case class QueryPlanWithMetrics(plan: SparkPlanInfoWithStage, metrics: Map[Long, Long])

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with additions/changes to fit our needs.
 * A graph used for storing information of an executionPlan of DataFrame.
 *
 * Each graph is defined with a set of nodes and a set of edges. Each node represents a node in the
 * SparkPlan tree, and each edge represents a parent-child relationship between two nodes.
 */
case class SparkPlanGraph(
    nodes: Seq[SparkPlanGraphNode],
    edges: Seq[SparkPlanGraphEdge],
    appId: String,
    sqlId: String,
    physicalPlan: String) {

  def makeDotFile(metrics: Map[Long, Long]): String = {
    val queryLabel = SparkPlanGraph.makeDotLabel(appId, sqlId, physicalPlan)

    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    dotFile.append(s"label=$queryLabel\n")
    dotFile.append("labelloc=b\n")
    dotFile.append("fontname=Courier\n")
    dotFile.append(s"""tooltip="APP: $appId Query: $sqlId"\n""")

    nodes.foreach(node => dotFile.append(node.makeDotNode(metrics) + "\n"))
    edges.foreach(edge => dotFile.append(edge.makeDotEdge + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }

  /**
   * All the SparkPlanGraphNodes, including those inside of WholeStageCodegen.
   */
  val allNodes: Seq[SparkPlanGraphNode] = {
    nodes.flatMap {
      case cluster: SparkPlanGraphCluster => cluster.nodes :+ cluster
      case node => Seq(node)
    }
  }
}

object SparkPlanGraph {

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfoWithStage,
      appId: String,
      sqlId: String,
      physicalPlan: String,
      stageIdToStageMetrics: Map[Int, StageMetrics]): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    val exchanges = mutable.HashMap[SparkPlanInfoWithStage, SparkPlanGraphNode]()
    buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, null, exchanges,
      stageIdToStageMetrics)
    new SparkPlanGraph(nodes, edges, appId, sqlId, physicalPlan)
  }

  @tailrec
  def isGpuPlan(plan: SparkPlanInfo): Boolean = {
    plan.nodeName match {
      case name if name contains "QueryStage" =>
        plan.children.isEmpty || isGpuPlan(plan.children.head)
      case name if name == "ReusedExchange" =>
        plan.children.isEmpty || isGpuPlan(plan.children.head)
      case name =>
        name.startsWith("Gpu") || name.startsWith("Execute Gpu")
    }
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfoWithStage,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      codeGen: SparkPlanGraphCluster,
      stage: StageGraphCluster,
      exchanges: mutable.HashMap[SparkPlanInfoWithStage, SparkPlanGraphNode],
      stageIdToStageMetrics: Map[Int, StageMetrics]): Unit = {

    def getOrMakeStage(planInfo: SparkPlanInfoWithStage): StageGraphCluster = {
      val stageId = planInfo.stageId
      val retStage = if (stage == null ||
          (stageId.nonEmpty && stage.getStageId > 0 && stage.getStageId != stageId.head)) {
        val ret = new StageGraphCluster(nodeIdGenerator.getAndIncrement(), stageIdToStageMetrics)
        nodes += ret
        ret
      } else {
        stage
      }
      stageId.foreach { stageId =>
        retStage.setStage(stageId)
      }
      retStage
    }

    planInfo.nodeName match {
      case name if name.startsWith("WholeStageCodegen") =>

        val codeGenCluster = new SparkPlanGraphCluster(
          nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName,
          planInfo.simpleString,
          mutable.ArrayBuffer[SparkPlanGraphNode](),
          planInfo.metrics)
        val s = getOrMakeStage(planInfo)
        s.nodes += codeGenCluster

        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, codeGenCluster,
          s, exchanges, stageIdToStageMetrics)
      case "InputAdapter" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, stage, exchanges,
          stageIdToStageMetrics)
      case "BroadcastQueryStage" | "ShuffleQueryStage" =>
        if (exchanges.contains(planInfo.children.head)) {
          // Point to the re-used exchange
          val node = exchanges(planInfo.children.head)
          edges += SparkPlanGraphEdge(node, parent)
        } else {
          buildSparkPlanGraphNode(
            planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, null, exchanges,
            stageIdToStageMetrics)
        }
      case "Subquery" if codeGen != null =>
        // Subquery should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null,
          null, exchanges, stageIdToStageMetrics)
      case "Subquery" if exchanges.contains(planInfo) =>
        // Point to the re-used subquery
        val node = exchanges(planInfo)
        edges += SparkPlanGraphEdge(node, parent)
      case "SubqueryBroadcast" if codeGen != null =>
        // SubqueryBroadcast should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null,
          null, exchanges, stageIdToStageMetrics)
      case "SubqueryBroadcast" if exchanges.contains(planInfo) =>
        // Point to the re-used SubqueryBroadcast
        val node = exchanges(planInfo)
        edges += SparkPlanGraphEdge(node, parent)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, codeGen, stage, exchanges,
          stageIdToStageMetrics)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += SparkPlanGraphEdge(node, parent)
      case name =>
        val metrics = planInfo.metrics
        val node = new SparkPlanGraphNode(
          nodeIdGenerator.getAndIncrement(), planInfo.nodeName,
          planInfo.simpleString, metrics, isGpuPlan(planInfo))

        val s = if (name.contains("Exchange") ||
            name == "Subquery" ||
            name == "SubqueryBroadcast") {
          exchanges += planInfo -> node
          null
        } else {
          getOrMakeStage(planInfo)
        }

        val toAddTo = Option(codeGen).map(_.nodes).getOrElse {
          Option(s).map(_.nodes).getOrElse(nodes)
        }
        toAddTo += node

        if (parent != null) {
          edges += SparkPlanGraphEdge(node, parent)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, codeGen, s,
            exchanges, stageIdToStageMetrics))
    }
  }

  val htmlLineBreak = """<br align="left"/>""" + "\n"

  def makeDotLabel(
    appId: String,
    sqlId: String,
    physicalPlan: String,
    maxLength: Int = 16384
  ): String = {
    val sqlPlanPlaceHolder = "%s"
    val queryLabelFormat =
      s"""<<table border="0">
         |<tr><td>Application: $appId, Query: $sqlId</td></tr>
         |<tr><td>$sqlPlanPlaceHolder</td></tr>
         |<tr><td>Large physical plans may be truncated. See output from
         |--print-plans captioned "Plan for SQL ID : $sqlId"
         |</td></tr>
         |</table>>""".stripMargin

    // pre-calculate size post substitutions
    val formatBytes = queryLabelFormat.length() - sqlPlanPlaceHolder.length()
    val numLinebreaks = physicalPlan.count(_ == '\n')
    val lineBreakBytes = numLinebreaks * htmlLineBreak.length()
    val maxPlanLength = maxLength - formatBytes - lineBreakBytes

    queryLabelFormat.format(
      physicalPlan.take(maxPlanLength)
        .replaceAll("\n", htmlLineBreak)
    )
  }
}

/**
 * Represent a node in the SparkPlan tree, along with its metrics.
 *
 * @param id generated by "SparkPlanGraph". There is no duplicate id in a graph
 * @param name the name of this SparkPlan node
 * @param metrics metrics that this SparkPlan node will track
 */
class SparkPlanGraphNode(
    val id: Long,
    val name: String,
    val desc: String,
    val metrics: Seq[SQLMetricInfo],
    val isGpuNode: Boolean) {

  def makeDotNode(metricsValue: Map[Long, Long]): String = {
    val builder = new mutable.StringBuilder(name)

    val values = for {
      metric <- metrics
      value <- metricsValue.get(metric.accumulatorId)
    } yield {
      s"${metric.name}: ${GenerateDot.formatMetric(metric, value)}"
    }

    val color = if (isGpuNode) { GenerateDot.GPU_COLOR } else { GenerateDot.CPU_COLOR }

    if (values.nonEmpty) {
      // If there are metrics, display each entry in a separate line.
      // Note: whitespace between two "\n"s is to create an empty line between the name of
      // SparkPlan and metrics. If removing it, it won't display the empty line in UI.
      builder ++= "\n\n"
      builder ++= values.mkString("\n")
      val labelStr = StringEscapeUtils.escapeJava(builder.toString())
      s"""  $id [shape=box,style="filled",color="$color",label="$labelStr",tooltip="$desc"];"""
    } else {
      // SPARK-30684: when there is no metrics, add empty lines to increase the height of the node,
      // so that there won't be gaps between an edge and a small node.
      s"""  $id [shape=box,style="filled",color="$color",label="\\n$name\\n\\n"];"""
    }
  }

  override def toString: String = s"NODE $name ($id)"
}

/**
 * A sub part of the plan. This may be used for WholeStageCodegen or for a Stage
 */
class SparkPlanGraphCluster(
    id: Long,
    name: String,
    desc: String,
    val nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    metrics: Seq[SQLMetricInfo])
    extends SparkPlanGraphNode(id, name, desc, metrics, isGpuNode = false) {

  override def makeDotNode(metricsValue: Map[Long, Long]): String = {
    val duration = metrics.filter(_.name.startsWith(WholeStageCodegenExec.PIPELINE_DURATION_METRIC))
    val labelStr = if (duration.nonEmpty) {
      require(duration.length == 1)
      val id = duration.head.accumulatorId
      if (metricsValue.contains(id)) {
        name + "\n \n" + duration.head.name + ": " + metricsValue(id)
      } else {
        name
      }
    } else {
      name
    }
    s"""
       |  subgraph cluster$id {
       |    isCluster="true";
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
       |    ${nodes.map(_.makeDotNode(metricsValue)).mkString("    \n")}
       |  }
     """.stripMargin
  }
}

class StageGraphCluster(
    id: Long,
    val stageIdToStageMetrics: Map[Int, StageMetrics])
    extends SparkPlanGraphCluster(id, "STAGE", "STAGE", mutable.ArrayBuffer.empty, Seq.empty) {
  private var stageId = -1

  def setStage(stageId: Int): Unit = {
    if (this.stageId >= 0) {
      require(this.stageId == stageId, s"Trying to set multiple ids for a single " +
          s"stage ${this.stageId} != $stageId")
    }
    this.stageId = stageId
  }

  def getStageId: Int = stageId

  override def makeDotNode(metricsValue: Map[Long, Long]): String = {
    val labelStr = if (stageId < 0) {
      "UNKNOWN STAGE\n"
    } else {
      val m = stageIdToStageMetrics.get(stageId).map { metrics =>
        s"""
           |numTasks: ${metrics.numTasks}

           |duration: ${metrics.duration}

           |
      """.stripMargin}.getOrElse("")
      s"STAGE $stageId\n$m"
    }

    s"""
       |  subgraph cluster$id {
       |    isCluster="true";
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
       |    ${nodes.map(_.makeDotNode(metricsValue)).mkString("    \n")}
       |  }
     """.stripMargin
  }

  override def toString: String =
    s"STAGE: $stageId ($id)"
}


/**
 * Represent an edge in the SparkPlan tree. `fromId` is the child node id, and `toId` is the parent
 * node id.
 */
case class SparkPlanGraphEdge(from: SparkPlanGraphNode, to: SparkPlanGraphNode) {

  def makeDotEdge: String = {
    val color = (from.isGpuNode, to.isGpuNode) match {
      case (true, true) => GenerateDot.GPU_COLOR
      case (false, false) => GenerateDot.CPU_COLOR
      case _ => GenerateDot.TRANSITION_COLOR
    }
    s"""  ${from.id}->${to.id} [color="$color",style=bold];\n"""
  }
}