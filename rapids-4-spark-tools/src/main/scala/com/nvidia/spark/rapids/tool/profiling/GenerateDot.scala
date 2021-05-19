package com.nvidia.spark.rapids.tool.profiling

import java.io.{File, FileWriter}
import java.util.concurrent.TimeUnit

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
   * @param includeCodegen Include WholeStageCodegen and InputAdapter nodes, if true
   */
  def generateDotGraph(
      plan: QueryPlanWithMetrics,
      comparisonPlan: Option[QueryPlanWithMetrics],
      dir: File,
      filename: String,
      includeCodegen: Boolean): Unit = {

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

    /**
     * Optionally remove code-gen nodes.
     */
    def normalize(plan: SparkPlanInfo): SparkPlanInfo = {
      if (!includeCodegen
        && (plan.nodeName.startsWith("WholeStageCodegen")
        || plan.nodeName.startsWith("InputAdapter"))) {
        plan.children.head
      } else {
        plan
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
        w: FileWriter,
        node: QueryPlanWithMetrics,
        comparisonNode: QueryPlanWithMetrics,
        id: Int = 0): Unit = {

      val nodeNormalized = normalize(node.plan)
      val comparisonNodeNormalized = normalize(comparisonNode.plan)
      if (nodeNormalized.nodeName == comparisonNodeNormalized.nodeName &&
        nodeNormalized.children.length == comparisonNodeNormalized.children.length) {

        val metricNames = (nodeNormalized.metrics.map(_.name) ++
          comparisonNodeNormalized.metrics.map(_.name)).distinct.sorted

        val metrics = metricNames.flatMap(name => {
          val l = nodeNormalized.metrics.find(_.name == name)
          val r = comparisonNodeNormalized.metrics.find(_.name == name)
          if (l.isDefined && r.isDefined) {
            val metric1 = l.get
            val metric2 = r.get
            (node.metrics.get(metric1.accumulatorId),
              comparisonNode.metrics.get(metric1.accumulatorId)) match {
              case (Some(value1), Some(value2)) =>
                if (value1 == value2) {
                  Some(s"$name: ${formatMetric(metric1, value1)}")
                } else {
                  metric1.metricType match {
                    case "nsTiming" | "timing" =>
                      val n1 = value1
                      val n2 = value2
                      val pct = (n2 - n1) * 100.0 / n1
                      val pctStr = if (pct < 0) {
                        f"$pct%.1f"
                      } else {
                        f"+$pct%.1f"
                      }
                      Some(s"$name: ${formatMetric(metric1, value1)} / " +
                        s"${formatMetric(metric2, value2)} ($pctStr %)")
                    case _ =>
                      Some(s"$name: ${formatMetric(metric1, value1)} / " +
                        s"${formatMetric(metric2, value2)}")
                  }
                }
              case _ => None
            }
          } else {
            None
          }
        }).mkString("\n")

        val color = if (isGpuPlan(nodeNormalized)) { GPU_COLOR } else { CPU_COLOR }

        val label = if (nodeNormalized.nodeName.contains("QueryStage")) {
          nodeNormalized.simpleString
        } else {
          nodeNormalized.nodeName
        }

        val nodeText =
          s"""node$id [shape=box,color="$color",style="filled",
             |label = "$label\n
             |$metrics"];
             |""".stripMargin

        w.write(nodeText)
        nodeNormalized.children.indices.foreach(i => {
          val childId = nextId
          nextId += 1
          writeGraph(
            w,
            QueryPlanWithMetrics(nodeNormalized.children(i), node.metrics),
            QueryPlanWithMetrics(comparisonNodeNormalized.children(i), comparisonNode.metrics),
            childId);

          val style = (isGpuPlan(nodeNormalized), isGpuPlan(nodeNormalized.children(i))) match {
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
             |${nodeNormalized.nodeName} vs ${comparisonNodeNormalized.nodeName}"];\n""".stripMargin)
      }
    }

    // write the dot graph to a file
    val file = new File(dir, filename)
    println(s"Writing ${file.getAbsolutePath}")
    val w = new FileWriter(file)
    w.write("digraph G {\n")
    writeGraph(w, plan, comparisonPlan.getOrElse(plan), 0)
    w.write("}\n")
    w.close()
  }
}

/**
 * Query plan with metrics.
 *
 * @param plan Query plan.
 * @param metrics Map of accumulatorId to metric.
 */
case class QueryPlanWithMetrics(plan: SparkPlanInfo, metrics: Map[Long, Long])