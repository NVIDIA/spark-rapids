/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuWindowExecMeta(windowExec: WindowExec,
                        conf: RapidsConf,
                        parent: Option[RapidsMeta[_, _, _]],
                        rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowExec](windowExec, conf, parent, rule) {

  /**
   * Fetches WindowExpressions in input `windowExec`, via reflection.
   * As a byproduct, determines whether to return the original input columns,
   * as part of the output.
   *
   * (Spark versions that use `projectList` expect result columns
   * *not* to include the input columns.
   * Apache Spark expects the input columns, before the aggregation output columns.)
   *
   * @return WindowExpressions within windowExec,
   *         and a boolean, indicating the result column semantics
   *         (i.e. whether result columns should be returned *without* including the
   *         input columns).
   */
  def getWindowExpression: (Seq[NamedExpression], Boolean) = {
    var resultColumnsOnly : Boolean = false
    val expr = try {
      val resultMethod = windowExec.getClass.getMethod("windowExpression")
      resultMethod.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        resultColumnsOnly = true
        val winExpr = windowExec.getClass.getMethod("projectList")
        winExpr.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    }
    (expr, resultColumnsOnly)
  }

  private val (inputWindowExpressions, resultColumnsOnly) = getWindowExpression

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    inputWindowExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    windowExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    windowExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
        "cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

  }

  override def convertToGpu(): GpuExec = {
    GpuWindowExec(
      windowExpressions.map(_.convertToGpu()),
      partitionSpec.map(_.convertToGpu()),
      orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
      childPlans.head.convertIfNeeded(),
      resultColumnsOnly
    )
  }
}

case class GpuWindowExec(
    windowExpressionAliases: Seq[Expression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan,
    resultColumnsOnly: Boolean
  ) extends UnaryExecNode with GpuExec {

  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, OP_TIME)
  )

  override def output: Seq[Attribute] = if (resultColumnsOnly) {
    windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  } else {
    child.output ++ windowExpressionAliases.map(_.asInstanceOf[NamedExpression].toAttribute)
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(RequireSingleBatch)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    val shims = ShimLoader.getSparkShims
    Seq(partitionSpec.map(shims.sortOrder(_, Ascending)) ++ orderSpec)
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // We require a single batch and that is what we produce
  override def outputBatching: CoalesceGoal = RequireSingleBatch

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not happen, in $this.")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val projectList = if (resultColumnsOnly) {
      windowExpressionAliases
    } else {
      child.output ++ windowExpressionAliases
    }

    val boundProjectList =
      GpuBindReferences.bindGpuReferences(projectList, child.output)

    child.executeColumnar().map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows
      GpuProjectExec.projectAndClose(cb, boundProjectList, opTime)
    }
  }
}
