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
package ai.rapids.spark

import ai.rapids.spark.GpuMetricNames.NUM_OUTPUT_ROWS
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuExpandExecMeta(
    expand: ExpandExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[ExpandExec](expand, conf, parent, rule) {

  private val gpuProjections: Seq[Seq[ExprMeta[_]]] =
    expand.projections.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))

  private val outputAttributes: Seq[ExprMeta[_]] =
    expand.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[ExprMeta[_]] = gpuProjections.flatten ++ outputAttributes

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec = {
    val projections = gpuProjections.map(_.map(_.convertToGpu()))
    val attributes = outputAttributes.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]]
    GpuExpandExec(projections, attributes, childPlans.head.convertIfNeeded())
  }
}

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param resultExpressions The output Schema
 * @param child       Child operator
 */
case class GpuExpandExec(
                       projections: Seq[Seq[GpuExpression]],
                       resultExpressions: Seq[NamedExpression],
                       child: SparkPlan)
  extends UnaryExecNode with GpuExec {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    GpuMetricNames.NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)

    val boundProjections: Seq[Seq[GpuExpression]] =
      projections.map(GpuBindReferences.bindReferences(_, child.output))

    child.executeColumnar().mapPartitions { it =>
      new GpuExpandIterator(boundProjections, numOutputRows, it)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

}

class GpuExpandIterator(
    boundProjections: Seq[Seq[GpuExpression]],
    numOutputRows: SQLMetric,
    it: Iterator[ColumnarBatch])
  extends Iterator[ColumnarBatch]
  with Arm {

  private var cb: ColumnarBatch = _
  private var projectionIndex = 0

  Option(TaskContext.get())
    .foreach(_.addTaskCompletionListener[Unit](_ => Option(cb).foreach(_.close())))

  override def hasNext: Boolean = cb != null || it.hasNext

  override def next(): ColumnarBatch = {

    if (cb == null) {
      cb = it.next()
    }

    val projectedColumns = boundProjections(projectionIndex).safeMap(expr => {
      expr.columnarEval(cb) match {
        case cv: GpuColumnVector => cv
        case lit: GpuLiteral =>
          withResource(GpuScalar.from(lit.value, lit.dataType)) { scalar =>
            GpuColumnVector.from(scalar, cb.numRows())
          }
        case other =>
          withResource(GpuScalar.from(other, expr.dataType)) { scalar =>
            GpuColumnVector.from(scalar, cb.numRows())
          }
      }
    })

    val projectedBatch = new ColumnarBatch(projectedColumns.toArray, cb.numRows())
    numOutputRows += cb.numRows()

    projectionIndex += 1
    if (projectionIndex == boundProjections.length) {
      // we have processed all projections against the current batch
      projectionIndex = 0

      cb.close()
      cb = null
    }

    projectedBatch
  }

}