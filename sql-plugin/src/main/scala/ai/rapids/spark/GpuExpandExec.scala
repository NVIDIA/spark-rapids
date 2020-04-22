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

import ai.rapids.cudf.Table
import ai.rapids.spark.GpuMetricNames.NUM_OUTPUT_ROWS
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer

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

  override lazy val metrics = Map(
    GpuMetricNames.NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)

    val rdd = child.executeColumnar()
    val boundProjections = projections.map(projection => GpuBindReferences.bindReferences(projection, child.output))
    rdd.map { cb =>

      val projectedColumns = new ListBuffer[Seq[GpuColumnVector]]()
      var success = false
      try {
        boundProjections.foreach(projection =>
          projectedColumns += projection.safeMap(expr => {
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
        )
        success = true

      } finally {
        if (!success) {
          projectedColumns.foreach(_.safeClose())
        }
      }

      val interleaved = projectedColumns.head.indices.map(i => {
        withResource(projectedColumns.map(p => p(i).getBase)) { vectors =>
          withResource(new Table(vectors: _*)) { table =>
            GpuColumnVector.from(table.interleaveColumns())
          }
        }
      })

      if (interleaved.head.getRowCount > Integer.MAX_VALUE) {
        throw new IllegalStateException("GpuExpandExec produced more than Integer.MAX_VALUE rows." +
          " Please try increasing your partition count.")
      }

      val interleavedRowCount = interleaved.head.getRowCount.toInt

      numOutputRows += interleavedRowCount

      new ColumnarBatch(interleaved.toArray, interleavedRowCount)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

}

