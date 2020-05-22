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

import ai.rapids.cudf.NvtxColor
import ai.rapids.spark.GpuMetricNames._
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

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext,
      "number of input rows"),
    NUM_INPUT_BATCHES -> SQLMetrics.createMetric(sparkContext,
      "number of input columnar batches"),
    PEAK_DEVICE_MEMORY -> SQLMetrics.createSizeMetric(sparkContext,
      "peak device memory"))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val boundProjections: Seq[Seq[GpuExpression]] =
      projections.map(GpuBindReferences.bindReferences(_, child.output))

    child.executeColumnar().mapPartitions { it =>
      new GpuExpandIterator(boundProjections, metrics, it)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

}

class GpuExpandIterator(
    boundProjections: Seq[Seq[GpuExpression]],
    metrics: Map[String, SQLMetric],
    it: Iterator[ColumnarBatch])
  extends Iterator[ColumnarBatch]
  with Arm {

  private var cb: ColumnarBatch = _
  private var projectionIndex = 0
  private var maxDeviceMemory = 0L
  private val numInputBatches = metrics(NUM_INPUT_BATCHES)
  private val numOutputBatches = metrics(NUM_OUTPUT_BATCHES)
  private val numInputRows = metrics(NUM_INPUT_ROWS)
  private val numOutputRows = metrics(NUM_OUTPUT_ROWS)
  private val totalTime = metrics(TOTAL_TIME)
  private val peakDeviceMemory = metrics(PEAK_DEVICE_MEMORY)

  Option(TaskContext.get())
    .foreach(_.addTaskCompletionListener[Unit](_ => Option(cb).foreach(_.close())))

  override def hasNext: Boolean = cb != null || it.hasNext

  override def next(): ColumnarBatch = {

    if (cb == null) {
      cb = it.next()
      numInputBatches += 1
      numInputRows += cb.numRows()
    }

    val projectedBatch = withResource(new NvtxWithMetrics(
      "ExpandExec projections", NvtxColor.GREEN, totalTime)) { _ =>

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

      new ColumnarBatch(projectedColumns.toArray, cb.numRows())
    }

    numOutputBatches += 1
    numOutputRows += projectedBatch.numRows()
    maxDeviceMemory = maxDeviceMemory.max(GpuColumnVector.getTotalDeviceMemoryUsed(projectedBatch))
    peakDeviceMemory.set(maxDeviceMemory)

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