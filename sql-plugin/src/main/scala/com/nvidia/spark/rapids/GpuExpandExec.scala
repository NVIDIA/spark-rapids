/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuExpandExecMeta(
    expand: ExpandExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ExpandExec](expand, conf, parent, rule) {

  private val gpuProjections: Seq[Seq[BaseExprMeta[_]]] =
    expand.projections.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))

  private val outputs: Seq[BaseExprMeta[_]] =
    expand.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = gpuProjections.flatten ++ outputs

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec = {
    val projections = gpuProjections.map(_.map(_.convertToGpu()))
    GpuExpandExec(projections, expand.output,
      childPlans.head.convertIfNeeded())(useTieredProject = conf.isTieredProjectEnabled)
  }
}

/**
 * Apply all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for an input row.
 *
 * @param projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter `output`
 * @param output      Attribute references to Output
 * @param child       Child operator
 */
case class GpuExpandExec(
    projections: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: SparkPlan)(
    useTieredProject: Boolean = false) extends ShimUnaryExecNode with GpuExec {

  override def otherCopyArgs: Seq[AnyRef] =
    Seq[AnyRef](useTieredProject.asInstanceOf[java.lang.Boolean])

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val boundProjections = projections.map { pl =>
      GpuBindReferences.bindGpuReferencesTiered(pl, child.output, useTieredProject)
    }

    // cache in a local to avoid serializing the plan
    val metricsMap = allMetrics

    child.executeColumnar().mapPartitions { it =>
      new GpuExpandIterator(boundProjections, metricsMap, it)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException("ROW BASED PROCESSING IS NOT SUPPORTED")
  }

}

class GpuExpandIterator(
    boundProjections: Seq[GpuTieredProject],
    metrics: Map[String, GpuMetric],
    it: Iterator[ColumnarBatch])
  extends Iterator[ColumnarBatch] {

  private var sb: Option[SpillableColumnarBatch] = None
  private var projectionIndex = 0
  private val numInputBatches = metrics(NUM_INPUT_BATCHES)
  private val numOutputBatches = metrics(NUM_OUTPUT_BATCHES)
  private val numInputRows = metrics(NUM_INPUT_ROWS)
  private val numOutputRows = metrics(NUM_OUTPUT_ROWS)
  private val opTime = metrics(OP_TIME)

  Option(TaskContext.get())
    .foreach(_.addTaskCompletionListener[Unit](_ => sb.foreach(_.close())))

  override def hasNext: Boolean = sb.isDefined || it.hasNext

  override def next(): ColumnarBatch = {

    if (sb.isEmpty) {
      val cb = it.next()
      numInputBatches += 1
      numInputRows += cb.numRows()
      sb = Some(SpillableColumnarBatch(cb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }

    val projectedBatch = withResource(new NvtxWithMetrics(
      "ExpandExec projections", NvtxColor.GREEN, opTime)) { _ =>
      boundProjections(projectionIndex).projectWithRetrySingleBatch(sb.get)
    }

    numOutputBatches += 1
    numOutputRows += projectedBatch.numRows()

    projectionIndex += 1
    if (projectionIndex == boundProjections.length) {
      // we have processed all projections against the current batch
      projectionIndex = 0

      sb.foreach(_.close())
      sb = None
    }
    projectedBatch
  }
}
