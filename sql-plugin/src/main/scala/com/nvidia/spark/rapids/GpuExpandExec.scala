/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, Scalar}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ExpandExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuExpandExecMeta(
    expand: ExpandExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ExpandExec](expand, conf, parent, rule) {

  private val gpuProjections: Seq[Seq[BaseExprMeta[_]]] =
    expand.projections.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))

  private val outputAttributes: Seq[BaseExprMeta[_]] =
    expand.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = gpuProjections.flatten ++ outputAttributes

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  override def convertToGpu(): GpuExec = {
    val projections = gpuProjections.map(_.map(_.convertToGpu()))
    GpuExpandExec(projections, expand.output,
      childPlans.head.convertIfNeeded())
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
    child: SparkPlan)
    extends UnaryExecNode with GpuExec {

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    PEAK_DEVICE_MEMORY -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY))

  // The GroupExpressions can output data with arbitrary partitioning, so set it
  // as UNKNOWN partitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(projections.flatten.flatMap(_.references))

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val boundProjections: Seq[Seq[GpuExpression]] =
      projections.map(GpuBindReferences.bindGpuReferences(_, child.output))

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
    boundProjections: Seq[Seq[GpuExpression]],
    metrics: Map[String, GpuMetric],
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

    val uniqueDeviceColumns = mutable.ListBuffer[GpuColumnVector]()

    val projectedBatch = withResource(new NvtxWithMetrics(
      "ExpandExec projections", NvtxColor.GREEN, totalTime)) { _ =>

      // ExpandExec typically produces many null columns so we re-use them where possible
      val nullCVs = mutable.Map[DataType, GpuColumnVector]()

      /**
       * Create a null column vector for the specified data type, returning the vector and
       * a boolean indicating whether an existing vector was re-used.
       */
      def getOrCreateNullCV(dataType: DataType): (GpuColumnVector, Boolean) = {
        nullCVs.get(dataType) match {
          case Some(cv) =>
            (cv.incRefCount(), true)
          case None =>
            val cv = GpuColumnVector.fromNull(cb.numRows(), dataType)
            nullCVs.put(dataType, cv)
            (cv, false)
        }
      }

      val projectedColumns = boundProjections(projectionIndex).safeMap(fn = expr => {
        val sparkType = expr.dataType
        val (cv, nullColumnReused) = expr.columnarEval(cb) match {
          case null => getOrCreateNullCV(sparkType)
          case other =>
            (GpuExpressionsUtils.resolveColumnVector(other, cb.numRows, sparkType), false)
        }
        if (!nullColumnReused) {
          uniqueDeviceColumns += cv
        }
        cv
      })

      new ColumnarBatch(projectedColumns.toArray, cb.numRows())
    }

    val deviceMemory = GpuColumnVector.getTotalDeviceMemoryUsed(uniqueDeviceColumns.toArray)

    numOutputBatches += 1
    numOutputRows += projectedBatch.numRows()
    maxDeviceMemory = maxDeviceMemory.max(deviceMemory)
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
