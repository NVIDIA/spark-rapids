/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.GpuMetricNames._

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

object GpuMetricNames {

  // Metric names.
  val BUFFER_TIME = "bufferTime"
  val GPU_DECODE_TIME = "gpuDecodeTime"
  val NUM_INPUT_ROWS = "numInputRows"
  val NUM_INPUT_BATCHES = "numInputBatches"
  val NUM_OUTPUT_ROWS = "numOutputRows"
  val NUM_OUTPUT_BATCHES = "numOutputBatches"
  val TOTAL_TIME = "totalTime"
  val PEAK_DEVICE_MEMORY = "peakDevMemory"

  // Metric Descriptions.
  val DESCRIPTION_NUM_INPUT_ROWS = "number of input rows"
  val DESCRIPTION_NUM_INPUT_BATCHES = "number of input columnar batches"
  val DESCRIPTION_NUM_OUTPUT_ROWS = "number of output rows"
  val DESCRIPTION_NUM_OUTPUT_BATCHES = "number of output columnar batches"
  val DESCRIPTION_TOTAL_TIME = "total time"
  val DESCRIPTION_PEAK_DEVICE_MEMORY = "peak device memory"

  def buildGpuScanMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    Map(
      NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_BATCHES),
      TOTAL_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext, DESCRIPTION_TOTAL_TIME),
      GPU_DECODE_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext, "GPU decode time"),
      BUFFER_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext, "buffer time"),
      PEAK_DEVICE_MEMORY -> SQLMetrics.createSizeMetric(sparkContext,
        DESCRIPTION_PEAK_DEVICE_MEMORY))
  }
}

trait GpuExec extends SparkPlan with Arm {
  /**
   * If true is returned batches after this will be coalesced.  This should
   * really be used in cases where it is known that the size of a batch may
   * shrink a lot.
   */
  def coalesceAfter: Boolean = false

  /**
   * A goal to coalesce batches as the input to this operation.  In some cases an
   * operation will only work if all of the data is in a single batch.  In other
   * cases it may be much faster if it is in a single batch, but can tolerate multiple
   * batches.  This provides a way to express those desires.
   */
  def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq.fill(children.size)(null)

  override def supportsColumnar = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_BATCHES),
    TOTAL_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext,
      DESCRIPTION_TOTAL_TIME)) ++ additionalMetrics

  lazy val additionalMetrics: Map[String, SQLMetric] = Map.empty

  /**
   * Returns true if there is something in the exec that cannot work when batches between
   * multiple file partitions are combined into a single batch (coalesce).
   */
  def disableCoalesceUntilInput(): Boolean =
    expressions.exists {
      case c: GpuExpression => c.disableCoalesceUntilInput()
      case _ => false
    }

  /**
   * Defines how the canonicalization should work for the current plan.
   */
  override protected def doCanonicalize(): SparkPlan = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExpressions(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)
      case a: GpuAlias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExpressions(a.child, allAttributes)
        GpuAlias(normalizedChild, "")(ExprId(id), a.qualifier)
      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the exprId too.
        id += 1
        ar.withExprId(ExprId(id)).canonicalized
      case other => QueryPlan.normalizeExpressions(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }
}
