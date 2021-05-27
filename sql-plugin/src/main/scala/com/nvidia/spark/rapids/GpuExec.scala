/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.StorageTier.{DEVICE, DISK, GDS, HOST, StorageTier}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

sealed class MetricsLevel(val num: Integer) extends Serializable {
  def >=(other: MetricsLevel): Boolean =
    num >= other.num
}

object MetricsLevel {
  def apply(str: String): MetricsLevel = str match {
    case "ESSENTIAL" => GpuMetric.ESSENTIAL_LEVEL
    case "MODERATE" => GpuMetric.MODERATE_LEVEL
    case _ => GpuMetric.DEBUG_LEVEL
  }
}

object GpuMetric extends Logging {
  // Metric names.
  val BUFFER_TIME = "bufferTime"
  val GPU_DECODE_TIME = "gpuDecodeTime"
  val NUM_INPUT_ROWS = "numInputRows"
  val NUM_INPUT_BATCHES = "numInputBatches"
  val NUM_OUTPUT_ROWS = "numOutputRows"
  val NUM_OUTPUT_BATCHES = "numOutputBatches"
  val PARTITION_SIZE = "partitionSize"
  val NUM_PARTITIONS = "numPartitions"
  val TOTAL_TIME = "totalTime"
  val OP_TIME = "opTime"
  val FETCH_TIME = "fetchTime"
  val PEAK_DEVICE_MEMORY = "peakDevMemory"
  val COLLECT_TIME = "collectTime"
  val CONCAT_TIME = "concatTime"
  val SORT_TIME = "sortTime"
  val AGG_TIME = "computeAggTime"
  val JOIN_TIME = "joinTime"
  val JOIN_OUTPUT_ROWS = "joinOutputRows"
  val FILTER_TIME = "filterTime"
  val BUILD_DATA_SIZE = "buildDataSize"
  val BUILD_TIME = "buildTime"
  val STREAM_TIME = "streamTime"
  val SPILL_AMOUNT = "spillData"
  val SPILL_AMOUNT_DISK = "spillDisk"
  val SPILL_AMOUNT_HOST = "spillHost"

  // Metric Descriptions.
  val DESCRIPTION_BUFFER_TIME = "buffer time"
  val DESCRIPTION_GPU_DECODE_TIME = "GPU decode time"
  val DESCRIPTION_NUM_INPUT_ROWS = "input rows"
  val DESCRIPTION_NUM_INPUT_BATCHES = "input columnar batches"
  val DESCRIPTION_NUM_OUTPUT_ROWS = "output rows"
  val DESCRIPTION_NUM_OUTPUT_BATCHES = "output columnar batches"
  val DESCRIPTION_PARTITION_SIZE = "partition data size"
  val DESCRIPTION_NUM_PARTITIONS = "partitions"
  val DESCRIPTION_TOTAL_TIME = "total time"
  val DESCRIPTION_OP_TIME = "op time"
  val DESCRIPTION_FETCH_TIME = "fetch time"
  val DESCRIPTION_PEAK_DEVICE_MEMORY = "peak device memory"
  val DESCRIPTION_COLLECT_TIME = "collect batch time"
  val DESCRIPTION_CONCAT_TIME = "concat batch time"
  val DESCRIPTION_SORT_TIME = "sort time"
  val DESCRIPTION_AGG_TIME = "aggregation time"
  val DESCRIPTION_JOIN_TIME = "join time"
  val DESCRIPTION_JOIN_OUTPUT_ROWS = "join output rows"
  val DESCRIPTION_FILTER_TIME = "filter time"
  val DESCRIPTION_BUILD_DATA_SIZE = "build side size"
  val DESCRIPTION_BUILD_TIME = "build time"
  val DESCRIPTION_STREAM_TIME = "stream time"
  val DESCRIPTION_SPILL_AMOUNT = "bytes spilled from GPU"
  val DESCRIPTION_SPILL_AMOUNT_DISK = "bytes spilled to disk"
  val DESCRIPTION_SPILL_AMOUNT_HOST = "bytes spilled to host"

  def unwrap(input: GpuMetric): SQLMetric = input match {
    case w :WrappedGpuMetric => w.sqlMetric
    case i => throw new IllegalArgumentException(s"found unsupported GpuMetric ${i.getClass}")
  }

  def unwrap(input: Map[String, GpuMetric]): Map[String, SQLMetric] = input.filter {
    // remove the metrics that are not registered
    case (_, NoopMetric) => false
    case _ => true
    // sadly mapValues produces a non-serializable result, so we have to hack it a bit to force
    // it to be materialized
  }.mapValues(unwrap).toArray.toMap

  def wrap(input: SQLMetric): GpuMetric = WrappedGpuMetric(input)

  def wrap(input: Map[String, SQLMetric]): Map[String, GpuMetric] =
  // sadly mapValues produces a non-serializable result, so we have to hack it a bit to force
  // it to be materialized
    input.mapValues(wrap).toArray.toMap

  object DEBUG_LEVEL extends MetricsLevel(0)
  object MODERATE_LEVEL extends MetricsLevel(1)
  object ESSENTIAL_LEVEL extends MetricsLevel(2)

  def makeSpillCallback(allMetrics: Map[String, GpuMetric]): RapidsBuffer.SpillCallback = {
    val spillAmount = allMetrics(SPILL_AMOUNT)
    val disk = allMetrics(SPILL_AMOUNT_DISK)
    val host = allMetrics(SPILL_AMOUNT_HOST)
    def updateMetrics(from: StorageTier, to: StorageTier, amount: Long): Unit = {
      from match {
        case DEVICE =>
          spillAmount += amount
        case _ => // ignored
      }
      to match {
        case HOST =>
          host += amount
        case GDS | DISK =>
          disk += amount
        case _ =>
          logWarning(s"Spill to $to is unsupported in metrics: $amount")
      }
    }
    updateMetrics
  }
}

sealed abstract class GpuMetric extends Serializable {
  def value: Long
  def set(v: Long): Unit
  def +=(v: Long): Unit
  def add(v: Long): Unit
}

object NoopMetric extends GpuMetric {
  override def +=(v: Long): Unit = ()
  override def add(v: Long): Unit = ()
  override def set(v: Long): Unit = ()
  override def value: Long = 0
}

case class WrappedGpuMetric(sqlMetric: SQLMetric) extends GpuMetric {
  def +=(v: Long): Unit = sqlMetric.add(v)
  def add(v: Long): Unit = sqlMetric.add(v)
  override def set(v: Long): Unit = sqlMetric.set(v)
  override def value: Long = sqlMetric.value
}

object GpuExec {
  def outputBatching(sp: SparkPlan): CoalesceGoal = sp match {
    case gpu: GpuExec => gpu.outputBatching
    case _ => null
  }
}

trait GpuExec extends SparkPlan with Arm {
  import GpuMetric._
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

  /**
   * Lets a SparkPlan indicate what guarantees, if any, its output batch has.
   * This lets us bypass GpuCoalesceBatch calls where ever possible.
   * Returning a null indicates no guarantee at all, which is the default.
   */
  def outputBatching: CoalesceGoal = null

  private [this] lazy val metricsConf = MetricsLevel(RapidsConf.METRICS_LEVEL.get(conf))

  private [this] def createMetricInternal(level: MetricsLevel, f: => SQLMetric): GpuMetric = {
    if (level >= metricsConf) {
      WrappedGpuMetric(f)
    } else {
      NoopMetric
    }
  }

  protected def createMetric(level: MetricsLevel, name: String): GpuMetric =
    createMetricInternal(level, SQLMetrics.createMetric(sparkContext, name))

  protected def createNanoTimingMetric(level: MetricsLevel, name: String): GpuMetric =
    createMetricInternal(level, SQLMetrics.createNanoTimingMetric(sparkContext, name))

  protected def createSizeMetric(level: MetricsLevel, name: String): GpuMetric =
    createMetricInternal(level, SQLMetrics.createSizeMetric(sparkContext, name))

  protected def createAverageMetric(level: MetricsLevel, name: String): GpuMetric =
    createMetricInternal(level, SQLMetrics.createAverageMetric(sparkContext, name))

  protected def createTimingMetric(level: MetricsLevel, name: String): GpuMetric =
    createMetricInternal(level, SQLMetrics.createTimingMetric(sparkContext, name))

  override def supportsColumnar = true

  protected val outputRowsLevel: MetricsLevel = DEBUG_LEVEL
  protected val outputBatchesLevel: MetricsLevel = DEBUG_LEVEL

  lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES)) ++
      additionalMetrics

  def gpuLongMetric(name: String): GpuMetric = allMetrics(name)

  final override lazy val metrics: Map[String, SQLMetric] = unwrap(allMetrics)

  lazy val additionalMetrics: Map[String, GpuMetric] = Map.empty

  protected def spillMetrics: Map[String, GpuMetric] = Map(
    SPILL_AMOUNT -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_SPILL_AMOUNT),
    SPILL_AMOUNT_DISK -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_SPILL_AMOUNT_DISK),
    SPILL_AMOUNT_HOST -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_SPILL_AMOUNT_HOST)
  )

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
        ShimLoader.getSparkShims.alias(normalizedChild, "")(ExprId(id), a.qualifier)
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
