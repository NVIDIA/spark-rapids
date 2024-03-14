/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.filecache.FileCacheConf
import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.LocationPreservingMapPartitionsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuTaskMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

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
  val COPY_BUFFER_TIME = "copyBufferTime"
  val GPU_DECODE_TIME = "gpuDecodeTime"
  val NUM_INPUT_ROWS = "numInputRows"
  val NUM_INPUT_BATCHES = "numInputBatches"
  val NUM_OUTPUT_ROWS = "numOutputRows"
  val NUM_OUTPUT_BATCHES = "numOutputBatches"
  val PARTITION_SIZE = "partitionSize"
  val NUM_PARTITIONS = "numPartitions"
  val OP_TIME = "opTime"
  val COLLECT_TIME = "collectTime"
  val CONCAT_TIME = "concatTime"
  val SORT_TIME = "sortTime"
  val AGG_TIME = "computeAggTime"
  val JOIN_TIME = "joinTime"
  val FILTER_TIME = "filterTime"
  val BUILD_DATA_SIZE = "buildDataSize"
  val BUILD_TIME = "buildTime"
  val STREAM_TIME = "streamTime"
  val NUM_TASKS_FALL_BACKED = "numTasksFallBacked"
  val READ_FS_TIME = "readFsTime"
  val WRITE_BUFFER_TIME = "writeBufferTime"
  val FILECACHE_FOOTER_HITS = "filecacheFooterHits"
  val FILECACHE_FOOTER_HITS_SIZE = "filecacheFooterHitsSize"
  val FILECACHE_FOOTER_MISSES = "filecacheFooterMisses"
  val FILECACHE_FOOTER_MISSES_SIZE = "filecacheFooterMissesSize"
  val FILECACHE_DATA_RANGE_HITS = "filecacheDataRangeHits"
  val FILECACHE_DATA_RANGE_HITS_SIZE = "filecacheDataRangeHitsSize"
  val FILECACHE_DATA_RANGE_MISSES = "filecacheDataRangeMisses"
  val FILECACHE_DATA_RANGE_MISSES_SIZE = "filecacheDataRangeMissesSize"
  val FILECACHE_FOOTER_READ_TIME = "filecacheFooterReadTime"
  val FILECACHE_DATA_RANGE_READ_TIME = "filecacheDataRangeReadTime"

  // Metric Descriptions.
  val DESCRIPTION_BUFFER_TIME = "buffer time"
  val DESCRIPTION_COPY_BUFFER_TIME = "copy buffer time"
  val DESCRIPTION_GPU_DECODE_TIME = "GPU decode time"
  val DESCRIPTION_NUM_INPUT_ROWS = "input rows"
  val DESCRIPTION_NUM_INPUT_BATCHES = "input columnar batches"
  val DESCRIPTION_NUM_OUTPUT_ROWS = "output rows"
  val DESCRIPTION_NUM_OUTPUT_BATCHES = "output columnar batches"
  val DESCRIPTION_PARTITION_SIZE = "partition data size"
  val DESCRIPTION_NUM_PARTITIONS = "partitions"
  val DESCRIPTION_OP_TIME = "op time"
  val DESCRIPTION_COLLECT_TIME = "collect batch time"
  val DESCRIPTION_CONCAT_TIME = "concat batch time"
  val DESCRIPTION_SORT_TIME = "sort time"
  val DESCRIPTION_AGG_TIME = "aggregation time"
  val DESCRIPTION_JOIN_TIME = "join time"
  val DESCRIPTION_FILTER_TIME = "filter time"
  val DESCRIPTION_BUILD_DATA_SIZE = "build side size"
  val DESCRIPTION_BUILD_TIME = "build time"
  val DESCRIPTION_STREAM_TIME = "stream time"
  val DESCRIPTION_NUM_TASKS_FALL_BACKED = "number of sort fallback tasks"
  val DESCRIPTION_READ_FS_TIME = "time to read fs data"
  val DESCRIPTION_WRITE_BUFFER_TIME = "time to write data to buffer"
  val DESCRIPTION_FILECACHE_FOOTER_HITS = "cached footer hits"
  val DESCRIPTION_FILECACHE_FOOTER_HITS_SIZE = "cached footer hits size"
  val DESCRIPTION_FILECACHE_FOOTER_MISSES = "cached footer misses"
  val DESCRIPTION_FILECACHE_FOOTER_MISSES_SIZE = "cached footer misses size"
  val DESCRIPTION_FILECACHE_DATA_RANGE_HITS = "cached data hits"
  val DESCRIPTION_FILECACHE_DATA_RANGE_HITS_SIZE = "cached data hits size"
  val DESCRIPTION_FILECACHE_DATA_RANGE_MISSES = "cached data misses"
  val DESCRIPTION_FILECACHE_DATA_RANGE_MISSES_SIZE = "cached data misses size"
  val DESCRIPTION_FILECACHE_FOOTER_READ_TIME = "cached footer read time"
  val DESCRIPTION_FILECACHE_DATA_RANGE_READ_TIME = "cached data read time"

  def unwrap(input: GpuMetric): SQLMetric = input match {
    case w :WrappedGpuMetric => w.sqlMetric
    case i => throw new IllegalArgumentException(s"found unsupported GpuMetric ${i.getClass}")
  }

  def unwrap(input: Map[String, GpuMetric]): Map[String, SQLMetric] = input.collect {
    // remove the metrics that are not registered
    case (k, w) if w != NoopMetric => (k, unwrap(w))
  }

  def wrap(input: SQLMetric): GpuMetric = WrappedGpuMetric(input)

  def wrap(input: Map[String, SQLMetric]): Map[String, GpuMetric] = input.map {
    case (k, v) => (k, wrap(v))
  }

  def ns[T](metrics: GpuMetric*)(f: => T): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      val taken = System.nanoTime() - start
      metrics.foreach(_.add(taken))
    }
  }

  object DEBUG_LEVEL extends MetricsLevel(0)
  object MODERATE_LEVEL extends MetricsLevel(1)
  object ESSENTIAL_LEVEL extends MetricsLevel(2)
}

sealed abstract class GpuMetric extends Serializable {
  def value: Long
  def set(v: Long): Unit
  def +=(v: Long): Unit
  def add(v: Long): Unit

  final def ns[T](f: => T): T = {
    val start = System.nanoTime()
    try {
      f
    } finally {
      add(System.nanoTime() - start)
    }
  }
}

object NoopMetric extends GpuMetric {
  override def +=(v: Long): Unit = ()
  override def add(v: Long): Unit = ()
  override def set(v: Long): Unit = ()
  override def value: Long = 0
}

final case class WrappedGpuMetric(sqlMetric: SQLMetric) extends GpuMetric {
  def +=(v: Long): Unit = sqlMetric.add(v)
  def add(v: Long): Unit = sqlMetric.add(v)
  override def set(v: Long): Unit = sqlMetric.set(v)
  override def value: Long = sqlMetric.value
}

/** A GPU metric class that just accumulates into a variable without implicit publishing. */
final class LocalGpuMetric extends GpuMetric {
  private var lval = 0L
  override def value: Long = lval
  override def set(v: Long): Unit = { lval = v }
  override def +=(v: Long): Unit = { lval += v }
  override def add(v: Long): Unit = { lval += v }
}

class CollectTimeIterator[T](
    nvtxName: String,
    it: Iterator[T],
    collectTime: GpuMetric) extends Iterator[T] {
  override def hasNext: Boolean = {
    withResource(new NvtxWithMetrics(nvtxName, NvtxColor.BLUE, collectTime)) { _ =>
      it.hasNext
    }
  }

  override def next(): T = {
    withResource(new NvtxWithMetrics(nvtxName, NvtxColor.BLUE, collectTime)) { _ =>
      it.next
    }
  }
}

object GpuExec {
  def outputBatching(sp: SparkPlan): CoalesceGoal = sp match {
    case gpu: GpuExec => gpu.outputBatching
    case _ => null
  }

  val TASK_METRICS_TAG = new TreeNodeTag[GpuTaskMetrics]("gpu_task_metrics")
}

trait GpuExec extends SparkPlan {
  import GpuMetric._
  def sparkSession: SparkSession = {
    SparkShimImpl.sessionFromPlan(this)
  }

  /**
   * Return the expressions for this plan node that should be GPU expressions.
   * For most nodes this will be the same as the list of expressions, but some
   * nodes use CPU expressions directly in some cases and will need to override this.
   */
  def gpuExpressions: Seq[Expression] = expressions

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

  protected def createFileCacheMetrics(): Map[String, GpuMetric] = {
    if (FileCacheConf.FILECACHE_ENABLED.get(conf)) {
      Map(
        FILECACHE_FOOTER_HITS -> createMetric(MODERATE_LEVEL, DESCRIPTION_FILECACHE_FOOTER_HITS),
        FILECACHE_FOOTER_HITS_SIZE -> createSizeMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_FOOTER_HITS_SIZE),
        FILECACHE_FOOTER_MISSES -> createMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_FOOTER_MISSES),
        FILECACHE_FOOTER_MISSES_SIZE -> createSizeMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_FOOTER_MISSES_SIZE),
        FILECACHE_DATA_RANGE_HITS -> createMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_DATA_RANGE_HITS),
        FILECACHE_DATA_RANGE_HITS_SIZE -> createSizeMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_DATA_RANGE_HITS_SIZE),
        FILECACHE_DATA_RANGE_MISSES -> createMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_DATA_RANGE_MISSES),
        FILECACHE_DATA_RANGE_MISSES_SIZE -> createSizeMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_DATA_RANGE_MISSES_SIZE),
        FILECACHE_FOOTER_READ_TIME -> createNanoTimingMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_FOOTER_READ_TIME),
        FILECACHE_DATA_RANGE_READ_TIME -> createNanoTimingMetric(MODERATE_LEVEL,
          DESCRIPTION_FILECACHE_DATA_RANGE_READ_TIME))
    } else {
      Map.empty
    }
  }

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

  // This is ugly, we don't need to access these metrics directly, but we do need to make sure
  // that we can send them over the wire to the executor so that things work as expected
  def setTaskMetrics(gpuTaskMetrics: GpuTaskMetrics): Unit =
    setTagValue(GpuExec.TASK_METRICS_TAG, gpuTaskMetrics)

  def getTaskMetrics: Option[GpuTaskMetrics] =
    this.getTagValue(GpuExec.TASK_METRICS_TAG)

  final override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val orig = internalDoExecuteColumnar()
    val metrics = getTaskMetrics
    metrics.map { gpuMetrics =>
      // This is ugly, but it reduces the need to change all exec nodes, so we are doing it here
      LocationPreservingMapPartitionsRDD(orig) { iter =>
        gpuMetrics.makeSureRegistered()
        iter
      }
    }.getOrElse(orig)
  }

  protected def internalDoExecuteColumnar(): RDD[ColumnarBatch]
}
