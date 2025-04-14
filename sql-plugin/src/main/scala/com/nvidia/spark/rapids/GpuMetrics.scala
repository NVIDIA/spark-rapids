/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import scala.collection.immutable.TreeMap

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuTaskMetrics

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

class GpuMetricFactory(metricsConf: MetricsLevel, context: SparkContext) {

  private [this] def createInternal(level: MetricsLevel, f: => SQLMetric): GpuMetric = {
    if (level >= metricsConf) {
      // only enable companion metrics (excluding semaphore wait time) for DEBUG_LEVEL
      WrappedGpuMetric(f, withMetricsExclSemWait = GpuMetric.DEBUG_LEVEL >= metricsConf)
    } else {
      NoopMetric
    }
  }

  def create(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createMetric(context, name))

  def createNanoTiming(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createNanoTimingMetric(context, name))

  def createSize(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createSizeMetric(context, name))

  def createAverage(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createAverageMetric(context, name))

  def createTiming(level: MetricsLevel, name: String): GpuMetric =
    createInternal(level, SQLMetrics.createTimingMetric(context, name))
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
  val REPARTITION_TIME = "repartitionTime"
  val AGG_TIME = "computeAggTime"
  val JOIN_TIME = "joinTime"
  val FILTER_TIME = "filterTime"
  val BUILD_DATA_SIZE = "buildDataSize"
  val BUILD_TIME = "buildTime"
  val STREAM_TIME = "streamTime"
  val NUM_TASKS_FALL_BACKED = "numTasksFallBacked"
  val NUM_TASKS_REPARTITIONED = "numTasksRepartitioned"
  val NUM_TASKS_SKIPPED_AGG = "numTasksSkippedAgg"
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
  val DELETION_VECTOR_SCATTER_TIME = "deletionVectorScatterTime"
  val DELETION_VECTOR_SIZE = "deletionVectorSize"
  val COPY_TO_HOST_TIME = "d2hMemCopyTime"

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
  val DESCRIPTION_REPARTITION_TIME = "repartition time"
  val DESCRIPTION_AGG_TIME = "aggregation time"
  val DESCRIPTION_JOIN_TIME = "join time"
  val DESCRIPTION_FILTER_TIME = "filter time"
  val DESCRIPTION_BUILD_DATA_SIZE = "build side size"
  val DESCRIPTION_BUILD_TIME = "build time"
  val DESCRIPTION_STREAM_TIME = "stream time"
  val DESCRIPTION_NUM_TASKS_FALL_BACKED = "number of sort fallback tasks"
  val DESCRIPTION_NUM_TASKS_REPARTITIONED = "number of tasks repartitioned for agg"
  val DESCRIPTION_NUM_TASKS_SKIPPED_AGG = "number of tasks skipped aggregation"
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
  val DESCRIPTION_DELETION_VECTOR_SCATTER_TIME = "deletion vector scatter time"
  val DESCRIPTION_DELETION_VECTOR_SIZE = "deletion vector size"
  val DESCRIPTION_COPY_TO_HOST_TIME = "deviceToHost memory copy time"

  def unwrap(input: GpuMetric): SQLMetric = input match {
    case w :WrappedGpuMetric => w.sqlMetric
    case i => throw new IllegalArgumentException(s"found unsupported GpuMetric ${i.getClass}")
  }

  def unwrap(input: Map[String, GpuMetric]): Map[String, SQLMetric] = {
    val ret = input.collect {
      // remove the metrics that are not registered
      case (k, w) if w != NoopMetric => (k, unwrap(w))
    }
    val companions = input.collect {
      // add the companions
      case (k, w) if w.companionGpuMetric.isDefined =>
        (k + "_exSemWait", unwrap(w.companionGpuMetric.get))
    }

    TreeMap.apply((ret ++ companions).toSeq: _*)
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

  private var isTimerActive = false

  // For timing GpuMetrics, we additionally create a companion GpuMetric to track elapsed time
  // excluding semaphore wait time
  var companionGpuMetric: Option[GpuMetric] = None
  private var semWaitTimeWhenActivated = 0L

  final def tryActivateTimer(): Boolean = {
    if (!isTimerActive) {
      isTimerActive = true
      semWaitTimeWhenActivated = GpuTaskMetrics.get.getSemWaitTime()
      true
    } else {
      false
    }
  }

  final def deactivateTimer(duration: Long): Unit = {
    if (isTimerActive) {
      isTimerActive = false
      companionGpuMetric.foreach(c =>
        c.add(duration - (GpuTaskMetrics.get.getSemWaitTime() - semWaitTimeWhenActivated)))
      semWaitTimeWhenActivated = 0L
      add(duration)
    }
  }

  final def ns[T](f: => T): T = {
    if (tryActivateTimer()) {
      val start = System.nanoTime()
      try {
        f
      } finally {
        deactivateTimer(System.nanoTime() - start)
      }
    } else {
      f
    }
  }
}

object NoopMetric extends GpuMetric {
  override def +=(v: Long): Unit = ()
  override def add(v: Long): Unit = ()
  override def set(v: Long): Unit = ()
  override def value: Long = 0
}

final case class WrappedGpuMetric(sqlMetric: SQLMetric, withMetricsExclSemWait: Boolean = false)
  extends GpuMetric {

  if (withMetricsExclSemWait) {
    //  SQLMetrics.NS_TIMING_METRIC and SQLMetrics.TIMING_METRIC is private,
    //  so we have to use the string directly
    if (sqlMetric.metricType == "nsTiming") {
      companionGpuMetric = Some(WrappedGpuMetric.apply(SQLMetrics.createNanoTimingMetric(
        SparkSession.getActiveSession.get.sparkContext, sqlMetric.name.get + " (excl. SemWait)")))
    }
  }

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
