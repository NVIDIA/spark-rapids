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

import org.apache.spark.{SparkContext, TaskContext}
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
  val BUFFER_TIME_WITH_SEM = "bufferTimeWithSem"
  val SCAN_TIME = "scanTime"
  val COPY_BUFFER_TIME = "copyBufferTime"
  val GPU_DECODE_TIME = "gpuDecodeTime"
  val NUM_INPUT_ROWS = "numInputRows"
  val NUM_INPUT_BATCHES = "numInputBatches"
  val NUM_OUTPUT_ROWS = "numOutputRows"
  val NUM_OUTPUT_BATCHES = "numOutputBatches"
  val PARTITION_SIZE = "partitionSize"
  val NUM_PARTITIONS = "numPartitions"
  val OP_TIME = "opTime"
  val OP_TIME_NEW = "opTimeNew"
  val OP_TIME_NEW_SHUFFLE_WRITE = "opTimeNewShuffleWrite"
  val COLLECT_TIME = "collectTime"
  val CONCAT_TIME = "concatTime"
  val SORT_TIME = "sortTime"
  val REPARTITION_TIME = "repartitionTime"
  val AGG_TIME = "computeAggTime"
  val JOIN_TIME = "joinTime"
  val FILTER_TIME = "filterTime"
  val FILTER_TIME_WITH_SEM = "filterTimeWithSem"
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
  val READ_THROTTLING_TIME = "readThrottlingTime"
  val SYNC_READ_TIME = "shuffleSyncReadTime"
  val ASYNC_READ_TIME = "shuffleAsyncReadTime"

  // Metric Descriptions.
  val DESCRIPTION_BUFFER_TIME = "buffer time"
  val DESCRIPTION_BUFFER_TIME_WITH_SEM = "buffer time with Sem."
  val DESCRIPTION_COPY_BUFFER_TIME = "copy buffer time"
  val DESCRIPTION_GPU_DECODE_TIME = "GPU decode time"
  val DESCRIPTION_NUM_INPUT_ROWS = "input rows"
  val DESCRIPTION_NUM_INPUT_BATCHES = "input columnar batches"
  val DESCRIPTION_NUM_OUTPUT_ROWS = "output rows"
  val DESCRIPTION_NUM_OUTPUT_BATCHES = "output columnar batches"
  val DESCRIPTION_PARTITION_SIZE = "partition data size"
  val DESCRIPTION_NUM_PARTITIONS = "partitions"
  val DESCRIPTION_OP_TIME = "op time"
  val DESCRIPTION_OP_TIME_NEW = "operator time"
  val DESCRIPTION_OP_TIME_NEW_SHUFFLE_WRITE = "operator time (shuffle write partition & serial)"
  val DESCRIPTION_COLLECT_TIME = "collect batch time"
  val DESCRIPTION_CONCAT_TIME = "concat batch time"
  val DESCRIPTION_SORT_TIME = "sort time"
  val DESCRIPTION_REPARTITION_TIME = "repartition time"
  val DESCRIPTION_AGG_TIME = "aggregation time"
  val DESCRIPTION_JOIN_TIME = "join time"
  val DESCRIPTION_FILTER_TIME = "filter time"
  val DESCRIPTION_FILTER_TIME_WITH_SEM = "filter time with Sem."
  val DESCRIPTION_SCAN_TIME = "scan time"
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
  val DESCRIPTION_READ_THROTTLING_TIME = "read throttling time"
  val DESCRIPTION_SYNC_READ_TIME = "sync read time"
  val DESCRIPTION_ASYNC_READ_TIME = "async read time"

  /**
   * Determine if a GpuMetric wraps a TimingMetric or NanoTimingMetric.
   *
   * NOTE: SQLMetrics.NS_TIMING_METRIC and SQLMetrics.TIMING_METRIC is private, so we have to use
   * the string directly
   */
  def isTimeMetric(input: GpuMetric): Boolean = input match {
    case w: WrappedGpuMetric =>
      w.sqlMetric.metricType.toLowerCase.contains("timing")
    case _ =>
      false
  }

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
    ns(metrics, Seq.empty)(f)
  }

  def ns[T](metrics: Seq[GpuMetric], excludeMetrics: Seq[GpuMetric])(f: => T): T = {
    val initedMetrics = metrics.map(m => (m, m.tryActivateTimer(excludeMetrics)))
    val start = System.nanoTime()
    try {
      f
    } finally {
      val taken = System.nanoTime() - start
      initedMetrics.foreach { case (m, isTrack) =>
        if (isTrack) m.deactivateTimer(taken, excludeMetrics)
      }
    }
  }

  /**
   * Give compute block, track its GpuSemaphore withholding time along with the wall time.
   *
   * The computation of semaphoreHoldingTime is a typical interval problem. There are 3 kinds of
   * semaphore holding intervals:
   *  1. The partial lead one, only the tail part is overlapped with the compute block.
   *
   *  2. The complete ones, which are fully overlapped with the compute block. The complete
   *     intervals can be got from the difference of `getSemaphoreHoldingTime`, since
   *     semaphoreHoldingTime will only be updated when the semaphore is released.
   *
   *  3. The partial lag one, only the head part is overlapped with the compute block.
   *
   * Consider the timeline and the relevant durations based on the following diagram style:
   *
   * Acq.     Rel.   Acq.        Rel.             Acq.          Rel.
   *  |--------|      |-----------|                |-------------|
   *      |----------------------------------------------| (Compute Block)
   *      <---->      <----------->                <----->
   *       (1)             (2)                       (3)
   */
  def withSemaphoreTime[T](
      wallTime: GpuMetric,
      semTime: GpuMetric,
      ctx: TaskContext)(f: => T): T = {
    val start = System.nanoTime()
    val semStart = GpuTaskMetrics.get.getSemaphoreHoldingTime
    // Compute the preBlockHead if there exists a partial lead interval. The preBlockHead will
    // be explicitly subtracted in final:
    //   Sem.Acq                                 Sem.Rel
    //      |---------------------------------------| (Partial Lead Interval)
    //                     |--------------------------------------| (Compute Block)
    //        preBlockHead
    //      <-------------->
    val preBlockHead = GpuSemaphore.getLastSemAcqAndRelTime(ctx) match {
      case (acqTime, relTime) if acqTime > relTime && acqTime < start =>
        start - acqTime
      case _ =>
        0L
    }

    try {
      f
    } finally {
      val end = System.nanoTime()
      val semEnd = GpuTaskMetrics.get.getSemaphoreHoldingTime
      // Compute the partial tail if there exists a partial lag interval. The partial tail will
      // be explicitly added in final:
      //                              Sem.Acq           Sem.Rel
      //                                 |-----------------| (Partial Lag Interval)
      //      |--------------------------------------| (Compute Block)
      //                                  partialTail
      //                                 <----------->
      val partialTail = GpuSemaphore.getLastSemAcqAndRelTime(ctx) match {
        case (acqTime, relTime) if acqTime > relTime =>
          // directly minus acqTime instead of `acqTime max start` because the ahead part has
          // been subtracted ahead of time (`semHoldTimeBefore`)
          end - acqTime
        case _ =>
          0L
      }
      wallTime.add(System.nanoTime() - start)
      semTime.add(semEnd - semStart - preBlockHead + partialTail)
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
  private var excludeMetricsWhenActivated: Seq[Long] = Seq.empty

  def tryActivateTimer(excludeMetrics: Seq[GpuMetric]): Boolean = {
    if (!isTimerActive) {
      isTimerActive = true
      semWaitTimeWhenActivated = GpuTaskMetrics.get.getSemWaitTime()
      excludeMetricsWhenActivated = excludeMetrics.map(_.value)
      true
    } else {
      false
    }
  }

  def deactivateTimer(duration: Long, excludeMetrics: Seq[GpuMetric]): Unit = {
    if (isTimerActive) {
      isTimerActive = false

      val totalExcludeTime = if (excludeMetrics.length == excludeMetricsWhenActivated.length) {
        excludeMetrics.zip(excludeMetricsWhenActivated)
          .map { case (metric, startValue) => metric.value - startValue }
          .sum
      } else {
        throw new IllegalStateException(
          s"the excludeMetrics size ${excludeMetrics.length} does not match " +
            s"excludeMetricsWhenActivated size ${excludeMetricsWhenActivated.length}")
      }

      companionGpuMetric.foreach(c =>
        c.add(duration
          - (GpuTaskMetrics.get.getSemWaitTime() - semWaitTimeWhenActivated)
          - totalExcludeTime
        ))
      semWaitTimeWhenActivated = 0L
      excludeMetricsWhenActivated = Seq.empty

      add(duration - totalExcludeTime)
    }
  }

  final def ns[T](f: => T): T = {
    ns(Seq.empty)(f)
  }

  final def ns[T](excludeMetrics: Seq[GpuMetric])(f: => T): T = {
    if (tryActivateTimer(excludeMetrics)) {
      val start = System.nanoTime()
      try {
        f
      } finally {
        deactivateTimer(System.nanoTime() - start, excludeMetrics)
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

  override def tryActivateTimer(excludeMetrics: Seq[GpuMetric]): Boolean = false
  override def deactivateTimer(duration: Long, excludeMetrics: Seq[GpuMetric]): Unit = ()
}

final case class WrappedGpuMetric(sqlMetric: SQLMetric, withMetricsExclSemWait: Boolean = false)
  extends GpuMetric {

  if (withMetricsExclSemWait) {
    if (GpuMetric.isTimeMetric(this)) {
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
