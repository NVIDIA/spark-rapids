/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsConf.LORE_SKIP_DUMPING_PLAN
import com.nvidia.spark.rapids.filecache.FileCacheConf
import com.nvidia.spark.rapids.lore.{GpuLore, GpuLoreDumpRDD}
import com.nvidia.spark.rapids.lore.GpuLore.{loreIdOf, LORE_DUMP_PATH_TAG, LORE_DUMP_RDD_TAG}
import org.apache.hadoop.fs.Path

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rapids.LocationPreservingMapPartitionsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuTaskMetrics
import org.apache.spark.sql.rapids.execution.GpuCustomShuffleReaderExec
import org.apache.spark.sql.rapids.shims.SparkSessionUtils
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims.SparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * RDD wrapper that tracks opTime while excluding child operators' opTime
 */
private class GpuOpTimeTrackingRDD(
    prev: RDD[ColumnarBatch],
    opTimeMetric: GpuMetric,
    childOpTimeMetrics: Seq[GpuMetric]) extends RDD[ColumnarBatch](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val childIterator = firstParent[ColumnarBatch].compute(split, context)
    
    // Create wrapper iterator that tracks opTime excluding child opTime
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = {
        if (childOpTimeMetrics.nonEmpty) {
          opTimeMetric.ns(childOpTimeMetrics) {
            childIterator.hasNext
          }
        } else {
          opTimeMetric.ns {
            childIterator.hasNext
          }
        }
      }

      override def next(): ColumnarBatch = {
        if (childOpTimeMetrics.nonEmpty) {
          opTimeMetric.ns(childOpTimeMetrics) {
            childIterator.next()
          }
        } else {
          opTimeMetric.ns {
            childIterator.next()
          }
        }
      }
    }
  }

  override def getPartitions: Array[Partition] = firstParent[ColumnarBatch].partitions

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[ColumnarBatch].preferredLocations(split)
}

object GpuExec {
  /**
   * Create an op time tracking RDD wrapper
   * @param rdd The RDD to wrap
   * @param opTimeMetric The op time metric to track
   * @param childOpTimeMetrics Child op time metrics to exclude
   * @return Wrapped RDD with op time tracking
   */
  def createOpTimeTrackingRDD(
      rdd: RDD[ColumnarBatch],
      opTimeMetric: GpuMetric,
      childOpTimeMetrics: Seq[GpuMetric]): RDD[ColumnarBatch] = {
    new GpuOpTimeTrackingRDD(rdd, opTimeMetric, childOpTimeMetrics)
  }

  def outputBatching(sp: SparkPlan): CoalesceGoal = sp match {
    case gpu: GpuExec => gpu.outputBatching
    case _ => null
  }

  val TASK_METRICS_TAG = new TreeNodeTag[GpuTaskMetrics]("gpu_task_metrics")
}

trait GpuExec extends SparkPlan with Logging {
  import GpuMetric._

  def sparkSession: SparkSession = {
    SparkSessionUtils.sessionFromPlan(this)
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

  @transient private [this] lazy val metricFactory = new GpuMetricFactory(
    MetricsLevel(RapidsConf.METRICS_LEVEL.get(conf)), sparkContext)

  def createMetric(level: MetricsLevel, name: String): GpuMetric =
    metricFactory.create(level, name)

  def createNanoTimingMetric(level: MetricsLevel, name: String): GpuMetric =
    metricFactory.createNanoTiming(level, name)

  def createSizeMetric(level: MetricsLevel, name: String): GpuMetric =
    metricFactory.createSize(level, name)

  def createAverageMetric(level: MetricsLevel, name: String): GpuMetric =
    metricFactory.createAverage(level, name)

  def createTimingMetric(level: MetricsLevel, name: String): GpuMetric =
    metricFactory.createTiming(level, name)

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
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
    OP_TIME_NEW -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW)) ++
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

  /**
   * Get OP_TIME_NEW metrics from child GpuExec operators to exclude them from this
   * operator's OP_TIME_NEW. Recursively collects all descendant
   * OP_TIME_NEW metrics and deduplicates them
   */
  protected def getChildOpTimeMetrics: Seq[GpuMetric] = {
    def collectChildOpTimeMetricsRecursive(
        plan: SparkPlan, visited: Set[SparkPlan]): Set[GpuMetric] = {
      if (visited.contains(plan)) {
        // Avoid infinite recursion and duplicate collection for shared operators
        Set.empty
      } else {
        val newVisited = visited + plan
        plan match {
          case gpuExec: GpuExec =>
            // Use OP_TIME_NEW_SHUFFLE_READ for Exchange and GpuCustomShuffleReaderExec,
            // OP_TIME_NEW for others
            val metricKey = gpuExec match {
              case _: Exchange | _: GpuCustomShuffleReaderExec => OP_TIME_NEW_SHUFFLE_READ
              case _ => OP_TIME_NEW
            }
            val currentMetric = gpuExec.allMetrics.get(metricKey).toSet
            // Log warning if the expected metric is not found
            if (currentMetric.isEmpty) {
              logWarning(s"Expected metric '$metricKey' not found in " +
                s"${gpuExec.getClass.getSimpleName}")
            }
            // Recursively collect from children
            val childMetrics = gpuExec.children.flatMap { child =>
              collectChildOpTimeMetricsRecursive(child, newVisited)
            }.toSet
            currentMetric ++ childMetrics
          case _ =>
            // For non-GPU operators, still recurse into their children
            plan.children.flatMap { child =>
              collectChildOpTimeMetricsRecursive(child, newVisited)
            }.toSet
        }
      }
    }

    // Start recursive collection from direct children, excluding this operator itself
    val allChildMetrics = children.flatMap { child =>
      collectChildOpTimeMetricsRecursive(child, Set.empty)
    }.toSet

    allChildMetrics.toSeq
  }

  final override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    this.dumpLoreMetaInfo()
    val orig = this.dumpLoreRDD(internalDoExecuteColumnar())
    
    // Get child opTime metrics for exclusion (both old and new implementations)
    val childOpTimeMetrics = getChildOpTimeMetrics

    // Wrap RDD for new OP_TIME_NEW metric (new implementation with exclusion)
    val wrappedForNewOpTime = allMetrics.get(OP_TIME_NEW) match {
      case Some(opTimeNewMetric) =>
        new GpuOpTimeTrackingRDD(orig, opTimeNewMetric, childOpTimeMetrics)
      case None => orig
    }
    
    val metrics = getTaskMetrics
    metrics.map { gpuMetrics =>
      // This is ugly, but it reduces the need to change all exec nodes, so we are doing it here
      LocationPreservingMapPartitionsRDD(wrappedForNewOpTime) { iter =>
        gpuMetrics.makeSureRegistered()
        iter
      }
    }.getOrElse(wrappedForNewOpTime)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ loreArgs

  protected def loreArgs: Iterator[String] = {
    val loreIdStr = loreIdOf(this).map(id => s"[loreId=$id]")
    val lorePathStr = getTagValue(LORE_DUMP_PATH_TAG).map(path => s"[lorePath=$path]")
    val loreRDDInfoStr = getTagValue(LORE_DUMP_RDD_TAG).map(info => s"[loreRDDInfo=$info]")

    List(loreIdStr, lorePathStr, loreRDDInfoStr).flatten.iterator
  }

  private def dumpLoreMetaInfo(): Unit = {
    if (!LORE_SKIP_DUMPING_PLAN.get(conf)) {
      getTagValue(LORE_DUMP_PATH_TAG).foreach { rootPath =>
        GpuLore.dumpPlan(this, new Path(rootPath))
      }
    }
  }

  protected def dumpLoreRDD(inner: RDD[ColumnarBatch]): RDD[ColumnarBatch] = {
    getTagValue(LORE_DUMP_RDD_TAG).map { info =>
      val rdd = new GpuLoreDumpRDD(info, inner)
      rdd.saveMeta()
      rdd
    }.getOrElse(inner)
  }

  protected def internalDoExecuteColumnar(): RDD[ColumnarBatch]
}
