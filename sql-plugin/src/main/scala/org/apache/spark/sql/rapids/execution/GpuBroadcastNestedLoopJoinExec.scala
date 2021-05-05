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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val condition: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

  override def tagPlanForGpu(): Unit = {
    JoinTypeChecks.tagForGpu(join.joinType, this)

    join.joinType match {
      case Inner =>
      case Cross =>
      case _ => willNotWorkOnGpu(s"${join.joinType} currently is not supported")
    }

    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    val Seq(leftPlan, rightPlan) = childPlans
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => leftPlan
      case GpuBuildRight => rightPlan
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu(
        "the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)
    ShimLoader.getSparkShims.getGpuBroadcastNestedLoopJoinShim(
      left, right, join,
      join.joinType,
      condition.map(_.convertToGpu()),
      conf.gpuTargetBatchSizeBytes)
  }
}

/**
 * An iterator that does a cross join against a stream of batches.
 */
class CrossJoinIterator(
    builtBatch: LazySpillableColumnarBatch,
    private val stream: Iterator[LazySpillableColumnarBatch],
    val targetSize: Long,
    val buildSide: GpuBuildSide,
    private val joinTime: GpuMetric,
    private val totalTime: GpuMetric) extends Iterator[ColumnarBatch] with Arm {

  private var nextCb: Option[ColumnarBatch] = None
  private var gathererStore: Option[JoinGatherer] = None

  private var closed = false

  def close(): Unit = {
    if (!closed) {
      nextCb.foreach(_.close())
      nextCb = None
      gathererStore.foreach(_.close())
      gathererStore = None
      // Close the build batch we are done with it.
      builtBatch.close()
      closed = true
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("cross join gather", NvtxColor.DARK_GREEN, joinTime)) { _ =>
      val ret = gathererStore.map { gather =>
        val nextRows = JoinGatherer.getRowsInNextBatch(gather, targetSize)
        gather.gatherNext(nextRows)
      }
      if (gathererStore.exists(_.isDone)) {
        gathererStore.foreach(_.close())
        gathererStore = None
      }

      if (ret.isDefined) {
        // We are about to return something. We got everything we need from it so now let it spill
        // if there is more to be gathered later on.
        gathererStore.foreach(_.allowSpilling())
      }
      ret
    }
  }

  private def makeGatherer(streamBatch: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    // Don't close the built side because it will be used for each stream and closed
    // when the iterator is done.
    val (leftBatch, rightBatch) = buildSide match {
      case GpuBuildLeft => (LazySpillableColumnarBatch.spillOnly(builtBatch), streamBatch)
      case GpuBuildRight => (streamBatch, LazySpillableColumnarBatch.spillOnly(builtBatch))
    }

    val leftMap = LazySpillableGatherMap.leftCross(leftBatch.numRows, rightBatch.numRows)
    val rightMap = LazySpillableGatherMap.rightCross(leftBatch.numRows, rightBatch.numRows)

    val joinGatherer = (leftBatch.numCols, rightBatch.numCols) match {
      case (_, 0) =>
        rightBatch.close()
        rightMap.close()
        JoinGatherer(leftMap, leftBatch)
      case (0, _) =>
        leftBatch.close()
        leftMap.close()
        JoinGatherer(rightMap, rightBatch)
      case (_, _) => JoinGatherer(leftMap, leftBatch, rightMap, rightBatch)
    }
    if (joinGatherer.isDone) {
      joinGatherer.close()
      None
    } else {
      Some(joinGatherer)
    }
  }

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    var mayContinue = true
    while (nextCb.isEmpty && mayContinue) {
      val startTime = System.nanoTime()
      if (gathererStore.exists(!_.isDone)) {
        nextCb = nextCbFromGatherer()
      } else if (stream.hasNext) {
        // Need to refill the gatherer
        gathererStore.foreach(_.close())
        gathererStore = None
        gathererStore = makeGatherer(stream.next())
        nextCb = nextCbFromGatherer()
      } else {
        mayContinue = false
      }
      totalTime += (System.nanoTime() - startTime)
    }
    if (nextCb.isEmpty) {
      // Nothing is left to return so close ASAP.
      close()
    }
    nextCb.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val ret = nextCb.get
    nextCb = None
    ret
  }
}

object GpuBroadcastNestedLoopJoinExecBase extends Arm {
  def innerLikeJoin(
      builtBatch: LazySpillableColumnarBatch,
      stream: Iterator[LazySpillableColumnarBatch],
      targetSize: Long,
      buildSide: GpuBuildSide,
      boundCondition: Option[Expression],
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      joinTime: GpuMetric,
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    val joinIterator =
      new CrossJoinIterator(builtBatch, stream, targetSize, buildSide, joinTime, totalTime)
    if (boundCondition.isDefined) {
      val condition = boundCondition.get
      joinIterator.flatMap { cb =>
        joinOutputRows += cb.numRows()
        withResource(
          GpuFilter(cb, condition, numOutputRows, numOutputBatches, filterTime)) { filtered =>
          if (filtered.numRows == 0) {
            // Not sure if there is a better way to work around this
            numOutputBatches.set(numOutputBatches.value - 1)
            None
          } else {
            Some(GpuColumnVector.incRefCounts(filtered))
          }
        }
      }
    } else {
      joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }

  def divideIntoBatches(
      rowCounts: RDD[Long],
      targetSizeBytes: Long,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): RDD[ColumnarBatch] = {
    // Hash aggregate explodes the rows out, so if we go too large
    // it can blow up. The size of a Long is 8 bytes so we just go with
    // that as our estimate, no nulls.
    val maxRowCount = targetSizeBytes / 8

    def divideIntoBatches(rows: Long): Iterable[ColumnarBatch] = {
      val numBatches = (rows + maxRowCount - 1) / maxRowCount
      (0L until numBatches).map(i => {
        val ret = new ColumnarBatch(new Array[ColumnVector](0))
        if ((i + 1) * maxRowCount > rows) {
          ret.setNumRows((rows - (i * maxRowCount)).toInt)
        } else {
          ret.setNumRows(maxRowCount.toInt)
        }
        numOutputRows += ret.numRows()
        numOutputBatches += 1
        ret
      })
    }

    rowCounts.flatMap(divideIntoBatches)
  }
}

abstract class GpuBroadcastNestedLoopJoinExecBase(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long) extends BinaryExecNode with GpuExec {

  import GpuMetric._

  // Spark BuildSide, BuildRight, BuildLeft changed packages between Spark versions
  // so return a GPU version that is agnostic to the Spark version.
  def getGpuBuildSide: GpuBuildSide

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    FILTER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_FILTER_TIME)) ++ spillMetrics

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = getGpuBuildSide match {
    case GpuBuildRight => (left, right)
    case GpuBuildLeft => (right, left)
  }

  def broadcastExchange: GpuBroadcastExchangeExecBase = broadcast match {
    case BroadcastQueryStageExec(_, gpu: GpuBroadcastExchangeExecBase) => gpu
    case BroadcastQueryStageExec(_, reused: ReusedExchangeExec) =>
      reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
    case gpu: GpuBroadcastExchangeExecBase => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
  }

  override def requiredChildDistribution: Seq[Distribution] = getGpuBuildSide match {
    case GpuBuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case GpuBuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  private[this] def makeBuiltBatch(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      val ret = broadcastRelation.value.batch
      buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
      GpuColumnVector.incRefCounts(ret)
    }
  }

  private[this] def computeBuildRowCount(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      buildDataSize += 0
      broadcastRelation.value.batch.numRows()
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val filterTime = gpuLongMetric(FILTER_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)

    val boundCondition = condition.map(GpuBindReferences.bindGpuReference(_, output))

    val buildTime = gpuLongMetric(BUILD_TIME)
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)

    joinType match {
      case _: InnerLike => // The only thing we support right now
      case _ => throw new IllegalArgumentException(s"$joinType + $getGpuBuildSide is not" +
          " supported and should be run on the CPU")
    }

    val broadcastRelation =
      broadcastExchange.executeColumnarBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

    if (output.isEmpty) {
      assert(boundCondition.isEmpty)

      lazy val buildCount: Int = computeBuildRowCount(broadcastRelation, buildTime, buildDataSize)

      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val counts = streamed.executeColumnar().map(getRowCountAndClose)
      GpuBroadcastNestedLoopJoinExecBase.divideIntoBatches(
        counts.map(s => s * buildCount),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    } else {
      lazy val builtBatch: ColumnarBatch =
        makeBuiltBatch(broadcastRelation, buildTime, buildDataSize)
      val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
      streamed.executeColumnar().mapPartitions { streamedIter =>
        val lazyStream = streamedIter.map { cb =>
          withResource(cb) { cb =>
            LazySpillableColumnarBatch(cb, spillCallback, "stream_batch")
          }
        }
        GpuBroadcastNestedLoopJoinExecBase.innerLikeJoin(
          LazySpillableColumnarBatch(builtBatch, spillCallback, "built_batch"),
          lazyStream, targetSizeBytes, getGpuBuildSide, boundCondition,
          numOutputRows, joinOutputRows, numOutputBatches,
          joinTime, filterTime, totalTime)
      }
    }
  }
}

