/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{GatherMap, NvtxColor, NvtxRange, OutOfBoundsPolicy}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Base class for iterators producing the results of a join.
 * @param gatherNvtxName name to use for the NVTX range when producing the join gather maps
 * @param targetSize configured target batch size in bytes
 * @param opTime metric to record op time in the iterator
 * @param joinTime metric to record GPU time spent in join
 */
abstract class AbstractGpuJoinIterator(
    gatherNvtxName: String,
    targetSize: Long,
    val opTime: GpuMetric,
    joinTime: GpuMetric) extends Iterator[ColumnarBatch] with Arm with AutoCloseable {
  private[this] var nextCb: Option[ColumnarBatch] = None
  private[this] var gathererStore: Option[JoinGatherer] = None

  protected[this] var closed = false

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  /** Returns whether there are any more batches on the stream side of the join */
  protected def hasNextStreamBatch: Boolean

  /**
   * Called to setup the next join gatherer instance when the previous instance is done or
   * there is no previous instance. Because this is likely to call next or has next on the
   * stream side all implementations must track their own opTime metrics.
   * @param startNanoTime system nanoseconds timestamp at the top of the iterator loop, useful for
   *                      calculating the time spent producing the next stream batch
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def setupNextGatherer(): Option[JoinGatherer]

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    var mayContinue = true
    while (nextCb.isEmpty && mayContinue) {
      if (gathererStore.exists(!_.isDone)) {
        opTime.ns {
          nextCb = nextCbFromGatherer()
        }
      } else {
        if (hasNextStreamBatch) {
          // Need to refill the gatherer
          opTime.ns {
            gathererStore.foreach(_.close())
            gathererStore = None
          }
          gathererStore = setupNextGatherer()
          opTime.ns {
            nextCb = nextCbFromGatherer()
          }
        } else {
          mayContinue = false
        }
      }
    }
    if (nextCb.isEmpty) {
      // Nothing is left to return so close ASAP.
      opTime.ns(close())
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

  override def close(): Unit = {
    if (!closed) {
      nextCb.foreach(_.close())
      nextCb = None
      gathererStore.foreach(_.close())
      gathererStore = None
      closed = true
    }
  }

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics(gatherNvtxName, NvtxColor.DARK_GREEN, joinTime)) { _ =>
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
}

/**
 * Base class for join iterators that split and spill batches to avoid GPU OOM errors.
 * @param gatherNvtxName name to use for the NVTX range when producing the join gather maps
 * @param stream iterator to produce the batches for the streaming side input of the join
 * @param streamAttributes attributes corresponding to the streaming side input
 * @param builtBatch batch for the built side input of the join
 * @param targetSize configured target batch size in bytes
 * @param spillCallback callback to use when spilling
 * @param opTime metric to record time spent for this operation
 * @param joinTime metric to record GPU time spent in join
 */
abstract class SplittableJoinIterator(
    gatherNvtxName: String,
    stream: Iterator[LazySpillableColumnarBatch],
    streamAttributes: Seq[Attribute],
    builtBatch: LazySpillableColumnarBatch,
    targetSize: Long,
    spillCallback: SpillCallback,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends AbstractGpuJoinIterator(
      gatherNvtxName,
      targetSize,
      opTime = opTime,
      joinTime = joinTime) with Logging {
  // For some join types even if there is no stream data we might output something
  private var isInitialJoin = true
  // If the join explodes this holds batches from the stream side split into smaller pieces.
  private val pendingSplits = mutable.Queue[SpillableColumnarBatch]()

  protected def computeNumJoinRows(cb: ColumnarBatch): Long

  /**
   * Create a join gatherer.
   * @param cb next column batch from the streaming side of the join
   * @param numJoinRows if present, the number of join output rows computed for this batch
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def createGatherer(cb: ColumnarBatch, numJoinRows: Option[Long]): Option[JoinGatherer]

  override def hasNextStreamBatch: Boolean = {
    if (pendingSplits.nonEmpty) {
      // there's at least one cached stream-side batch ready to go
      true
    } else {
      // Querying the stream side may involve network or other non-GPU computation,
      // so ensure our memory buffers are spillable and release the GPU to allow other
      // threads to use the GPU in the interim. The stream iterator will re-grab the GPU
      // semaphore when it's ready to produce a batch.
      withResource(new NvtxRange("allow batch spill", NvtxColor.PURPLE)) { _ =>
        builtBatch.allowSpilling()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      }
      stream.hasNext || isInitialJoin
    }
  }

  override def setupNextGatherer(): Option[JoinGatherer] = {
    val wasInitialJoin = isInitialJoin
    isInitialJoin = false
    if (pendingSplits.nonEmpty || stream.hasNext) {
      val cb = if (pendingSplits.nonEmpty) {
        opTime.ns {
          withResource(pendingSplits.dequeue()) {
            _.getColumnarBatch()
          }
        }
      } else {
        val batch = withResource(stream.next()) { lazyBatch =>
          opTime.ns {
            lazyBatch.releaseBatch()
          }
        }
        batch
      }
      opTime.ns {
        withResource(cb) { cb =>
          val numJoinRows = computeNumJoinRows(cb)

          // We want the gather maps size to be around the target size. There are two gather maps
          // that are made up of ints, so compute how many rows on the stream side will produce the
          // desired gather maps size.
          val maxJoinRows = Math.max(1, targetSize / (2 * Integer.BYTES))
          if (numJoinRows > maxJoinRows && cb.numRows() > 1) {
            // Need to split the batch to reduce the gather maps size. This takes a simplistic
            // approach of assuming the data is uniformly distributed in the stream table.
            val numSplits = Math.min(cb.numRows(),
              Math.ceil(numJoinRows.toDouble / maxJoinRows).toInt)
            splitAndSave(cb, numSplits)

            // Return no gatherer so the outer loop will try again
            return None
          }

          createGatherer(cb, Some(numJoinRows))
        }
      }
    } else {
      opTime.ns {
        assert(wasInitialJoin)
        import scala.collection.JavaConverters._
        withResource(GpuColumnVector.emptyBatch(streamAttributes.asJava)) { cb =>
          createGatherer(cb, None)
        }
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtBatch.close()
      pendingSplits.foreach(_.close())
      pendingSplits.clear()
    }
  }

  /**
   * Split a stream-side input batch, making all splits spillable, and replacing this batch with
   * the splits in the stream-side input
   * @param cb stream-side input batch to split
   * @param numBatches number of splits to produce with approximately the same number of rows each
   * @param oom a prior OOM exception that this will try to recover from by splitting
   */
  protected def splitAndSave(
      cb: ColumnarBatch,
      numBatches: Int,
      oom: Option[OutOfMemoryError] = None): Unit = {
    val batchSize = cb.numRows() / numBatches
    if (oom.isDefined && batchSize < 100) {
      // We just need some kind of cutoff to not get stuck in a loop if the batches get to be too
      // small but we want to at least give it a chance to work (mostly for tests where the
      // targetSize can be set really small)
      throw oom.get
    }
    val msg = s"Split stream batch into $numBatches batches of about $batchSize rows"
    if (oom.isDefined) {
      logWarning(s"OOM Encountered: $msg")
    } else {
      logInfo(msg)
    }
    val splits = withResource(GpuColumnVector.from(cb)) { tab =>
      val splitIndexes = (1 until numBatches).map(num => num * batchSize)
      tab.contiguousSplit(splitIndexes: _*)
    }
    withResource(splits) { splits =>
      val schema = GpuColumnVector.extractTypes(cb)
      pendingSplits ++= splits.map { ct =>
        SpillableColumnarBatch(ct, schema,
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
      }
    }
  }

  /**
   * Create a join gatherer from gather maps.
   * @param maps gather maps produced from a cudf join
   * @param leftData batch corresponding to the left table in the join
   * @param rightData batch corresponding to the right table in the join
   * @return some gatherer or None if the are no rows to gather in this join batch
   */
  protected def makeGatherer(
      maps: Array[GatherMap],
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch,
      joinType: JoinType): Option[JoinGatherer] = {
    assert(maps.length > 0 && maps.length <= 2)
    try {
      val leftMap = maps.head
      val rightMap = if (maps.length > 1) {
        if (rightData.numCols == 0) {
          // No data so don't bother with it
          None
        } else {
          Some(maps(1))
        }
      } else {
        None
      }

      val lazyLeftMap = LazySpillableGatherMap(leftMap, spillCallback, "left_map")
      val gatherer = rightMap match {
        case None =>
          // When there isn't a `rightMap` we are in either LeftSemi or LeftAnti joins.
          // In these cases, the map and the table are both the left side, and everything in the map
          // is a match on the left table, so we don't want to check for bounds.
          rightData.close()
          JoinGatherer(lazyLeftMap, leftData, OutOfBoundsPolicy.DONT_CHECK)
        case Some(right) =>
          // Inner joins -- manifest the intersection of both left and right sides. The gather maps
          //   contain the number of rows that must be manifested, and every index
          //   must be within bounds, so we can skip the bounds checking.
          //
          // Left outer  -- Left outer manifests all rows for the left table. The left gather map
          //   must contain valid indices, so we skip the check for the left side. The right side
          //   has to be checked, since we need to produce nulls (for the right) for those
          //   rows on the left side that don't have a match on the right.
          //
          // Right outer -- Is the opposite from left outer (skip right bounds check, keep left)
          //
          // Full outer  -- Can produce nulls for any left or right rows that don't have a match
          //   in the opposite table. So we must check both gather maps.
          //
          val leftOutOfBoundsPolicy = joinType match {
            case _: InnerLike | LeftOuter => OutOfBoundsPolicy.DONT_CHECK
            case _ => OutOfBoundsPolicy.NULLIFY
          }
          val rightOutOfBoundsPolicy = joinType match {
            case _: InnerLike | RightOuter => OutOfBoundsPolicy.DONT_CHECK
            case _ => OutOfBoundsPolicy.NULLIFY
          }
          val lazyRightMap = LazySpillableGatherMap(right, spillCallback, "right_map")
          JoinGatherer(lazyLeftMap, leftData, lazyRightMap, rightData,
            leftOutOfBoundsPolicy, rightOutOfBoundsPolicy)
      }
      if (gatherer.isDone) {
        // Nothing matched...
        gatherer.close()
        None
      } else {
        Some(gatherer)
      }
    } finally {
      maps.foreach(_.close())
    }
  }
}
