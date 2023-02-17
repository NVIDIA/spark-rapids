/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{GatherMap, NvtxColor, OutOfBoundsPolicy}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{RetryOOM, RmmSpark, SplitAndRetryOOM}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.rapids.execution.GpuHashJoin
import org.apache.spark.sql.vectorized.ColumnarBatch

trait TaskAutoCloseableResource extends AutoCloseable {
  protected var closed = false
  // iteration-independent resources
  private val resources = scala.collection.mutable.ArrayBuffer[AutoCloseable]()
  def use[T <: AutoCloseable](ac: T): T = {
    resources += ac
    ac
  }

  override def close() = if (!closed) {
    closed = true
    resources.reverse.safeClose()
    resources.clear()
  }

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())
}

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
    joinTime: GpuMetric)
    extends Iterator[ColumnarBatch]
    with Arm
    with TaskAutoCloseableResource {
  private[this] var nextCb: Option[ColumnarBatch] = None
  private[this] var gathererStore: Option[JoinGatherer] = None

  /** Returns whether there are any more batches on the stream side of the join */
  protected def hasNextStreamBatch: Boolean

  /**
   * Called to setup the next join gatherer instance when the previous instance is done or
   * there is no previous instance. Because this is likely to call next or has next on the
   * stream side all implementations must track their own opTime metrics.
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def setupNextGatherer(): Option[JoinGatherer]

  protected def getFinalBatch(): Option[ColumnarBatch] = None

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
      nextCb = getFinalBatch()
      if (nextCb.isEmpty) {
        // Nothing is left to return so close ASAP.
        opTime.ns(close())
      }
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

  private def nextCbFromGathererWithRetry(gather: JoinGatherer, nextRows: Int): ColumnarBatch = {
    var numRetries = 0
    var doSplit = false
    var done = false
    var fail = false
    var retryRowCount = nextRows
    var retryExcept: Throwable = null
    var result : ColumnarBatch = null
    // Temporary hack for testing
    RmmSpark.associateCurrentThreadWithTask(1)
    while (!done && !fail) {
      // If we are retrying, block until we get the go ahead
      if (numRetries > 0) {
        RmmSpark.blockThreadUntilReady()
        gather.restore // restore from last checkpoint
      }
      if (GpuHashJoin.retryGatherCount > 0) {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
        GpuHashJoin.retryGatherCount -= 1
      } else if (GpuHashJoin.splitGatherCount > 0) {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
        GpuHashJoin.splitGatherCount -= 1
      }
      if (doSplit) {
        doSplit = false
        val rowsLeft = gather.numRowsLeft
        retryRowCount = Math.min(Math.min(retryRowCount / 2, rowsLeft), Integer.MAX_VALUE).toInt
        if (retryRowCount < GpuHashJoin.minBatchSize) {
          val oom = new OutOfMemoryError("Out of Memory - unable to split input data")
          if (retryExcept != null) {
            oom.addSuppressed(retryExcept)
          }
          throw oom
        }
      }
      gather.checkpoint
      try {
        result = gather.gatherNext(retryRowCount)
        done = true
      } catch {
        case retryOOM: RetryOOM =>
          if (retryExcept == null) {
            retryExcept = retryOOM
          } else {
            retryExcept.addSuppressed(retryOOM)
          }
          numRetries = numRetries + 1
          println(s"Retrying, tries so far is ${numRetries}")
        case splitAndRetryOOM: SplitAndRetryOOM => // we are the only thread
          if (retryExcept == null) {
            retryExcept = splitAndRetryOOM
          } else {
            retryExcept.addSuppressed(splitAndRetryOOM)
          }
          numRetries = numRetries + 1
          println(s"Split, tries so far is ${numRetries}")
          doSplit = true
        case other: Throwable =>
          if (retryExcept == null) {
            retryExcept = other
          } else {
            retryExcept.addSuppressed(other)
          }
          throw retryExcept
      }
    }
    // Temporary for testing
    RmmSpark.removeCurrentThreadAssociation()
    result
  }

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics(gatherNvtxName, NvtxColor.DARK_GREEN, joinTime)) { _ =>
      val ret = gathererStore.map { gather =>
        val nextRows = JoinGatherer.getRowsInNextBatch(gather, targetSize)
        nextCbFromGathererWithRetry(gather, nextRows)
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
  private val pendingSplits = scala.collection.mutable.Queue[SpillableColumnarBatch]()

  protected def computeNumJoinRows(scb: SpillableColumnarBatch): Long

  /**
   * Create a join gatherer.
   * @param scb next column batch from the streaming side of the join
   * @param numJoinRows if present, the number of join output rows computed for this batch
   * @return some gatherer to use next or None if there is no next gatherer or the loop should try
   *         to build the gatherer again (e.g.: to skip a degenerate join result batch)
   */
  protected def createGatherer(scb: SpillableColumnarBatch,
      numJoinRows:Option[Long]): Option[JoinGatherer]

  override def hasNextStreamBatch: Boolean = {
    isInitialJoin || pendingSplits.nonEmpty || stream.hasNext
  }

  override def setupNextGatherer(): Option[JoinGatherer] = {
    val wasInitialJoin = isInitialJoin
    isInitialJoin = false
    if (pendingSplits.nonEmpty || stream.hasNext) {
      val scb = if (pendingSplits.nonEmpty) {
        pendingSplits.dequeue()
      } else {
        val batch = withResource(stream.next()) { lazyBatch =>
          opTime.ns {
            SpillableColumnarBatch(lazyBatch.releaseBatch(),
              SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
          }
        }
        batch
      }
      opTime.ns {
        withResource(scb) { _ =>
          val numJoinRows = computeNumJoinRows(scb)

          // We want the gather maps size to be around the target size. There are two gather maps
          // that are made up of ints, so compute how many rows on the stream side will produce the
          // desired gather maps size.
          val maxJoinRows = Math.max(1, targetSize / (2 * Integer.BYTES))
          if (numJoinRows > maxJoinRows && scb.numRows() > 1) {
            // Need to split the batch to reduce the gather maps size. This takes a simplistic
            // approach of assuming the data is uniformly distributed in the stream table.
            val numSplits = Math.min(scb.numRows(),
              Math.ceil(numJoinRows.toDouble / maxJoinRows).toInt)
            splitAndSave(scb, numSplits)

            // Return no gatherer so the outer loop will try again
            return None
          }

          createGathererWithRetry(scb, Some(numJoinRows))
        }
      }
    } else {
      opTime.ns {
        assert(wasInitialJoin)
        import scala.collection.JavaConverters._
        withResource(
          SpillableColumnarBatch(GpuColumnVector.emptyBatch(streamAttributes.asJava),
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)) { scb =>
          createGathererWithRetry(scb, None)
        }
      }
    }
  }

  protected def createGathererHandleSplit(scb: SpillableColumnarBatch,
      except: Throwable): Option[JoinGatherer] = {
    val oom = new OutOfMemoryError("Out of Memory - unable to split input data")
    if (except != null) {
      oom.addSuppressed(except)
    }
    throw oom
  }

  protected def createGathererWithRetry(
      scb: SpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    var numRetries = 0
    var doSplit = false
    var done = false
    var fail = false
    var retryExcept: Throwable = null
    var result : Option[JoinGatherer] = None
    // Temporary hack for testing
    RmmSpark.associateCurrentThreadWithTask(1)
    while (!done && !doSplit && !fail) {
      // If we are retrying, block until we get the go ahead
      if (numRetries > 0) {
        RmmSpark.blockThreadUntilReady()
      }
      if (GpuHashJoin.retryOOMCount > 0) {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
        GpuHashJoin.retryOOMCount -= 1
      } else if (GpuHashJoin.doSplitOOM) {
        RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId)
        GpuHashJoin.doSplitOOM = false
      }
      try {
        result = createGatherer(scb, numJoinRows)
        done = true
      } catch {
        case retryOOM: RetryOOM =>
          if (retryExcept == null) {
            retryExcept = retryOOM
          } else {
            retryExcept.addSuppressed(retryOOM)
          }
          numRetries = numRetries + 1
          println(s"Retrying, retries so far is ${numRetries}")
        case splitAndRetryOOM: SplitAndRetryOOM => // we are the only thread
          if (retryExcept == null) {
            retryExcept = splitAndRetryOOM
          } else {
            retryExcept.addSuppressed(splitAndRetryOOM)
          }
          println(s"Split, retries so far is ${numRetries}")
          doSplit = true
        // Temporary: This case is here to keep current functionality
        case outOfMem: OutOfMemoryError =>
          if (retryExcept == null) {
            retryExcept = outOfMem
          } else {
            retryExcept.addSuppressed(outOfMem)
          }
          println(s"Split, retries so far is ${numRetries}")
          doSplit = true
        case other: Throwable =>
          if (retryExcept == null) {
            retryExcept = other
          } else {
            retryExcept.addSuppressed(other)
          }
          fail = true
      }
    }
    // Temporary for testing
    RmmSpark.removeCurrentThreadAssociation()
    if (done) {
      result
    } else if (doSplit) {
      createGathererHandleSplit(scb, retryExcept)
    } else { // fail case
      throw retryExcept
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
   * @param scb stream-side input batch to split
   * @param numBatches number of splits to produce with approximately the same number of rows each
   * @param except a prior OOM exception that this will try to recover from by splitting
   */
  protected def splitAndSave(
      scb: SpillableColumnarBatch,
      numBatches: Int,
      except: Option[Throwable] = None): Unit = {
    val batchSize = scb.numRows() / numBatches
    if (except.isDefined && batchSize < GpuHashJoin.minBatchSize) {
      // We just need some kind of cutoff to not get stuck in a loop if the batches get to be too
      // small but we want to at least give it a chance to work (mostly for tests where the
      // targetSize can be set really small)
      val oom = new OutOfMemoryError(
        s"Out of Memory - reached split limit of ${GpuHashJoin.minBatchSize} rows")
      oom.addSuppressed(except.get)
      throw oom
    }
    val msg = s"Split stream batch into $numBatches batches of about $batchSize rows"
    if (except.isDefined) {
      logWarning(s"OOM Encountered: $msg")
    } else {
      logInfo(msg)
    }
    withResource(scb.getColumnarBatch()) { cb =>
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
