/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{HashType, Table}
import com.nvidia.spark.rapids.{Arm, GpuBoundReference, GpuColumnVector, GpuExpression, GpuMetric, GpuShuffledHashJoinExec, SerializedTableColumn, SpillableColumnarBatch, SpillCallback, SpillPriorities, TaskAutoCloseableResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iterator that can tell if the data size of all the batches from current position
 * is larger than the specified `targetBatchSize`, and whether the batches are
 * serialized.
 */
class BatchTypeSizeAwareIterator(
    inputIter: Iterator[ColumnarBatch],
    targetBatchSize: Long,
    intputBatchSize: GpuMetric) extends Iterator[ColumnarBatch]
  with Arm with TaskAutoCloseableResource {

  assert(targetBatchSize > 0,
    s"Target batch size should be positive, but got $targetBatchSize")

  private val readBatchesQueue = ArrayBuffer.empty[ColumnarBatch]

  private var anyBatchSerialized: Option[Boolean] = None

  private[this] def getBatchSize(batch: ColumnarBatch): Long = {
    val isSerializedBatch = anyBatchSerialized.getOrElse {
      val ret = GpuShuffledHashJoinExec.isBatchSerialized(batch)
      // cache the result because it applies to all the input batches.
      anyBatchSerialized = Some(ret)
      ret
    }
    if (isSerializedBatch) {
      // Need to take care of this case because of the optimization introduced by
      // https://github.com/NVIDIA/spark-rapids/pull/4588.
      // Roughly return the serialized data length as the batch size.
      SerializedTableColumn.getMemoryUsed(batch)
    } else {
      GpuColumnVector.getTotalDeviceMemoryUsed(batch)
    }
  }

  private[this] def pullAndCacheOneBatch(): ColumnarBatch = {
    val batch = inputIter.next()
    readBatchesQueue += batch
    batch
  }

  override def close(): Unit = if (!closed) {
    readBatchesQueue.safeClose()
    readBatchesQueue.clear()
    super.close()
  }

  override def hasNext: Boolean = readBatchesQueue.nonEmpty || inputIter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException()
    closeOnExcept {
      if (readBatchesQueue.nonEmpty) {
        readBatchesQueue.remove(0)
      } else {
        inputIter.next()
      }
    } { batch =>
      intputBatchSize += getBatchSize(batch)
      batch
    }
  }

  /**
   * Whether the data size of all the batches from current position is larger than
   * the given `targetBatchSize`.
   */
  def isBatchesSizeOverflow: Boolean = {
    var readBatchesSize = readBatchesQueue.map(getBatchSize).sum
    while (readBatchesSize <= targetBatchSize && inputIter.hasNext) {
      readBatchesSize += getBatchSize(pullAndCacheOneBatch())
    }
    readBatchesSize > targetBatchSize
  }

  /**
   * Whether the batches in the input iterator are serialized.
   */
  def areBatchesSerialized: Boolean = anyBatchSerialized.getOrElse {
    val anyBatch = if (readBatchesQueue.nonEmpty) {
      Some(readBatchesQueue.head)
    } else if (inputIter.hasNext) {
      Some(pullAndCacheOneBatch())
    } else None
    val ret = anyBatch.exists(GpuShuffledHashJoinExec.isBatchSerialized)
    // cache the result because it applies to all the input batches.
    anyBatchSerialized = Some(ret)
    ret
  }
}

object GpuSubPartitionHashJoin extends Arm {
  /**
   * The seed for sub partitioner for hash join.
   * Differ from the default value: 0.
   */
  val SUB_PARTITION_HASH_SEED: Int = 100

  /**
   * Concatenate the input batches into a single one.
   * The caller is responsible for closing the returned batch.
   *
   * @param spillBatches the batches to be concatenated, will be closed after the call
   *                     returns.
   * @return the concatenated SpillableColumnarBatch or None if the input is empty.
   */
  def concatSpillBatchesAndClose(
      spillBatches: Seq[SpillableColumnarBatch],
      spillCallback: SpillCallback): Option[SpillableColumnarBatch] = {
    val retBatch = if (spillBatches.length >= 2) {
      // two or more batches, concatenate them
      val (concatTable, types) = withResource(spillBatches) { _ =>
        withResource(spillBatches.safeMap(_.getColumnarBatch())) { batches =>
          val batchTypes = GpuColumnVector.extractTypes(batches.head)
          withResource(batches.safeMap(GpuColumnVector.from)) { tables =>
            (Table.concatenate(tables: _*), batchTypes)
          }
        }
      }
      // Make the concatenated table spillable.
      withResource(concatTable) { _ =>
        SpillableColumnarBatch(GpuColumnVector.from(concatTable, types),
          SpillPriorities.ACTIVE_BATCHING_PRIORITY, spillCallback)
      }
    } else if (spillBatches.length == 1) {
      // only one batch
      spillBatches.head
    } else null

    Option(retBatch)
  }

  /**
   * Create an iterator that takes over the input resources, and make sure closing
   * all the resources when needed.
   */
  def safeIteratorFromSeq[R <: AutoCloseable](closeables: Seq[R]): Iterator[R] = {
    new Iterator[R] with TaskAutoCloseableResource {

      private[this] val remainingCloseables: ArrayBuffer[R] =
        ArrayBuffer(closeables: _*)

      override def hasNext: Boolean = remainingCloseables.nonEmpty && !closed

      override def next(): R = {
        if (!hasNext) throw new NoSuchElementException()
        remainingCloseables.remove(0)
      }

      override def close(): Unit = {
        remainingCloseables.safeClose()
        remainingCloseables.clear()
        super.close()
      }
    }
  }
}

/**
 * Drain the batches in the input iterator and partition each batch into smaller parts.
 * It assumes all the batches are on GPU.
 * e.g. There are two batches as below
 *     (2,4,6), (6,8,8),
 * and split them into 6 partitions. The result will be
 *   0 -> (2)
 *   1 -> (6), (6) (two separate not merged sub batches)
 *   2 -> empty
 *   3 -> (4)
 *   4 -> empty
 *   5 -> (8, 8) (A single batch)
 */
class GpuBatchSubPartitioner(
    inputIter: Iterator[ColumnarBatch],
    inputBoundKeys: Seq[GpuExpression],
    numPartitions: Int,
    spillCallback: SpillCallback) extends AutoCloseable with Arm {

  private var isNotInited = true
  private var numCurBatches = 0
  // At least two partitions
  private val realNumPartitions = Math.max(2, numPartitions)
  private val pendingParts =
    Array.fill(realNumPartitions)(ArrayBuffer.empty[SpillableColumnarBatch])

  private val keyIndices = {
    val boundIndices = ArrayBuffer.empty[Int]
    inputBoundKeys.foreach { e =>
      boundIndices ++= e.collect {
        case bound: GpuBoundReference => bound.ordinal
      }
    }
    boundIndices.distinct.toArray
  }

  /** The actual count of partitions */
  def partitionsCount: Int = realNumPartitions

  /**
   * Get the count of remaining batches (nonempty) in all the partitions currently.
   */
  def batchesCount: Int = {
    initPartitions()
    numCurBatches
  }

  /**
   * Get a partition data as a Seq of SpillableColumnarBatch. The caller should NOT close
   * the returned batches.
   * If the caller wants to own the returned batches, call `releaseBatchByPartition` instead.
   * @param partId the partition id. An exception will be raised up if it is out of range.
   * @return a Seq of SpillableColumnarBatch if the given "partId" is in range, or null if
   *         the partition has been released.
   */
  def getBatchesByPartition(partId: Int): Seq[SpillableColumnarBatch] = {
    initPartitions()
    pendingParts(partId)
  }

  /**
   * Release a partition data as a Seq of SpillableColumnarBatch, and the caller is
   * responsible for closing the returned batch.
   *
   * @param partId the partition id. An exception will be raised up if it is out of range.
   * @return a Seq of SpillableColumnarBatch if the given "partId" is in range, or null if
   *         the partition has been released.
   */
  def releaseBatchesByPartition(partId: Int): Seq[SpillableColumnarBatch] = {
    initPartitions()
    val ret = pendingParts(partId)
    if (ret != null) {
      numCurBatches -= ret.length
      pendingParts(partId) = null
    }
    ret
  }

  override def close(): Unit = {
    pendingParts.filterNot(_ == null).flatten.safeClose()
    // batches are closed, safe to set all to null
    (0 until realNumPartitions).foreach(releaseBatchesByPartition)
  }

  private def initPartitions(): Unit = {
    if (isNotInited) {
      partitionBatches()
      isNotInited = false
    }
  }

  private[this] def partitionBatches(): Unit = {
    while (inputIter.hasNext) {
      val gpuBatch = inputIter.next()
      if (gpuBatch.numRows() > 0 && gpuBatch.numCols() > 0) {
        val types = GpuColumnVector.extractTypes(gpuBatch)
        // 1) Hash partition on the batch
        val partedTable = withResource(gpuBatch) { _ =>
          withResource(GpuColumnVector.from(gpuBatch)) { table =>
            table.onColumns(keyIndices: _*)
              .hashPartition(HashType.MURMUR3, realNumPartitions,
                GpuSubPartitionHashJoin.SUB_PARTITION_HASH_SEED)
          }
        }
        // 2) Split into smaller tables according to partitions
        val subTables = withResource(partedTable) { _ =>
          partedTable.getTable.contiguousSplit(partedTable.getPartitions.tail: _*)
        }
        // 3) Make each smaller table spillable and cache them in the queue
        withResource(subTables) { _ =>
          subTables.zipWithIndex.foreach { case (table, id) =>
            // skip empty tables
            if (table.getRowCount > 0) {
              pendingParts(id) += SpillableColumnarBatch(table, types,
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
              numCurBatches += 1
            }
          }
        }
      } else if (gpuBatch.numRows() > 0 && gpuBatch.numCols() == 0) {
        // Rows only batch. This should never happen for a hash join in Spark.
        gpuBatch.close()
      } else {
        // Skip empty batches
        gpuBatch.close()
      }
    } // end of while
  }
}

/**
 * Iterate all the partitions in the input "batchSubPartitioner," and each call to
 * "next()" will return one or multiple parts as a single "SpillableColumnarBatch",
 * or None for an empty partition, along with its partition id(s).
 */
class GpuBatchSubPartitionIterator(
    batchSubPartitioner: GpuBatchSubPartitioner,
    targetBatchSize: Long,
    spillCallback: SpillCallback)
  extends Iterator[(Seq[Int], Option[SpillableColumnarBatch])] with Arm with Logging {

  // The partitions to be read. Initially it is all the partitions.
  private val remainingPartIds: ArrayBuffer[Int] =
    ArrayBuffer.range(0, batchSubPartitioner.partitionsCount)

  override def hasNext: Boolean = remainingPartIds.nonEmpty

  override def next(): (Seq[Int], Option[SpillableColumnarBatch]) = {
    if (!hasNext) throw new NoSuchElementException()
    // Get the next partition ids for this output.
    val partIds = nextPartitions()
    // Take over the batches of one or multiple partitions according to the ids. And
    // concatenate them in a single batch.
    val spillBatches = closeOnExcept(ArrayBuffer.empty[SpillableColumnarBatch]) { buf =>
      partIds.foreach { pid =>
        buf ++= batchSubPartitioner.releaseBatchesByPartition(pid)
      }
      buf
    }
    val retBatch = GpuSubPartitionHashJoin.concatSpillBatchesAndClose(
      spillBatches, spillCallback)
    closeOnExcept(retBatch) { _ =>
      // Update the remaining partitions
      remainingPartIds --= partIds
      (partIds, retBatch)
    }
  }

  private[this] def nextPartitions(): Seq[Int] = {
    val ret = ArrayBuffer.empty[Int]
    var accPartitionSize = 0L
    // always append the first one.
    val firstPartId = remainingPartIds.head
    val firstPartSize = computePartitionSize(firstPartId)
    if (firstPartSize > targetBatchSize) {
      logWarning(s"Got partition that size($firstPartSize) is larger than" +
        s" target size($targetBatchSize)")
    }
    ret += firstPartId
    accPartitionSize += firstPartSize
    // For each output, try to collect small nonempty partitions to reach
    // "targetBatchSize" as much as possible.
    // e.g. We have 5 partitions as below. And the "targetBatchSize" is 16 bytes.
    //    p1: (3,4)            -> 8 bytes
    //    p2: (1,2), (11, 22)  -> 16 bytes
    //    p3: (5,6)            -> 8 bytes
    //    p4: <empty>          -> 0 bytes
    //    p5: <empty>          -> 0 bytes
    // Then the p1 and p3 will be put together (8 bytes + 8 bytes) in one output.
    //    next: (3,4), (5,6)
    //    next: (1,2), (11, 22)
    //    next: <empty>
    //    next: <empty>
    if (firstPartSize > 0) {
      remainingPartIds.tail.foreach { partId =>
        val partSize = computePartitionSize(partId)
        // Do not coalesce empty partitions, and output them one by one.
        if (partSize > 0 && (accPartitionSize + partSize) <= targetBatchSize) {
          ret += partId
          accPartitionSize += partSize
        }
      }
    }
    ret
  }

  private[this] def computePartitionSize(partId: Int): Long = {
    val batches = batchSubPartitioner.getBatchesByPartition(partId)
    if (batches != null) batches.map(_.sizeInBytes).sum else 0L
  }
}

/**
 * An utils class that will take over the two input resources representing data from
 * build side and stream side separately.
 * build data is passed in as a Option of a single batch, while stream data is as
 * a Seq of batches.
 */
class PartitionPair(
    private var build: Option[SpillableColumnarBatch],
    private var stream: Seq[SpillableColumnarBatch]) extends AutoCloseable {

  /**
   * Whether the PartitionPair pair is empty.
   * A pair is empty when both build and stream are empty.
   */
  def isEmpty: Boolean = build.isEmpty && stream.isEmpty

  /**
   * Get the batches from two sides as a Tuple(build, stream).
   */
  def get: (Option[SpillableColumnarBatch], Seq[SpillableColumnarBatch]) = {
    (build, stream)
  }

  override def close(): Unit = {
    stream.safeClose()
    stream = Seq.empty[SpillableColumnarBatch]
    build.foreach(_.safeClose())
    build = None
  }
}

/**
 * Iterator that returns a pair of batches (build side, stream side) with the same key set
 * generated by sub-partitioning algorithm when each call to "next".
 * Each pair may have data from one or multiple partitions. And for build side, batches are
 * concatenated into a single one.
 *
 * It will skip the empty pairs by default. Set "skipEmptyPairs" to false to also get
 * the empty pairs.
 */
class GpuSubPartitionPairIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    numPartitions: Int,
    targetBatchSize: Long,
    spillCallback: SpillCallback,
    skipEmptyPairs: Boolean = true)
  extends Iterator[PartitionPair] with Arm with AutoCloseable {

  private val buildSubPartitioner =
    new GpuBatchSubPartitioner(buildIter, boundBuildKeys, numPartitions, spillCallback)
  private val buildSubIterator =
    new GpuBatchSubPartitionIterator(buildSubPartitioner, targetBatchSize, spillCallback)
  private val streamSubPartitioner =
    new GpuBatchSubPartitioner(streamIter, boundStreamKeys, numPartitions, spillCallback)

  private[this] var closed = false

  private[this] var partitionPair: Option[PartitionPair] = None
  private[this] var pairConsumed = true

  override def hasNext: Boolean = {
    if (closed) return false
    if (pairConsumed) {
      do {
        partitionPair.foreach(_.close())
        partitionPair = None
        partitionPair = tryPullNextPair()
      } while (partitionPair.exists(_.isEmpty) && skipEmptyPairs)
      pairConsumed = false
    }
    partitionPair.isDefined
  }

  override def next(): PartitionPair = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val pair = partitionPair.get
    partitionPair = None
    pairConsumed = true
    pair
  }

  override def close(): Unit = if (!closed) {
    closed = true
    // for real safe close
    val e = new Exception()
    buildSubPartitioner.safeClose(e)
    streamSubPartitioner.safeClose(e)
    partitionPair.foreach(_.close())
    partitionPair = None
  }

  private[this] val hasNextBatch: () => Boolean = if (skipEmptyPairs) {
    // Check the batch numbers directly can stop early when the remaining partitions
    // are all empty on both build side and stream side.
    () => buildSubPartitioner.batchesCount > 0 || streamSubPartitioner.batchesCount > 0
  } else {
    () => buildSubIterator.hasNext
  }

  private[this] def tryPullNextPair(): Option[PartitionPair] = {
    if(hasNextBatch()) {
      val (partIds, spillBuildBatch) = buildSubIterator.next()
      closeOnExcept(spillBuildBatch) { _ =>
        closeOnExcept(ArrayBuffer.empty[SpillableColumnarBatch]) { streamBuf =>
          partIds.foreach { id =>
            streamBuf ++= streamSubPartitioner.releaseBatchesByPartition(id)
          }
          Some(new PartitionPair(spillBuildBatch, streamBuf))
        }
      }
    } else None
  }
}

/** Base class for joins by sub-partitioning algorithm */
abstract class BaseSubHashJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    numPartitions: Int,
    targetSize: Long,
    spillCallback: SpillCallback,
    opTime: GpuMetric)
  extends Iterator[ColumnarBatch] with Arm with TaskAutoCloseableResource {

  // skip empty partition pairs
  private[this] val subPartitionPairIter = new GpuSubPartitionPairIterator(buildIter,
    boundBuildKeys, streamIter, boundStreamKeys, numPartitions, targetSize, spillCallback)

  private[this] var joinIter: Option[Iterator[ColumnarBatch]] = None
  private[this] var nextCb: Option[ColumnarBatch] = None

  override def close(): Unit = {
    nextCb.foreach(_.safeClose(new Exception))
    nextCb = None
    subPartitionPairIter.close()
    super.close()
  }

  override def hasNext: Boolean = {
    if (closed) return false
    var mayContinue = true
    // Loop to support optimizing out some pairs by returning a None instead
    // of a join iterator.
    while (nextCb.isEmpty && mayContinue) {
      if (joinIter.exists(_.hasNext)) {
        nextCb = joinIter.map(_.next())
      } else {
        val hasNextPair = opTime.ns {
          subPartitionPairIter.hasNext
        }
        if (hasNextPair) {
          // Need to refill the join iterator
          joinIter.foreach {
            case closeable: AutoCloseable => closeable.close()
            case _ => // noop
          }
          joinIter = None
          opTime.ns {
            withResource(subPartitionPairIter.next()) { pair =>
              joinIter = setupJoinIterator(pair)
            }
          }
          // try to pull next batch right away to avoid a loop again
          if (joinIter.exists(_.hasNext)) {
            nextCb = joinIter.map(_.next())
          }
        } else {
          mayContinue = false
        }
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

  protected def setupJoinIterator(pair: PartitionPair): Option[Iterator[ColumnarBatch]]
}

trait GpuSubPartitionHashJoin extends Arm with Logging { self: GpuHashJoin =>

  protected lazy val buildSchema: StructType = StructType.fromAttributes(buildPlan.output)

  def doJoinBySubPartition(
      builtIter: Iterator[ColumnarBatch],
      streamIter: Iterator[ColumnarBatch],
      targetSize: Long,
      numPartitions: Int,
      spillCallback: SpillCallback,
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {

    // A log for test to verify that sub-partitioning is used.
    logInfo(s"$joinType hash join is executed by sub-partitioning " +
      s"in task ${TaskContext.get().taskAttemptId()}")

    new BaseSubHashJoinIterator(builtIter, boundBuildKeys, streamIter,
        boundStreamKeys, numPartitions, targetSize, spillCallback, opTime) {

      private[this] def canOptimizeOut(pair: PartitionPair): Boolean = {
        val (build, stream) = pair.get
        joinType match {
          case _: InnerLike =>
            // For inner join, no need to run if either side is empty
            build.isEmpty || stream.isEmpty
          case _ => false
        }
      }

      override def setupJoinIterator(pair: PartitionPair): Option[Iterator[ColumnarBatch]] = {
        if (canOptimizeOut(pair)) {
          // Skip it due to optimization
          None
        } else {
          val (build, stream) = pair.get
          val buildCb = build.map(_.getColumnarBatch())
            .getOrElse(GpuColumnVector.emptyBatch(buildSchema))
          withResource(buildCb) { _ =>
            val streamIter = closeOnExcept(stream.safeMap(_.getColumnarBatch())) { streamCbs =>
              GpuSubPartitionHashJoin.safeIteratorFromSeq(streamCbs)
            }

            // Leverage the original join iterators
            val joinIter = doJoin(buildCb, streamIter, targetSize, spillCallback,
              numOutputRows, joinOutputRows, numOutputBatches, opTime, joinTime)
            Some(joinIter)
          }
        }
      }

    } // end of "new BaseSubHashJoinIterator"
  }
}
