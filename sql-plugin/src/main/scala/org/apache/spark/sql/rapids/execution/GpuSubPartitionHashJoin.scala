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

import ai.rapids.cudf.{GatherMap, HashType, NvtxColor, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids.{AbstractGpuJoinIterator, Arm, GpuBoundReference, GpuBuildLeft, GpuBuildRight, GpuBuildSide, GpuColumnVector, GpuExpression, GpuMetric, GpuProjectExec, GpuShuffledHashJoinExec, JoinGatherer, LazySpillableColumnarBatch, NvtxWithMetrics, SerializedTableColumn, SpillableColumnarBatch, SpillCallback, SpillPriorities, SplittableJoinIterator, TaskAutoCloseableResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iterator that can tell if the data size of all the batches from current position
 * is larger than the specified `targetBatchSize`, and whether the batches are
 * serialized.
 */
class BatchTypeSizeAwareIterator(
    inputIter: Iterator[ColumnarBatch],
    targetBatchSize: Long) extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  assert(targetBatchSize >= 0,
    s"Target batch size should not be negative, but get $targetBatchSize")

  private val readBatchesQueue = ArrayBuffer.empty[ColumnarBatch]

  // Initialize it early by prefetching one batch to avoid duplicate checks on each batch
  private val anyBatchSerialized = {
    if (inputIter.hasNext) {
      readBatchesQueue += inputIter.next()
    }
    readBatchesQueue.nonEmpty &&
      GpuShuffledHashJoinExec.isBatchSerialized(readBatchesQueue.head)
  }

  private lazy val getBatchSize: ColumnarBatch => Long = if (anyBatchSerialized) {
    // Need to take care of this case because of the optimization introduced by
    // https://github.com/NVIDIA/spark-rapids/pull/4588.
    // Roughly return the serialized data length as the batch size.
    SerializedTableColumn.getMemoryUsed
  } else {
    GpuColumnVector.getTotalDeviceMemoryUsed
  }

  override def close(): Unit = if (!closed) {
    readBatchesQueue.safeClose()
    readBatchesQueue.clear()
    super.close()
  }

  override def hasNext: Boolean = readBatchesQueue.nonEmpty || inputIter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException()
    if (readBatchesQueue.nonEmpty) {
      readBatchesQueue.remove(0)
    } else inputIter.next()
  }

  /**
   * Whether the data size of all the batches from current position is larger than
   * the given `targetBatchSize`.
   */
  def isBatchesSizeOverflow: Boolean = {
    var readBatchesSize = readBatchesQueue.map(getBatchSize).sum
    while (inputIter.hasNext && readBatchesSize <= targetBatchSize) {
      val batch = inputIter.next()
      readBatchesQueue += batch
      readBatchesSize += getBatchSize(batch)
    }
    readBatchesSize > targetBatchSize
  }

  /**
   * Whether the batches in the input iterator are serialized.
   */
  def areBatchesSerialized: Boolean = anyBatchSerialized
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
        withResource(GpuColumnVector.from(concatTable, types)) { concatBatch =>
          SpillableColumnarBatch(concatBatch, SpillPriorities.ACTIVE_BATCHING_PRIORITY,
            spillCallback)
        }
      }
    } else if (spillBatches.length == 1) {
      // only one batch
      spillBatches.head
    } else null

    Option(retBatch)
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
private[rapids] class GpuBatchSubPartitioner(
    inputIter: Iterator[ColumnarBatch],
    inputBoundKeys: Seq[GpuExpression],
    numPartitions: Int,
    spillCallback: SpillCallback) extends AutoCloseable with Arm {

  private var isNotInited = true
  private var numTotalBatches, numCurBatches = 0
  // At least two partitions
  private val realNumPartitions = Math.max(2, numPartitions)
  private val partsQueue =
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
   * The count of batches (nonempty) in all the partitions just after partition is done,
   * and no batch has been released yet.
   */
  def batchesTotalCount: Int = {
    initPartitions()
    numTotalBatches
  }

  /**
   * The count of batches (nonempty) in all the partitions currently when being called.
   * Maybe some batches have been released.
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
    if (0 <= partId && partId < realNumPartitions) {
      partsQueue(partId)
    } else {
      throw new IndexOutOfBoundsException(s"$partId")
    }
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
    if (0 <= partId && partId < realNumPartitions) {
      val ret = partsQueue(partId)
      if (ret != null) {
        numCurBatches -= ret.length
      }
      partsQueue(partId)  = null
      ret
    } else {
      throw new IndexOutOfBoundsException(s"$partId")
    }
  }

  override def close(): Unit = {
    partsQueue.flatten.safeClose()
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
              partsQueue(id) += SpillableColumnarBatch(table, types,
                SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
              numCurBatches += 1
            }
          }
        }
      } else if (gpuBatch.numRows() > 0 && gpuBatch.numCols() == 0) {
        // Rows only batch
        // FIXME how to handle this case correctly ?
        throw new IllegalStateException("Rows-only batch is not allowed")
      } else {
        // Skip empty batches
        gpuBatch.close()
      }
    } // end of while
    numTotalBatches = numCurBatches
  }
}

/**
 * Iterate all the partitions in the input "batchSubPartitioner," and each call to
 * "next()" will return one or multiple parts as a single "SpillableColumnarBatch",
 * or None for an empty part, along with its partition id(s).
 */
class GpuBatchSubPartitionIterator(
    batchSubPartitioner: GpuBatchSubPartitioner,
    spillCallback: SpillCallback)
  extends Iterator[(Array[Int], Option[SpillableColumnarBatch])] with Arm {

  private lazy val avgNumBatches =
    batchSubPartitioner.batchesTotalCount/batchSubPartitioner.partitionsCount

  // The partitions to be read. Initially it is all the partitions.
  // Stored as
  //   [(partition1_id, batch_count1), (partition2_id, batch_count2), ...]
  private lazy val remainingPartIdsAndSizes =
    ArrayBuffer.range(0, batchSubPartitioner.partitionsCount).map { id =>
      (id, batchSubPartitioner.getBatchesByPartition(id).length)
    }

  override def hasNext: Boolean = remainingPartIdsAndSizes.nonEmpty

  override def next(): (Array[Int], Option[SpillableColumnarBatch]) = {
    if (!hasNext) throw new NoSuchElementException()
    // Get the next partition ids for this output. It will try to coalesce small
    // partitions to reach "avgNumBatches" batches as much as possible.
    val (ids, poss) = nextPartitionIdsAndPoss().unzip
    // Take over the batches of one or multiple partitions according to the ids. And
    // concatenate them in a single batch.
    val spillBatches = closeOnExcept(ArrayBuffer.empty[SpillableColumnarBatch]) { buf =>
      ids.foreach { id =>
        buf ++= batchSubPartitioner.releaseBatchesByPartition(id)
      }
      buf
    }
    val retBatch = GpuSubPartitionHashJoin.concatSpillBatchesAndClose(
      spillBatches, spillCallback)

    // Update the remaining partitions
    poss.foreach(remainingPartIdsAndSizes.remove)
    (ids, retBatch)
  }

  private[this] def nextPartitionIdsAndPoss(): Array[(Int, Int)] = {
    val ret = ArrayBuffer.empty[(Int, Int)]
    var totalBatches = 0
    // always append the first one
    val (firstId, firstBatchCnt) = remainingPartIdsAndSizes.head
    ret += ((firstId, 0))
    totalBatches += firstBatchCnt
    // For each output, try to collect small nonempty partitions to reach "avgNumBatches"
    // batches as much as possible.
    // e.g. We have 5 partitions as below. The first partition has two batches, and the
    // other two partitions have one batch for each.
    //    p1: (3,4)
    //    p2: (1,2), (11, 22)
    //    p3: (5,6)
    //    p4: <empty>
    //    p5: <empty>
    // Then the p1 and p3 will be put together in one output.
    //    next: (3,4), (5,6)
    //    next: (1,2), (11, 22)
    //    next: <empty>
    //    next: <empty>
    if (firstBatchCnt > 0) {
      remainingPartIdsAndSizes.tail.zipWithIndex.foreach { case ((id, batchCnt), pos) =>
        // Do not coalesce empty partitions, and output them one by one.
        if (batchCnt > 0 && (totalBatches + batchCnt) <= avgNumBatches) {
          ret += ((id, pos))
          totalBatches += batchCnt
        }
      }
    }
    ret.toArray
  }
}

/**
 * Iterator that returns a pair of batches (build batch, stream batch) generated by
 * by sub-partition algorithm when each call to "next".
 */
class GpuBatchPairSubPartitionIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    numPartitions: Int,
    spillCallback: SpillCallback)
  extends Iterator[(Option[ColumnarBatch], Option[ColumnarBatch])]
    with Arm with AutoCloseable {

  private val buildSubPartitioner =
    new GpuBatchSubPartitioner(buildIter, boundBuildKeys, numPartitions, spillCallback)
  private val buildSubIterator =
    new GpuBatchSubPartitionIterator(buildSubPartitioner, spillCallback)
  private val streamSubPartitioner =
    new GpuBatchSubPartitioner(streamIter, boundStreamKeys, numPartitions, spillCallback)

  private[this] var curBuiltBatch: Option[SpillableColumnarBatch] = None
  private[this] var curPartIds: Array[Int] = Array.empty[Int]
  private[this] var closed = false

  override def hasNext: Boolean = {
    // Stop early when there is no data left in both build and stream, instead of
    // iterating all the partitions by "buildSubIterator.hasNext" here.
    val hasData =
      buildSubPartitioner.batchesCount > 0 || streamSubPartitioner.batchesCount > 0
    (curPartIds.nonEmpty || hasData) && !closed
  }

  override def next(): (Option[ColumnarBatch], Option[ColumnarBatch]) = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (curPartIds.isEmpty && buildSubIterator.hasNext) {
      // Refresh the current ids and batch with a new one from the build side.
      val (partIds, spillBatch) = buildSubIterator.next()
      curPartIds = partIds
      curBuiltBatch = spillBatch
    }

    // "curPartIds" can not be empty if coming here.
    val curId = curPartIds.head
    // One partition each time, do not coalesce partitions for stream side.
    // Stream side has the same number of partitions with the build side, so its
    // data will be drained as iterating the build side partitions.
    val streamSpillBatch = GpuSubPartitionHashJoin.concatSpillBatchesAndClose(
      streamSubPartitioner.releaseBatchesByPartition(curId), spillCallback)
    withResource(streamSpillBatch) { _ =>
      // refresh the cache
      curPartIds = curPartIds.tail
      if (curPartIds.isEmpty) {
        // Done for the current built batch, close it right away
        curBuiltBatch.foreach(_.close())
        curBuiltBatch = None
      }
      (curBuiltBatch.map(_.getColumnarBatch()),
        streamSpillBatch.map(_.getColumnarBatch()))
    }
  }

  override def close(): Unit = if (!closed) {
    // for real safe close
    val e = new Exception()
    curBuiltBatch.foreach(_.safeClose(e))
    curBuiltBatch = None
    buildSubPartitioner.safeClose(e)
    streamSubPartitioner.safeClose(e)
    closed = true
  }
}

/** Iterators for joins */
class SubHashExistenceJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    val boundBuildKeys: Seq[GpuExpression],
    buildSchema: StructType,
    streamIter: Iterator[ColumnarBatch],
    val boundStreamKeys: Seq[GpuExpression],
    val boundCondition: Option[GpuExpression],
    val numFirstConditionTableColumns: Int,
    val compareNullsEqual: Boolean,
    numPartitions: Int,
    spillCallback: SpillCallback,
    val opTime: GpuMetric,
    val joinTime: GpuMetric)
  extends Iterator[ColumnarBatch] with Arm with GpuHashExistenceJoinLike {

  private[this] val batchPairSubIterator = new GpuBatchPairSubPartitionIterator(
    buildIter, boundBuildKeys, streamIter, boundStreamKeys, numPartitions, spillCallback)
  use(batchPairSubIterator)

  private[this] var nextBatch: Option[ColumnarBatch] = None
  // Support repeated calls to "hasNext" without a call to "next"
  private[this] var nextBatchConsumed = true

  override def hasNext: Boolean = opTime.ns {
    if (closed) return false
    if (nextBatchConsumed) {
      var mayContinue = true
      while (nextBatch.isEmpty && mayContinue) {
        if (batchPairSubIterator.hasNext) {
          val (buildCb, streamCb) = batchPairSubIterator.next()
          nextBatch = joinAndClose(buildCb, streamCb)
        } else {
          mayContinue = false
        }
      }
      nextBatchConsumed = false
    }
    nextBatch.isDefined
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException()
    val ret = nextBatch.get
    nextBatch = None
    nextBatchConsumed = true
    ret
  }

  override def close(): Unit = {
    nextBatch.foreach(_.safeClose(new Exception()))
    nextBatch = None
    nextBatchConsumed = false
    super.close()
  }

  private[this] def joinAndClose(
      build: Option[ColumnarBatch],
      stream: Option[ColumnarBatch]): Option[ColumnarBatch] = {
    withResource(build) { _ =>
      withResource(stream) { _ =>
        // FIXME what's expected if getting at least one empty batch for existence join?
        // Now return None if stream batch is empty, since existence join always uses
        // stream as the left side.
        stream.map { streamBatch =>
          val buildBatch = build.getOrElse(GpuColumnVector.emptyBatch(buildSchema))
          existenceJoinNextBatch(streamBatch, buildBatch)
        }
      }
    }
  }
}

abstract class BaseSubHashJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    buildSchema: StructType,
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamSchema: StructType,
    numPartitions: Int,
    buildSide: GpuBuildSide,
    joinType: JoinType,
    targetSize: Long,
    spillCallback: SpillCallback,
    opTime: GpuMetric,
    joinTime: GpuMetric)
  extends AbstractGpuJoinIterator(s"hash $joinType gather by sub-partition",
    targetSize, opTime, joinTime) {

  private[this] val batchPairSubIterator = new GpuBatchPairSubPartitionIterator(
    buildIter, boundBuildKeys, streamIter, boundStreamKeys, numPartitions, spillCallback)
  use(batchPairSubIterator)

  override protected def hasNextStreamBatch: Boolean = opTime.ns {
    // This may be a little tricky. Because it always returns true when there is some
    // build data, even no stream data at all. Since sub-partition algorithm is likely
    // to produce multiple build batches instead of a single one compared to the normal
    // join path. It needs to go through all the build batches besides stream ones.
    // For now it is OK because it can leverage the "AbstractGpuJoinIterator" without
    // any refactor.
    batchPairSubIterator.hasNext
  }

  override protected def setupNextGatherer(): Option[JoinGatherer] = opTime.ns {
    val (buildCb, streamCb) = batchPairSubIterator.next()
    if ((buildCb.nonEmpty || streamCb.nonEmpty) && !canOptimizeOut(buildCb, streamCb)) {
      // At least one side has data and can not be optimized out
      joinGathererAndClose(buildCb, streamCb)
    } else {
      // Neither build nor stream has data, or can be ignored due to optimization.
      buildCb.foreach(_.close())
      streamCb.foreach(_.close())
      None
    }
  }

  private[this] def canOptimizeOut(
      build: Option[ColumnarBatch],
      stream: Option[ColumnarBatch]): Boolean = {
    closeOnExcept(build) { _ =>
      closeOnExcept(stream) { _ =>
        joinType match {
          case _: InnerLike =>
            // For inner join, no need to run if either side is empty
            build.isEmpty || stream.isEmpty
          case _ => false
        }
      }
    }
  }

  private[this] def joinGathererAndClose(
      build: Option[ColumnarBatch],
      stream: Option[ColumnarBatch]): Option[JoinGatherer] = {
    withResource(build.getOrElse(GpuColumnVector.emptyBatch(buildSchema))) { buildCb =>
      withResource(stream.getOrElse(GpuColumnVector.emptyBatch(streamSchema))) { streamCb =>
        buildSide match {
          case GpuBuildLeft =>
            joinGathererLeftRight(buildCb, boundBuildKeys, streamCb, boundStreamKeys)
          case GpuBuildRight =>
            joinGathererLeftRight(streamCb, boundStreamKeys, buildCb, boundBuildKeys)
        }
      }
    }
  }

  private def joinGathererLeftRight(
      leftData: ColumnarBatch,
      leftBoundKeys: Seq[GpuExpression],
      rightData: ColumnarBatch,
      rightBoundKeys: Seq[GpuExpression]): Option[JoinGatherer] = {
      val leftKeys = withResource(GpuProjectExec.project(leftData, leftBoundKeys)) { keyCb =>
        GpuColumnVector.from(keyCb)
      }
      val rightKeys = closeOnExcept(leftKeys) { _ =>
        withResource(GpuProjectExec.project(rightData, rightBoundKeys)) { keyCb =>
          GpuColumnVector.from(keyCb)
        }
      }
      withResource(Seq(leftKeys, rightKeys)) { _ =>
        joinGathererLeftRight(leftKeys, leftData, rightKeys, rightData)
      }
  }

  /**
   * Create a join gatherer from gather maps.
   *
   * @param maps      gather maps produced from a cudf join
   * @param leftData  batch corresponding to the left table in the join
   * @param rightData batch corresponding to the right table in the join
   * @return some gatherer or None if the are no rows to gather in this join batch
   */
  protected def makeGatherer(
      maps: Array[GatherMap],
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    SplittableJoinIterator.makeGatherer(maps, leftData, rightData, joinType, spillCallback)
  }

  /**
   * Perform a hash join, returning a ColumnarBatch if there is a join result.
   *
   * @param leftKeys table of join keys from the left table
   * @param leftBatch batch containing the full data from the left table
   * @param rightKeys table of join keys from the right table
   * @param rightBatch batch containing the full data from the right table, or None
   *                   if there is no data for the current key set
   * @return a ColumnarBatch if there are join results
   */
  protected def joinGathererLeftRight(
      leftKeys: Table,
      leftBatch: ColumnarBatch,
      rightKeys: Table,
      rightBatch: ColumnarBatch): Option[JoinGatherer]
}

class SubHashJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    buildSchema: StructType,
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamSchema: StructType,
    numPartitions: Int,
    buildSide: GpuBuildSide,
    targetSize: Long,
    compareNullsEqual: Boolean,
    joinType: JoinType,
    spillCallback: SpillCallback,
    opTime: GpuMetric,
    joinTime: GpuMetric)
  extends BaseSubHashJoinIterator(buildIter, boundBuildKeys, buildSchema, streamIter,
    boundStreamKeys, streamSchema, numPartitions, buildSide, joinType, targetSize,
    spillCallback, opTime, joinTime) {

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftBatch: ColumnarBatch,
      rightKeys: Table,
      rightBatch: ColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("sub hash join gather map", NvtxColor.ORANGE, joinTime)) {
      _ =>
        val maps = GpuHashJoin.gatherMapsLeftRightUnconditionally(leftKeys, rightKeys,
          compareNullsEqual, joinType)
        // maps will be closed inside "makeGatherer"
        makeGatherer(
          maps,
          LazySpillableColumnarBatch(leftBatch, spillCallback, "sub left batch"),
          LazySpillableColumnarBatch(rightBatch, spillCallback, "sub right batch"))
    }
  }
}

class SubConditionalHashJoinIterator(
    buildIter: Iterator[ColumnarBatch],
    boundBuildKeys: Seq[GpuExpression],
    buildSchema: StructType,
    streamIter: Iterator[ColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamSchema: StructType,
    numPartitions: Int,
    buildSide: GpuBuildSide,
    targetSize: Long,
    compareNullsEqual: Boolean,
    joinType: JoinType,
    compiledCondition: CompiledExpression,
    spillCallback: SpillCallback,
    opTime: GpuMetric,
    joinTime: GpuMetric)
  extends BaseSubHashJoinIterator(buildIter, boundBuildKeys, buildSchema, streamIter,
    boundStreamKeys, streamSchema, numPartitions, buildSide, joinType, targetSize,
    spillCallback, opTime, joinTime) {

  use(compiledCondition)

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftBatch: ColumnarBatch,
      rightKeys: Table,
      rightBatch: ColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("sub hash join gather map", NvtxColor.ORANGE, joinTime)) {
      _ =>
        val maps = GpuHashJoin.gatherMapsLeftRightConditionally(leftKeys, leftBatch,
          rightKeys, rightBatch, compiledCondition, compareNullsEqual, joinType)
        // maps will be closed inside "makeGatherer"
        makeGatherer(
          maps,
          LazySpillableColumnarBatch(leftBatch, spillCallback, "sub left batch"),
          LazySpillableColumnarBatch(rightBatch, spillCallback, "sub right batch"))
    }
  }
}

trait GpuSubPartitionHashJoin extends Arm { self: GpuHashJoin =>

  protected lazy val buildSchema: StructType = StructType.fromAttributes(buildPlan.output)
  protected lazy val streamSchema: StructType = StructType.fromAttributes(streamedPlan.output)

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
    // (Same as the computation in "doJoin")
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    val realTarget = Math.max(targetSize, 10 * 1024)

    val filteredBuildIter = builtIter.map { cb =>
      withResource(cb) { _ =>
        filterNullsForBuild(cb)
      }
    }

    val joinIterator = joinType match {
      case ExistenceJoin(_) =>
        new SubHashExistenceJoinIterator(filteredBuildIter, boundBuildKeys, buildSchema,
          streamIter, boundStreamKeys, boundCondition, numFirstConditionTableColumns,
          compareNullsEqual, numPartitions, spillCallback, opTime, joinTime)
      // case FullOuter =>
        // TBD
      case _ =>
        boundCondition.map { bCondition =>
          // SubConditionalHashJoinIterator will close the compiled condition
          val compiledCond = bCondition.convertToAst(numFirstConditionTableColumns).compile()
          new SubConditionalHashJoinIterator(filteredBuildIter, boundBuildKeys, buildSchema,
            streamIter, boundStreamKeys, streamSchema, numPartitions, buildSide, realTarget,
            compareNullsEqual, joinType, compiledCond, spillCallback, opTime, joinTime)
        }.getOrElse(
          new SubHashJoinIterator(filteredBuildIter, boundBuildKeys, buildSchema, streamIter,
            boundStreamKeys, streamSchema, numPartitions, buildSide, realTarget,
            compareNullsEqual, joinType, spillCallback, opTime, joinTime)
        )
    }

    joinIterator.map { cb =>
      joinOutputRows += cb.numRows()
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }
}
