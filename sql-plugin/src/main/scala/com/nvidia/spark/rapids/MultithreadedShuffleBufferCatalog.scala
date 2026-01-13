/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import _root_.io.netty.buffer.Unpooled
import com.nvidia.spark.rapids.spill.SpillablePartialFileHandle

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

/**
 * A segment of data within a SpillablePartialFileHandle.
 * 
 * @param handle the partial file handle containing the data
 * @param offset starting offset within the handle
 * @param length number of bytes in this segment
 */
case class PartitionSegment(
    handle: SpillablePartialFileHandle,
    offset: Long,
    length: Long)

/**
 * Catalog for managing shuffle data in MULTITHREADED mode without merging.
 * 
 * Instead of merging partial files into a single shuffle file, this catalog
 * stores references to segments within partial files. When a reducer requests
 * a shuffle block, the catalog dynamically assembles the data from all
 * relevant segments.
 * 
 * This approach avoids the I/O cost of merging. The data may be kept in memory
 * (MEMORY_WITH_SPILL mode) or stored directly on disk (ONLY_FILE mode) depending
 * on memory pressure - both modes work with this skip-merge design.
 */
class MultithreadedShuffleBufferCatalog extends Logging {

  /**
   * Map from ShuffleBlockId to list of segments.
   * A partition may have multiple segments if there are multiple batches.
   */
  private val partitionSegments = 
    new ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[PartitionSegment]]()

  /** Track active shuffles for cleanup */
  private val activeShuffles = new ConcurrentHashMap[Int, java.lang.Boolean]()

  /**
   * Register a shuffle as active.
   * Must be called before adding any partitions for this shuffle.
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffles.put(shuffleId, true)
  }

  /**
   * Add a partition segment to the catalog.
   * 
   * @param shuffleId shuffle identifier
   * @param mapId map task identifier
   * @param partitionId reduce partition identifier
   * @param handle the partial file handle containing the data
   * @param offset starting offset within the handle
   * @param length number of bytes for this partition
   */
  def addPartition(
      shuffleId: Int,
      mapId: Long,
      partitionId: Int,
      handle: SpillablePartialFileHandle,
      offset: Long,
      length: Long): Unit = {
    if (length <= 0) {
      return // Skip empty partitions
    }

    val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
    val segment = PartitionSegment(handle, offset, length)

    partitionSegments.compute(blockId, (_, existing) => {
      val segments = if (existing == null) new ArrayBuffer[PartitionSegment]() else existing
      segments += segment
      segments
    })
  }

  /**
   * Check if the catalog has data for a given block.
   */
  def hasData(blockId: ShuffleBlockId): Boolean = {
    partitionSegments.containsKey(blockId)
  }

  /**
   * Check if a shuffle is being managed by this catalog.
   */
  def hasActiveShuffle(shuffleId: Int): Boolean = {
    activeShuffles.containsKey(shuffleId)
  }

  /**
   * Get all active shuffle IDs.
   * Used during executor shutdown to clean up all remaining shuffles.
   */
  def getActiveShuffleIds: Seq[Int] = {
    import scala.collection.JavaConverters._
    activeShuffles.keySet().asScala.map(_.intValue()).toSeq
  }

  /**
   * Get a ManagedBuffer that reads data from all segments for a block.
   * The buffer dynamically assembles data from multiple partial files if needed.
   */
  def getMergedBuffer(blockId: ShuffleBlockId): ManagedBuffer = {
    val segments = partitionSegments.get(blockId)
    if (segments == null || segments.isEmpty) {
      throw new IllegalArgumentException(s"No data found for block $blockId")
    }

    new MultiBatchManagedBuffer(segments.toSeq)
  }

  /**
   * Get a ManagedBuffer for a batch of shuffle blocks (used in batch fetch optimization).
   * This method handles ShuffleBlockBatchId which represents multiple reduce partitions.
   */
  def getMergedBatchBuffer(batchId: ShuffleBlockBatchId): ManagedBuffer = {
    val allSegments = new ArrayBuffer[PartitionSegment]()

    for (reduceId <- batchId.startReduceId until batchId.endReduceId) {
      val blockId = ShuffleBlockId(batchId.shuffleId, batchId.mapId, reduceId)
      val segments = partitionSegments.get(blockId)
      if (segments != null) {
        allSegments ++= segments
      }
    }

    if (allSegments.isEmpty) {
      throw new IllegalArgumentException(s"No data found for batch block $batchId")
    }

    new MultiBatchManagedBuffer(allSegments.toSeq)
  }

  /**
   * Unregister a shuffle and clean up all associated data.
   *
   * @param shuffleId the shuffle ID to unregister
   * @return optional cleanup statistics (None if this catalog has no data for the shuffle)
   */
  def unregisterShuffle(shuffleId: Int): Option[ShuffleCleanupStats] = {
    activeShuffles.remove(shuffleId)

    // Find and remove all blocks for this shuffle
    val iterator = partitionSegments.keySet().iterator()
    val toRemove = new ArrayBuffer[ShuffleBlockId]()
    while (iterator.hasNext) {
      val blockId = iterator.next()
      if (blockId.shuffleId == shuffleId) {
        toRemove += blockId
      }
    }

    // Collect unique handles and gather statistics before closing
    val closedHandles = new java.util.HashSet[SpillablePartialFileHandle]()
    var bytesFromMemory = 0L
    var bytesFromDisk = 0L

    toRemove.foreach { blockId =>
      val segments = partitionSegments.remove(blockId)
      if (segments != null) {
        segments.foreach { segment =>
          // Only process each handle once (multiple partitions may share a handle)
          if (!closedHandles.contains(segment.handle)) {
            closedHandles.add(segment.handle)

            // Collect statistics before closing
            val handle = segment.handle
            val totalBytes = handle.getTotalBytesWritten
            if (handle.isMemoryBased && !handle.isSpilled) {
              bytesFromMemory += totalBytes
            } else {
              bytesFromDisk += totalBytes
            }

            try {
              handle.close()
            } catch {
              case e: Exception =>
                logWarning(s"Failed to close handle for shuffle $shuffleId", e)
            }
          }
        }
      }
    }

    logInfo(s"Unregistered shuffle $shuffleId: closed ${closedHandles.size()} handles, " +
      s"bytesFromMemory=$bytesFromMemory, bytesFromDisk=$bytesFromDisk")

    // Return statistics if we had any data
    if (bytesFromMemory > 0 || bytesFromDisk > 0) {
      Some(ShuffleCleanupStats(shuffleId, bytesFromMemory, bytesFromDisk))
    } else {
      None
    }
  }
}

/**
 * A ManagedBuffer that reads data from multiple partition segments.
 * 
 * This buffer dynamically assembles data from multiple SpillablePartialFileHandle
 * segments when createInputStream() is called. Each segment may be in memory or
 * on disk, and the buffer handles both cases transparently.
 */
class MultiBatchManagedBuffer(segments: Seq[PartitionSegment]) extends ManagedBuffer {

  override def size(): Long = segments.map(_.length).sum

  override def nioByteBuffer(): ByteBuffer = {
    // Read all data into a single ByteBuffer
    val totalSize = size().toInt
    val buffer = ByteBuffer.allocate(totalSize)
    val bytes = new Array[Byte](8192) // Read buffer

    segments.foreach { segment =>
      var remaining = segment.length
      var position = segment.offset
      while (remaining > 0) {
        val toRead = math.min(remaining, bytes.length).toInt
        val bytesRead = segment.handle.readAt(position, bytes, 0, toRead)
        if (bytesRead <= 0) {
          throw new IOException(
            s"Unexpected EOF reading segment at position $position, " +
            s"expected ${segment.length} bytes")
        }
        buffer.put(bytes, 0, bytesRead)
        position += bytesRead
        remaining -= bytesRead
      }
    }

    buffer.flip()
    buffer
  }

  override def createInputStream(): InputStream = {
    new MultiSegmentInputStream(segments)
  }

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this

  override def convertToNetty(): AnyRef = {
    // For network transfer, wrap ByteBuffer in Netty ByteBuf
    Unpooled.wrappedBuffer(nioByteBuffer())
  }
}

/**
 * An InputStream that reads from multiple partition segments sequentially.
 */
class MultiSegmentInputStream(segments: Seq[PartitionSegment]) extends InputStream {

  private var currentSegmentIndex: Int = 0
  private var currentPosition: Long = if (segments.nonEmpty) segments.head.offset else 0
  private var bytesReadInCurrentSegment: Long = 0

  override def read(): Int = {
    val buf = new Array[Byte](1)
    val n = read(buf, 0, 1)
    if (n == -1) -1 else buf(0) & 0xFF
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (currentSegmentIndex >= segments.size) {
      return -1 // EOF
    }

    val segment = segments(currentSegmentIndex)
    val remainingInSegment = segment.length - bytesReadInCurrentSegment
    
    if (remainingInSegment <= 0) {
      // Move to next segment
      currentSegmentIndex += 1
      if (currentSegmentIndex >= segments.size) {
        return -1 // EOF
      }
      currentPosition = segments(currentSegmentIndex).offset
      bytesReadInCurrentSegment = 0
      return read(b, off, len) // Recursive call for next segment
    }

    val toRead = math.min(len, remainingInSegment).toInt
    val bytesRead = segment.handle.readAt(currentPosition, b, off, toRead)
    
    if (bytesRead > 0) {
      currentPosition += bytesRead
      bytesReadInCurrentSegment += bytesRead
    }
    
    bytesRead
  }

  override def available(): Int = {
    if (currentSegmentIndex >= segments.size) {
      0
    } else {
      val remaining = segments.drop(currentSegmentIndex).map { seg =>
        if (seg == segments(currentSegmentIndex)) {
          seg.length - bytesReadInCurrentSegment
        } else {
          seg.length
        }
      }.sum
      math.min(remaining, Int.MaxValue).toInt
    }
  }

  /**
   * Close is a no-op because the underlying SpillablePartialFileHandle resources
   * are managed by MultithreadedShuffleBufferCatalog and will be closed when
   * the shuffle is unregistered.
   */
  override def close(): Unit = {
    // No-op: handles are managed by MultithreadedShuffleBufferCatalog
  }
}


