/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle

import scala.collection.mutable.ArrayBuffer

// Helper trait that callers can use to add blocks to the iterator
// as long as they can provide a size
trait BlockWithSize {
  /**
   * Abstract method to return the size in bytes of this block
   * @return Long - size in bytes
   */
  def size: Long
}

/**
 * Specifies a start and end range of byes for a block.
 * @param block - a BlockWithSize instance
 * @param rangeStart - byte offset for the start of the range
 * @param rangeEnd - byte offset for the end of the range
 * @tparam T - the specific type of `BlockWithSize`
 */
case class BlockRange[T <: BlockWithSize](
    block: T, rangeStart: Long, rangeEnd: Long) {

  /**
   * Returns the size of this range in bytes
   * @return - Long - size in bytes
   */
  def rangeSize(): Long = rangeEnd - rangeStart + 1

  def isComplete(): Boolean = rangeEnd == block.size - 1
}

/**
 * Given a set of blocks, this iterator returns BlockRanges
 * of such blocks that fit `windowSize`.
 *
 * If a block is too large for the window, the block will be
 * returned in `next()` until the full block could be covered.
 *
 * @param transferBlocks - sequence of blocks to manage
 * @param windowSize - the size (in bytes) that block ranges should fit
 * @tparam T - the specific type of `BlockWithSize`
 * @note this class does not own `transferBlocks`
 * @note this class is not thread safe
 */
class WindowedBlockIterator[T <: BlockWithSize](blocks: Seq[T], windowSize: Long)
    extends Iterator[Seq[BlockRange[T]]] {

  require(windowSize > 0, s"Invalid window size specified $windowSize")

  case class BlockWindow(start: Long, size: Long) {
    val end = start + size - 1
    def move(): BlockWindow = {
      val windowLength = size - start
      BlockWindow(start + size, size)
    }
  }

  // start the window at byte 0
  private[this] var window = BlockWindow(0, windowSize)
  private[this] var done = false

  // helper class that captures the start/end byte offset
  // for `block` on creation
  case class BlockWithOffset[T <: BlockWithSize](
      block: T, startOffset: Long, endOffset: Long)

  private var lastOffset = 0L
  private[this] val blocksWithOffsets = blocks.map { block =>
    require(block.size > 0, "Invalid 0-byte block")
    val startOffset = lastOffset
    val endOffset = startOffset + block.size - 1
    lastOffset = endOffset + 1 // for next block
    BlockWithOffset(block, startOffset, endOffset)
  }

  case class BlocksForWindow(lastBlockIndex: Option[Int],
      blockRanges: Seq[BlockRange[T]],
      hasMoreBlocks: Boolean)

  private def getBlocksForWindow(
      window: BlockWindow,
      startingBlock: Int = 0): BlocksForWindow = {
    val blockRangesInWindow = new ArrayBuffer[BlockRange[T]]()
    var continue = true
    var thisBlock = startingBlock
    var lastBlockIndex: Option[Int] = None
    while (continue && thisBlock < blocksWithOffsets.size) {
      val b = blocksWithOffsets(thisBlock)
      // if at least 1 byte fits within the window, this block should be included
      if (window.start <= b.endOffset && window.end >= b.startOffset) {
        var rangeStart = window.start - b.startOffset
        if (rangeStart < 0) {
          rangeStart = 0
        }
        var rangeEnd = window.end - b.startOffset
        if (window.end > b.endOffset) {
          rangeEnd = b.endOffset - b.startOffset
        }
        blockRangesInWindow.append(BlockRange[T](b.block, rangeStart, rangeEnd))
        lastBlockIndex = Some(thisBlock)
      } else {
        // skip this block, unless it's before our window starts
        continue = b.endOffset < window.start
      }
      thisBlock = thisBlock + 1
    }
    val lastBlock = blockRangesInWindow.last
    BlocksForWindow(lastBlockIndex,
      blockRangesInWindow,
      !continue || !lastBlock.isComplete())
  }

  private var lastSeenBlock = 0
  def next(): Seq[BlockRange[T]] = {
    if (!hasNext) {
      throw new NoSuchElementException(s"BounceBufferWindow $window has been exhausted.")
    }

    val blocksForWindow = getBlocksForWindow(window, lastSeenBlock)
    lastSeenBlock = blocksForWindow.lastBlockIndex.getOrElse(0)

    if (blocksForWindow.hasMoreBlocks) {
      window = window.move()
    } else {
      done = true
    }

    blocksForWindow.blockRanges
  }

  override def hasNext: Boolean = !done && blocksWithOffsets.nonEmpty
}
