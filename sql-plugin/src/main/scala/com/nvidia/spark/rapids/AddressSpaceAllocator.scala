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

package com.nvidia.spark.rapids

import scala.collection.mutable

/** Allocates blocks from an address space using a best-fit algorithm. */
class AddressSpaceAllocator(addressSpaceSize: Long) {
  /** Free blocks mapped by size of block for efficient size matching.  */
  private[this] val freeBlocks = new mutable.TreeMap[Long, mutable.Set[Block]]

  /** Allocated blocks mapped by block address. */
  private[this] val allocatedBlocks = new mutable.HashMap[Long, Block]

  /** Amount of memory allocated */
  private[this] var allocatedBytes: Long = 0L

  addFreeBlock(new Block(0, addressSpaceSize, allocated = false))

  private def addFreeBlock(block: Block): Unit = {
    val set = freeBlocks.getOrElseUpdate(block.blockSize, new mutable.HashSet[Block])
    val added = set.add(block)
    assert(added, "block was already in free map")
  }

  private def removeFreeBlock(block: Block): Unit = {
    val set = freeBlocks.getOrElse(block.blockSize,
      throw new IllegalStateException("block not in free map"))
    val removed = set.remove(block)
    assert(removed, "block not in free map")
    if (set.isEmpty) {
      freeBlocks.remove(block.blockSize)
    }
  }

  def allocate(size: Long): Option[Long] = synchronized {
    if (size <= 0) {
      return None
    }
    val it = freeBlocks.valuesIteratorFrom(size)
    if (it.hasNext) {
      val blockSet = it.next()
      val block = blockSet.head
      removeFreeBlock(block)
      allocatedBytes += size
      Some(block.allocate(size))
    } else {
      None
    }
  }

  def free(address: Long): Unit = synchronized {
    val block = allocatedBlocks.remove(address).getOrElse(
      throw new IllegalArgumentException(s"$address not allocated"))
    allocatedBytes -= block.blockSize
    block.free()
  }

  def allocatedSize: Long = synchronized {
    allocatedBytes
  }

  def available: Long = synchronized {
    addressSpaceSize - allocatedBytes
  }

  def numAllocatedBlocks: Long = synchronized {
    allocatedBlocks.size
  }

  def numFreeBlocks: Long = synchronized {
    freeBlocks.valuesIterator.map(_.size).sum
  }

  private class Block(
      val address: Long,
      var blockSize: Long,
      var lowerBlock: Option[Block] = None,
      var upperBlock: Option[Block] = None,
      var allocated: Boolean = false) {
    def allocate(amount: Long): Long = {
      assert(!allocated, "block being allocated already allocated")
      assert(amount <= blockSize, "allocating beyond block")
      if (amount != blockSize) {
        // split into an allocated and unallocated block
        val unallocated = new Block(
          address + amount,
          blockSize - amount,
          Some(this),
          upperBlock,
          allocated = false)
        addFreeBlock(unallocated)
        upperBlock.foreach { b =>
          assert(b.lowerBlock.get == this, "block linkage broken")
          assert(address + blockSize == b.address, "block adjacency broken")
          b.lowerBlock = Some(unallocated)
        }
        upperBlock = Some(unallocated)
        blockSize = amount
      }
      allocated = true
      allocatedBlocks.put(address, this)
      this.address
    }

    def free(): Unit = {
      assert(allocated, "block being freed not allocated")
      allocated = false
      upperBlock.foreach { b =>
        if (!b.allocated) {
          removeFreeBlock(b)
          coalesceUpper()
        }
      }
      var freeBlock = this
      lowerBlock.foreach { b =>
        if (!b.allocated) {
          removeFreeBlock(b)
          b.coalesceUpper()
          freeBlock = b
        }
      }
      addFreeBlock(freeBlock)
    }

    /** Coalesce the upper block into this block. Does not update freeBlocks. */
    private def coalesceUpper(): Unit = {
      val upper = upperBlock.getOrElse(throw new IllegalStateException("no upper block"))
      assert(upper.lowerBlock.orNull == this, "block linkage broken")
      assert(address + blockSize == upper.address, "block adjacency broken")
      blockSize += upper.blockSize
      upperBlock = upper.upperBlock
      upperBlock.foreach(_.lowerBlock = Some(this))
    }
  }
}
