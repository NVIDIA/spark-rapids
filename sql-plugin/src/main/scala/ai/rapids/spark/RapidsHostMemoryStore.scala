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

package ai.rapids.spark

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, PinnedMemoryPool}
import ai.rapids.spark.StorageTier.StorageTier
import ai.rapids.spark.format.TableMeta

/**
 * A buffer store using host memory.
 * @param catalog buffer catalog to use with this store
 * @param maxSize maximum size in bytes for all buffers in this store
 */
class RapidsHostMemoryStore(
    catalog: RapidsBufferCatalog,
    maxSize: Long) extends RapidsBufferStore("host", catalog) {
  private[this] val pool = HostMemoryBuffer.allocate(maxSize, false)
  private[this] val addressAllocator = new AddressSpaceAllocator(maxSize)

  // Returns an allocated host buffer and whether the allocation is from the pinned pool
  private def allocateHostBuffer(size: Long): (HostMemoryBuffer, Boolean) = {
    require(size <= maxSize, s"allocating a buffer of $size bytes beyond max of $maxSize")
    var buffer: HostMemoryBuffer = null
    while (buffer == null) {
      buffer = PinnedMemoryPool.tryAllocate(size)
      if (buffer != null) {
        return (buffer, true)
      }

      val allocation = addressAllocator.allocate(size)
      if (allocation.isDefined) {
        buffer = pool.slice(allocation.get, size)
      } else {
        val targetSize = math.max(currentSize - size, 0)
        synchronousSpill(targetSize)
      }
    }
    (buffer, false)
  }

  override protected def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    val (hostBuffer, isPinned) = allocateHostBuffer(other.size)
    try {
      val otherBuffer = other.getMemoryBuffer
      try {
        otherBuffer match {
          case devBuffer: DeviceMemoryBuffer =>
            if (stream != null) {
              hostBuffer.copyFromDeviceBuffer(devBuffer, stream)
            } else {
              hostBuffer.copyFromDeviceBuffer(devBuffer)
            }
          case _ => throw new IllegalStateException("copying from buffer without device memory")
        }
      } finally {
        otherBuffer.close()
      }
    } catch {
      case e: Exception =>
        hostBuffer.close()
        throw e
    }
    new RapidsHostMemoryBuffer(
      other.id,
      other.size,
      other.meta,
      other.getSpillPriority,
      hostBuffer,
      isPinned)
  }

  def numBytesFree: Long = maxSize - currentSize

  override def close(): Unit = {
    super.close()
    pool.close()
  }

  class RapidsHostMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      buffer: HostMemoryBuffer,
      isPinned: Boolean) extends RapidsBufferBase(id, size, meta, spillPriority) {
    override val storageTier: StorageTier = StorageTier.HOST

    override def getMemoryBuffer: MemoryBuffer = buffer.slice(0, buffer.getLength)

    override protected def releaseResources(): Unit = {
      if (!isPinned) {
        assert(buffer.getAddress >= pool.getAddress)
        assert(buffer.getAddress < pool.getAddress + pool.getLength)
        addressAllocator.free(buffer.getAddress - pool.getAddress)
      }
      buffer.close()
    }
  }
}
