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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, PinnedMemoryPool}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * A buffer store using host memory.
 * @param catalog buffer catalog to use with this store
 * @param maxSize maximum size in bytes for all buffers in this store
 */
class RapidsHostMemoryStore(
    maxSize: Long,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
    deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
    extends RapidsBufferStore(StorageTier.HOST, catalog) {
  private[this] val pool = HostMemoryBuffer.allocate(maxSize, false)
  private[this] val addressAllocator = new AddressSpaceAllocator(maxSize)
  private[this] var haveLoggedMaxExceeded = false

  // Returns an allocated host buffer and whether the allocation is from the internal pool
  private def allocateHostBuffer(size: Long): (HostMemoryBuffer, Boolean) = {
    // spill to keep within the targeted size
    val amountSpilled = synchronousSpill(math.max(maxSize - size, 0))
    if (amountSpilled != 0) {
      logInfo(s"Spilled $amountSpilled bytes from the host memory store")
      TrampolineUtil.incTaskMetricsDiskBytesSpilled(amountSpilled)
    }

    var buffer: HostMemoryBuffer = null
    while (buffer == null) {
      buffer = PinnedMemoryPool.tryAllocate(size)
      if (buffer != null) {
        return (buffer, false)
      }

      if (size > maxSize) {
        if (!haveLoggedMaxExceeded) {
          logWarning(s"Exceeding host spill max of $maxSize bytes to accommodate a buffer of " +
              s"$size bytes. Consider increasing host spill store size.")
          haveLoggedMaxExceeded = true
        }
        return (HostMemoryBuffer.allocate(size), false)
      }

      val allocation = addressAllocator.allocate(size)
      if (allocation.isDefined) {
        buffer = pool.slice(allocation.get, size)
      } else {
        val targetSize = math.max(currentSize - size, 0)
        synchronousSpill(targetSize)
      }
    }
    (buffer, true)
  }

  override protected def createBuffer(other: RapidsBuffer, otherBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val (hostBuffer, isPinned) = allocateHostBuffer(other.size)
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
        isPinned,
        other.spillCallback,
        deviceStorage)
    }
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
      isInternalPoolAllocated: Boolean,
      spillCallback: RapidsBuffer.SpillCallback,
      deviceStorage: RapidsDeviceMemoryStore)
      extends RapidsBufferBase(
        id, size, meta, spillPriority, spillCallback, deviceStorage = deviceStorage) {
    override val storageTier: StorageTier = StorageTier.HOST

    override def getMemoryBuffer: MemoryBuffer = {
      buffer.incRefCount()
      buffer
    }

    override protected def releaseResources(): Unit = {
      if (isInternalPoolAllocated) {
        assert(buffer.getAddress >= pool.getAddress)
        assert(buffer.getAddress < pool.getAddress + pool.getLength)
        addressAllocator.free(buffer.getAddress - pool.getAddress)
      }
      buffer.close()
    }
  }
}
