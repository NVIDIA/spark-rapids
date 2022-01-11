/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.SpillPriorities.{HOST_MEMORY_BUFFER_DIRECT_OFFSET, HOST_MEMORY_BUFFER_PAGEABLE_OFFSET, HOST_MEMORY_BUFFER_PINNED_OFFSET}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * A buffer store using host memory.
 * @param catalog buffer catalog to use with this store
 * @param pageableMemoryPoolSize maximum size in bytes for the internal pageable memory pool
 */
class RapidsHostMemoryStore(
    pageableMemoryPoolSize: Long,
    pinnedMemoryPoolSize: Long,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
    deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
    extends RapidsBufferStore(StorageTier.HOST, catalog) {
  private[this] val pool = HostMemoryBuffer.allocate(pageableMemoryPoolSize, false)
  private[this] val addressAllocator = new AddressSpaceAllocator(pageableMemoryPoolSize)
  private[this] var haveLoggedMaxExceeded = false

  private sealed abstract class AllocationMode(val spillPriorityOffset: Long)
  private case object Pinned extends AllocationMode(HOST_MEMORY_BUFFER_PINNED_OFFSET)
  private case object Pooled extends AllocationMode(HOST_MEMORY_BUFFER_PAGEABLE_OFFSET)
  private case object Direct extends AllocationMode(HOST_MEMORY_BUFFER_DIRECT_OFFSET)

  // Returns an allocated host buffer and its allocation mode
  private def allocateHostBuffer(size: Long): (HostMemoryBuffer, AllocationMode) = {
    // spill to keep within the targeted size
    val targetSize = math.max(pageableMemoryPoolSize + pinnedMemoryPoolSize - size, 0)
    val amountSpilled = synchronousSpill(targetSize)
    if (amountSpilled != 0) {
      logInfo(s"Spilled $amountSpilled bytes from the host memory store")
      TrampolineUtil.incTaskMetricsDiskBytesSpilled(amountSpilled)
    }

    var buffer: HostMemoryBuffer = null
    while (buffer == null) {
      buffer = PinnedMemoryPool.tryAllocate(size)
      if (buffer != null) {
        return (buffer, Pinned)
      }

      if (size > pageableMemoryPoolSize) {
        if (!haveLoggedMaxExceeded) {
          logWarning(s"Exceeding host spill max of $pageableMemoryPoolSize bytes to accommodate " +
              s"a buffer of $size bytes. Consider increasing host spill store size.")
          haveLoggedMaxExceeded = true
        }
        return (HostMemoryBuffer.allocate(size, false), Direct)
      }

      val allocation = addressAllocator.allocate(size)
      if (allocation.isDefined) {
        buffer = pool.slice(allocation.get, size)
      } else {
        val targetSize = math.max(currentSize - size, 0)
        synchronousSpill(targetSize)
      }
    }
    (buffer, Pooled)
  }

  override protected def createBuffer(other: RapidsBuffer, otherBuffer: MemoryBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(otherBuffer) { _ =>
      val (hostBuffer, allocationMode) = allocateHostBuffer(other.size)
      try {
        otherBuffer match {
          case devBuffer: DeviceMemoryBuffer => hostBuffer.copyFromDeviceBuffer(devBuffer, stream)
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
        other.getSpillPriority + allocationMode.spillPriorityOffset,
        hostBuffer,
        allocationMode,
        other.spillCallback,
        deviceStorage)
    }
  }

  def numBytesFree: Long = pageableMemoryPoolSize - currentSize

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
      allocationMode: AllocationMode,
      spillCallback: SpillCallback,
      deviceStorage: RapidsDeviceMemoryStore)
      extends RapidsBufferBase(
        id, size, meta, spillPriority, spillCallback, deviceStorage = deviceStorage) {
    override val storageTier: StorageTier = StorageTier.HOST

    override def getMemoryBuffer: MemoryBuffer = {
      buffer.incRefCount()
      buffer
    }

    override protected def releaseResources(): Unit = {
      allocationMode match {
        case Pooled =>
          assert(buffer.getAddress >= pool.getAddress)
          assert(buffer.getAddress < pool.getAddress + pool.getLength)
          addressAllocator.free(buffer.getAddress - pool.getAddress)
        case _ =>
      }
      buffer.close()
    }
  }
}
