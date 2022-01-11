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
    pinnedSize: Long,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
    deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
    extends RapidsBufferStore(StorageTier.HOST, catalog) {
  private[this] val pool = HostMemoryBuffer.allocate(maxSize, false)
  private[this] val addressAllocator = new AddressSpaceAllocator(maxSize)
  private[this] var haveLoggedMaxExceeded = false

  private object AllocationMode extends Enumeration {
    type AllocationMode = Value
    val Pinned, Pooled, Direct = Value
  }
  import AllocationMode._

  // Returns an allocated host buffer and its allocation mode
  private def allocateHostBuffer(size: Long): (HostMemoryBuffer, AllocationMode) = {
    // spill to keep within the targeted size
    val targetSize = math.max(maxSize + pinnedSize - size, 0)
    val amountSpilled = synchronousSpill(targetSize)
    if (amountSpilled != 0) {
      logInfo(s"Spilled $amountSpilled bytes from the host memory store")
      TrampolineUtil.incTaskMetricsDiskBytesSpilled(amountSpilled)
    }

    var buffer: HostMemoryBuffer = null
    while (buffer == null) {
      if (size > maxSize && size > pinnedSize) {
        if (!haveLoggedMaxExceeded) {
          logWarning(s"Exceeding host spill max of $maxSize bytes and pinned memory pool size of " +
              s"$pinnedSize bytes to accommodate a buffer of $size bytes. Consider increasing " +
              s"host spill store size or pinned memory pool size.")
          haveLoggedMaxExceeded = true
        }
        return (HostMemoryBuffer.allocate(size), Direct)
      }

      buffer = PinnedMemoryPool.tryAllocate(size)
      if (buffer != null) {
        return (buffer, Pinned)
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
      val spillPriority = other.getSpillPriority - (allocationMode match {
        case Pinned => 200
        case Pooled => 100
        case Direct => 0
      })
      new RapidsHostMemoryBuffer(
        other.id,
        other.size,
        other.meta,
        spillPriority,
        hostBuffer,
        allocationMode,
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
      if (allocationMode == Pooled) {
        assert(buffer.getAddress >= pool.getAddress)
        assert(buffer.getAddress < pool.getAddress + pool.getLength)
        addressAllocator.free(buffer.getAddress - pool.getAddress)
      }
      buffer.close()
    }
  }
}
