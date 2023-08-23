/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange, PinnedMemoryPool}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, freeOnExcept, withResource}
import com.nvidia.spark.rapids.SpillPriorities.{applyPriorityOffset, HOST_MEMORY_BUFFER_SPILL_OFFSET}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

/**
 * A buffer store using host memory.
 * @param maxSize maximum size in bytes for all buffers in this store
 * @param pageableMemoryPoolSize maximum size in bytes for the internal pageable memory pool
 */
class RapidsHostMemoryStore(
    maxSize: Long)
    extends RapidsBufferStore(StorageTier.HOST) {

  override def spillableOnAdd: Boolean = false

  override protected def setSpillable(buffer: RapidsBufferBase, spillable: Boolean): Unit = {
    doSetSpillable(buffer, spillable)
  }

  override def getMaxSize: Option[Long] = Some(maxSize)

  private def allocateHostBuffer(
      size: Long,
      preferPinned: Boolean = true): HostMemoryBuffer = {
    var buffer: HostMemoryBuffer = null
    if (preferPinned) {
      buffer = PinnedMemoryPool.tryAllocate(size)
      if (buffer != null) {
        return buffer
      }
    }

    HostMemoryBuffer.allocate(size, false)
  }

  def addBuffer(
      id: RapidsBufferId,
      buffer: HostMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBuffer = {
    buffer.incRefCount()
    val rapidsBuffer = new RapidsHostMemoryBuffer(
      id,
      buffer.getLength,
      tableMeta,
      initialSpillPriority,
      buffer)
    freeOnExcept(rapidsBuffer) { _ =>
      logDebug(s"Adding host buffer for: [id=$id, size=${buffer.getLength}, " +
        s"uncompressed=${rapidsBuffer.meta.bufferMeta.uncompressedSize}, " +
        s"meta_id=${tableMeta.bufferMeta.id}, " +
        s"meta_size=${tableMeta.bufferMeta.size}]")
      addBuffer(rapidsBuffer, needsSync)
      rapidsBuffer
    }
  }

  override protected def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    withResource(other.getCopyIterator) { otherBufferIterator =>
      val isChunked = otherBufferIterator.isChunked
      val totalCopySize = otherBufferIterator.getTotalCopySize
      closeOnExcept(allocateHostBuffer(totalCopySize)) { hostBuffer =>
        withResource(new NvtxRange("spill to host", NvtxColor.BLUE)) { _ =>
          var hostOffset = 0L
          val start = System.nanoTime()
          while (otherBufferIterator.hasNext) {
            val otherBuffer = otherBufferIterator.next()
            withResource(otherBuffer) { _ =>
              otherBuffer match {
                case devBuffer: DeviceMemoryBuffer =>
                  hostBuffer.copyFromMemoryBufferAsync(
                    hostOffset, devBuffer, 0, otherBuffer.getLength, stream)
                  hostOffset += otherBuffer.getLength
                case _ =>
                  throw new IllegalStateException("copying from buffer without device memory")
              }
            }
          }
          stream.sync()
          val end = System.nanoTime()
          val szMB = (totalCopySize.toDouble / 1024.0 / 1024.0).toLong
          val bw = (szMB.toDouble / ((end - start).toDouble / 1000000000.0)).toLong
          logDebug(s"Spill to host (chunked=$isChunked) " +
              s"size=$szMB MiB bandwidth=$bw MiB/sec")
        }
        new RapidsHostMemoryBuffer(
          other.id,
          totalCopySize,
          other.meta,
          applyPriorityOffset(other.getSpillPriority, HOST_MEMORY_BUFFER_SPILL_OFFSET),
          hostBuffer)
      }
    }
  }

  def numBytesFree: Long = maxSize - currentSize

  class RapidsHostMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      buffer: HostMemoryBuffer)
      extends RapidsBufferBase(id, meta, spillPriority)
        with MemoryBuffer.EventHandler {
    override val storageTier: StorageTier = StorageTier.HOST

    override def getMemoryBuffer: MemoryBuffer = synchronized {
      buffer.synchronized {
        setSpillable(this, false)
        buffer.incRefCount()
        buffer
      }
    }

    override def updateSpillability(): Unit = {
      if (buffer.getRefCount == 1) {
        setSpillable(this, true)
      }
    }

    override protected def releaseResources(): Unit = {
      buffer.close()
    }

    /** The size of this buffer in bytes. */
    override def getMemoryUsedBytes: Long = size

    // If this require triggers, we are re-adding a `HostMemoryBuffer` outside of
    // the catalog lock, which should not possible. The event handler is set to null
    // when we free the `RapidsHostMemoryBuffer` and if the buffer is not free, we
    // take out another handle (in the catalog).
    // TODO: This is not robust (to rely on outside locking and addReference/free)
    //  and should be revisited.
    require(buffer.setEventHandler(this) == null,
      "HostMemoryBuffer with non-null event handler failed to add!!")

    /**
     * Override from the MemoryBuffer.EventHandler interface.
     *
     * If we are being invoked we have the `buffer` lock, as this callback
     * is being invoked from `MemoryBuffer.close`
     *
     * @param refCount - buffer's current refCount
     */
    override def onClosed(refCount: Int): Unit = {
      // refCount == 1 means only 1 reference exists to `buffer` in the
      // RapidsHostMemoryBuffer (we own it)
      if (refCount == 1) {
        // setSpillable is being called here as an extension of `MemoryBuffer.close()`
        // we hold the MemoryBuffer lock and we could be called from a Spark task thread
        // Since we hold the MemoryBuffer lock, `incRefCount` waits for us. The only other
        // call to `setSpillable` is also under this same MemoryBuffer lock (see:
        // `getMemoryBuffer`)
        setSpillable(this, true)
      }
    }

    /**
     * We overwrite free to make sure we don't have a handler for the underlying
     * buffer, since this `RapidsBuffer` is no longer tracked.
     */
    override def free(): Unit = synchronized {
      if (isValid) {
        // it is going to be invalid when calling super.free()
        buffer.setEventHandler(null)
      }
      super.free()
    }
  }
}


