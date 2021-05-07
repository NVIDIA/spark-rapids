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

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.StorageTier.{DEVICE, StorageTier}
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object RapidsBufferStore {
  private val FREE_WAIT_TIMEOUT = 10 * 1000
}

/**
 * Base class for all buffer store types.
 *
 * @param tier storage tier of this store
 * @param catalog catalog to register this store
 */
abstract class RapidsBufferStore(
    val tier: StorageTier,
    catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton)
    extends AutoCloseable with Logging with Arm {

  val name: String = tier.toString

  private class BufferTracker {
    private[this] val comparator: Comparator[RapidsBufferBase] =
      (o1: RapidsBufferBase, o2: RapidsBufferBase) =>
        java.lang.Long.compare(o1.getSpillPriority, o2.getSpillPriority)
    private[this] val buffers = new java.util.HashMap[RapidsBufferId, RapidsBufferBase]
    private[this] val spillable = new HashedPriorityQueue[RapidsBufferBase](comparator)
    private[this] var totalBytesStored: Long = 0L

    def add(buffer: RapidsBufferBase): Unit = synchronized {
      val old = buffers.put(buffer.id, buffer)
      if (old != null) {
        throw new DuplicateBufferException(s"duplicate buffer registered: ${buffer.id}")
      }
      spillable.offer(buffer)
      totalBytesStored += buffer.size
    }

    def remove(id: RapidsBufferId): Unit = synchronized {
      val obj = buffers.remove(id)
      if (obj != null) {
        spillable.remove(obj)
        totalBytesStored -= obj.size
      }
    }

    def freeAll(): Unit = synchronized {
      val values = buffers.values().toArray(new Array[RapidsBufferBase](0))
      values.foreach(_.free())
      buffers.clear()
      spillable.clear()
    }

    def nextSpillableBuffer(): RapidsBufferBase = synchronized {
      spillable.poll()
    }

    def updateSpillPriority(buffer: RapidsBufferBase, priority:Long): Unit = synchronized {
      buffer.updateSpillPriorityValue(priority)
      spillable.priorityUpdated(buffer)
    }

    def getTotalBytes: Long = synchronized { totalBytesStored }
  }

  private[this] val pendingFreeBytes = new AtomicLong(0L)

  private[this] val buffers = new BufferTracker

  /** Tracks buffers that are waiting on outstanding references to be freed. */
  private[this] val pendingFreeBuffers = new ConcurrentHashMap[RapidsBufferId, RapidsBufferBase]

  /** A monitor that can be used to wait for memory to be freed from this store. */
  protected[this] val memoryFreedMonitor = new Object

  /** A store that can be used for spilling. */
  private[this] var spillStore: RapidsBufferStore = _

  private[this] val nvtxSyncSpillName: String = name + " sync spill"

  /** Return the current byte total of buffers in this store. */
  def currentSize: Long = buffers.getTotalBytes

  /**
   * Specify another store that can be used when this store needs to spill.
   * @note Only one spill store can be registered. This will throw if a
   * spill store has already been registered.
   */
  def setSpillStore(store: RapidsBufferStore): Unit = {
    require(spillStore == null, "spill store already registered")
    spillStore = store
  }

  /**
   * Adds an existing buffer from another store to this store. The buffer must already
   * have an active reference by the caller and needs to be eventually closed by the caller
   * (i.e.: this method will not take ownership of the incoming buffer object).
   * This does not need to update the catalog, the caller is responsible for that.
   * @param buffer data from another store
   * @param memoryBuffer memory buffer obtained from the specified Rapids buffer. It will be closed
   *                     by this method
   * @param stream CUDA stream to use for copy or null
   * @return new buffer that was created
   */
  def copyBuffer(buffer: RapidsBuffer, memoryBuffer: MemoryBuffer, stream: Cuda.Stream)
  : RapidsBufferBase = {
    freeOnExcept(createBuffer(buffer, memoryBuffer, stream)) { newBuffer =>
      buffers.add(newBuffer)
      catalog.registerNewBuffer(newBuffer)
      newBuffer
    }
  }

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   * @param targetTotalSize maximum total size of this store after spilling completes
   * @return number of bytes that were spilled
   */
  def synchronousSpill(targetTotalSize: Long): Long = synchronousSpill(targetTotalSize, null)

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   * @param targetTotalSize maximum total size of this store after spilling completes
   * @param stream CUDA stream to use or null for default stream
   * @return number of bytes that were spilled
   */
  def synchronousSpill(targetTotalSize: Long, stream: Cuda.Stream): Long = {
    require(targetTotalSize >= 0, s"Negative spill target size: $targetTotalSize")

    var totalSpilled: Long = 0
    if (buffers.getTotalBytes > targetTotalSize) {
      val nvtx = new NvtxRange(nvtxSyncSpillName, NvtxColor.ORANGE)
      try {
        logDebug(s"$name store spilling to reduce usage from " +
            s"${buffers.getTotalBytes} to $targetTotalSize bytes")
        var waited = false
        var exhausted = false
        while (!exhausted && buffers.getTotalBytes > targetTotalSize) {
          val amountSpilled = trySpillAndFreeBuffer(stream)
          if (amountSpilled != 0) {
            totalSpilled += amountSpilled
            waited = false
          } else {
            if (!waited && pendingFreeBytes.get > 0) {
              waited = true
              logWarning(s"Cannot spill further, waiting for ${pendingFreeBytes.get} " +
                  " bytes of pending buffers to be released")
              memoryFreedMonitor.synchronized {
                val memNeeded = buffers.getTotalBytes - targetTotalSize
                if (memNeeded > 0 && memNeeded <= pendingFreeBytes.get) {
                  // This could be a futile wait if the thread(s) holding the pending buffers open
                  // are here waiting for more memory.
                  memoryFreedMonitor.wait(RapidsBufferStore.FREE_WAIT_TIMEOUT)
                }
              }
            } else {
              logWarning("Unable to spill enough to meet request. " +
                  s"Total=${buffers.getTotalBytes} Target=$targetTotalSize")
              exhausted = true
            }
          }
        }
        logDebug(s"$this spill complete")
      } finally {
        nvtx.close()
      }
    }

    totalSpilled
  }

  /**
   * Create a new buffer from an existing buffer in another store.
   * If the data transfer will be performed asynchronously, this method is responsible for
   * adding a reference to the existing buffer and later closing it when the transfer completes.
   * @note DO NOT close the buffer unless adding a reference!
   * @param buffer data from another store
   * @param memoryBuffer memory buffer obtained from the specified Rapids buffer. It will be closed
   *                     by this method
   * @param stream CUDA stream to use or null
   * @return new buffer tracking the data in this store
   */
  protected def createBuffer(buffer: RapidsBuffer, memoryBuffer: MemoryBuffer, stream: Cuda.Stream)
  : RapidsBufferBase

  /** Update bookkeeping for a new buffer */
  protected def addBuffer(buffer: RapidsBufferBase): Unit = synchronized {
    buffers.add(buffer)
    catalog.registerNewBuffer(buffer)
  }

  override def close(): Unit = {
    buffers.freeAll()
  }

  private def trySpillAndFreeBuffer(stream: Cuda.Stream): Long = synchronized {
    val bufferToSpill = buffers.nextSpillableBuffer()
    if (bufferToSpill != null) {
      spillAndFreeBuffer(bufferToSpill, stream)
      bufferToSpill.size
    } else {
      0
    }
  }

  private def spillAndFreeBuffer(buffer: RapidsBufferBase, stream: Cuda.Stream): Unit = {
    if (spillStore == null) {
      throw new OutOfMemoryError("Requested to spill without a spill store")
    }
    // If we fail to get a reference then this buffer has since been freed and probably best
    // to return back to the outer loop to see if enough has been freed.
    if (buffer.addReference()) {
      try {
        if (catalog.isBufferSpilled(buffer.id, buffer.storageTier)) {
          logDebug(s"Skipping spilling $buffer ${buffer.id} to ${spillStore.name} as it is " +
              s"already stored in multiple tiers total mem=${buffers.getTotalBytes}")
          catalog.removeBufferTier(buffer.id, buffer.storageTier)
        } else {
          logDebug(s"Spilling $buffer ${buffer.id} to ${spillStore.name} " +
              s"total mem=${buffers.getTotalBytes}")
          buffer.spillCallback(buffer.storageTier, spillStore.tier, buffer.size)
          spillStore.copyBuffer(buffer, buffer.getMemoryBuffer, stream)
        }
      } finally {
        buffer.close()
      }
      catalog.removeBufferTier(buffer.id, buffer.storageTier)
      buffer.free()
    }
  }

  /** Base class for all buffers in this store. */
  abstract class RapidsBufferBase(
      override val id: RapidsBufferId,
      override val size: Long,
      override val meta: TableMeta,
      initialSpillPriority: Long,
      override val spillCallback: RapidsBuffer.SpillCallback,
      catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
      deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
      extends RapidsBuffer with Arm {
    private val MAX_UNSPILL_ATTEMPTS = 100
    private[this] var isValid = true
    protected[this] var refcount = 0
    private[this] var spillPriority: Long = initialSpillPriority

    /** Release the underlying resources for this buffer. */
    protected def releaseResources(): Unit

    /**
     * Materialize the memory buffer from the underlying storage.
     *
     * If the buffer resides in device or host memory, only reference count is incremented.
     * If the buffer resides in secondary storage, a new host or device memory buffer is created,
     * with the data copied to the new buffer.
     * The caller must have successfully acquired the buffer beforehand.
     * @see [[addReference]]
     * @note It is the responsibility of the caller to close the buffer.
     * @note This is an internal API only used by Rapids buffer stores.
     */
    protected def materializeMemoryBuffer: MemoryBuffer = getMemoryBuffer

    /**
     * Determine if a buffer is currently acquired.
     * @note Unless this is called by the thread that currently "owns" an
     * acquired buffer, the acquisition state could be changing
     * asynchronously, and therefore the result cannot always be used as a
     * proxy for the result obtained from the addReference method.
     */
    def isAcquired: Boolean = synchronized {
      refcount > 0
    }

    override def addReference(): Boolean = synchronized {
      if (isValid) {
        refcount += 1
      }
      isValid
    }

    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      // NOTE: Cannot hold a lock on this buffer here because memory is being
      // allocated. Allocations can trigger synchronous spills which can
      // deadlock if another thread holds the device store lock and is trying
      // to spill to this store.
      withResource(getDeviceMemoryBuffer) { deviceBuffer =>
        columnarBatchFromDeviceBuffer(deviceBuffer, sparkTypes)
      }
    }

    protected def columnarBatchFromDeviceBuffer(devBuffer: DeviceMemoryBuffer,
        sparkTypes: Array[DataType]): ColumnarBatch = {
      val bufferMeta = meta.bufferMeta()
      if (bufferMeta == null || bufferMeta.codecBufferDescrsLength == 0) {
        MetaUtils.getBatchFromMeta(devBuffer, meta, sparkTypes)
      } else {
        GpuCompressedColumnVector.from(devBuffer, meta)
      }
    }

    override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long,
        length: Long, stream: Cuda.Stream): Unit = {
      withResource(getMemoryBuffer) { memBuff =>
        dst match {
          case _: HostMemoryBuffer =>
            // TODO: consider moving to the async version.
            dst.copyFromMemoryBuffer(dstOffset, memBuff, srcOffset, length, stream)
          case _: DeviceMemoryBuffer =>
            dst.copyFromMemoryBufferAsync(dstOffset, memBuff, srcOffset, length, stream)
          case _ =>
            throw new IllegalStateException(s"Infeasible destination buffer type ${dst.getClass}")
        }
      }
    }

    override def getDeviceMemoryBuffer: DeviceMemoryBuffer = {
      if (RapidsBufferCatalog.shouldUnspill) {
        (0 until MAX_UNSPILL_ATTEMPTS).foreach { _ =>
          catalog.acquireBuffer(id, DEVICE) match {
            case Some(buffer) =>
              withResource(buffer) { _ =>
                return buffer.getDeviceMemoryBuffer
              }
            case _ =>
              try {
                logDebug(s"Unspilling $this $id to $DEVICE")
                val newBuffer = deviceStorage.copyBuffer(
                  this, materializeMemoryBuffer, Cuda.DEFAULT_STREAM)
                if (newBuffer.addReference()) {
                  withResource(newBuffer) { _ =>
                    return newBuffer.getDeviceMemoryBuffer
                  }
                }
              } catch {
                case _: DuplicateBufferException =>
                  logDebug(s"Lost device buffer registration race for buffer $id, retrying...")
              }
          }
        }
        throw new IllegalStateException(s"Unable to get device memory buffer for ID: $id")
      } else {
        materializeMemoryBuffer match {
          case h: HostMemoryBuffer =>
            withResource(h) { _ =>
              closeOnExcept(DeviceMemoryBuffer.allocate(size)) { deviceBuffer =>
                logDebug(s"copying from host $h to device $deviceBuffer")
                deviceBuffer.copyFromHostBuffer(h)
                deviceBuffer
              }
            }
          case d: DeviceMemoryBuffer => d
          case b => throw new IllegalStateException(s"Unrecognized buffer: $b")
        }
      }
    }

    override def close(): Unit = synchronized {
      if (refcount == 0) {
        throw new IllegalStateException("Buffer already closed")
      }
      refcount -= 1
      if (refcount == 0 && !isValid) {
        pendingFreeBuffers.remove(id)
        pendingFreeBytes.addAndGet(-size)
        freeBuffer()
      }
    }

    /**
     * Mark the buffer as freed and no longer valid.
     * @note The resources may not be immediately released if the buffer has outstanding references.
     * In that case the resources will be released when the reference count reaches zero.
     */
    override def free(): Unit = synchronized {
      if (isValid) {
        isValid = false
        buffers.remove(id)
        if (refcount == 0) {
          freeBuffer()
        } else {
          pendingFreeBuffers.put(id, this)
          pendingFreeBytes.addAndGet(size)
        }
      } else {
        logWarning(s"Trying to free an invalid buffer => $id, size = $size, $this")
      }
    }

    override def getSpillPriority: Long = spillPriority

    override def setSpillPriority(priority: Long): Unit =
      buffers.updateSpillPriority(this, priority)

    private[RapidsBufferStore] def updateSpillPriorityValue(priority: Long): Unit = {
      spillPriority = priority
    }

    /** Must be called with a lock on the buffer */
    private def freeBuffer(): Unit = {
      releaseResources()
      memoryFreedMonitor.synchronized {
        memoryFreedMonitor.notifyAll()
      }
    }

    override def toString: String = s"$name buffer size=$size"
  }
}
