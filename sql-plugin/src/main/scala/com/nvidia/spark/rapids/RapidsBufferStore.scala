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

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
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
    // buffers: contains all buffers in this store, whether spillable or not
    private[this] val buffers = new java.util.HashMap[RapidsBufferId, RapidsBufferBase]
    // spillable: contains only those buffers that are currently spillable
    private[this] val spillable = new HashedPriorityQueue[RapidsBufferBase](comparator)
    // spilling: contians only those buffers that are currently being spilled, but
    // have not been removed from the store
    private[this] val spilling = new mutable.HashSet[RapidsBufferId]()
    // total bytes stored, regardless of spillable status
    private[this] var totalBytesStored: Long = 0L
    // total bytes that are currently eligible to be spilled
    private[this] var totalBytesSpillable: Long = 0L

    def add(buffer: RapidsBufferBase): Unit = synchronized {
      val old = buffers.put(buffer.id, buffer)
      // it is unlikely that the buffer was in this collection, but removing
      // anyway. We assume the buffer is safe in this tier, and is not spilling
      spilling.remove(buffer.id)
      if (old != null) {
        throw new DuplicateBufferException(s"duplicate buffer registered: ${buffer.id}")
      }
      totalBytesStored += buffer.size

      // device buffers "spillability" is handled via DeviceMemoryBuffer ref counting
      // so spillableOnAdd should be false, all other buffer tiers are spillable at
      // all times.
      if (spillableOnAdd) {
        if (spillable.offer(buffer)) {
          totalBytesSpillable += buffer.size
        }
      }
    }

    def remove(id: RapidsBufferId): Unit = synchronized {
      // when removing a buffer we no longer need to know if it was spilling
      spilling.remove(id)
      val obj = buffers.remove(id)
      if (obj != null) {
        totalBytesStored -= obj.size
        if (spillable.remove(obj)) {
          totalBytesSpillable -= obj.size
        }
      }
    }

    def freeAll(): Unit = {
      val values = synchronized {
        val buffs = buffers.values().toArray(new Array[RapidsBufferBase](0))
        buffers.clear()
        spillable.clear()
        spilling.clear()
        buffs
      }
      // We need to release the `RapidsBufferStore` lock to prevent a lock order inversion
      // deadlock: (1) `RapidsBufferBase.free`     calls  (2) `RapidsBufferStore.remove` and
      //           (1) `RapidsBufferStore.freeAll` calls  (2) `RapidsBufferBase.free`.
      values.safeFree()
    }

    /**
     * Sets a buffers state to spillable or non-spillable.
     *
     * If the buffer is currently being spilled or it is no longer in the `buffers` collection
     * (e.g. it is not in this store), the action is skipped.
     *
     * @param buffer      the buffer to mark as spillable or not
     * @param isSpillable whether the buffer should now be spillable
     */
    def setSpillable(buffer: RapidsBufferBase, isSpillable: Boolean): Unit = synchronized {
      if (isSpillable) {
        // if this buffer is in the store and isn't currently spilling
        if (!spilling.contains(buffer.id) && buffers.containsKey(buffer.id)) {
          // try to add it to the spillable collection
          if (spillable.offer(buffer)) {
            totalBytesSpillable += buffer.size
            logDebug(s"Buffer ${buffer.id} is spillable. " +
              s"total=${totalBytesStored} spillable=${totalBytesSpillable}")
          } // else it was already there (unlikely)
        }
      } else {
        if (spillable.remove(buffer)) {
          totalBytesSpillable -= buffer.size
          logDebug(s"Buffer ${buffer.id} is not spillable. " +
            s"total=${totalBytesStored}, spillable=${totalBytesSpillable}")
        } // else it was already removed
      }
    }

    def nextSpillableBuffer(): RapidsBufferBase = synchronized {
      val buffer = spillable.poll()
      if (buffer != null) {
        // mark the id as "spilling" (this buffer is in the middle of a spill operation)
        spilling.add(buffer.id)
        totalBytesSpillable -= buffer.size
        logDebug(s"Spilling buffer ${buffer.id}. size=${buffer.size} " +
          s"total=${totalBytesStored}, new spillable=${totalBytesSpillable}")
      }
      buffer
    }

    def updateSpillPriority(buffer: RapidsBufferBase, priority:Long): Unit = synchronized {
      buffer.updateSpillPriorityValue(priority)
      spillable.priorityUpdated(buffer)
    }

    def getTotalBytes: Long = synchronized { totalBytesStored }

    def getTotalSpillableBytes: Long = synchronized { totalBytesSpillable }
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

  private[this] val spillCount = new AtomicLong(0L)

  /** Return the current byte total of buffers in this store. */
  def currentSize: Long = buffers.getTotalBytes

  def currentSpillableSize: Long = buffers.getTotalSpillableBytes

  /**
   * A store that manages spillability of buffers should override this method
   * to false, otherwise `BufferTracker` treats buffers as always spillable.
   */
  protected def spillableOnAdd: Boolean = true

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
   * @param memoryBuffer memory buffer obtained from the specified Rapids buffer. The ownership
   *                     for `memoryBuffer` is transferred to this store. The store may close
   *                     `memoryBuffer` if necessary.
   * @param stream CUDA stream to use for copy or null
   * @return new buffer that was created
   */
  def copyBuffer(buffer: RapidsBuffer, memoryBuffer: MemoryBuffer, stream: Cuda.Stream)
  : RapidsBufferBase = {
    freeOnExcept(createBuffer(buffer, memoryBuffer, stream)) { newBuffer =>
      addBuffer(newBuffer)
      newBuffer
    }
  }

  protected def doSetSpillable(buffer: RapidsBufferBase, isSpillable: Boolean): Unit = {
    buffers.setSpillable(buffer, isSpillable)
  }

  protected def setSpillable(buffer: RapidsBufferBase, isSpillable: Boolean): Unit = {
    throw new NotImplementedError(s"This store ${this} does not implement setSpillable")
  }

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   *
   * @param targetTotalSize maximum total size of this store after spilling completes
   * @return optionally number of bytes that were spilled, or None if this called
   *         made no attempt to spill due to a detected spill race
   */
  def synchronousSpill(targetTotalSize: Long): Option[Long] =
    synchronousSpill(targetTotalSize, Cuda.DEFAULT_STREAM)

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   * @param targetTotalSize maximum total size of this store after spilling completes
   * @param stream CUDA stream to use or null for default stream
   * @return optionally number of bytes that were spilled, or None if this called
   *         made no attempt to spill due to a detected spill race
   */
  def synchronousSpill(targetTotalSize: Long, stream: Cuda.Stream): Option[Long] = {
    require(targetTotalSize >= 0, s"Negative spill target size: $targetTotalSize")

    var shouldRetry = false
    var totalSpilled: Long = 0

    if (buffers.getTotalSpillableBytes > targetTotalSize) {
      withResource(new NvtxRange(nvtxSyncSpillName, NvtxColor.ORANGE)) { _ =>
        logDebug(s"$name store spilling to reduce usage from " +
          s"${buffers.getTotalBytes} total (${buffers.getTotalSpillableBytes} spillable) " +
          s"to $targetTotalSize bytes")

        def waitForPending(): Unit = {
          logWarning(s"Cannot spill further, waiting for ${pendingFreeBytes.get} " +
            " bytes of pending buffers to be released")
          memoryFreedMonitor.synchronized {
            val memNeeded = buffers.getTotalSpillableBytes - targetTotalSize
            if (memNeeded > 0 && memNeeded <= pendingFreeBytes.get) {
              // This could be a futile wait if the thread(s) holding the pending buffers
              // open are here waiting for more memory.
              memoryFreedMonitor.wait(RapidsBufferStore.FREE_WAIT_TIMEOUT)
            }
          }
        }

        var waited = false
        var exhausted = false

        while (!exhausted && !shouldRetry && buffers.getTotalSpillableBytes > targetTotalSize) {
          val mySpillCount = spillCount.incrementAndGet()
          val maybeAmountSpilled = synchronized {
            if (spillCount.get() == mySpillCount) {
              Some(trySpillAndFreeBuffer(stream))
            } else {
              None
            }
          }
          maybeAmountSpilled match {
            case None =>
              // another thread won and spilled before this thread did. Lets retry the original
              // allocation again
              shouldRetry = true
            case Some(amountSpilled) =>
              if (amountSpilled != 0) {
                totalSpilled += amountSpilled
                waited = false
              } else {
                // we didn't spill in this iteration, and we'll try to wait a bit to see if
                // other threads finish up their work and release pointers to the released
                // buffer
                if (!waited && pendingFreeBytes.get > 0) {
                  waited = true
                  waitForPending()
                } else {
                  exhausted = true
                  logWarning("Unable to spill enough to meet request. " +
                    s"Total=${buffers.getTotalBytes} " +
                    s"Spillable=${buffers.getTotalSpillableBytes} " +
                    s"Target=$targetTotalSize")
                }
              }
          }
        }
        logDebug(s"$this spill complete")
      }
    }

    if (totalSpilled > 0) {
      Some(totalSpilled)
    } else if (shouldRetry) {
      // if we are going to retry, and didn't spill, returning None prevents extra
      // logs where we say we spilled 0 bytes from X store
      None
    } else {
      Some(0)
    }
  }

  /**
   * Create a new buffer from an existing buffer in another store.
   * If the data transfer will be performed asynchronously, this method is responsible for
   * adding a reference to the existing buffer and later closing it when the transfer completes.
   * @note DO NOT close the buffer unless adding a reference!
   * @note `createBuffer` impls should synchronize against `stream` before returning, if needed.
   * @param buffer data from another store
   * @param memoryBuffer memory buffer obtained from the specified Rapids buffer. The ownership
   *                     for `memoryBuffer` is transferred to this store. The store may close
   *                     `memoryBuffer` if necessary.
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
            s"already stored in multiple tiers total mem=${buffers.getTotalBytes} " +
            s"(${buffers.getTotalSpillableBytes} spillable)")
          catalog.removeBufferTier(buffer.id, buffer.storageTier)
        } else {
          logDebug(s"Spilling $buffer ${buffer.id} to ${spillStore.name} " +
              s"total mem=${buffers.getTotalBytes} (${buffers.getTotalSpillableBytes} spillable)")
          val spillCallback = buffer.getSpillCallback
          spillCallback(buffer.storageTier, spillStore.tier, buffer.size)
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
      initialSpillCallback: SpillCallback,
      catalog: RapidsBufferCatalog = RapidsBufferCatalog.singleton,
      deviceStorage: RapidsDeviceMemoryStore = RapidsBufferCatalog.getDeviceStorage)
      extends RapidsBuffer with Arm {
    private val MAX_UNSPILL_ATTEMPTS = 100
    private[this] var isValid = true
    protected[this] var refcount = 0

    private[this] var spillPriority: Long = initialSpillPriority
    private[this] var spillCallback: SpillCallback = initialSpillCallback

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
          case _: BaseDeviceMemoryBuffer =>
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

    /**
     * close() is called by client code to decrease the ref count of this RapidsBufferBase.
     * In the off chance that by the time close is invoked, the buffer was freed (not valid)
     * then this close call winds up freeing the resources of the rapids buffer.
     */
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
     *  setInvalid updates the buffer that it is no longer tracked by the store, and that
     *  it is either about to be removed, or about to be placed in the pending map for
     *  later cleanup, once all references are closed.
     */
    protected def setInvalid(): Unit = synchronized {
      isValid = false
    }

    /**
     * Mark the buffer as freed and no longer valid. This is called by the store when removing a
     * buffer (it is no longer tracked).
     *
     * @note The resources may not be immediately released if the buffer has outstanding references.
     * In that case the resources will be released when the reference count reaches zero.
     */
    override def free(): Unit = synchronized {
      if (isValid) {
        setInvalid()
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

    override def getSpillCallback: SpillCallback = spillCallback

    override def setSpillCallback(callback: SpillCallback): Unit = {
      spillCallback = callback
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
