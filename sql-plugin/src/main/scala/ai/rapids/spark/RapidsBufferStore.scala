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

import java.util.{Comparator, Optional}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, Cuda, DeviceMemoryBuffer, DType, HostMemoryBuffer, NvtxColor, NvtxRange}
import ai.rapids.spark.StorageTier.StorageTier
import ai.rapids.spark.format.{ColumnMeta, SubBufferMeta, TableMeta}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

object RapidsBufferStore {
  private val FREE_WAIT_TIMEOUT = 10 * 1000

  /**
   * Construct a columnar batch from a contiguous device buffer and a
   * TableMeta message describing the schema of the buffer data.
   * @param deviceBuffer contiguous buffer
   * @param meta schema metadata
   * @return columnar batch that must be closed by the caller
   */
  def getBatchFromMeta(deviceBuffer: DeviceMemoryBuffer, meta: TableMeta): ColumnarBatch = {
    val columns = new ArrayBuffer[GpuColumnVector](meta.columnMetasLength())
    try {
      val columnMeta = new ColumnMeta
      (0 until meta.columnMetasLength).foreach { i =>
        columns.append(makeColumn(deviceBuffer, meta.columnMetas(columnMeta, i)))
      }
      new ColumnarBatch(columns.toArray, meta.rowCount.toInt)
    } catch {
      case e: Exception =>
        columns.foreach(_.close())
        throw e
    }
  }

  private def makeColumn(buffer: DeviceMemoryBuffer, meta: ColumnMeta): GpuColumnVector = {
    def getSubBuffer(s: SubBufferMeta): DeviceMemoryBuffer =
      if (s != null) buffer.slice(s.offset, s.length) else null

    assert(meta.childrenLength() == 0, "child columns are not yet supported")
    val dtype = DType.fromNative(meta.dtype)
    val nullCount = if (meta.nullCount >= 0) {
      Optional.of(java.lang.Long.valueOf(meta.nullCount))
    } else {
      Optional.empty[java.lang.Long]
    }
    val dataBuffer = getSubBuffer(meta.data)
    val validBuffer = getSubBuffer(meta.validity)
    val offsetsBuffer = getSubBuffer(meta.offsets)
    GpuColumnVector.from(new ColumnVector(dtype, meta.rowCount, nullCount,
      dataBuffer, validBuffer, offsetsBuffer))
  }
}

/**
 * Base class for all buffer store types.
 *
 * @param name name of this store
 * @param catalog catalog to register this store
 */
abstract class RapidsBufferStore(
    val name: String,
    catalog: RapidsBufferCatalog) extends AutoCloseable with Logging {

  private class BufferTracker {
    private[this] val comparator: Comparator[RapidsBufferBase] =
      (o1: RapidsBufferBase, o2: RapidsBufferBase) => java.lang.Long.compare(o1.getSpillPriority, o2.getSpillPriority)
    private[this] val buffers = new java.util.HashMap[RapidsBufferId, RapidsBufferBase]
    private[this] val spillable = new HashedPriorityQueue[RapidsBufferBase](comparator)
    private[this] var totalBytesStored: Long = 0L

    def add(buffer: RapidsBufferBase): Unit = synchronized {
      val old = buffers.put(buffer.id, buffer)
      require(old == null, s"duplicate buffer registered: ${buffer.id}")
      spillable.offer(buffer)
      totalBytesStored += buffer.size
    }

    def get(id: RapidsBufferId): RapidsBufferBase = synchronized {
      buffers.get(id)
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

  private[this] val spilledBytesStored = new AtomicLong(0L)
  private[this] val pendingFreeBytes = new AtomicLong(0L)

  private[this] val buffers = new BufferTracker

  /** Tracks buffers that are waiting on outstanding references to be freed. */
  private[this] val pendingFreeBuffers = new ConcurrentHashMap[RapidsBufferId, RapidsBufferBase]

  /** Tracks buffers that have been spilled but remain in this store. */
  private[this] val spilledBuffers = new ConcurrentHashMap[RapidsBufferId, RapidsBufferBase]

  /** A monitor that can be used to wait for memory to be freed from this store. */
  protected[this] val memoryFreedMonitor = new Object

  /** A store that can be used for spilling. */
  private[this] var spillStore: RapidsBufferStore = _

  private[this] val nvtxSyncSpillName: String = name + " sync spill"

  catalog.registerStore(this)

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
   * @param stream CUDA stream to use for copy or null
   * @return new buffer that was created
   */
  def copyBuffer(buffer: RapidsBuffer, stream: Cuda.Stream): RapidsBufferBase = {
    val newBuffer = createBuffer(buffer, stream)
    buffers.add(newBuffer)
    newBuffer
  }

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   * @param targetTotalSize maximum total size of this store after spilling completes
   */
  def synchronousSpill(targetTotalSize: Long): Unit = synchronousSpill(targetTotalSize, null)

  /**
   * Free memory in this store by spilling buffers to the spill store synchronously.
   * @param targetTotalSize maximum total size of this store after spilling completes
   * @param stream CUDA stream to use or null for default stream
   */
  def synchronousSpill(targetTotalSize: Long, stream: Cuda.Stream): Unit = {
    assert(targetTotalSize >= 0)

    if (buffers.getTotalBytes > targetTotalSize) {
      val nvtx = new NvtxRange(nvtxSyncSpillName, NvtxColor.ORANGE)
      try {
        logInfo(s"$name store spilling to reduce usage from " +
            s"${buffers.getTotalBytes} to $targetTotalSize bytes")
        var waited = false
        var exhausted = false
        while (!exhausted && buffers.getTotalBytes > targetTotalSize) {
          if (freeSpilledBuffers(targetTotalSize)) {
            waited = false
          } else if (trySpillAndFreeBuffer(stream)) {
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
  }

  def asyncSpillSingleBuffer(targetUnspilledSize: Long, stream: Cuda.Stream): Boolean = synchronized {
    require(targetUnspilledSize >= 0)
    val unspilledSize = buffers.getTotalBytes - spilledBytesStored.get
    if (unspilledSize <= targetUnspilledSize) {
      return false
    }
    val bufferToSpill = buffers.nextSpillableBuffer()
    if (bufferToSpill == null) {
      return false
    }
    // If unable to get a reference then its a race condition where the buffer was invalidated
    // just as we were going to spill it. Either way, indicate to the caller that a spillable
    // buffer was found and more may be available in subsequent invocations.
    if (bufferToSpill.addReference()) {
      val newBuffer = try {
        logInfo(s"Async spilling $bufferToSpill to ${spillStore.name}")
        spillStore.copyBuffer(bufferToSpill, stream)
      } finally {
        bufferToSpill.close()
      }
      if (newBuffer != null) {
        bufferToSpill.markAsSpilled()
      }
    }
    true
  }

  /**
   * Free buffers that has already been spilled via asynchronous spill to
   * reach the specified target store total size.
   * @param targetTotalSize desired total size of the store
   * @return true if at least one spilled buffer was found, false otherwise
   */
  def freeSpilledBuffers(targetTotalSize: Long): Boolean = {
    val it = spilledBuffers.values().iterator()
    val result = it.hasNext
    while (it.hasNext && buffers.getTotalBytes > targetTotalSize) {
      val buffer = it.next()
      buffer.free()
    }
    result
  }

  /**
   * Create a new buffer from an existing buffer in another store.
   * If the data transfer will be performed asynchronously, this method is responsible for
   * adding a reference to the existing buffer and later closing it when the transfer completes.
   * @note DO NOT close the buffer unless adding a reference!
   * @param buffer data from another store
   * @param stream CUDA stream to use or null
   * @return new buffer tracking the data in this store
   */
  protected def createBuffer(buffer: RapidsBuffer, stream: Cuda.Stream): RapidsBufferBase

  /** Update bookkeeping for a new buffer */
  protected def addBuffer(buffer: RapidsBufferBase): Unit = {
    buffers.add(buffer)
    catalog.registerNewBuffer(buffer)
  }

  override def close(): Unit = {
    buffers.freeAll()
  }

  private def trySpillAndFreeBuffer(stream: Cuda.Stream): Boolean = synchronized {
    val bufferToSpill = buffers.nextSpillableBuffer()
    if (bufferToSpill != null) {
      spillAndFreeBuffer(bufferToSpill, stream)
    }
    bufferToSpill != null
  }

  private def spillAndFreeBuffer(buffer: RapidsBufferBase, stream: Cuda.Stream): Unit = {
    if (spillStore == null) {
      throw new OutOfMemoryError("Requested to spill without a spill store")
    }
    // If we fail to get a reference then this buffer has since been freed and probably best
    // to return back to the outer loop to see if enough has been freed.
    if (buffer.addReference()) {
      val newBuffer = try {
        logInfo(s"Spilling $buffer ${buffer.id} to ${spillStore.name} total mem=${buffers.getTotalBytes}")
        spillStore.copyBuffer(buffer, stream)
      } finally {
        buffer.close()
      }
      if (newBuffer != null) {
        catalog.updateBufferMap(buffer.storageTier, newBuffer)
      }
      buffer.free()
    }
  }

  /**
   * Update the catalog for an already spilled buffer that was freed.
   * Since the buffer was spilled, another store in the spill chain
   * now contains the buffer that should be listed in the catalog,
   * and this method finds which store in the chain has that buffer.
   * @param tier the storage tier of the spilled buffer that was freed
   * @param id the buffer ID of the spilled buffer that was freed
   */
  @scala.annotation.tailrec
  private def updateCatalog(tier: StorageTier, id: RapidsBufferId): Unit = {
    val buffer = buffers.get(id)
    if (buffer != null) {
      catalog.updateBufferMap(tier, buffer)
    } else {
      // buffer is not in this store, try the next store
      if (spillStore != null) {
        spillStore.updateCatalog(tier, id)
      } else {
        // buffer may have been deleted in another thread
        logDebug(s"Ignoring catalog update on unknown buffer $id")
      }
    }
  }

  /** Base class for all buffers in this store. */
  abstract class RapidsBufferBase(
      override val id: RapidsBufferId,
      override val size: Long,
      override val meta: TableMeta,
      initialSpillPriority: Long) extends RapidsBuffer {
    private[this] var isValid = true
    protected[this] var refcount = 0
    private[this] var spillPriority: Long = initialSpillPriority

    /** Release the underlying resources for this buffer. */
    protected def releaseResources(): Unit

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

    def markAsSpilled(): Unit = synchronized {
      if (isValid) {
        spilledBuffers.put(id, this)
        spilledBytesStored.addAndGet(size)
      } else {
        // Spilled a buffer that was freed in the interim. The spill store
        // needs to update the catalog or free if no longer in the catalog.
        spillStore.updateCatalog(storageTier, id)
      }
    }

    override def addReference(): Boolean = synchronized {
      if (isValid) {
        refcount += 1
      }
      isValid
    }

    override def getColumnarBatch: ColumnarBatch = {
      // NOTE: Cannot hold a lock on this buffer here because memory is being
      // allocated. Allocations can trigger synchronous spills which can
      // deadlock if another thread holds the device store lock and is trying
      // to spill to this store.
      val deviceBuffer = DeviceMemoryBuffer.allocate(size)
      try {
        val buffer = getMemoryBuffer
        try {
          buffer match {
            case h: HostMemoryBuffer =>
              logDebug(s"copying from host $h to device $deviceBuffer")
              deviceBuffer.copyFromHostBuffer(h)
            case _ => throw new IllegalStateException(
              "must override getColumnarBatch if not providing a host buffer")
          }
        } finally {
          buffer.close()
        }
        RapidsBufferStore.getBatchFromMeta(deviceBuffer, meta)
      } finally {
        deviceBuffer.close()
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
        if (spilledBuffers.remove(id) != null) {
          spillStore.updateCatalog(storageTier, id)
          spilledBytesStored.addAndGet(-size)
          logDebug(s"$name store freed pre-spilled buffer size=$size")
        }

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

    override def setSpillPriority(priority: Long): Unit = buffers.updateSpillPriority(this, priority)

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
