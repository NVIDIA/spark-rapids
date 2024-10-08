/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import java.io.DataOutputStream
import java.nio.channels.{Channels, WritableByteChannel}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostColumnVector, HostMemoryBuffer, JCudfSerialization, MemoryBuffer}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, freeOnExcept, withResource}
import com.nvidia.spark.rapids.SpillPriorities.{applyPriorityOffset, HOST_MEMORY_BUFFER_SPILL_OFFSET}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.TaskContext
import org.apache.spark.sql.rapids.GpuTaskMetrics
import org.apache.spark.sql.rapids.storage.RapidsStorageUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A buffer store using host memory.
 * @param maxSize maximum size in bytes for all buffers in this store
 */
class RapidsHostMemoryStore(
    maxSize: Option[Long])
    extends RapidsBufferStore(StorageTier.HOST) {

  override protected def spillableOnAdd: Boolean = false

  override def getMaxSize: Option[Long] = maxSize

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

  def addBatch(id: RapidsBufferId,
               hostCb: ColumnarBatch,
               initialSpillPriority: Long,
               needsSync: Boolean): RapidsBuffer = {
    RapidsHostColumnVector.incRefCounts(hostCb)
    val rapidsBuffer = new RapidsHostColumnarBatch(
      id,
      hostCb,
      initialSpillPriority)
    freeOnExcept(rapidsBuffer) { _ =>
      addBuffer(rapidsBuffer, needsSync)
      rapidsBuffer
    }
  }

  override protected def trySpillToMaximumSize(
      buffer: RapidsBuffer,
      catalog: RapidsBufferCatalog,
      stream: Cuda.Stream): Boolean = {
    maxSize.forall { ms =>
      // this spillStore has a maximum size requirement (host only). We need to spill from it
      // in order to make room for `buffer`.
      val targetTotalSize = ms - buffer.memoryUsedBytes
      if (targetTotalSize < 0) {
        // lets not spill to host when the buffer we are about
        // to spill is larger than our limit
        false
      } else {
        val amountSpilled = synchronousSpill(targetTotalSize, catalog, stream)
        if (amountSpilled != 0) {
          logDebug(s"Task ${TaskContext.get.taskAttemptId()} spilled $amountSpilled bytes from" +
            s"${name} to make room for ${buffer.id}")
        }
        // if after spill we can fit the new buffer, return true
        buffer.memoryUsedBytes <= (ms - currentSize)
      }
    }
  }

  override protected def createBuffer(
      other: RapidsBuffer,
      catalog: RapidsBufferCatalog,
      stream: Cuda.Stream): Option[RapidsBufferBase] = {
    val wouldFit = trySpillToMaximumSize(other, catalog, stream)
    if (!wouldFit) {
      // skip host
      logWarning(s"Buffer $other with size ${other.memoryUsedBytes} does not fit " +
          s"in the host store, skipping tier.")
      None
    } else {
      // If the other is from the local disk store, we are unspilling to host memory.
      if (other.storageTier == StorageTier.DISK) {
        logDebug(s"copying RapidsDiskStore buffer ${other.id} to a HostMemoryBuffer")
        val hostBuffer = other.getMemoryBuffer.asInstanceOf[HostMemoryBuffer]
        Some(new RapidsHostMemoryBuffer(
          other.id,
          hostBuffer.getLength(),
          other.meta,
          applyPriorityOffset(other.getSpillPriority, HOST_MEMORY_BUFFER_SPILL_OFFSET),
          hostBuffer))
      } else {
        withResource(other.getCopyIterator) { otherBufferIterator =>
          val isChunked = otherBufferIterator.isChunked
          val totalCopySize = otherBufferIterator.getTotalCopySize
          closeOnExcept(HostAlloc.tryAlloc(totalCopySize)) { hb =>
            hb.map { hostBuffer =>
              val spillNs = GpuTaskMetrics.get.spillToHostTime {
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
                System.nanoTime() - start
              }
              val szMB = (totalCopySize.toDouble / 1024.0 / 1024.0).toLong
              val bw = (szMB.toDouble / (spillNs.toDouble / 1000000000.0)).toLong
              logDebug(s"Spill to host (chunked=$isChunked) " +
                  s"size=$szMB MiB bandwidth=$bw MiB/sec")
              new RapidsHostMemoryBuffer(
                other.id,
                totalCopySize,
                other.meta,
                applyPriorityOffset(other.getSpillPriority, HOST_MEMORY_BUFFER_SPILL_OFFSET),
                hostBuffer)
            }.orElse {
              // skip host
              logWarning(s"Buffer $other with size ${other.memoryUsedBytes} does not fit " +
                  s"in the host store, skipping tier.")
              None
            }
          }
        }
      }
    }
  }

  def numBytesFree: Option[Long] = maxSize.map(_ - currentSize)

  class RapidsHostMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      spillPriority: Long,
      buffer: HostMemoryBuffer)
      extends RapidsBufferBase(id, meta, spillPriority)
        with RapidsBufferChannelWritable
        with MemoryBuffer.EventHandler {
    override val storageTier: StorageTier = StorageTier.HOST

    override def getMemoryBuffer: MemoryBuffer = getHostMemoryBuffer

    override def getHostMemoryBuffer: HostMemoryBuffer = synchronized {
      buffer.synchronized {
        setSpillable(this, false)
        buffer.incRefCount()
        buffer
      }
    }

    override def writeToChannel(outputChannel: WritableByteChannel, ignored: Cuda.Stream): Long = {
      var written: Long = 0L
      val iter = new HostByteBufferIterator(buffer)
      iter.foreach { bb =>
        try {
          while (bb.hasRemaining) {
            written += outputChannel.write(bb)
          }
        } finally {
          RapidsStorageUtils.dispose(bb)
        }
      }
      written
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
    override val memoryUsedBytes: Long = size

    // If this require triggers, we are re-adding a `HostMemoryBuffer` outside of
    // the catalog lock, which should not possible. The event handler is set to null
    // when we free the `RapidsHostMemoryBuffer` and if the buffer is not free, we
    // take out another handle (in the catalog).
    HostAlloc.addEventHandler(buffer, this)

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
        HostAlloc.removeEventHandler(buffer, this)
      }
      super.free()
    }
  }

  /**
   * A per cuDF host column event handler that handles calls to .close()
   * inside of the `HostColumnVector` lock.
   */
  class RapidsHostColumnEventHandler
    extends HostColumnVector.EventHandler {

    // Every RapidsHostColumnarBatch that references this column has an entry in this map.
    // The value represents the number of times (normally 1) that a ColumnVector
    // appears in the RapidsHostColumnarBatch. This is also the HosColumnVector refCount at which
    // the column is considered spillable.
    // The map is protected via the ColumnVector lock.
    private val registration = new mutable.HashMap[RapidsHostColumnarBatch, Int]()

    /**
     * Every RapidsHostColumnarBatch iterates through its columns and either creates
     * a `RapidsHostColumnEventHandler` object and associates it with the column's
     * `eventHandler` or calls into the existing one, and registers itself.
     *
     * The registration has two goals: it accounts for repetition of a column
     * in a `RapidsHostColumnarBatch`. If a batch has the same column repeated it must adjust
     * the refCount at which this column is considered spillable.
     *
     * The second goal is to account for aliasing. If two host batches alias this column
     * we are going to mark it as non spillable.
     *
     * @param rapidsHostCb - the host batch that is registering itself with this tracker
     */
    def register(rapidsHostCb: RapidsHostColumnarBatch, repetition: Int): Unit = {
      registration.put(rapidsHostCb, repetition)
    }

    /**
     * This is invoked during `RapidsHostColumnarBatch.free` in order to remove the entry
     * in `registration`.
     *
     * @param rapidsHostCb - the batch that is de-registering itself
     */
    def deregister(rapidsHostCb: RapidsHostColumnarBatch): Unit = {
      registration.remove(rapidsHostCb)
    }

    // called with the cudf HostColumnVector lock held from cuDF's side
    override def onClosed(cudfCv: HostColumnVector, refCount: Int): Unit = {
      // we only handle spillability if there is a single batch registered
      // (no aliasing)
      if (registration.size == 1) {
        val (rapidsHostCb, spillableRefCount) = registration.head
        if (spillableRefCount == refCount) {
          rapidsHostCb.onColumnSpillable(cudfCv)
        }
      }
    }
  }

  /**
   * A `RapidsHostColumnarBatch` is the spill store holder of ColumnarBatch backed by
   * HostColumnVector.
   *
   * This class owns the host batch and will close it when `close` is called.
   *
   * @param id the `RapidsBufferId` this batch is associated with
   * @param batch the host ColumnarBatch we are managing
   * @param spillPriority a starting spill priority
   */
  class RapidsHostColumnarBatch(
      id: RapidsBufferId,
      hostCb: ColumnarBatch,
      spillPriority: Long)
      extends RapidsBufferBase(
        id,
        null,
        spillPriority)
          with RapidsBufferChannelWritable
          with RapidsHostBatchBuffer {

    override val storageTier: StorageTier = StorageTier.HOST

    // By default all columns are NOT spillable since we are not the only owners of
    // the columns (the caller is holding onto a ColumnarBatch that will be closed
    // after instantiation, triggering onClosed callbacks)
    // This hash set contains the columns that are currently spillable.
    private val columnSpillability = new ConcurrentHashMap[HostColumnVector, Boolean]()

    private val numDistinctColumns = RapidsHostColumnVector.extractBases(hostCb).distinct.size

    // we register our event callbacks as the very first action to deal with
    // spillability
    registerOnCloseEventHandler()

    /** Release the underlying resources for this buffer. */
    override protected def releaseResources(): Unit = {
      hostCb.close()
    }

    override def meta: TableMeta = {
      null
    }

    // This is the current size in batch form. It is to be used while this
    // batch hasn't migrated to another store.
    override val memoryUsedBytes: Long = RapidsHostColumnVector.getTotalHostMemoryUsed(hostCb)

    /**
     * Mark a column as spillable
     *
     * @param column the ColumnVector to mark as spillable
     */
    def onColumnSpillable(column: HostColumnVector): Unit = {
      columnSpillability.put(column, true)
      updateSpillability()
    }

    /**
     * Update the spillability state of this RapidsHostColumnarBatch. This is invoked from
     * two places:
     *
     * - from the onColumnSpillable callback, which is invoked from a
     * HostColumnVector.EventHandler.onClosed callback.
     *
     * - after adding a batch to the store to mark the batch as spillable if
     * all columns are spillable.
     */
    override def updateSpillability(): Unit = {
      setSpillable(this, columnSpillability.size == numDistinctColumns)
    }

    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      throw new UnsupportedOperationException(
        "RapidsHostColumnarBatch does not support getColumnarBatch")
    }

    override def getHostColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      columnSpillability.clear()
      setSpillable(this, false)
      RapidsHostColumnVector.incRefCounts(hostCb)
    }

    override def getMemoryBuffer: MemoryBuffer = {
      throw new UnsupportedOperationException(
        "RapidsHostColumnarBatch does not support getMemoryBuffer")
    }

    override def getCopyIterator: RapidsBufferCopyIterator = {
      throw new UnsupportedOperationException(
        "RapidsHostColumnarBatch does not support getCopyIterator")
    }

    override def writeToChannel(outputChannel: WritableByteChannel, ignored: Cuda.Stream): Long = {
      withResource(Channels.newOutputStream(outputChannel)) { outputStream =>
        withResource(new DataOutputStream(outputStream)) { dos =>
          val columns = RapidsHostColumnVector.extractBases(hostCb)
          JCudfSerialization.writeToStream(columns, dos, 0, hostCb.numRows())
          dos.size()
        }
      }
    }

    override def free(): Unit = {
      // lets remove our handler from the chain of handlers for each column
      removeOnCloseEventHandler()
      super.free()
    }

    private def registerOnCloseEventHandler(): Unit = {
      val columns = RapidsHostColumnVector.extractBases(hostCb)
      // cudfColumns could contain duplicates. We need to take this into account when we are
      // deciding the floor refCount for a duplicated column
      val repetitionPerColumn = new mutable.HashMap[HostColumnVector, Int]()
      columns.foreach { col =>
        val repetitionCount = repetitionPerColumn.getOrElse(col, 0)
        repetitionPerColumn(col) = repetitionCount + 1
      }
      repetitionPerColumn.foreach { case (distinctCv, repetition) =>
        // lock the column because we are setting its event handler, and we are inspecting
        // its refCount.
        distinctCv.synchronized {
          val eventHandler = distinctCv.getEventHandler match {
            case null =>
              val eventHandler = new RapidsHostColumnEventHandler
              distinctCv.setEventHandler(eventHandler)
              eventHandler
            case existing: RapidsHostColumnEventHandler =>
              existing
            case other =>
              throw new IllegalStateException(
                s"Invalid column event handler $other")
          }
          eventHandler.register(this, repetition)
          if (repetition == distinctCv.getRefCount) {
            onColumnSpillable(distinctCv)
          }
        }
      }
    }

    // this method is called from free()
    private def removeOnCloseEventHandler(): Unit = {
      val distinctColumns = RapidsHostColumnVector.extractBases(hostCb).distinct
      distinctColumns.foreach { distinctCv =>
        distinctCv.synchronized {
          distinctCv.getEventHandler match {
            case eventHandler: RapidsHostColumnEventHandler =>
              eventHandler.deregister(this)
            case t =>
              throw new IllegalStateException(
                s"Invalid column event handler $t")
          }
        }
      }
    }
  }
}


