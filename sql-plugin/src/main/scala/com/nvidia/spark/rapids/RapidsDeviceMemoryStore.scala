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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.sql.rapids.TempSpillBufferId
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Buffer storage using device memory.
 * @param chunkedPackBounceBufferSize this is the size of the bounce buffer to be used
 *    during spill in chunked_pack. The parameter defaults to 1MB,
 *    since that is the minimum size of this buffer, but
 *    ideally it should be closer to 128MB for an A100 with the
 *    rule-of-thumb of 1MB per SM.
 */
class RapidsDeviceMemoryStore(chunkedPackBounceBufferSize: Long = 1L*1024*1024)
  extends RapidsBufferStore(StorageTier.DEVICE) {

  // The RapidsDeviceMemoryStore handles spillability via ref counting
  override protected def spillableOnAdd: Boolean = false

  // bounce buffer to be used during chunked pack in GPU to host memory spill
  private var chunkedPackBounceBuffer: DeviceMemoryBuffer =
    DeviceMemoryBuffer.allocate(chunkedPackBounceBufferSize)

  override protected def createBuffer(
      other: RapidsBuffer,
      stream: Cuda.Stream): RapidsBufferBase = {
    val memoryBuffer = withResource(other.getCopyIterator) { copyIterator =>
      copyIterator.next()
    }
    withResource(memoryBuffer) { _ =>
      val deviceBuffer = {
        memoryBuffer match {
          case d: DeviceMemoryBuffer => d
          case h: HostMemoryBuffer =>
            closeOnExcept(DeviceMemoryBuffer.allocate(memoryBuffer.getLength)) { deviceBuffer =>
              logDebug(s"copying from host $h to device $deviceBuffer")
              deviceBuffer.copyFromHostBuffer(h, stream)
              deviceBuffer
            }
          case b => throw new IllegalStateException(s"Unrecognized buffer: $b")
        }
      }
      new RapidsDeviceMemoryBuffer(
        other.id,
        deviceBuffer.getLength,
        other.getMeta,
        deviceBuffer,
        other.getSpillPriority)
    }
  }

  /**
   * Adds a buffer to the device storage. This does NOT take ownership of the
   * buffer, so it is the responsibility of the caller to close it.
   *
   * This function is called only from the RapidsBufferCatalog, under the
   * catalog lock.
   *
   * @param id the RapidsBufferId to use for this buffer
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   * @param needsSync whether the spill framework should stream synchronize while adding
   *                  this device buffer (defaults to true)
   * @return the RapidsBuffer instance that was added.
   */
  def addBuffer(
      id: RapidsBufferId,
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBuffer = {
    buffer.incRefCount()
    val rapidsBuffer = new RapidsDeviceMemoryBuffer(
      id,
      buffer.getLength,
      tableMeta,
      buffer,
      initialSpillPriority)
    freeOnExcept(rapidsBuffer) { _ =>
      logDebug(s"Adding receive side table for: [id=$id, size=${buffer.getLength}, " +
        s"uncompressed=${rapidsBuffer.getMeta().bufferMeta.uncompressedSize}, " +
        s"meta_id=${tableMeta.bufferMeta.id}, " +
        s"meta_size=${tableMeta.bufferMeta.size}]")
      addBuffer(rapidsBuffer, needsSync)
      rapidsBuffer
    }
  }

  /**
   * Adds a table to the device storage.
   *
   * This takes ownership of the table.
   *
   * This function is called only from the RapidsBufferCatalog, under the
   * catalog lock.
   *
   * @param id                   the RapidsBufferId to use for this table
   * @param table                table that will be owned by the store
   * @param initialSpillPriority starting spill priority value
   * @param needsSync            whether the spill framework should stream synchronize while adding
   *                             this table (defaults to true)
   * @return the RapidsBuffer instance that was added.
   */
  def addTable(
      id: TempSpillBufferId,
      table: Table,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBuffer = {
    val rapidsTable = new RapidsTable(
      id,
      table,
      initialSpillPriority)
    freeOnExcept(rapidsTable) { _ =>
      addBuffer(rapidsTable, needsSync)
      rapidsTable.updateSpillability()
      rapidsTable
    }
  }

  /**
   * Adds a device buffer to the spill framework, stream synchronizing with the producer
   * stream to ensure that the buffer is fully materialized, and can be safely copied
   * as part of the spill.
   *
   * @param needsSync true if we should stream synchronize before adding the buffer
   */
  private def addBuffer(
      buffer: RapidsBufferBase,
      needsSync: Boolean): Unit = {
    if (needsSync) {
      Cuda.DEFAULT_STREAM.sync()
    }
    addBuffer(buffer)
  }

  /**
   * The RapidsDeviceMemoryStore is the only store that supports setting a buffer spillable
   * or not.
   */
  override protected def setSpillable(buffer: RapidsBufferBase, spillable: Boolean): Unit = {
    doSetSpillable(buffer, spillable)
  }

  /**
   * A per cuDF column event handler that handles calls to .close()
   * inside of the `ColumnVector` lock.
   * @param rapidsTable the `RapidsTable` this handler was associated with
   * @param columnIx the index of the column that this handler belongs to
   * @param repetitionCount the number of times that this column appeared in `RapidsTable`
   * @param wrapped an optional RapidsDeviceColumnEventHandler that could be
   *                not None if this column has been added multiple times to the
   *                spill store.
   */
  class RapidsDeviceColumnEventHandler(
      val rapidsTable: RapidsTable,
      column: ColumnVector,
      repetitionCount: Int,
      var wrapped: Option[RapidsDeviceColumnEventHandler] = None)
      extends ColumnVector.EventHandler {

    override def onClosed(refCount: Int): Unit = {
      // We trigger callbacks iff we reach `refCount` of repetitionCount for this column.
      // repetitionCount == 1 for a column that is not repeated in a table, so this means
      // we are looking for a refCount of 1, but if the column is aliased several times in the
      // table, refCount will be equal to the number of aliases (aka repetitionCount).
      // This signals the `RapidsTable` that a column at index `columnIx` has become
      // spillable again.
      if (refCount == repetitionCount) {
        rapidsTable.onColumnSpillable(column)
        wrapped.foreach(_.onClosed(refCount))
      }
    }
  }

  /**
   * A `RapidsTable` is the spill store holder of a cuDF `Table`.
   *
   * The table is not contiguous in GPU memory. Instead, this `RapidsBuffer` instance
   * allows us to use the cuDF chunked_pack API to make the table contiguous as the spill
   * is happening.
   *
   * This class owns the cuDF table and will close it when `close` is called.
   *
   * @param id the `RapidsBufferId` this table is associated with
   * @param table the cuDF table that we are managing
   * @param spillPriority a starting spill priority
   */
  class RapidsTable(
      id: TempSpillBufferId,
      table: Table,
      spillPriority: Long)
      extends RapidsBufferBase(
        id,
        null,
        spillPriority) {

    /** The storage tier for this buffer */
    override val storageTier: StorageTier = StorageTier.DEVICE

    override val supportsChunkedPacker: Boolean = true

    private var initializedChunkedPacker: Boolean = false

    lazy val chunkedPacker: ChunkedPacker = {
      initializedChunkedPacker = true
      new ChunkedPacker(id, table, chunkedPackBounceBuffer)
    }

    // This is the current size in batch form. It is to be used while this
    // table hasn't migrated to another store.
    private val unpackedSizeInBytes: Long = GpuColumnVector.getTotalDeviceMemoryUsed(table)

    // By default all columns are NOT spillable since we are not the only owners of
    // the columns (the caller is holding onto a ColumnarBatch that will be closed
    // after instantiation, triggering onClosed callbacks)
    // This hash set contains the columns that are currently spillable
    private val columnSpillability = new ConcurrentHashMap[ColumnVector, Boolean]()

    private val distinctColumns =
      (0 until table.getNumberOfColumns).map(table.getColumn).distinct.toArray

    // we register our event callbacks as the very first action to deal with
    // spillability
    registerOnCloseEventHandler()

    /** Release the underlying resources for this buffer. */
    override protected def releaseResources(): Unit = {
      table.close()
    }

    override def getMeta(): TableMeta = {
      chunkedPacker.getMeta
    }

    override def getMemoryUsedBytes: Long = unpackedSizeInBytes

    override def getPackedSizeBytes: Long = getChunkedPacker.getTotalContiguousSize

    override def getChunkedPacker: ChunkedPacker = {
      chunkedPacker
    }

    /**
     * Called from RapidsDeviceColumnEventHandler.onClosed while holding onto the lock
     * of a column in our table.
     *
     * @param columnIx - index of column to mark spillable
     */
    def onColumnSpillable(column: ColumnVector): Unit = {
      columnSpillability.put(column, true)
      updateSpillability()
    }

    /**
     * This is called after adding this RapidsTable to the spillable store
     * in order to update its spillability status.
     */
    def updateSpillability(): Unit = {
      doSetSpillable(this, columnSpillability.size == distinctColumns.size)
    }

    /**
     * Produce a `ColumnarBatch` from our table, and in the process make ourselves
     * not spillable.
     *
     * @param sparkTypes the spark data types the batch should have
     */
    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      columnSpillability.clear()
      doSetSpillable(this, false)
      GpuColumnVector.from(table, sparkTypes)
    }

    /**
     * Get the underlying memory buffer. This may be either a HostMemoryBuffer or a
     * DeviceMemoryBuffer depending on where the buffer currently resides.
     * The caller must have successfully acquired the buffer beforehand.
     *
     * @see [[addReference]]
     * @note It is the responsibility of the caller to close the buffer.
     */
    override def getMemoryBuffer: MemoryBuffer = {
      throw new UnsupportedOperationException(
        "RapidsDeviceMemoryBatch doesn't support getMemoryBuffer")
    }

    override def free(): Unit = {
      // lets remove our handler from the chain of handlers for each column
      removeOnCloseEventHandler()
      super.free()
      if (initializedChunkedPacker) {
        chunkedPacker.close()
        initializedChunkedPacker = false
      }
    }

    private def registerOnCloseEventHandler(): Unit = {
      val cudfColumns = (0 until table.getNumberOfColumns).map(table.getColumn)
      // cudfColumns could contain duplicates. We need to take this into account when we are
      // deciding the floor refCount for a duplicated column
      val repetitionPerColumn = new mutable.HashMap[ColumnVector, Int]()
      cudfColumns.foreach { col =>
        val repetitionCount = repetitionPerColumn.getOrElse(col, 0)
        repetitionPerColumn(col) = repetitionCount + 1
      }

      distinctColumns.foreach { cv =>
        cv.synchronized {
          val priorEventHandler = cv.getEventHandler.asInstanceOf[RapidsDeviceColumnEventHandler]
          val columnEventHandler =
            new RapidsDeviceColumnEventHandler(
              this,
              cv,
              repetitionPerColumn(cv),
              Option(priorEventHandler))
          cv.setEventHandler(columnEventHandler)
          if (cv.getRefCount == repetitionPerColumn(cv)) {
            onColumnSpillable(cv)
          }
        }
      }
    }

    private def removeOnCloseEventHandler(): Unit = {
      distinctColumns.foreach { cv =>
        cv.synchronized {
          cv.getEventHandler match {
            case handler: RapidsDeviceColumnEventHandler =>
              // find the event handler that belongs to this rapidsBuffer
              var priorEventHandler = handler
              var parent = priorEventHandler
              while (priorEventHandler != null && priorEventHandler.rapidsTable != this) {
                parent = priorEventHandler
                priorEventHandler = priorEventHandler.wrapped.orNull
              }
              // if our handler is at the head
              if (priorEventHandler == handler) {
                cv.setEventHandler(priorEventHandler.wrapped.orNull)
              } else {
                // remove ourselves from the chain
                parent.wrapped = if (priorEventHandler != null) {
                  priorEventHandler.wrapped
                } else {
                  null
                }
              }
            case t =>
              throw new IllegalStateException(s"Unknown column event handler $t")
          }
        }
      }
    }
  }

  class RapidsDeviceMemoryBuffer(
      id: RapidsBufferId,
      size: Long,
      meta: TableMeta,
      contigBuffer: DeviceMemoryBuffer,
      spillPriority: Long)
      extends RapidsBufferBase(id, meta, spillPriority)
        with MemoryBuffer.EventHandler {

    override def getMemoryUsedBytes(): Long = size

    override val storageTier: StorageTier = StorageTier.DEVICE

    // If this require triggers, we are re-adding a `DeviceMemoryBuffer` outside of
    // the catalog lock, which should not possible. The event handler is set to null
    // when we free the `RapidsDeviceMemoryBuffer` and if the buffer is not free, we
    // take out another handle (in the catalog).
    // TODO: This is not robust (to rely on outside locking and addReference/free)
    //  and should be revisited.
    require(contigBuffer.setEventHandler(this) == null,
      "DeviceMemoryBuffer with non-null event handler failed to add!!")

    /**
     * Override from the MemoryBuffer.EventHandler interface.
     *
     * If we are being invoked we have the `contigBuffer` lock, as this callback
     * is being invoked from `MemoryBuffer.close`
     *
     * @param refCount - contigBuffer's current refCount
     */
    override def onClosed(refCount: Int): Unit = {
      // refCount == 1 means only 1 reference exists to `contigBuffer` in the
      // RapidsDeviceMemoryBuffer (we own it)
      if (refCount == 1) {
        // setSpillable is being called here as an extension of `MemoryBuffer.close()`
        // we hold the MemoryBuffer lock and we could be called from a Spark task thread
        // Since we hold the MemoryBuffer lock, `incRefCount` waits for us. The only other
        // call to `setSpillable` is also under this same MemoryBuffer lock (see:
        // `getDeviceMemoryBuffer`)
        setSpillable(this, true)
      }
    }

    override protected def releaseResources(): Unit = synchronized {
      // we need to disassociate this RapidsBuffer from the underlying buffer
      contigBuffer.close()
    }

    /**
     * Get and increase the reference count of the device memory buffer
     * in this RapidsBuffer, while making the RapidsBuffer non-spillable.
     *
     * @note It is the responsibility of the caller to close the DeviceMemoryBuffer
     */
    override def getDeviceMemoryBuffer: DeviceMemoryBuffer = synchronized {
      contigBuffer.synchronized {
        setSpillable(this, false)
        contigBuffer.incRefCount()
        contigBuffer
      }
    }

    override def getMemoryBuffer: MemoryBuffer = getDeviceMemoryBuffer

    override def getColumnarBatch(sparkTypes: Array[DataType]): ColumnarBatch = {
      // calling `getDeviceMemoryBuffer` guarantees that we have marked this RapidsBuffer
      // as not spillable and increased its refCount atomically
      withResource(getDeviceMemoryBuffer) { buff =>
        columnarBatchFromDeviceBuffer(buff, sparkTypes)
      }
    }

    /**
     * We overwrite free to make sure we don't have a handler for the underlying
     * contigBuffer, since this `RapidsBuffer` is no longer tracked.
     */
    override def free(): Unit = synchronized {
      if (isValid) {
        // it is going to be invalid when calling super.free()
        contigBuffer.setEventHandler(null)
      }
      super.free()
    }
  }
  override def close(): Unit = {
    super.close()
    chunkedPackBounceBuffer.close()
    chunkedPackBounceBuffer = null
  }
}
