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

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Consumer, IntUnaryOperator}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.ShuffleBlockId

/** Identifier for a shuffle buffer that holds the data for a table */
case class ShuffleBufferId(
    blockId: ShuffleBlockId,
    override val tableId: Int) extends RapidsBufferId {
  val shuffleId: Int = blockId.shuffleId
  val mapId: Long = blockId.mapId

  override val canShareDiskPaths: Boolean = true

  override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File = {
    diskBlockManager.getFile(blockId)
  }
}

/** Catalog for lookup of shuffle buffers by block ID */
class ShuffleBufferCatalog(
    catalog: RapidsBufferCatalog,
    diskBlockManager: RapidsDiskBlockManager) extends Logging {

  private val deviceStore = RapidsBufferCatalog.getDeviceStorage

  private val bufferIdToHandle = new ConcurrentHashMap[RapidsBufferId, RapidsBufferHandle]()

  private def trackCachedHandle(
      bufferId: ShuffleBufferId,
      bufferHandle: RapidsBufferHandle): Unit = {
    bufferIdToHandle.put(bufferId, bufferHandle)
  }

  def removeCachedHandles(): Unit = {
    bufferIdToHandle.forEach { case (_, handle) =>
      removeBuffer(handle)
    }
  }

  /**
   * Adds a contiguous table shuffle table to the device storage. This does NOT take ownership of
   * the contiguous table, so it is the responsibility of the caller to close it.
   * The refcount of the underlying device buffer will be incremented so the contiguous table
   * can be closed before this buffer is destroyed.
   *
   * @param blockId Spark's `ShuffleBlockId` that identifies this buffer
   * @param contigTable contiguous table to track in storage
   * @param initialSpillPriority starting spill priority value for the buffer
   * @param needsSync whether the spill framework should stream synchronize while adding
   *                  this device buffer (defaults to true)
   * @return RapidsBufferHandle identifying this table
   */
  def addContiguousTable(
      blockId: ShuffleBlockId,
      contigTable: ContiguousTable,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBufferHandle = {
    val bufferId = nextShuffleBufferId(blockId)
    withResource(contigTable) { _ =>
      val handle = catalog.addContiguousTable(
        bufferId,
        contigTable,
        initialSpillPriority,
        needsSync)
      trackCachedHandle(bufferId, handle)
      handle
    }
  }

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   *
   * @param blockId Spark's `ShuffleBlockId` that identifies this buffer
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   * @return RapidsBufferHandle associated with this buffer
   */
  def addBuffer(
      blockId: ShuffleBlockId,
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBufferHandle = {
    val bufferId = nextShuffleBufferId(blockId)
    // update the table metadata for the buffer ID generated above
    tableMeta.bufferMeta.mutateId(bufferId.tableId)
    val handle = catalog.addBuffer(
      bufferId,
      buffer,
      tableMeta,
      initialSpillPriority,
      needsSync)
    trackCachedHandle(bufferId, handle)
    handle
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same block ID (extremely unlikely)
   */
  def addDegenerateRapidsBuffer(
      blockId: ShuffleBlockId,
      meta: TableMeta): RapidsBufferHandle = {
    val bufferId = nextShuffleBufferId(blockId)
    val handle = catalog.registerDegenerateBuffer(bufferId, meta)
    trackCachedHandle(bufferId, handle)
    handle
  }

  /**
   * Information stored for each active shuffle.
   * NOTE: ArrayBuffer in blockMap must be explicitly locked when using it!
   *
   * @param blockMap mapping of block ID to array of buffers for the block
   */
  private case class ShuffleInfo(
      blockMap: ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[ShuffleBufferId]])

  /** shuffle information for each active shuffle */
  private[this] val activeShuffles = new ConcurrentHashMap[Int, ShuffleInfo]

  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  /**
   * Register a new shuffle.
   * This must be called before any buffer identifiers associated with this shuffle can be tracked.
   * @param shuffleId shuffle identifier
   */
  def registerShuffle(shuffleId: Int): Unit = {
    activeShuffles.computeIfAbsent(shuffleId, _ => ShuffleInfo(
      new ConcurrentHashMap[ShuffleBlockId, ArrayBuffer[ShuffleBufferId]]))
  }

  /** Frees all buffers that correspond to the specified shuffle. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    // This might be called on a background thread that has not set the device yet.
    GpuDeviceManager.getDeviceId().foreach(Cuda.setDevice)

    val info = activeShuffles.remove(shuffleId)
    if (info != null) {
      val bufferRemover: Consumer[ArrayBuffer[ShuffleBufferId]] = { bufferIds =>
        // NOTE: Not synchronizing array buffer because this shuffle should be inactive.
        bufferIds.foreach { id =>
          tableMap.remove(id.tableId)
          val handle = bufferIdToHandle.remove(id)
          if (handle != null) {
            handle.close()
          }
        }
      }
      info.blockMap.forEachValue(Long.MaxValue, bufferRemover)

      val fileRemover: Consumer[ShuffleBlockId] = { blockId =>
        val file = diskBlockManager.getFile(blockId)
        logDebug(s"Deleting file $file")
        if (!file.delete() && file.exists()) {
          logWarning(s"Unable to delete $file")
        }
      }
      info.blockMap.forEachKey(Long.MaxValue, fileRemover)
    } else {
      // currently shuffle unregister can get called on the driver which never saw a register
      if (!TrampolineUtil.isDriver(SparkEnv.get)) {
        logWarning(s"Ignoring unregister of unknown shuffle $shuffleId")
      }
    }
  }

  def hasActiveShuffle(shuffleId: Int): Boolean = activeShuffles.containsKey(shuffleId)

  /** Get all the buffer IDs that correspond to a shuffle block identifier. */
  def blockIdToBuffersIds(blockId: ShuffleBlockId): Array[ShuffleBufferId] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle $blockId.shuffleId")
    }
    val entries = info.blockMap.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized {
      entries.toArray
    }
  }

  def blockIdToBufferHandles(blockId: ShuffleBlockId): Array[RapidsBufferHandle] = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new NoSuchElementException(s"unknown shuffle $blockId.shuffleId")
    }
    val entries = info.blockMap.get(blockId)
    if (entries == null) {
      throw new NoSuchElementException(s"unknown shuffle block $blockId")
    }
    entries.synchronized {
      entries.map(bufferIdToHandle.get).toArray
    }
  }

  /** Get all the buffer metadata that correspond to a shuffle block identifier. */
  def blockIdToMetas(blockId: ShuffleBlockId): Seq[TableMeta] = {
    blockIdToBuffersIds(blockId).map(catalog.getBufferMeta)
  }

  /** Allocate a new shuffle buffer identifier and update the shuffle block mapping. */
  def nextShuffleBufferId(blockId: ShuffleBlockId): ShuffleBufferId = {
    val info = activeShuffles.get(blockId.shuffleId)
    if (info == null) {
      throw new IllegalStateException(s"unknown shuffle ${blockId.shuffleId}")
    }

    val tableId = tableIdCounter.getAndUpdate(ShuffleBufferCatalog.TABLE_ID_UPDATER)
    val id = ShuffleBufferId(blockId, tableId)
    val prev = tableMap.put(tableId, id)
    if (prev != null) {
      throw new IllegalStateException(s"table ID $tableId is already in use")
    }

    // associate this new buffer with the shuffle block
    val blockBufferIds = info.blockMap.computeIfAbsent(blockId, _ => 
      new ArrayBuffer[ShuffleBufferId])
    blockBufferIds.synchronized {
      blockBufferIds.append(id)
    }
    id
  }

  /** Lookup the shuffle buffer handle that corresponds to the specified table identifier. */
  def getShuffleBufferHandle(tableId: Int): RapidsBufferHandle = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      throw new NoSuchElementException(s"unknown table ID $tableId")
    }
    bufferIdToHandle.get(shuffleBufferId)
  }

  /**
   * Update the spill priority of a shuffle buffer that soon will be read locally.
   * @param handle shuffle buffer handle of buffer to update
   */
  def updateSpillPriorityForLocalRead(handle: RapidsBufferHandle): Unit = {
    handle.setSpillPriority(SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
  }

  /**
   * Lookup the shuffle buffer that corresponds to the specified buffer handle and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   * @param handle shuffle buffer handle
   * @return shuffle buffer that has been acquired
   */
  def acquireBuffer(handle: RapidsBufferHandle): RapidsBuffer = {
    val buffer = catalog.acquireBuffer(handle)
    // Shuffle buffers that have been read are less likely to be read again,
    // so update the spill priority based on this access
    handle.setSpillPriority(SpillPriorities.getShuffleOutputBufferReadPriority)
    buffer
  }

  /**
   * Remove a buffer and table given a buffer handle
   * NOTE: This function is not thread safe! The caller should only invoke if
   * the handle being removed is not being utilized by another thread.
   * @param handle buffer handle
   */
  def removeBuffer(handle: RapidsBufferHandle): Unit = {
    val id = handle.id
    tableMap.remove(id.tableId)
    handle.close()
  }
}

object ShuffleBufferCatalog {
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }
}
