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

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import ai.rapids.cudf.{ContiguousTable, DeviceMemoryBuffer, Rmm, Table}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsDiskBlockManager

/**
 * Catalog for lookup of buffers by ID. The constructor is only visible for testing, generally
 * `RapidsBufferCatalog.singleton` should be used instead.
 */
class RapidsBufferCatalog extends Logging {
  /** Map of buffer IDs to buffers */
  private[this] val bufferMap = new ConcurrentHashMap[RapidsBufferId, RapidsBuffer]

  /**
   * Lookup the buffer that corresponds to the specified buffer ID and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   * @param id buffer identifier
   * @return buffer that has been acquired
   */
  def acquireBuffer(id: RapidsBufferId): RapidsBuffer = {
    (0 until RapidsBufferCatalog.MAX_BUFFER_LOOKUP_ATTEMPTS).foreach { _ =>
      val buffer = bufferMap.get(id)
      if (buffer == null) {
        throw new NoSuchElementException(s"Cannot locate buffer associated with ID: $id")
      }
      if (buffer.addReference()) {
        return buffer
      }
    }
    throw new IllegalStateException(s"Unable to acquire buffer for ID: $id")
  }

  /** Get the table metadata corresponding to a buffer ID. */
  def getBufferMeta(id: RapidsBufferId): TableMeta = {
    val buffer = bufferMap.get(id)
    if (buffer == null) {
      throw new NoSuchElementException(s"Cannot locate buffer associated with ID: $id")
    }
    buffer.meta
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same buffer ID.
   */
  def registerNewBuffer(buffer: RapidsBuffer): Unit = {
    val old = bufferMap.putIfAbsent(buffer.id, buffer)
    if (old != null) {
      throw new IllegalStateException(s"Buffer ID ${buffer.id} already registered $old")
    }
  }

  /**
   * Replace the mapping at the specified tier with a specified buffer.
   * NOTE: The mapping will not be updated if the current mapping is to a higher priority
   * storage tier.
   * @param tier the storage tier of the buffer being replaced
   * @param buffer the new buffer to associate
   */
  def updateBufferMap(tier: StorageTier, buffer: RapidsBuffer): Unit = {
    val updater = new BiFunction[RapidsBufferId, RapidsBuffer, RapidsBuffer] {
      override def apply(key: RapidsBufferId, value: RapidsBuffer): RapidsBuffer = {
        if (value == null || value.storageTier >= tier) {
          buffer
        } else {
          value
        }
      }
    }
    bufferMap.compute(buffer.id, updater)
  }

  /** Remove a buffer ID from the catalog and release the resources of the registered buffer. */
  def removeBuffer(id: RapidsBufferId): Unit = {
    val buffer = bufferMap.remove(id)
    if (buffer != null) {
      buffer.free()
    }
  }

  /** Return the number of buffers currently in the catalog. */
  def numBuffers: Int = bufferMap.size()
}

object RapidsBufferCatalog extends Logging with Arm {
  private val MAX_BUFFER_LOOKUP_ATTEMPTS = 100

  val singleton = new RapidsBufferCatalog
  private var deviceStorage: RapidsDeviceMemoryStore = _
  private var hostStorage: RapidsHostMemoryStore = _
  private var diskStorage: RapidsDiskStore = _
  private var gdsStorage: RapidsGdsStore = _
  private var memoryEventHandler: DeviceMemoryEventHandler = _

  private lazy val conf: SparkConf = {
    val env = SparkEnv.get
    if (env != null) {
      env.conf
    } else {
      // For some unit tests
      new SparkConf()
    }
  }

  def init(rapidsConf: RapidsConf): Unit = {
    // We are going to re-initialize so make sure all of the old things were closed...
    closeImpl()
    assert(memoryEventHandler == null)
    deviceStorage = new RapidsDeviceMemoryStore()
    val diskBlockManager = new RapidsDiskBlockManager(conf)
    if (rapidsConf.isGdsSpillEnabled) {
      gdsStorage = new RapidsGdsStore(diskBlockManager)
      deviceStorage.setSpillStore(gdsStorage)
    } else {
      hostStorage = new RapidsHostMemoryStore(rapidsConf.hostSpillStorageSize)
      diskStorage = new RapidsDiskStore(diskBlockManager)
      deviceStorage.setSpillStore(hostStorage)
      hostStorage.setSpillStore(diskStorage)
    }

    logInfo("Installing GPU memory handler for spill")
    memoryEventHandler = new DeviceMemoryEventHandler(deviceStorage, rapidsConf.gpuOomDumpDir)
    Rmm.setEventHandler(memoryEventHandler)
  }

  def close(): Unit = {
    logInfo("Closing storage")
    closeImpl()
  }

  private def closeImpl(): Unit = {
    if (memoryEventHandler != null) {
      // Workaround for shutdown ordering problems where device buffers allocated with this handler
      // are being freed after the handler is destroyed
      //Rmm.clearEventHandler()
      memoryEventHandler = null
    }

    if (deviceStorage != null) {
      deviceStorage.close()
      deviceStorage = null
    }
    if (hostStorage != null) {
      hostStorage.close()
      hostStorage = null
    }
    if (diskStorage != null) {
      diskStorage.close()
      diskStorage = null
    }
    if (gdsStorage != null) {
      gdsStorage.close()
      gdsStorage = null
    }
  }

  def getDeviceStorage: RapidsDeviceMemoryStore = deviceStorage

  /**
   * Adds a contiguous table to the device storage, taking ownership of the table.
   * @param id buffer ID to associate with this buffer
   * @param table cudf table based from the contiguous buffer
   * @param contigBuffer device memory buffer backing the table
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   */
  def addTable(
      id: RapidsBufferId,
      table: Table,
      contigBuffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long): Unit =
    deviceStorage.addTable(id, table, contigBuffer, tableMeta, initialSpillPriority)

  /**
   * Adds a contiguous table to the device storage, taking ownership of the table.
   * @param id buffer ID to associate with this buffer
   * @param contigTable contiguos table to track in device storage
   * @param initialSpillPriority starting spill priority value for the buffer
   */
  def addContiguousTable(
      id: RapidsBufferId,
      contigTable: ContiguousTable,
      initialSpillPriority: Long): Unit =
    deviceStorage.addContiguousTable(id, contigTable, initialSpillPriority)

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   * @param id buffer ID to associate with this buffer
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   */
  def addBuffer(
      id: RapidsBufferId,
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long): Unit =
    deviceStorage.addBuffer(id, buffer, tableMeta, initialSpillPriority)

  /**
   * Lookup the buffer that corresponds to the specified buffer ID and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   * @param id buffer identifier
   * @return buffer that has been acquired
   */
  def acquireBuffer(id: RapidsBufferId): RapidsBuffer = singleton.acquireBuffer(id)

  /** Remove a buffer ID from the catalog and release the resources of the registered buffer. */
  def removeBuffer(id: RapidsBufferId): Unit = singleton.removeBuffer(id)
}