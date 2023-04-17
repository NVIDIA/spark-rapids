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
import java.util.function.IntUnaryOperator

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsDiskBlockManager

/** Identifier for a shuffle buffer that holds the data for a table on the read side */

case class ShuffleReceivedBufferId(
    override val tableId: Int) extends RapidsBufferId {
  override val canShareDiskPaths: Boolean = false

  override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File = {
    diskBlockManager.getFile(s"temp_shuffle_${tableId}")
  }
}

/** Catalog for lookup of shuffle buffers by block ID */
class ShuffleReceivedBufferCatalog(
    catalog: RapidsBufferCatalog) extends Logging {

  private val deviceStore = RapidsBufferCatalog.getDeviceStorage

  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleReceivedBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  /** Allocate a new shuffle buffer identifier and update the shuffle block mapping. */
  private def nextShuffleReceivedBufferId(): ShuffleReceivedBufferId = {
    val tableId = tableIdCounter.getAndUpdate(ShuffleReceivedBufferCatalog.TABLE_ID_UPDATER)
    val id = ShuffleReceivedBufferId(tableId)
    val prev = tableMap.put(tableId, id)
    if (prev != null) {
      throw new IllegalStateException(s"table ID $tableId is already in use")
    }
    id
  }

  /**
   * Adds a buffer to the device storage, taking ownership of the buffer.
   *
   * This method associates a new `bufferId` which is tracked internally in this catalog.
   *
   * @param buffer buffer that will be owned by the store
   * @param tableMeta metadata describing the buffer layout
   * @param initialSpillPriority starting spill priority value for the buffer
   * @param needsSync tells the store a synchronize in the current stream is required
   *                  before storing this buffer
   * @return RapidsBufferHandle associated with this buffer
   */
  def addBuffer(
      buffer: DeviceMemoryBuffer,
      tableMeta: TableMeta,
      initialSpillPriority: Long,
      needsSync: Boolean): RapidsBufferHandle = {
    val bufferId = nextShuffleReceivedBufferId()
    tableMeta.bufferMeta.mutateId(bufferId.tableId)
    // when we call `addBuffer` the store will incRefCount
    withResource(buffer) { _ =>
      catalog.addBuffer(
        bufferId,
        buffer,
        tableMeta,
        initialSpillPriority,
        needsSync)
    }
  }

  /**
   * Adds a degenerate buffer (zero rows or columns)
   *
   * @param meta metadata describing the buffer layout
   * @return RapidsBufferHandle associated with this buffer
   */
  def addDegenerateRapidsBuffer(
      meta: TableMeta): RapidsBufferHandle = {
    val bufferId = nextShuffleReceivedBufferId()
    catalog.registerDegenerateBuffer(bufferId, meta)
  }

  /**
   * Lookup the shuffle buffer that corresponds to the specified shuffle buffer
   * handle and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   *
   * @param handle shuffle buffer handle
   * @return shuffle buffer that has been acquired
   */
  def acquireBuffer(handle: RapidsBufferHandle): RapidsBuffer = catalog.acquireBuffer(handle)

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

object ShuffleReceivedBufferCatalog{
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }
}
