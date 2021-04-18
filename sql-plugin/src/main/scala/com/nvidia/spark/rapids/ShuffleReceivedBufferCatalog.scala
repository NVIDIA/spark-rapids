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

import java.io.File
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Consumer, IntUnaryOperator}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer}
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.{BlockId, ShuffleBlockId}

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
  /** Mapping of table ID to shuffle buffer ID */
  private[this] val tableMap = new ConcurrentHashMap[Int, ShuffleReceivedBufferId]

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  /** Allocate a new shuffle buffer identifier and update the shuffle block mapping. */
  def nextShuffleReceivedBufferId(): ShuffleReceivedBufferId = {
    val tableId = tableIdCounter.getAndUpdate(ShuffleReceivedBufferCatalog.TABLE_ID_UPDATER)
    val id = ShuffleReceivedBufferId(tableId)
    val prev = tableMap.put(tableId, id)
    if (prev != null) {
      throw new IllegalStateException(s"table ID $tableId is already in use")
    }
    id
  }

  /** Lookup the shuffle buffer identifier that corresponds to the specified table identifier. */
  def getShuffleBufferId(tableId: Int): ShuffleReceivedBufferId = {
    val shuffleBufferId = tableMap.get(tableId)
    if (shuffleBufferId == null) {
      throw new NoSuchElementException(s"unknown table ID $tableId")
    }
    shuffleBufferId
  }

  /**
   * Register a new buffer with the catalog. An exception will be thrown if an
   * existing buffer was registered with the same buffer ID.
   */
  def registerNewBuffer(buffer: RapidsBuffer): Unit = catalog.registerNewBuffer(buffer)

  /**
   * Lookup the shuffle buffer that corresponds to the specified shuffle buffer ID and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   * @param id shuffle buffer identifier
   * @return shuffle buffer that has been acquired
   */
  def acquireBuffer(id: ShuffleReceivedBufferId): RapidsBuffer = catalog.acquireBuffer(id)

  /**
   * Lookup the shuffle buffer that corresponds to the specified table ID and acquire it.
   * NOTE: It is the responsibility of the caller to close the buffer.
   * @param tableId table identifier
   * @return shuffle buffer that has been acquired
   */
  def acquireBuffer(tableId: Int): RapidsBuffer = {
    val shuffleBufferId = getShuffleBufferId(tableId)
    acquireBuffer(shuffleBufferId)
  }

  /**
   * Remove a buffer and table given a buffer ID
   * NOTE: This function is not thread safe! The caller should only invoke if
   * the [[ShuffleReceivedBufferId]] being removed is not being utilized by another thread.
   * @param id buffer identifier
   */
  def removeBuffer(id: ShuffleReceivedBufferId): Unit = {
    tableMap.remove(id.tableId)
    catalog.removeBuffer(id)
  }
}

object ShuffleReceivedBufferCatalog{
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }
}
