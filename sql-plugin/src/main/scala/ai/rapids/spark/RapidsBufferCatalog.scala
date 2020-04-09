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

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import scala.collection.mutable.ArrayBuffer

import ai.rapids.spark.format.TableMeta
import ai.rapids.spark.StorageTier.StorageTier

import org.apache.spark.internal.Logging

/** Catalog for lookup of buffers by ID */
class RapidsBufferCatalog extends Logging {
  /** Tracks all buffer stores using this catalog */
  private[this] val stores = new ArrayBuffer[RapidsBufferStore]

  /** Map of buffer IDs to buffers */
  private[this] val bufferMap = new ConcurrentHashMap[RapidsBufferId, RapidsBuffer]

  /**
   * Register a buffer store that is using this catalog.
   * NOTE: It is assumed all stores are registered before any buffers are added to the catalog.
   * @param store buffer store
   */
  def registerStore(store: RapidsBufferStore): Unit = {
    require(store.currentSize == 0, "Store must not have any buffers when registered")
    stores.append(store)
  }

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
   * NOTE: The mapping will not be updated if the current mapping is to a higher priority storage tier.
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
}

object RapidsBufferCatalog {
  private val MAX_BUFFER_LOOKUP_ATTEMPTS = 100
}
