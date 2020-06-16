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

package ai.rapids.spark.shuffle

import java.util

import ai.rapids.cudf.MemoryBuffer

import org.apache.spark.internal.Logging

/**
  * This classes manages a set of bounce buffers, that are instances of `MemoryBuffer`.
  * The size/quantity of buffers is configurable, and so is the allocator.
  * @param poolName a human-friendly name to use for debug logs
  * @param bufferSize the size of buffer to use
  * @param numBuffers the number of buffers to allocate on instantiation
  * @param allocator function that takes a size, and returns a `MemoryBuffer` instance.
  * @tparam T the specific type of MemoryBuffer i.e. `DeviceMemoryBuffer`,
  *           `HostMemoryBuffer`, etc.
  */
class BounceBufferManager[T <: MemoryBuffer](
    poolName: String,
    val bufferSize: Long,
    val numBuffers: Int,
    allocator: Long => T)
  extends AutoCloseable
  with Logging {

  private[this] val freeBufferMap = new util.BitSet(numBuffers)

  private[this] val rootBuffer = allocator(bufferSize * numBuffers)

  freeBufferMap.set(0, numBuffers)

  /**
    * Acquires a [[MemoryBuffer]] from the pool. Blocks if the pool is empty.
    *
    * @note calls to this function should have a lock on this [[BounceBufferManager]]
    * @return the acquired `MemoryBuffer`
    */
  private def acquireBuffer(): MemoryBuffer = {
    val start = System.currentTimeMillis()
    var bufferIndex = freeBufferMap.nextSetBit(0)
    while (bufferIndex < 0) {
      logDebug(s"Buffer pool $poolName exhausted. Waiting...")
      wait()
      bufferIndex = freeBufferMap.nextSetBit(0)
    }

    logDebug(s"$poolName: Buffer index: ${bufferIndex}")
    freeBufferMap.clear(bufferIndex)
    val res = rootBuffer.slice(bufferIndex * bufferSize, bufferSize)
    logDebug(s"It took ${System.currentTimeMillis() - start} ms to allocBuffer in $poolName")
    res
  }

  private def numFree(): Int = synchronized {
    freeBufferMap.cardinality()
  }

  /**
    * Acquire `possibleNumBuffers` buffers from the pool. This method will not block.
    * @param possibleNumBuffers number of buffers to acquire
    * @return a sequence of `MemoryBuffer`s, or empty if the request can't be satisfied
    */
  def acquireBuffersNonBlocking(possibleNumBuffers: Int): Seq[MemoryBuffer] = synchronized {
    if (numFree < possibleNumBuffers) {
      // would block
      logTrace(s"$poolName at capacity. numFree: ${numFree}, " +
        s"buffers required ${possibleNumBuffers}")
      return Seq.empty
    }
    // we won't block, and we are still holding the lock, so get the promised buffers
    acquireBuffersBlocking(possibleNumBuffers)
  }

  /**
    * Acquire `possibleNumBuffers` buffers from the pool. This method will block until
    * it can get the buffers requested.
    * @param possibleNumBuffers number of buffers to acquire
    * @return a sequence of `MemoryBuffer`s
    */
  def acquireBuffersBlocking(possibleNumBuffers: Int): Seq[MemoryBuffer] = synchronized {
    val res = (0 until possibleNumBuffers).map(_ => acquireBuffer())
    logDebug(s"$poolName at acquire. Has numFree ${numFree}")
    res
  }

  /**
    * Free a `MemoryBuffer`, putting it back into the pool.
    * @param buffer the memory buffer to free
    */
  def freeBuffer(buffer: MemoryBuffer): Unit = synchronized {
    require(buffer.getAddress >= rootBuffer.getAddress
      && (buffer.getAddress - rootBuffer.getAddress) % bufferSize == 0,
      s"$poolName: foreign buffer being freed")
    val bufferIndex = (buffer.getAddress - rootBuffer.getAddress) / bufferSize
    require(bufferIndex < numBuffers,
      s"$poolName: buffer index invalid $bufferIndex should be less than $numBuffers")

    logDebug(s"$poolName: Free buffer index ${bufferIndex}")
    buffer.close()
    freeBufferMap.set(bufferIndex.toInt)
    notifyAll()
  }

  /**
    * Returns the root (backing) `MemoryBuffer`. This is used for a transport
    * that wants to register the bounce buffers against hardware, for pinning purposes.
    * @return the root (backing) memory buffer
    */
  def getRootBuffer(): MemoryBuffer = rootBuffer

  override def close(): Unit = rootBuffer.close()
}
