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

package com.nvidia.spark.rapids.shuffle

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, NvtxColor, NvtxRange, Rmm}
import com.nvidia.spark.rapids.Arm
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging

case class ConsumedBatchFromBounceBuffer(
    contigBuffer: DeviceMemoryBuffer,
    meta: TableMeta,
    handler: RapidsShuffleFetchHandler)

/**
 * A helper case class to maintain the state associated with a transfer request to a peer.
 *
 * The class implements the Iterator interface.
 *
 * On next(), a bounce buffer is returned earmarked for a range "window". The window
 * is composed of eventual target buffers that will be handed off to the catalog.
 *
 * By convention, the tag used is that of the first buffer contained in the payload. The
 * server follows the same convention in `BufferSendState`.
 *
 * `consumeWindow` is called when data has arrived at the bounce buffer, copying
 * onto the ranges the bytes received.
 *
 * It also is AutoCloseable. close() should be called to free bounce buffers.
 *
 * @param bounceBuffer - bounce buffer to use (device memory strictly)
 * @param requests - collection of `PendingTransferRequest` as issued by iterators
 *                 currently requesting
 * @param stream - CUDA stream to use for allocations and copies
 */
class BufferReceiveState(
    bounceBuffer: BounceBuffer,
    requests: Seq[PendingTransferRequest],
    stream: Cuda.Stream = Cuda.DEFAULT_STREAM)
    extends Iterator[AddressLengthTag]
        with AutoCloseable with Logging with Arm {

  class ReceiveBlock(val request: PendingTransferRequest) extends BlockWithSize {
    override def size: Long = request.getLength
    def tag: Long = request.tag
  }

  // flags whether we need to populate a window, and if the client should send a
  // transfer request
  private[this] var iterated = false

  // block ranges we'll work on next
  private[this] var nextBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // ranges we are working on now
  private[this] var currentBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // if a block overshoots a window, it will be in `workingOn`. This happens if the
  // block is larger than window also.
  private[this] var workingOn: DeviceMemoryBuffer = null

  // offset tracking
  private[this] var workingOnOffset: Long = 0L
  private[this] var bounceBufferByteOffset = 0L

  // get block ranges for us to work with
  private[this] val windowedBlockIterator = new WindowedBlockIterator[ReceiveBlock](
    requests.map(r => new ReceiveBlock(r)), bounceBuffer.buffer.getLength)

  private[this] var hasMoreBuffers = windowedBlockIterator.hasNext

  def getRequests: Seq[PendingTransferRequest] = requests

  def hasIterated: Boolean = synchronized { iterated }

  override def close(): Unit = synchronized {
    if (bounceBuffer != null) {
      bounceBuffer.close()
    }
    if (workingOn != null) {
      logWarning(s"BufferReceiveState closing, but there are unfinished batches")
      workingOn.close()
    }
  }

  /**
   * Calls `transferError` on each `RapidsShuffleFetchHandler`
   * @param errMsg - the message to pass onto the handlers
   */
  def errorOcurred(errMsg: String, throwable: Throwable = null): Unit = {
    currentBlocks.foreach(_.block.request.handler.transferError(errMsg, throwable))
  }

  override def hasNext: Boolean = synchronized { hasMoreBuffers }

  override def next(): AddressLengthTag = synchronized {
    if (!hasMoreBuffers) {
      throw new NoSuchElementException(
        "BufferReceiveState is done, yet received a call to next")
    }

    if (!iterated) {
      nextBlocks = windowedBlockIterator.next()
      iterated = true
    }
    currentBlocks = nextBlocks

    val firstTag = getFirstTag(currentBlocks)

    val alt = AddressLengthTag.from(bounceBuffer.buffer, firstTag)
    alt.resetLength(currentBlocks.map(_.rangeSize()).sum)

    if (windowedBlockIterator.hasNext) {
      nextBlocks = windowedBlockIterator.next()
    } else {
      nextBlocks = Seq.empty
      hasMoreBuffers = false
    }
    alt
  }

  private def getFirstTag(blockRanges: Seq[BlockRange[ReceiveBlock]]): Long =
    blockRanges.head.block.tag

  /**
   * When a receive is complete, the client calls `consumeWindow` to copy out
   * of the bounce buffer in this `BufferReceiveState` any complete batches, or to
   * buffer up a remaining batch in `workingOn`
   *
   * @return - a sequence of batches that were successfully consumed, or an empty
   *         sequence if still working on a batch.
   */
  def consumeWindow(): Seq[ConsumedBatchFromBounceBuffer] = synchronized {
    val windowRange = new NvtxRange("consumeWindow", NvtxColor.PURPLE)
    val toClose = new ArrayBuffer[DeviceMemoryBuffer]()
    try {
      val results = currentBlocks.flatMap { b =>
        val pendingTransferRequest = b.block.request

        val fullSize = pendingTransferRequest.tableMeta.bufferMeta().size()

        var contigBuffer: DeviceMemoryBuffer = null

        // Receive buffers are always in the device, and so it is safe to assume
        // that they are `DeviceMemoryBuffer`s here.
        val deviceBounceBuffer = bounceBuffer.buffer.asInstanceOf[DeviceMemoryBuffer]

        if (fullSize == b.rangeSize()) {
          // we have the full buffer!
          contigBuffer = Rmm.alloc(b.rangeSize(), stream)
          toClose.append(contigBuffer)

          contigBuffer.copyFromDeviceBufferAsync(0, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)
        } else {
          if (workingOn != null) {
            workingOn.copyFromDeviceBufferAsync(workingOnOffset, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)

            workingOnOffset += b.rangeSize()
            if (workingOnOffset == fullSize) {
              contigBuffer = workingOn
              workingOn = null
              workingOnOffset = 0
            }
          } else {
            // need to keep it around
            workingOn = Rmm.alloc(fullSize, stream)
            toClose.append(workingOn)

            workingOn.copyFromDeviceBufferAsync(0, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)

            workingOnOffset += b.rangeSize()
          }
        }
        bounceBufferByteOffset += b.rangeSize()
        if (bounceBufferByteOffset >= deviceBounceBuffer.getLength) {
          bounceBufferByteOffset = 0
        }

        if (contigBuffer != null) {
          Some(ConsumedBatchFromBounceBuffer(
            contigBuffer, pendingTransferRequest.tableMeta, pendingTransferRequest.handler))
        } else {
          None
        }
      }

      // Sync once, instead of for each copy.
      // We need to synchronize, because we can't ask ucx to overwrite our bounce buffer
      // unless all that data has truly moved to our final buffer in our stream
      stream.sync()

      results
    } catch {
      case t: Throwable =>
        toClose.safeClose(t)
        throw t
    } finally {
      windowRange.close()
    }
  }
}
