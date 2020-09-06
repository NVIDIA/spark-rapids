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

package com.nvidia.spark.rapids.shuffle

import ai.rapids.cudf.{Cuda, CudaUtil, DeviceMemoryBuffer, NvtxColor, NvtxRange}
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
        with AutoCloseable with Logging {

  class ReceiveBlock(val request: PendingTransferRequest) extends BlockWithSize {
    override def size: Long = request.getLength
    def tag: Long = request.tag
  }

  // flags whether we need to populate a window, and if the client should send a
  // transfer request
  private[this] var firstTime = true

  private[this] var markedAsDone = false

  // block ranges we'll work on next
  private[this] var nextBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // ranges we are working on now
  private[this] var currentBlocks: Seq[BlockRange[ReceiveBlock]] = Seq.empty

  // if a block overshoots a window, it will be in `workingOn`. This happens if the
  // block is larger than window also.
  private[this] var workingOn: DeviceMemoryBuffer = null

  // offset tracking
  private[this] var workingOnSoFar: Long = 0L
  private[this] var bounceBufferByteOffset = 0L

  // get block ranges for us to work with
  private[this] val windowedBlockIterator = new WindowedBlockIterator[ReceiveBlock](
    requests.map(r => new ReceiveBlock(r)), bounceBuffer.buffer.getLength)

  def getRequests: Seq[PendingTransferRequest] = requests

  def isFirstTime: Boolean = synchronized { firstTime }

  override def close(): Unit = synchronized {
    if (bounceBuffer != null) {
      bounceBuffer.close()
    }
    if (workingOn != null) {
      throw new IllegalStateException(
        s"BufferReceiveState closing, but there are unfinished batches")
    }
  }

  /**
   * Calls `transferError` on each `RapidsShuffleFetchHandler`
   * @param errMsg - the message to pass onto the handlers
   */
  def errorOcurred(errMsg: String): Unit = {
    // get handlers for blocks that are currently being worked
    val currentHandlers = currentBlocks.map(_.block.request.handler)
   // signal error to all handlers
    currentHandlers.foreach(_.transferError(errMsg))
  }

  override def hasNext: Boolean = synchronized { !markedAsDone }

  override def next(): AddressLengthTag = synchronized {
    if (firstTime) {
      nextBlocks = windowedBlockIterator.next()
      firstTime = false
    }
    currentBlocks = nextBlocks

    val firstTag = getFirstTag(currentBlocks)

    val alt = AddressLengthTag.from(bounceBuffer.buffer, firstTag)
    alt.resetLength(currentBlocks.map(_.rangeSize()).sum)

    if (windowedBlockIterator.hasNext) {
      nextBlocks = windowedBlockIterator.next()
    } else {
      nextBlocks = Seq.empty
      markedAsDone = true
    }
    alt
  }

  private def getFirstTag(blockRanges: Seq[BlockRange[ReceiveBlock]]): Long =
    blockRanges.head.block.tag

  def consumeWindow(): Seq[ConsumedBatchFromBounceBuffer] = synchronized {
    val windowRange = new NvtxRange("consumeWindow", NvtxColor.PURPLE)
    try {
      val results = currentBlocks.flatMap { case b =>
        val pendingTransferRequest = b.block.request

        val fullSize = pendingTransferRequest.tableMeta.bufferMeta().size()

        var contigBuffer: DeviceMemoryBuffer = null
        val deviceBounceBuffer = bounceBuffer.buffer.asInstanceOf[DeviceMemoryBuffer]

        if (fullSize == b.rangeSize()) {
          // we have the full buffer!
          contigBuffer = CudaUtil.deviceAllocateOnStream(b.rangeSize(), stream)
          contigBuffer.copyFromDeviceBufferAsync(0, deviceBounceBuffer,
            bounceBufferByteOffset, b.rangeSize(), stream)
        } else {
          if (workingOn != null) {
            workingOn.copyFromDeviceBufferAsync(workingOnSoFar, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)

            workingOnSoFar += b.rangeSize()
            if (workingOnSoFar == fullSize) {
              contigBuffer = workingOn
              workingOn = null
              workingOnSoFar = 0
            }
          } else {
            // need to keep it around
            workingOn = CudaUtil.deviceAllocateOnStream(fullSize, stream)

            workingOn.copyFromDeviceBufferAsync( 0, deviceBounceBuffer,
              bounceBufferByteOffset, b.rangeSize(), stream)

            workingOnSoFar += b.rangeSize()
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
    } finally {
      windowRange.close()
    }
  }
}
