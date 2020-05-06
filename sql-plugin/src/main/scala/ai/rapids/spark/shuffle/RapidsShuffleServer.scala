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

import java.util.concurrent.{ConcurrentLinkedQueue, Executor}

import ai.rapids.cudf.{MemoryBuffer, NvtxColor, NvtxRange}
import ai.rapids.spark._
import ai.rapids.spark.format.{BufferMeta, TableMeta}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockBatchId}

/**
  * Trait used for the server to get buffer metadata (for metadata requests), and
  * also to acquire a buffer (for transfer requests)
  */
trait RapidsShuffleRequestHandler {
  /**
    * This is a query into the manager to get the [[TableMeta]] corresponding to this shuffle block,
    * described by the arguments.
    * @param shuffleBlockBatchId - spark's [[ShuffleBlockBatchId]] with (shuffleId, mapId, startReduceId, endReduceId)
    * @return - a sequence of [[TableMeta]] describing batches corresponding to the [[shuffleBlockBatchId]]
    */
  def getShuffleBufferMetas(shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta]

  /**
    * Acquires (locks w.r.t. the memory tier) a [[RapidsBuffer]] corresponding to a [[tableId]].
    * @param tableId - the unique id for a table in the catalog
    * @return - a [[RapidsBuffer]] which is reference counted, and should be closed by the acquirer
    */
  def acquireShuffleBuffer(tableId: Int): RapidsBuffer
}

/**
  * A server that replies to shuffle metadata messages, and issues device/host memory sends.
  *
  * A single command thread is used to orchestrate sends/receives and to remove
  * from transport's progress thread.
  *
  * @param transport - the transport we were configured with
  * @param serverConnection - a connection object, which contains functions to send/receive
  * @param originalShuffleServerId - spark's [[BlockManagerId]] for this executor
  * @param requestHandler - instance of [[RapidsShuffleRequestHandler]]
  * @param exec - Executor used to handle tasks that take time, and should not be in the transport's thread
  * @param rapidsConf - plugin configuration instance
  */
class RapidsShuffleServer(transport: RapidsShuffleTransport,
                          serverConnection: ServerConnection,
                          val originalShuffleServerId: BlockManagerId,
                          requestHandler: RapidsShuffleRequestHandler,
                          exec: Executor,
                          copyExec: Executor,
                          bssExec: Executor,
                          rapidsConf: RapidsConf) extends AutoCloseable with Logging {
  /**
    * On close, this is set to false to indicate that the server is shutting down.
    */
  private[this] var started = true

  private object ShuffleServerOps {
    /**
      * When a transfer request is received during a callback, the handle code is offloaded via this
      * event to the server thread.
      * @param tx - the live transaction that should be closed by the handler
      * @param metaRequestBuffer - contains the metadata request that should be closed by the handler
      */
    case class HandleMeta(tx: Transaction, metaRequestBuffer: RefCountedDirectByteBuffer)

    /**
      * When transfer request is received (to begin sending buffers), the handling is offloaded via
      * this event on the server thread. Note that, [[BufferSendState]] encapsulates one more more
      * requests to send buffers, and [[HandleTransferRequest]] may be posted multiple times
      * in order to handle the request fully.
      * @param sendState - instance of [[BufferSendState]] used to complete a transfer request.
      */
    case class HandleTransferRequest(sendState: BufferSendState)
  }

  import ShuffleServerOps._

  private var port: Int = -1

  /**
    * Returns a TCP port that is expected to respond to rapids shuffle protocol.
    * Throws if this server is not started yet, which is an illegal state.
    * @return - the port
    */
  def getPort: Int = {
    if (port == -1) {
      throw new IllegalStateException("RapidsShuffleServer port is not initialized")
    }
    port
  }

  /**
    * Kick off the underlying connection, and listen for initial requests.
    */
  def start(): Unit = {
    port = serverConnection.startManagementPort(originalShuffleServerId.host)
    // kick off our first receives
    doIssueReceive(RequestType.MetadataRequest)
    doIssueReceive(RequestType.TransferRequest)
  }

  def handleOp(serverTask: Any): Unit = {
    try {
      serverTask match {
        case HandleMeta(tx, metaRequestBuffer) =>
          doHandleMeta(tx, metaRequestBuffer)
        case HandleTransferRequest(wt: BufferSendState) =>
          doHandleTransferRequest(wt)
      }
    } catch {
      case t: Throwable => {
        logError("Exception occurred while handling shuffle server task.", t)
      }
    }
  }

  /**
    * Pushes a task onto the queue to be handled by the server executor.
    *
    * All callbacks handled in the server (from the transport) need to be offloaded into
    * this pool. Note, if this thread blocks we are blocking the progress thread of the transport.
    *
    * @param op - One of the case classes in [[ShuffleServerOps]]
    */
  def asyncOrBlock(op: Any): Unit = {
    exec.execute(() => handleOp(op))
  }

  /**
    * Pushes a task onto the queue to be handled by the server's copy executor.
    *
    * @note - at this stage, tasks in this pool can block (it will grow as needed)
    *
    * @param op - One of the case classes in [[ShuffleServerOps]]
    */
  private[this] def asyncOnCopyThread(op: Any): Unit = {
    copyExec.execute(() => handleOp(op))
  }

  /**
    * Keep a list of BufferSendState that are waiting for bounce buffers.
    */
  private[this] val bssQueue = new ConcurrentLinkedQueue[BufferSendState]()

  /**
    * Executor that loops until it finds bounce buffers for [[BufferSendState]],
    * and when it does it hands them off to a thread pool for handling.
    */
  bssExec.execute(() => {
    while (started) {
      var bss: BufferSendState = null
      try {
        bss = bssQueue.peek()
        if (bss != null) {
          bssExec.synchronized {
            if (bss.acquireBounceBuffersNonBlocking) {
              bssQueue.remove(bss)
              asyncOnCopyThread(HandleTransferRequest(bss))
            } else {
              bssExec.synchronized {
                bssExec.wait(100)
              }
            }
          }
        } else {
          bssExec.synchronized {
            bssExec.wait(100)
          }
        }
      } catch {
        case t: Throwable => {
          logError("Error while handling BufferSendState", t)
          if (bss != null) {
            bss.close()
            bss = null
          }
        }
      }
    }
  })
  /**
    * Handler for a metadata request. It queues request handlers for either [[RequestType.MetadataRequest]],
    * or [[RequestType.TransferRequest]], and re-issues receives for either type of request.
    *
    * NOTE: This call must be non-blocking. It is called from the progress thread.
    *
    * @param requestType - The request type received
    */
  private def doIssueReceive(requestType: RequestType.Value): Unit = {
    logInfo(s"Waiting for a new connection. Posting ${requestType} receive.")
    val metaRequest = transport.getMetaBuffer(rapidsConf.shuffleMaxMetadataSize)

    val alt = AddressLengthTag.from(
      metaRequest.acquire(),
      serverConnection.composeRequestTag(requestType))

    serverConnection.receive(alt,
      tx => {
        val handleMetaRange = new NvtxRange("Handle Meta Request", NvtxColor.PURPLE)
        try {
          if (requestType == RequestType.MetadataRequest) {
            doIssueReceive(RequestType.MetadataRequest)
            doHandleMeta(tx, metaRequest)
          } else {
            val bss = new BufferSendState(tx, metaRequest)
            bssQueue.add(bss)

            // tell the bssExec to wake up to try to handle the new BufferSendState
            bssExec.synchronized {
              bssExec.notifyAll()
            }
            logInfo(s"Got a transfer request ${bss} from ${tx}. I now have ${bssQueue.size} BSSs")
            doIssueReceive(RequestType.TransferRequest)
          }
        } finally {
          handleMetaRange.close()
        }
      })
  }

  /**
    * Function to handle [[HandleMeta]] events. It will populate and issue a response for the
    * appropriate client.
    *
    * @param tx - the inbound [[Transaction]]
    * @param metaRequest - a ref counted buffer holding a MetadataRequest message.
    */
  def doHandleMeta(tx: Transaction, metaRequest: RefCountedDirectByteBuffer): Unit = {
    val doHandleMetaRange = new NvtxRange("doHandleMeta", NvtxColor.PURPLE)
    val start = System.currentTimeMillis()
    try {
      if (tx.getStatus == TransactionStatus.Error) {
        logError("error getting metadata request: " + tx)
        metaRequest.close() // the buffer is not going to be handed anywhere else, so lets close it
        throw new IllegalStateException(s"Error occurred while while handling metadata $tx")
      } else {
        logInfo(s"Received metadata request: $tx => $metaRequest")
        try {
          handleMetadataRequest(metaRequest)
        } catch {
          case e: Throwable => {
            logError(s"Exception while handling metadata request from $tx: ", e)
            throw e
          }
        }
      }
    } finally {
      logInfo(s"Metadata request handled in ${TransportUtils.timeDiffMs(start)} ms")
      doHandleMetaRange.close()
      tx.close()
    }
  }

  /**
    * Handles the very first message that a client will send, in order to request Table/Buffer info.
    * @param metaRequest - the metadata request buffer
    */
  def handleMetadataRequest(metaRequest: RefCountedDirectByteBuffer): Unit = {
    try {
      val req = ShuffleMetadata.getMetadataRequest(metaRequest.getBuffer())

      // target executor to respond to
      val peerExecutorId = req.executorId()

      // tag to use for the response message
      val responseTag = req.responseTag()

      logDebug(s"Received request req:\n: ${ShuffleMetadata.printRequest(req)}")
      logInfo(s"HandleMetadataRequest for peerExecutorId $peerExecutorId and " +
        s"responseTag ${TransportUtils.formatTag(req.responseTag())}")

      // NOTE: MetaUtils will have a simpler/better way of handling creating a response.
      // That said, at this time, I see some issues with that approach from the flatbuffer
      // library, so the code to create the metadata response will likely change.
      val responseTables = (0 until req.blockIdsLength()).flatMap { i =>
        val blockId = req.blockIds(i)
        // this is getting shuffle buffer ids
        requestHandler.getShuffleBufferMetas(
          ShuffleBlockBatchId(blockId.shuffleId(), blockId.mapId(),
            blockId.startReduceId(), blockId.endReduceId()))
      }

      val metadataResponse = ShuffleMetadata.buildMetaResponse(responseTables, req.maxResponseSize())
      // Wrap the buffer so we keep a reference to it, and we destroy it later on .close
      val respBuffer = new RefCountedDirectByteBuffer(metadataResponse)
      val materializedResponse = ShuffleMetadata.getMetadataResponse(metadataResponse)

      logDebug(s"Response will be at tag ${TransportUtils.formatTag(responseTag)}:\n"+
        s"${ShuffleMetadata.printResponse("responding", materializedResponse)}")

      val response = AddressLengthTag.from(respBuffer.acquire(), responseTag)

      // Issue the send against [[peerExecutorId]] as described by the metadata message
      val tx = serverConnection.send(peerExecutorId, response, tx => {
        try {
          if (tx.getStatus == TransactionStatus.Error) {
            logError(s"Error sending metadata response in tx $tx")
            throw new IllegalStateException(s"Error while handling a metadata response send for $tx")
          } else {
            val stats = tx.getStats
            logInfo(s"Sent metadata ${stats.sendSize} in ${stats.txTimeMs} ms")
          }
        } finally {
          respBuffer.close()
          tx.close()
        }
      })
      logDebug(s"Waiting for send metadata to complete: $tx")
    } finally {
      metaRequest.close()
    }
  }

  /**
    * A helper case class to maintain the state associated with a [[TransferRequest]].
    *
    * This class is *not thread safe*. The way the code is currently designed, bounce buffers being used
    * to send, or copied to, are acted on a sequential basis, in time and in space.
    *
    * Callers use this class, like so:
    *
    * 1) [[getBuffersToSend]]: is used to get bounce buffers that the server should .send on.
    *    -- first time:
    *          a) the corresponding catalog table is acquired,
    *          b) bounce buffers are acquired,
    *          c) data is copied from the original catalog table into the bounce buffers available
    *          d) the length of the last bounce buffer is adjusted if it would satisfy the full length of the
    *             catalog-backed buffer.
    *          e) bounce buffers are returned
    *
    *    -- subsequent times:
    *      if we are not done sending the acquired table:
    *          a) data is copied from the original catalog table into the bounce buffers available
    *             at sequentially incrementing offsets.
    *          b) the length of the last bounce buffer is adjusted if it would satisfy the full length of the
    *             catalog-backed buffer.
    *          c) bounce buffers are returned
    *
    * 2) [[freeBounceBuffersIfNecessary]]: called  when a send finishes, in order to free bounce buffers,
    *    if the current table is done sending.
    *
    * 3) [[close]]: used to free state as the [[BufferSendState]] object is no longer needed
    *
    * In terms of the lifecycle of this object, it begins with the client asking for transfers to start,
    * it lasts through all buffers being transmitted, and ultimately finishes when a [[TransferResponse]] is
    * sent back to the client.
    *
    * @param tx      - the original [[ai.rapids.spark.format.TransferRequest]] transaction.
    * @param request - a transfer request
    */
  class BufferSendState(tx: Transaction, request: RefCountedDirectByteBuffer) extends AutoCloseable {
    private[this] var currentTableIndex = -1
    private[this] var lastTable = false
    private[this] var currentTableRemaining: Long = 0L
    private[this] var currentTableOffset: Long = 0L
    private[this] var currentAlt: AddressLengthTag = null
    private[this] val transferRequest = ShuffleMetadata.getTransferRequest(request.getBuffer())
    private[this] var bufferMetas = Seq[BufferMeta]()
    private[this] var isClosed = false

    def getTransferRequest() = synchronized { 
      transferRequest 
    }

    case class TableIdTag(tableId: Int, tag: Long)

    // this is the complete amount of buffers we need to send
    private[this] val tableIdAndTags: Seq[TableIdTag] = (0 until transferRequest.requestsLength()).map { btr =>
      val bufferTransferRequest = transferRequest.requests(btr)
      TableIdTag(bufferTransferRequest.bufferId(),
        bufferTransferRequest.tag())
    }

    require(tableIdAndTags.nonEmpty)

    def isDone: Boolean = synchronized {
      lastTable &&
        currentTableRemaining == 0
    }

    // while we acquire a table from the catalog, this value is non-null
    private[this] var acquiredTable: RapidsBuffer = null
    private[this] var acquiredBuffer: MemoryBuffer = null

    // the set of buffers we will acquire and use to work the entirety of this transfer.
    private[this] var bounceBuffers = Seq[AddressLengthTag]()

    override def toString: String = {
      s"BufferSendState(isDone=$isDone, currentTableRemaining=$currentTableRemaining, requests=$tableIdAndTags, currentTableOffset=$currentTableIndex, " +
        s"currentTableOffset=$currentTableOffset, bounceBuffers=$bounceBuffers)"
    }

    /**
      * Used by the [[bssExec]] to pop a [[BufferSendState]] from its queue if and only if
      * there are bounce buffers available
      * @return
      */
    def acquireBounceBuffersNonBlocking: Boolean = {
      // we need to secure the table we are about to send, in order to get the correct flavor of
      // bounce buffer
      acquireTable()

      if (bounceBuffers.isEmpty) {
        bounceBuffers = transport.tryGetSendBounceBuffers(
          currentAlt.isDeviceBuffer(),
          currentTableRemaining,
          // TODO: currently we have 2 buffers here, but this value could change and should perhaps be
          //   configurable, if we find that it should vary depending on the user's job
          2).map(buff => AddressLengthTag.from(buff, currentAlt.tag))
      }
      // else, we may want to make acquisition of the table and state separate so
      // the act of acquiring the table from the catalog and getting the bounce buffer
      // doesn't affect the state in [[BufferSendState]], this is in the case where we are at
      // the limit, and we want to spill everything in a tier (including the one buffer
      // we are trying to pop from the [[BufferSendState]] queue)
      bounceBuffers.nonEmpty
    }

    private[this] def getBounceBuffers(): Seq[AddressLengthTag] = {
      bounceBuffers.foreach(b => b.resetLength())
      bounceBuffers
    }

    private[this] def acquireTable(): AddressLengthTag = {
      if (currentTableRemaining > 0) {
        require(currentAlt != null)
        currentAlt
      } else {
        currentTableIndex = currentTableIndex + 1

        require(currentTableIndex < tableIdAndTags.size,
          "Something went wrong while handling a transfer request. Asking for more tables than expected.")

        if (acquiredBuffer != null) {
          acquiredBuffer.close()
          acquiredBuffer = null
        }
        if (acquiredTable != null) {
          acquiredTable.close()
          acquiredTable = null
          currentTableOffset = 0L
        }

        val TableIdTag(tableId, tag) = tableIdAndTags(currentTableIndex)

        logDebug(s"Handling buffer transfer request [peer_executor_id=${transferRequest.executorId()}, " +
          s"table_id=$tableId, tag=${TransportUtils.formatTag(tag)}]")

        // acquire the buffer, adding a reference to it, it will not be freed under us
        acquiredTable = requestHandler.acquireShuffleBuffer(tableId)
        acquiredBuffer = acquiredTable.getMemoryBuffer

        currentAlt = AddressLengthTag.from(acquiredBuffer, tag)

        currentTableRemaining = acquiredTable.size

        val bufferMeta = acquiredTable.meta.bufferMeta()

        bufferMetas = bufferMetas :+ bufferMeta

        lastTable = currentTableIndex == tableIdAndTags.size - 1

        currentAlt
      }
    }

    private[this] def freeBounceBuffers(): Unit = {
      if (bounceBuffers.nonEmpty) {
        transport.freeSendBounceBuffers(bounceBuffers.flatMap(_.memoryBuffer))
        bounceBuffers = Seq.empty

        // wake up the bssExec since bounce buffers became available
        bssExec.synchronized {
          bssExec.notifyAll()
        }
      }
    }


    def getTransferResponse(): RefCountedDirectByteBuffer = synchronized {
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildBufferTransferResponse(bufferMetas))
    }

    private def advance(toCopy: Long): Boolean = {
      currentTableRemaining = currentTableRemaining - toCopy
      currentTableOffset = currentTableOffset + toCopy
      currentTableRemaining == 0
    }

    override def close(): Unit = synchronized {
      require(isDone)
      if (isClosed){
        throw new IllegalStateException("ALREADY CLOSED!")
      }
      isClosed = true
      freeBounceBuffers()
      tx.close()
      request.close()
      if (acquiredBuffer != null) {
        acquiredBuffer.close()
        acquiredBuffer = null
      }
      if (acquiredTable != null) {
        acquiredTable.close()
        acquiredTable = null
      }
    }

    /**
      * This function returns bounce buffers that are ready to be sent. To get there,
      * it will:
      *   1) acquire the bounce buffers in the first place (if it hasn't already)
      *   2) copy data from the source buffer to the bounce buffers, updating the offset accordingly
      *   3) return either the full set of bounce buffers, or a subset, depending on how much is left to send.
      * @return - bounce buffers ready to be sent.
      */
    def getBuffersToSend(): Seq[AddressLengthTag] = synchronized {
      val alt = acquireTable()

      logInfo(s"Handling transfer request for ${alt}")

      logInfo(s"${currentTableRemaining} remaining, getting bounce buffers for tr ${alt}")

      // get bounce buffers, note we may block
      val bounceBuffers = getBounceBuffers()

      logInfo(s"${bounceBuffers} got, for tr ${alt}")

      var buffersToSend = Seq[AddressLengthTag]()

      try {
        // copy to the bounce buffer
        var bounceBufferIx = 0
        var done = false
        while (bounceBufferIx < bounceBuffers.size && !done) {
          val bb = bounceBuffers(bounceBufferIx)
          val toCopy = Math.min(currentTableRemaining, bb.length)
          alt.cudaCopyTo(bb, currentTableOffset, toCopy)
          done = advance(toCopy)
          if (done) {
            // got to the end, that last buffer needs the correct length
            bb.resetLength(toCopy)
          }
          buffersToSend = buffersToSend :+ bb
          bounceBufferIx = bounceBufferIx + 1
        }
      } catch {
        case t: Throwable =>
          logError("Error while copying to bounce buffers on send.", t)
          close()
          throw t
      }

      logInfo(s"Sending ${buffersToSend} for transfer request, with ${currentTableRemaining} left " +
        s"[peer_executor_id=${transferRequest.executorId()}, table_id=${acquiredTable.id}, " +
        s"tag=${TransportUtils.formatTag(alt.tag)}]")

      buffersToSend
    }

    def freeBounceBuffersIfNecessary(): Unit = synchronized {
      // let go of the buffers, we'll be acquiring them again on the next table we want to transfer
      if (currentTableRemaining <= 0) {
        logInfo(s"Freeing bounce buffers ${bounceBuffers}")
        freeBounceBuffers()
      }
    }
  }

  /**
    * This will kick off, or continue to work, a [[BufferSendState]] object
    * until all tables are fully transmitted.
    *
    * @param bufferSendState - state object tracking sends needed to fulfill a TransferRequest
    */
  def doHandleTransferRequest(bufferSendState: BufferSendState): Unit = {
    val doHandleTransferRequest = new NvtxRange("doHandleTransferRequest", NvtxColor.CYAN)
    val start = System.currentTimeMillis()
    try {
      require(!bufferSendState.isDone, "Attempting to handle a complete transfer request.")

      // [[BufferSendState]] will continue to return buffers to send, as long as there
      // is work to be done.
      val buffersToSend = bufferSendState.getBuffersToSend()

      val transferRequest = bufferSendState.getTransferRequest()

      logInfo(s"Handling transfer request for ${transferRequest.executorId()} with ${buffersToSend}")

      serverConnection.send(transferRequest.executorId(), buffersToSend, new TransactionCallback {
        override def apply(bufferTx: Transaction): Unit = {
          logInfo(s"Done with the send for ${bufferSendState} with ${buffersToSend}")
          try {
            if (!bufferSendState.isDone) {
              // continue issuing sends.
              logInfo(s"Buffer send state ${bufferSendState} is NOT done. I now have ${bssQueue.size} BSSs")
              asyncOnCopyThread(HandleTransferRequest(bufferSendState))
            } else {
              val transferResponse = bufferSendState.getTransferResponse()

              // send the transfer response
              serverConnection.send(
                transferRequest.executorId(),
                AddressLengthTag.from(transferResponse.acquire(), transferRequest.responseTag()),
                transferResponseTx => {
                  transferResponse.close()
                  transferResponseTx.close()
                })

              // close up the [[BufferSendState]] instance
              logInfo(s"Buffer send state ${bufferSendState} is done. Closing. I now have ${bssQueue.size} BSSs")
              bufferSendState.close()
            }
          } finally {
            bufferTx.close()
          }
        }
      })
    } finally {
      logDebug(s"Transfer request handled in ${TransportUtils.timeDiffMs(start)} ms")
      doHandleTransferRequest.close()
    }
  }

  override def close(): Unit = {
    started = false
    bssExec.synchronized {
      bssExec.notifyAll()
    }
  }
}
