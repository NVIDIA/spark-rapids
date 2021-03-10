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

import java.util.concurrent.{ConcurrentLinkedQueue, Executor}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{Cuda, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{Arm, RapidsBuffer, RapidsConf, ShuffleMetadata}
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockBatchId}


/**
 * Trait used for the server to get buffer metadata (for metadata requests), and
 * also to acquire a buffer (for transfer requests)
 */
trait RapidsShuffleRequestHandler {
  /**
   * This is a query into the manager to get the `TableMeta` corresponding to a
   * shuffle block.
   * @param shuffleBlockBatchId `ShuffleBlockBatchId` with (shuffleId, mapId,
   *                            startReduceId, endReduceId)
   * @return a sequence of `TableMeta` describing batches corresponding to a block.
   */
  def getShuffleBufferMetas(shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta]

  /**
   * Acquires (locks w.r.t. the memory tier) a [[RapidsBuffer]] corresponding to a table id.
   * @param tableId the unique id for a table in the catalog
   * @return a [[RapidsBuffer]] which is reference counted, and should be closed by the acquirer
   */
  def acquireShuffleBuffer(tableId: Int): RapidsBuffer
}

/**
 * A server that replies to shuffle metadata messages, and issues device/host memory sends.
 *
 * A single command thread is used to orchestrate sends/receives and to remove
 * from transport's progress thread.
 *
 * @param transport the transport we were configured with
 * @param serverConnection a connection object, which contains functions to send/receive
 * @param originalShuffleServerId spark's `BlockManagerId` for this executor
 * @param requestHandler instance of [[RapidsShuffleRequestHandler]]
 * @param exec Executor used to handle tasks that take time, and should not be in the
 *             transport's thread
 * @param copyExec Executor used to handle synchronous mem copies
 * @param bssExec Executor used to handle [[BufferSendState]]s that are waiting
 *                for bounce buffers to become available
 * @param rapidsConf plugin configuration instance
 */
class RapidsShuffleServer(transport: RapidsShuffleTransport,
                          serverConnection: ServerConnection,
                          val originalShuffleServerId: BlockManagerId,
                          requestHandler: RapidsShuffleRequestHandler,
                          exec: Executor,
                          copyExec: Executor,
                          bssExec: Executor,
                          rapidsConf: RapidsConf) extends AutoCloseable with Logging with Arm {

  def getId: BlockManagerId = {
    // upon seeing this port, the other side will try to connect to the port
    // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
    TrampolineUtil.newBlockManagerId(
      originalShuffleServerId.executorId,
      originalShuffleServerId.host,
      originalShuffleServerId.port,
      Some(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}=${getPort}"))
  }

  /**
   * On close, this is set to false to indicate that the server is shutting down.
   */
  private[this] var started = true

  private object ShuffleServerOps {
    /**
     * When a transfer request is received during a callback, the handle code is offloaded via this
     * event to the server thread.
     * @param tx the live transaction that should be closed by the handler
     * @param metaRequestBuffer contains the metadata request that should be closed by the
     *                          handler
     */
    case class HandleMeta(tx: Transaction, metaRequestBuffer: RefCountedDirectByteBuffer)

    /**
     * When transfer request is received (to begin sending buffers), the handling is offloaded via
     * this event on the server thread. Note that, [[BufferSendState]] encapsulates one more more
     * requests to send buffers, and [[HandleTransferRequest]] may be posted multiple times
     * in order to handle the request fully.
     * @param sendState instance of [[BufferSendState]] used to complete a transfer request.
     */
    case class HandleTransferRequest(sendState: Seq[BufferSendState])
  }

  import ShuffleServerOps._

  private var port: Int = -1

  /**
   * Returns a TCP port that is expected to respond to rapids shuffle protocol.
   * Throws if this server is not started yet, which is an illegal state.
   * @return the port
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
        case HandleTransferRequest(wt: Seq[BufferSendState]) =>
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
   * @param op One of the case classes in `ShuffleServerOps`
   */
  def asyncOrBlock(op: Any): Unit = {
    exec.execute(() => handleOp(op))
  }

  /**
   * Pushes a task onto the queue to be handled by the server's copy executor.
   *
   * @note - at this stage, tasks in this pool can block (it will grow as needed)
   *
   * @param op One of the case classes in [[ShuffleServerOps]]
   */
  private[this] def asyncOnCopyThread(op: Any): Unit = {
    copyExec.execute(() => handleOp(op))
  }

  /**
   * Keep a list of BufferSendState that are waiting for bounce buffers.
   */
  private[this] val pendingTransfersQueue = new ConcurrentLinkedQueue[PendingTransferResponse]()
  private[this] val bssContinueQueue = new ConcurrentLinkedQueue[BufferSendState]()

  /**
   * Executor that loops until it finds bounce buffers for [[BufferSendState]],
   * and when it does it hands them off to a thread pool for handling.
   */
  bssExec.execute(() => {
    while (started) {
      closeOnExcept(new ArrayBuffer[BufferSendState]()) { bssToIssue =>
        var bssContinue = bssContinueQueue.poll()
        while (bssContinue != null) {
          bssToIssue.append(bssContinue)
          bssContinue = bssContinueQueue.poll()
        }

        var continue = true
        while (!pendingTransfersQueue.isEmpty && continue) {
          // TODO: throttle on too big a send total so we don't acquire the world (in flight limit)
          val sendBounceBuffers =
            transport.tryGetSendBounceBuffers(1, 1)
          if (sendBounceBuffers.nonEmpty) {
            val pendingTransfer = pendingTransfersQueue.poll()
            bssToIssue.append(new BufferSendState(
              pendingTransfer.metaRequest,
              sendBounceBuffers.head, // there's only one bounce buffer here for now
              pendingTransfer.requestHandler,
              serverStream))
          } else {
            // TODO: make this a metric => "blocked while waiting on bounce buffers"
            logTrace(s"Can't acquire send bounce buffers")
            continue = false
          }
        }
        if (bssToIssue.nonEmpty) {
          asyncOnCopyThread(HandleTransferRequest(bssToIssue))
        }
      }

      bssExec.synchronized {
        if (bssContinueQueue.isEmpty && pendingTransfersQueue.isEmpty) {
          bssExec.wait(100)
        }
      }
    }
  })

  // NOTE: this stream will likely move to its own non-blocking stream in the future
  val serverStream = Cuda.DEFAULT_STREAM

  /**
   * Handler for a metadata request. It queues request handlers for either
   * [[RequestType.MetadataRequest]] or [[RequestType.TransferRequest]], and re-issues
   * receives for either type of request.
   *
   * NOTE: This call must be non-blocking. It is called from the progress thread.
   *
   * @param requestType The request type received
   */
  private def doIssueReceive(requestType: RequestType.Value): Unit = {
    logDebug(s"Waiting for a new connection. Posting ${requestType} receive.")
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
            val pendingTransfer = PendingTransferResponse(metaRequest, requestHandler)
            // tell the bssExec to wake up to try to handle the new BufferSendState
            bssExec.synchronized {
              pendingTransfersQueue.add(pendingTransfer)
              bssExec.notifyAll()
            }
            logDebug(s"Got a transfer request ${pendingTransfer} from ${tx}. " +
              s"Pending requests [new=${pendingTransfersQueue.size}, " +
                s"continuing=${bssContinueQueue.size}]")
            doIssueReceive(RequestType.TransferRequest)
          }
        } finally {
          handleMetaRange.close()
          tx.close()
        }
      })
  }

  case class PendingTransferResponse(
      metaRequest: RefCountedDirectByteBuffer,
      requestHandler: RapidsShuffleRequestHandler)

  /**
   * Function to handle `MetadataRequest`s. It will populate and issue a
   * `MetadataResponse` response for the appropriate client.
   *
   * @param tx the inbound [[Transaction]]
   * @param metaRequest a [[RefCountedDirectByteBuffer]] holding a `MetadataRequest` message.
   */
  def doHandleMeta(tx: Transaction, metaRequest: RefCountedDirectByteBuffer): Unit = {
    val doHandleMetaRange = new NvtxRange("doHandleMeta", NvtxColor.PURPLE)
    val start = System.currentTimeMillis()
    try {
      if (tx.getStatus == TransactionStatus.Error) {
        logError("error getting metadata request: " + tx)
        metaRequest.close() // the buffer is not going to be handed anywhere else, so lets close it
      } else {
        logDebug(s"Received metadata request: $tx => $metaRequest")
        handleMetadataRequest(metaRequest)
      }
    } finally {
      logDebug(s"Metadata request handled in ${TransportUtils.timeDiffMs(start)} ms")
      doHandleMetaRange.close()
    }
  }

  /**
   * Handles the very first message that a client will send, in order to request Table/Buffer info.
   * @param metaRequest a [[RefCountedDirectByteBuffer]] holding a `MetadataRequest` message.
   */
  def handleMetadataRequest(metaRequest: RefCountedDirectByteBuffer): Unit = {
    try {
      val req = ShuffleMetadata.getMetadataRequest(metaRequest.getBuffer())

      // target executor to respond to
      val peerExecutorId = req.executorId()

      // tag to use for the response message
      val responseTag = req.responseTag()

      logDebug(s"Received request req:\n: ${ShuffleMetadata.printRequest(req)}")
      logDebug(s"HandleMetadataRequest for peerExecutorId $peerExecutorId and " +
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

      val metadataResponse = 
        ShuffleMetadata.buildMetaResponse(responseTables, req.maxResponseSize())
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
          } else {
            val stats = tx.getStats
            logDebug(s"Sent metadata ${stats.sendSize} in ${stats.txTimeMs} ms")
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
   * This will kick off, or continue to work, a [[BufferSendState]] object
   * until all tables are fully transmitted.
   *
   * @param bufferSendStates state objects tracking sends needed to fulfill a TransferRequest
   */
  def doHandleTransferRequest(bufferSendStates: Seq[BufferSendState]): Unit = {
    closeOnExcept(bufferSendStates) { _ =>
      val bssBuffers = bufferSendStates.map { bufferSendState =>
        withResource(new NvtxRange(s"doHandleTransferRequest", NvtxColor.CYAN)) { _ =>
          require(bufferSendState.hasNext, "Attempting to handle a complete transfer request.")

          // For each `BufferSendState` we ask for a bounce buffer fill up
          // so the server is servicing N (`bufferSendStates`) requests
          val buffersToSend = bufferSendState.next()

          (bufferSendState, buffersToSend)
        }
      }

      serverStream.sync()

      // need to release at this point, we do this after the sync so
      // we are sure we actually copied everything to the bounce buffer
      bufferSendStates.foreach(_.releaseAcquiredToCatalog())

      bssBuffers.foreach {
        case (bufferSendState, buffersToSend) =>
          val transferRequest = bufferSendState.getTransferRequest
          serverConnection.send(transferRequest.executorId(), buffersToSend, bufferTx =>
            try {
              logDebug(s"Done with the send for ${bufferSendState} with ${buffersToSend}")

              if (bufferSendState.hasNext) {
                // continue issuing sends.
                logDebug(s"Buffer send state ${bufferSendState} is NOT done. " +
                    s"Still pending: ${pendingTransfersQueue.size}.")
                bssExec.synchronized {
                  bssContinueQueue.add(bufferSendState)
                  bssExec.notifyAll()
                }
              } else {
                val transferResponse = bufferSendState.getTransferResponse()

                logDebug(s"Handling transfer request for ${transferRequest.executorId()} " +
                    s"with ${buffersToSend}")

                // send the transfer response
                serverConnection.send(
                  transferRequest.executorId,
                  AddressLengthTag.from(transferResponse.acquire(), transferRequest.responseTag()),
                  transferResponseTx => {
                    transferResponse.close()
                    transferResponseTx.close()
                  })

                // wake up the bssExec since bounce buffers became available
                logDebug(s"Buffer send state ${buffersToSend.tag} is done. Closing. " +
                    s"Still pending: ${pendingTransfersQueue.size}.")
                bssExec.synchronized {
                  bufferSendState.close()
                  bssExec.notifyAll()
                }
              }
            } finally {
              bufferTx.close()
            })
      }
    }
  }

  override def close(): Unit = {
    started = false
    bssExec.synchronized {
      bssExec.notifyAll()
    }
  }
}
