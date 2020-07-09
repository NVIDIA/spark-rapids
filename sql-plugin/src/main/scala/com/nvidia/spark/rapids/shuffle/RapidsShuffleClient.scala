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

import java.util.concurrent.Executor

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DeviceMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.format.{CodecType, MetadataResponse, TableMeta, TransferState}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.storage.ShuffleBlockBatchId

/**
  * trait used by client consumers ([[RapidsShuffleIterator]]) to gather what the
  * expected number of batches (Tables) is, and the ids for each table as they are received.
  */
trait RapidsShuffleFetchHandler {
  /**
    * After a response for shuffle metadata is received, the expected number of columnar
    * batches is communicated back to the caller via this method.
    * @param expectedBatches - number of columnar batches
    */
  def start(expectedBatches: Int): Unit

  /**
    * Called when a buffer is received and has been handed off to the catalog.
    * @param bufferId - a tracked shuffle buffer id
    */
  def batchReceived(bufferId: ShuffleReceivedBufferId): Unit

  /**
    * Called when the transport layer is not able to handle a fetch error for metadata
    * or buffer fetches.
    *
    * @param errorMessage - a string containing an error message
    */
  def transferError(errorMessage: String): Unit
}

/**
  * A helper case class that describes a pending table. It allows
  * the transport to schedule/throttle these requests to fit within the maximum bytes in flight.
  * @param client client used to issue the requests
  * @param tableMeta shuffle metadata describing the table
  * @param tag a transport specific tag to use for this transfer
  * @param handler a specific handler that is waiting for this batch
  */
case class PendingTransferRequest(client: RapidsShuffleClient,
                                  tableMeta: TableMeta,
                                  tag: Long,
                                  handler: RapidsShuffleFetchHandler) {

  require(tableMeta.bufferMeta().codec() == CodecType.UNCOMPRESSED)

  def getLength: Long = tableMeta.bufferMeta().actualSize()
}

/**
  * Class describing the state of a set of [[PendingTransferRequest]]s.
  *
  * This class is *not thread safe*. The way the code is currently designed, bounce buffers being
  * used to receive, or copied from, are acted on a sequential basis, in time and in space.
  *
  * Callers use this class, like so:
  *
  * 1. [[getRequest]]
  *    - first call:
  *       - Initializes the state tracking the progress of `currentRequest` and our place in
  *          `requests` (e.g. offset, bytes remaining)
  *
  *    - subsequent calls:
  *       - if `currentRequest` is not done, it will return the current request. And perform
  *          sanity checks.
  *       - if `currentRequest` is done, it will perform sanity checks, and advance to the next
  *          request in `requests`
  *
  * 2. [[consumeBuffers]]
  *     - first call:
  *       - The first time around for `currentRequest` it will allocate the actual full device
  *         buffer that we will copy to, copy data sequentially from the bounce buffer to the 
  *         target buffer.
  *     - subsequent calls:
  *       - continue copy data sequentially from the bounce buffers passed in.
  *       - When `currentRequest` has been fully received, an optional `DeviceMemoryBuffer` is 
  *         set, and returned.
  *
  * 3. [[close]]
  *     - once the caller calls close, the bounce buffers are returned to the pool.
  *
  * @param transport a transport, which in this case is used to free bounce buffers
  * @param bounceMemoryBuffers a sequence of `MemoryBuffer` buffers to use for receives
  */
class BufferReceiveState(
    transport: RapidsShuffleTransport,
    val bounceMemoryBuffers: Seq[MemoryBuffer])
  extends AutoCloseable
  with Logging {

  private[this] var bounceBuffers: Seq[AddressLengthTag] = null
  private[this] val requests = new ArrayBuffer[PendingTransferRequest]()

  /**
    * Use by the transport to add to this [[BufferReceiveState]] requests it needs to handle.
    * @param pendingTransferRequest request to add to this [[BufferReceiveState]]
    */
  def addRequest(pendingTransferRequest: PendingTransferRequest): Unit = synchronized {
    requests.append(pendingTransferRequest)
  }

  /**
    * Holds the target device memory buffer. It is allocated at [[consumeBuffers]] when the first
    * bounce buffer resolves, and it is also handed off to the caller in the same function.
    */
  private[this] var buff: DeviceMemoryBuffer = null

  /**
    * This is the address/length/tag object for the target buffer. It is used only to access a cuda
    * synchronous copy method. We should do a regular copy from [[buff]] once we use the async
    * method and add the cuda event synchronization.
    */
  private[this] var alt: AddressLengthTag = null

  /**
    * True iff this is the last request we are handling. It is used in [[isDone]] to find the
    * stopping point.
    */
  private[this] var lastRequest: Boolean = false

  /**
    * The index into the [[requests]] sequence pointing to the current request we are handling.
    * We handle requests sequentially.
    */
  private[this] var currentRequestIndex = -1

  /**
    * Amount of bytes left in the current request.
    */
  private[this] var currentRequestRemaining: Long = 0L

  /**
    * Byte offset we are currently at. [[getRequest]] uses and resets it, and [[consumeBuffers]]
    * updates it.
    */
  private[this] var currentRequestOffset: Long = 0L

  /**
    * The current request (TableMeta, Handler (iterator))
    */
  private[this] var currentRequest: PendingTransferRequest = null

  /**
    * To help debug "closed multiple times" issues
    */
  private[this] var isClosed = false

  /**
    * Becomes true when there is an error detected, allowing the client to close this
    * [[BufferReceiveState]] prematurely.
    */
  private[this] var errorOcurred = false

  override def toString: String = {
    s"BufferReceiveState(isDone=$isDone, currentRequestDone=$currentRequestDone, " +
      s"requests=${requests.size}, currentReqIx=$currentRequestIndex, " +
      s"currentReqOffset=$currentRequestOffset, currentReqRemaining=$currentRequestRemaining" +
      s"bounceBuffers=$bounceBuffers)"
  }

  /**
    * When a receive transaction is successful, this function is called to consume the bounce
    * buffers received.
    *
    * @note If the target device buffer is not allocated, this function does so.
    * @param bounceBuffers sequence of buffers that have been received
    * @return if the current request is complete, returns a shallow copy of the target
    *         device buffer as an `Option`. Callers will need to close the buffer.
    */
  def consumeBuffers(
      bounceBuffers: Seq[AddressLengthTag]): Option[DeviceMemoryBuffer] = synchronized {
    var needsCleanup = true

    try {
      if (buff == null) {
        buff = DeviceMemoryBuffer.allocate(currentRequest.getLength)
        alt = AddressLengthTag.from(buff, currentRequest.tag)
      }

      bounceBuffers.foreach(bb => {
        currentRequestOffset = currentRequestOffset +
          alt.cudaCopyFrom(bb, currentRequestOffset)
      })

      // buffers are sent in sequential order, so the length check is all we need (at the moment)
      // to determine if we are done.
      if (currentRequestDone) {
        require(currentRequestOffset == currentRequest.getLength,
          "Current request marked as done, but not all bounce buffers were consumed?")
        logDebug(s"Done copying ${TransportUtils.formatTag(currentRequest.tag)}")
        val res = buff
        // The buffer [[buff]] is being handed off to the caller.
        // It is the job of the caller to close it.
        buff = null
        needsCleanup = false
        Some(res)
      } else {
        logDebug(s"More copying left for ${TransportUtils.formatTag(currentRequest.tag)}, " +
           s"current offset is ${currentRequestOffset} out of ${currentRequest.getLength}")
        needsCleanup = false
        None
      }
    } finally {
      if (needsCleanup) {
        if (buff != null) {
          buff.close()
          buff = null
        }
      }
    }
  }

  private[this] def currentRequestDone: Boolean = {
    currentRequestRemaining == 0
  }

  /**
    * Signals whether this [[BufferReceiveState]] is complete.
    * @return boolean when at the last request, and that request is fully received
    */
  def isDone: Boolean = synchronized {
    lastRequest && currentRequestDone
  }

  /**
    * Return the current (making the next request current, if we are done) request and
    * whether we advanced (want to kick off a transfer request to the server)
    *
    * @return returns the currently working transfer request, and
    *         true if this is a new request we should be asking the server to trigger
    */
  def getRequest: (PendingTransferRequest, Boolean) = synchronized {
    require(currentRequestIndex < requests.size,
      "Something went wrong while handling buffer receives. Asking for more buffers than expected")

    if (currentRequestRemaining > 0) {
      require(currentRequest != null,
        "Attempted to get the current request, but it was null")
      (currentRequest, false)
    } else {
      if (currentRequest != null) {
        logDebug(s"Done with ${currentRequest}, advancing.")
        require(currentRequestDone,
          s"Attempted to move on to the next buffer receive, but the prior buffer wasn't fully " +
            s"transmitted, offset ${currentRequestOffset} == ${currentRequest.getLength}")
        currentRequestOffset = 0L
        currentRequest = null
      }

      // advance to the next request
      currentRequestIndex = currentRequestIndex + 1

      require(currentRequestIndex < requests.size,
        s"getRequest was called too many times, looking for $currentRequestIndex out of " +
          s"${requests.size}")

      currentRequest = requests(currentRequestIndex)

      currentRequestRemaining = currentRequest.getLength

      // track if we are at the last request
      lastRequest = currentRequestIndex == requests.size - 1

      prepareBounceBuffers() // need to prepare these before returning the new request

      (currentRequest, true)
    }
  }

  private[this] def prepareBounceBuffers(): Unit = {
    bounceBuffers = bounceMemoryBuffers.map(bmb => AddressLengthTag.from(bmb, currentRequest.tag))
  }

  private[this] def advance(bounceBufferLength: Long): Long = {
    val lengthToRecv = Math.min(bounceBufferLength, currentRequestRemaining)
    currentRequestRemaining = currentRequestRemaining - lengthToRecv
    lengthToRecv
  }

  /**
    * Used to cut the subset of bounce buffers the client will need to issue receives with.
    *
    * This is a subset, because at the moment, the full target length worth of bounce buffers
    * could have been acquired. If we are at the tail end of receive, and it could be fulfilled
    * with 1 bounce buffer, for example, we would return 1 bounce buffer here, rather than the
    * number of buffers in acquired.
    *
    * @note that these extra buffers should really just be freed as soon as we realize
    *       they are of no use.
    *
    * @return sequence of [[AddressLengthTag]] pointing to the receive bounce buffers.
    */
  def getBounceBuffersForReceive(): Seq[AddressLengthTag] = synchronized {
    var bounceBufferIx = 0
    var bounceBuffersForTransfer = Seq[AddressLengthTag]()
    // we may have more bounce buffers than required for this send
    while (bounceBufferIx < bounceBuffers.size && !currentRequestDone) {
      val bb = bounceBuffers(bounceBufferIx)
      val lengthToRecv = advance(bb.length)
      bb.resetLength(lengthToRecv)

      logDebug(s"Buffer for ${currentRequest}: ${bb}")
      bounceBufferIx = bounceBufferIx + 1

      // at the very end, we may have a smaller set of buffers, than those we acquired
      bounceBuffersForTransfer = bounceBuffersForTransfer :+ bb
    }
    bounceBuffersForTransfer
  }

  // helpers used in logging
  def getCurrentRequestRemaining: Long = currentRequestRemaining
  def getCurrentRequestOffset : Long = currentRequestOffset

  def closeWithError(): Unit = synchronized {
    errorOcurred = true
    close()
  }

  override def close(): Unit = synchronized {
    require(isDone || errorOcurred)
    if (isClosed) {
      throw new IllegalStateException("ALREADY CLOSED")
    }
    isClosed = true
    transport.freeReceiveBounceBuffers(bounceMemoryBuffers)
  }
}

/**
  * The client makes requests via a [[Connection]] obtained from the [[RapidsShuffleTransport]].
  *
  * The [[Connection]] follows a single threaded callback model, so this class posts operations
  * to an `Executor` as quickly as it gets them from the [[Connection]].
  *
  * This class handles fetch requests from [[RapidsShuffleIterator]], turning them into
  * [[ShuffleMetadata]] messages, and shuffle `TransferRequest`s.
  *
  * Its counterpart is the [[RapidsShuffleServer]] on a specific peer executor, specified by
  * `connection`.
  *
  * @param localExecutorId this id is sent to the server, it is required for the protocol as
  *                        the server needs to pick an endpoint to send a response back to this
  *                        executor.
  * @param connection a connection object against a remote executor
  * @param transport used to get metadata buffers and to work with the throttle mechanism
  * @param exec Executor used to handle tasks that take time, and should not be in the
  *             transport's thread
  * @param clientCopyExecutor Executors used to handle synchronous mem copies
  * @param maximumMetadataSize The maximum metadata buffer size we are able to request
  *                            TODO: this should go away
  */
class RapidsShuffleClient(
    localExecutorId: Long,
    connection: ClientConnection,
    transport: RapidsShuffleTransport,
    exec: Executor,
    clientCopyExecutor: Executor,
    maximumMetadataSize: Long,
    devStorage: RapidsDeviceMemoryStore = GpuShuffleEnv.getDeviceStorage,
    catalog: ShuffleReceivedBufferCatalog = GpuShuffleEnv.getReceivedCatalog) extends Logging {

  object ShuffleClientOps {
    /**
      * When a metadata response is received, this event is issued to handle it.
      * @param tx the [[Transaction]] to be closed after consuming the response
      * @param resp the response metadata buffer
      * @param shuffleRequests blocks to be requested
      * @param rapidsShuffleFetchHandler the handler (iterator) to callback to
      */
    case class HandleMetadataResponse(tx: Transaction,
                                      resp: RefCountedDirectByteBuffer,
                                      shuffleRequests: Seq[ShuffleBlockBatchId],
                                      rapidsShuffleFetchHandler: RapidsShuffleFetchHandler)

    /**
      * Represents retry due to metadata being larger than expected.
      *
      * @param shuffleRequests request to retry
      * @param rapidsShuffleFetchHandler the handler (iterator) to callback to
      * @param fullResponseSize response size to allocate to fit the server's response in full
      */
    case class FetchRetry(shuffleRequests: Seq[ShuffleBlockBatchId],
                          rapidsShuffleFetchHandler: RapidsShuffleFetchHandler,
                          fullResponseSize: Long)

    /**
      * Used to have this client handle the enclosed [[BufferReceiveState]] asynchronously.
      *
      * Until [[bufferReceiveState]] completes, this event will continue to get posted.
      *
      * @param bufferReceiveState object containing the state of pending requests to the peer
      */
    case class IssueBufferReceives(bufferReceiveState: BufferReceiveState)

    /**
      * When a buffer is received, this event is posted to remove from the progress thread
      * the copy, and the callback into the iterator.
      *
      * Currently not used. There is a TODO below.
      *
      * @param tx live transaction for the buffer, to be closed after the buffer is handled
      * @param bufferReceiveState the object maintaining state for receives
      * @param currentRequest the request these bounce buffers belong to
      * @param bounceBuffers buffers used in the transfer, which contain the fragment of the data
      */
    case class HandleBounceBufferReceive(tx: Transaction,
                                         bufferReceiveState: BufferReceiveState,
                                         currentRequest: PendingTransferRequest,
                                         bounceBuffers: Seq[AddressLengthTag])
  }

  import ShuffleClientOps._

  private[this] def handleOp(op: Any): Unit = {
    try {
      op match {
        case HandleMetadataResponse(tx, resp, shuffleRequests, rapidsShuffleFetchHandler) =>
          doHandleMetadataResponse(tx, resp, shuffleRequests, rapidsShuffleFetchHandler)
        case FetchRetry(shuffleRequests, rapidsShuffleFetchHandler, fullResponseSize) =>
          doFetch(shuffleRequests, rapidsShuffleFetchHandler, fullResponseSize)
        case IssueBufferReceives(bufferReceiveState) =>
          doIssueBufferReceives(bufferReceiveState)
        case HandleBounceBufferReceive(tx, bufferReceiveState, currentRequest, bounceBuffers) =>
          doHandleBounceBufferReceive(tx, bufferReceiveState, currentRequest, bounceBuffers)
      }
    } catch {
      case t: Throwable => {
        logError("Exception occurred while handling shuffle client task.", t)
      }
    }
  }

  private[this] def asyncOrBlock(op: Any): Unit = {
    exec.execute(() => handleOp(op))
  }

  /**
    * Pushes a task onto the queue to be handled by the client's copy executor.
    *
    * @note - at this stage, tasks in this pool can block (it will grow as needed)
    *
    * @param op One of the case classes in [[ShuffleClientOps]]
    */
  private[this] def asyncOnCopyThread(op: Any): Unit = {
    clientCopyExecutor.execute(() => handleOp(op))
  }

  /**
    * Starts a fetch request for all the shuffleRequests, using `handler` to communicate
    * events back to the iterator.
    *
    * @param shuffleRequests blocks to fetch
    * @param handler iterator to callback to
    * @param metadataSize metadata size to use for this fetch
    */
  def doFetch(shuffleRequests: Seq[ShuffleBlockBatchId],
              handler: RapidsShuffleFetchHandler,
              metadataSize: Long = maximumMetadataSize): Unit = {
    val fetchRange = new NvtxRange("Client.fetch", NvtxColor.PURPLE)
    try {
      if (shuffleRequests.isEmpty) {
        throw new IllegalStateException("Sending empty blockIds in the MetadataRequest?")
      }

      // get a metadata response tag so we can send it with the request
      val responseTag = connection.assignResponseTag

      // serialize a request, note that this includes the responseTag in the message
      val metaReq = new RefCountedDirectByteBuffer(ShuffleMetadata.buildShuffleMetadataRequest(
        localExecutorId, // needed s.t. the server knows what endpoint to pick
        responseTag,
        shuffleRequests,
        metadataSize))

      logDebug(s"Requesting block_ids=[$shuffleRequests] from connection $connection, req: \n " +
        s"${ShuffleMetadata.printRequest(ShuffleMetadata.getMetadataRequest(metaReq.getBuffer()))}")

      val resp = transport.getMetaBuffer(metadataSize)

      // make request
      connection.request(
        AddressLengthTag.from(
          metaReq.acquire(),
          connection.composeRequestTag(RequestType.MetadataRequest)),
        AddressLengthTag.from(
          resp.acquire(),
          responseTag),
        tx => {
          try {
            asyncOrBlock(HandleMetadataResponse(tx, resp, shuffleRequests, handler))
          } finally {
            metaReq.close()
          }
        })
    } finally {
      fetchRange.close()
    }
  }

  /**
    * Function to handle MetadataResponses, as a result of the [[HandleMetadataResponse]] event.
    *
    * @param tx live metadata response transaction to be closed in this handler
    * @param resp response buffer, to be closed in this handler
    * @param shuffleRequests blocks to fetch
    * @param handler iterator to callback to
    */
  private[this] def doHandleMetadataResponse(
      tx: Transaction,
      resp: RefCountedDirectByteBuffer,
      shuffleRequests: Seq[ShuffleBlockBatchId],
      handler: RapidsShuffleFetchHandler): Unit = {
    val start = System.currentTimeMillis()
    val handleMetaRange = new NvtxRange("Client.handleMeta", NvtxColor.CYAN)
    try {
      tx.getStatus match {
        case TransactionStatus.Success =>
          // start the receives
          val respBuffer = resp.getBuffer()
          val metadataResponse: MetadataResponse = ShuffleMetadata.getMetadataResponse(respBuffer)

          logDebug(s"Received from ${tx} response: \n:" +
            s"${ShuffleMetadata.printResponse("received response", metadataResponse)}")

          if (metadataResponse.fullResponseSize() <= respBuffer.capacity()) {
            // signal to the handler how many batches are expected
            handler.start(metadataResponse.tableMetasLength())

            //TODO: nothing is preventing us from issuing transfers later, still letting
            //  the metadata requests through early on, but doing the transfer requests later
            //  (on iterator.next) saving connection/handshake time.

            // queue up the receives
            queueTransferRequests(metadataResponse, handler)
          } else {
            // NOTE: this path hasn't been tested yet.
            logWarning("Large metadata message received, widening the receive size.")
            asyncOrBlock(FetchRetry(shuffleRequests, handler, metadataResponse.fullResponseSize()))
          }
        case _ =>
          handler.transferError(
            tx.getErrorMessage.getOrElse(s"Unsuccessful metadata request ${tx}"))
      }
    } finally {
      logDebug(s"Metadata response handled in ${TransportUtils.timeDiffMs(start)} ms")
      handleMetaRange.close()
      resp.close()
      tx.close()
    }
  }

  /**
    * Used by the transport, to schedule receives. The requests are sent to the executor for this
    * client.
    * @param bufferReceiveState object tracking the state of pending TransferRequests
    */
  def issueBufferReceives(bufferReceiveState: BufferReceiveState): Unit = {
    asyncOnCopyThread(IssueBufferReceives(bufferReceiveState))
  }

  /**
    * Issues transfers requests (if the state of [[bufferReceiveState]] advances), or continue to
    * work a current request (continue receiving bounce buffer sized chunks from a larger receive).
    * @param bufferReceiveState object maintaining state of requests to be issued (current or
    *                           future). The requests included in this state object originated in
    *                           the transport's throttle logic.
    */
  private[shuffle] def doIssueBufferReceives(bufferReceiveState: BufferReceiveState): Unit = {
    logDebug(s"At issue for ${bufferReceiveState}, " +
      s"remaining: ${bufferReceiveState.getCurrentRequestRemaining}, " +
      s"offset: ${bufferReceiveState.getCurrentRequestOffset}")

    val (currentRequest, advanced) = bufferReceiveState.getRequest

    if (advanced) {
      // note this sends 1 at a time... need to experiment with `getRequest` returning more than
      // 1 buffer if there are bounce buffers available
      receiveBuffers(currentRequest, bufferReceiveState)
      sendTransferRequest(Seq(currentRequest))
    } else {
      logDebug(s"Not the first time around for ${currentRequest}, NOT sending transfer request.")
      receiveBuffers(currentRequest, bufferReceiveState)
    }
  }

  private def receiveBuffers(
      currentRequest: PendingTransferRequest,
      bufferReceiveState: BufferReceiveState): Transaction = {
    val buffersToReceive = bufferReceiveState.getBounceBuffersForReceive()

    logDebug(s"Issuing receive for ${TransportUtils.formatTag(currentRequest.tag)} at " +
      s"startingOffset ${bufferReceiveState.getCurrentRequestOffset} with " +
      s"${buffersToReceive.size} bounce buffers, and ${currentRequest.getLength} total length. " +
      s"buffers = ${buffersToReceive.map(_.length).mkString(",")}")

    connection.receive(buffersToReceive,
      tx => {
        tx.getStatus match {
          case TransactionStatus.Success =>
            // TODO: during code review we agreed to make this receive per buffer, s.t.
            //  buffers could be freed earlier and likely out of order.
            asyncOnCopyThread(HandleBounceBufferReceive(tx, bufferReceiveState,
              currentRequest, buffersToReceive))
          case _ => try {
            val errMsg = s"Unsuccessful buffer receive ${tx}"
            logError(errMsg)
            currentRequest.handler.transferError(errMsg)
          } finally {
            tx.close()
            bufferReceiveState.closeWithError()
          }
        }
      })
  }

  /**
   * Sends the [[com.nvidia.spark.rapids.format.TransferRequest]] metadata message, to ask the
   * server to get started.
   * @param toIssue sequence of [[PendingTransferRequest]] we want included in the server
   *                transfers
   */
  private[this] def sendTransferRequest(toIssue: Seq[PendingTransferRequest]): Unit = {
    logDebug(s"Sending a transfer request for " +
      s"${toIssue.map(r => TransportUtils.formatTag(r.tag)).mkString(",")}")

    // get a tag that the server can use to send its reply
    val responseTag = connection.assignResponseTag

    val transferReq = new RefCountedDirectByteBuffer(
      ShuffleMetadata.buildTransferRequest(localExecutorId, responseTag,
        toIssue.map(i => (i.tableMeta, i.tag))))

    if (transferReq.getBuffer().remaining() > maximumMetadataSize) {
      throw new IllegalStateException("Trying to send a transfer request metadata buffer that " +
        "is larger than the limit.")
    }

    val res = transport.getMetaBuffer(maximumMetadataSize)

    //issue the buffer transfer request
    connection.request(
      AddressLengthTag.from(
        transferReq.acquire(),
        connection.composeRequestTag(RequestType.TransferRequest)),
      AddressLengthTag.from(
        res.acquire(),
        responseTag),
      tx => {
        try {
          // make sure all bufferTxs are still valid (e.g. resp says that they have STARTED)
          val transferResponse = ShuffleMetadata.getTransferResponse(res.getBuffer())
          (0 until transferResponse.responsesLength()).foreach(r => {
            val response = transferResponse.responses(r)
            if (response.state() != TransferState.STARTED) {
              // we could either re-issue the request, cancelling and releasing memory
              // or we could re-issue, and leave the old receive waiting
              // for now, leaving the old receive waiting.
              throw new IllegalStateException("NOT IMPLEMENTED")
            }
          })
        } finally {
          transferReq.close()
          res.close()
          tx.close()
        }
      })
  }

  /**
    * Feed into the throttle thread in the transport [[PendingTransferRequest]], to be
    * issued later via the [[doIssueBufferReceives]] method.
    *
    * @param metaResponse metadata response flat buffer
    * @param handler callback trait (the iterator implements this)
    */
  private def queueTransferRequests(metaResponse: MetadataResponse,
                                    handler: RapidsShuffleFetchHandler): Unit = {
    val allTables = metaResponse.tableMetasLength()
    logDebug(s"Queueing transfer requests for ${allTables} tables " +
      s"from ${connection.getPeerExecutorId}")

    val ptrs = new ArrayBuffer[PendingTransferRequest](allTables)
    (0 until allTables).foreach { i =>
      val tableMeta = metaResponse.tableMetas(i)
      if (tableMeta.bufferMeta() != null) {
        ptrs += PendingTransferRequest(
          this,
          ShuffleMetadata.copyTableMetaToHeap(tableMeta),
          connection.assignBufferTag(tableMeta.bufferMeta().id),
          handler)
      } else {
        // Degenerate buffer (no device data) so no more data to request.
        // We need to trigger call in iterator, otherwise this batch is never handled.
        handler.batchReceived(track(null, tableMeta).asInstanceOf[ShuffleReceivedBufferId])
      }
    }

    if (ptrs.nonEmpty) {
      transport.queuePending(ptrs)
    }
  }

  /**
    * This function handles data received in `bounceBuffers`. The data should be copied out
    * of the buffers, and the function should call into `bufferReceiveState` to advance its
    * state (consumeBuffers)
    * @param tx live transaction for these bounce buffers, it should be closed in this function
    * @param bufferReceiveState state management objects for live transfer requests
    * @param currentRequest current transfer request being worked on
    * @param bounceBuffers bounce buffers (just received) containing data to be consumed
    */
  def doHandleBounceBufferReceive(tx: Transaction,
                                  bufferReceiveState: BufferReceiveState,
                                  currentRequest: PendingTransferRequest,
                                  bounceBuffers: Seq[AddressLengthTag]): Unit = {
    logDebug(s"At issue receive async with bounce buffers ${bounceBuffers} and ${currentRequest}" +
      s" and starting offset ${bufferReceiveState.getCurrentRequestOffset}")
    val nvtxRange = new NvtxRange("Buffer Callback", NvtxColor.RED)
    try {
      // consume buffers, which will return true if done for the current request
      val buff = bufferReceiveState.consumeBuffers(bounceBuffers)

      if (buff.isDefined) {
        logDebug(s"Done with receive [tag=${TransportUtils.formatTag(currentRequest.tag)}, " +
          s"tx=${tx}]")

        // release the bytes in flight
        transport.doneBytesInFlight(currentRequest.getLength)

        // hand buffer off to the catalog
        val rapidsBufferId = track(buff.get, currentRequest.tableMeta)

        // tell the iterator the batch has arrived, and is ready for processing
        currentRequest.handler.batchReceived(
          rapidsBufferId.asInstanceOf[ShuffleReceivedBufferId])
      } else {
        logDebug(s"Not done with: " +
          s"[tag=${TransportUtils.formatTag(currentRequest.tag)}, tx=${tx}, " +
          s"current_offset=${bufferReceiveState.getCurrentRequestOffset}, " +
          s"total_length=${currentRequest.getLength}]")
      }

      val stats = tx.getStats

      logDebug(s"Received buffer size ${stats.receiveSize} in" +
        s" ${stats.txTimeMs} ms @ bw: [recv: ${stats.recvThroughput}] GB/sec")

      if (!bufferReceiveState.isDone) {
        logDebug(s"${bufferReceiveState} is not done.")
        asyncOnCopyThread(IssueBufferReceives(bufferReceiveState))
      } else {
        logDebug(s"${bufferReceiveState} is DONE, closing.")
        bufferReceiveState.close()
      }
    } finally {
      nvtxRange.close()
      tx.close()
    }
  }

  /**
    * Hands [[table]] and [[buffer]] to the device storage/catalog, obtaining an id that can be
    * used to look up the buffer from the catalog going (e.g. from the iterator)
    * @param buffer contiguous [[DeviceMemoryBuffer]] with the tables' data
    * @param meta [[TableMeta]] describing [[buffer]]
    * @return the [[RapidsBufferId]] to be used to look up the buffer from catalog
    */
  private[shuffle] def track(buffer: DeviceMemoryBuffer, meta: TableMeta): RapidsBufferId = {
    val id: ShuffleReceivedBufferId = catalog.nextShuffleReceivedBufferId()
    logDebug(s"Adding buffer id ${id} to catalog")
    if (buffer != null) {
      // add the buffer to the catalog so it is available for spill
      devStorage.addBuffer(id, buffer, meta, SpillPriorities.INPUT_FROM_SHUFFLE_PRIORITY)
    } else {
      // no device data, just tracking metadata
      catalog.registerNewBuffer(new DegenerateRapidsBuffer(id, meta))
    }
    id
  }
}
