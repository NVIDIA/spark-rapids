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

package ai.rapids.spark.shuffle.ucx

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import ai.rapids.spark.shuffle._
import org.apache.spark.internal.Logging
import org.openucx.jucx.ucp.UcpRequest

/**
  * This is a private api used within the ucx package.
  * It is used by [[Transaction]] to call into the UCX functions. It adds the tag
  * as we use that to track the message and for debugging.
  */
private[ucx] abstract class UCXTagCallback {
  def onError(alt: AddressLengthTag, ucsStatus: Int, errorMsg: String): Unit
  def onMessageStarted(ucxMessage: UcpRequest): Unit
  def onSuccess(alt: AddressLengthTag): Unit
  def onCancel(alt: AddressLengthTag): Unit
}

class UCXServerConnection(ucx: UCX) extends UCXConnection(ucx) with ServerConnection {
  override def startManagementPort(host: String): Int = {
    ucx.startManagementPort(host)
  }

  override def send(sendPeerExecutorId: Long, bounceBuffers: Seq[AddressLengthTag],
    cb: TransactionCallback): Transaction =
    send(sendPeerExecutorId, null, bounceBuffers, cb)

  override def send(sendPeerExecutorId: Long, header: AddressLengthTag,
    cb: TransactionCallback): Transaction =
    send(sendPeerExecutorId, header, Seq.empty, cb)
}

class UCXClientConnection(peerExecutorId: Int, peerClientId: Long, ucx: UCX)
  extends UCXConnection(peerExecutorId, ucx)
  with ClientConnection {

  override def toString: String = {
    s"UCXClientConnection(ucx=$ucx, " +
      s"peerExecutorId=$peerExecutorId, " +
      s"peerClientId=$peerClientId) "
  }

  logInfo(s"UCX Client $this started")

  // tag used for a unique response to a request initiated by [[Transaction.request]]
  override def assignResponseTag: Long = composeResponseTag(peerClientId, ucx.assignResponseTag())

  override def assignBufferTag(msgId: Int): Long = composeBufferTag(peerClientId, msgId)

  override def getPeerExecutorId: Long = peerExecutorId

  /**
    * This performs a request/response, where the request and response are read from/deposited
    * from memory. This is used when an executor wants to request blocks from a remote executor,
    * it sends a [[ShuffleMetadataRequest]] in the [[reqAddress]] and expects a
    * [[ShuffleMatadataResponse]] in the [[respAddress]] at the [[response.tag]].
    *
    * @param cb - callback to call once the response is done
    * @return
    */
  override def request(request: AddressLengthTag,
                       response: AddressLengthTag,
                       cb: TransactionCallback): Transaction = {
    val tx = createTransaction

    tx.start(UCXTransactionType.Request, 2, cb)

    logInfo(s"Performing header request on tag ${TransportUtils.formatTag(request.tag)} for tx $tx")
    send(peerExecutorId, request, Seq.empty, (sendTx: Transaction) => {
      logInfo(s"UCX request send callback $sendTx")
      if (sendTx.getStatus == TransactionStatus.Success) {
        tx.incrementSendSize(request.length)
        if (tx.decrementPendingAndGet <= 0) {
          logInfo(s"Header request is done on send: ${sendTx.getStatus}, " +
            s"tag: ${TransportUtils.formatTag(request.tag)} for $tx")
          tx.txCallback(TransactionStatus.Success)
        }
      }
      sendTx.close()
    })

    receive(Seq(response), receiveTx => {
      logInfo(s"UCX request receive callback $receiveTx")
      if (receiveTx.getStatus == TransactionStatus.Success) {
        tx.incrementReceiveSize(response.length)
        if (tx.decrementPendingAndGet <= 0) {
          logInfo(s"Header request is done on receive: $this, " +
            s"tag: ${TransportUtils.formatTag(response.tag)}")
          tx.txCallback(TransactionStatus.Success)
        }
      }
      receiveTx.close()
    })
    tx
  }

}

class UCXConnection(peerExecutorId: Int, ucx: UCX) extends Connection with Logging {
  // alternate constructor for a server connection (-1 because it is not to a peer)
  def this(ucx: UCX) = this(-1, ucx)

  private[this] val pendingTransactions = new ConcurrentHashMap[Long, UCXTransaction]()

  /**
    * 1) client gets upper 28 bits
    * 2) then comes the type, which gets 4 bits
    * 3) the remaining 32 bits are used for buffer specific tags
    */
  private val requestMsgType:  Long = 0x00000000000000000L
  private val responseMsgType: Long = 0x0000000000000000AL
  private val bufferMsgType:   Long = 0x0000000000000000BL

  override def composeRequestTag(requestType: RequestType.Value): Long = {
    val requestTypeId = requestType match {
      case RequestType.MetadataRequest => 0
      case RequestType.TransferRequest => 1
    }
    requestMsgType | requestTypeId
  }

  protected def composeResponseTag(peerClientId: Long, bufferTag: Long): Long = {
    // response tags are [peerClientId, 1, bufferTag]
    composeTag(composeUpperBits(peerClientId, responseMsgType), bufferTag)
  }

  protected def composeBufferTag(peerClientId: Long, bufferTag: Long): Long = {
    // buffer tags are [peerClientId, 0, bufferTag]
    composeTag(composeUpperBits(peerClientId, bufferMsgType), bufferTag)
  }

  private def composeTag(upperBits: Long, lowerBits: Long): Long = {
    if ((upperBits & 0xFFFFFFFF00000000L) != upperBits) {
      throw new IllegalArgumentException(
        s"Invalid tag, upperBits would alias: ${TransportUtils.formatTag(upperBits)}")
    }
    // the lower 32bits aliasing is not a big deal, we expect it with msg rollover
    // so we don't check for it
    upperBits | (lowerBits & 0x00000000FFFFFFFFL)
  }

  private def composeUpperBits(peerClientId: Long, msgType: Long): Long = {
    if ((peerClientId & 0x000000000FFFFFFFL) != peerClientId) {
      throw new IllegalArgumentException(
        s"Invalid tag, peerClientId would alias: ${TransportUtils.formatTag(peerClientId)}")
    }
    if ((msgType & 0x000000000000000FL) != msgType) {
      throw new IllegalArgumentException(
        s"Invalid tag, msgType would alias: ${TransportUtils.formatTag(msgType)}")
    }
    (peerClientId << 36) | (msgType << 32)
  }

  private[ucx] def send(executorId: Long, alt: AddressLengthTag,
    ucxCallback: UCXTagCallback): Unit = {
    val sendRange = new NvtxRange("Connection Send", NvtxColor.PURPLE)
    try {
      ucx.send(executorId, alt, ucxCallback)
    } finally {
      sendRange.close()
    }
  }

  def send(sendPeerExecutorId: Long,
           header: AddressLengthTag,
           buffers: Seq[AddressLengthTag],
           cb: TransactionCallback): Transaction = {
    val tx = createTransaction
    buffers.foreach (alt => tx.registerForSend(alt))

    val numMessages = if (header != null) buffers.size + 1 else buffers.size

    tx.start(UCXTransactionType.Send, numMessages, cb)

    val ucxCallback = new UCXTagCallback {
      override def onError(alt: AddressLengthTag, ucsStatus: Int, errorMsg: String): Unit = {
        tx.handleTagError(alt.tag, errorMsg)
        logError(s"Error sending: $errorMsg, tx: $tx")
        tx.txCallback(TransactionStatus.Error)
      }

      override def onSuccess(alt: AddressLengthTag): Unit = {
        logDebug(s"Successful send: ${TransportUtils.formatTag(alt.tag)}, tx = $tx")
        tx.handleTagCompleted(alt)
        if (tx.decrementPendingAndGet <= 0) {
          tx.txCallback(TransactionStatus.Success)
        }
      }

      override def onCancel(alt: AddressLengthTag): Unit = {
        tx.handleTagCancelled(alt.tag)
        tx.txCallback(TransactionStatus.Cancelled)
      }

      override def onMessageStarted(ucxMessage: UcpRequest): Unit = {
        tx.registerPendingMessage(ucxMessage)
      }
    }

    // send the header
    if (header != null) {
      logDebug(s"Sending meta [executor_id=$sendPeerExecutorId, " +
        s"tag=${TransportUtils.formatTag(header.tag)}, size=${header.length}]")
      send(sendPeerExecutorId, header, ucxCallback)
      tx.incrementSendSize(header.length)
    }

    buffers.foreach { alt =>
      logDebug(s"Sending [executor_id=$sendPeerExecutorId, " +
        s"tag=${TransportUtils.formatTag(alt.tag)}, size=${alt.length}]")
      send(sendPeerExecutorId, alt, ucxCallback)
      tx.incrementSendSize(alt.length)
    }

    tx
  }

  override def receive(header: AddressLengthTag,
    cb: TransactionCallback): Transaction = receive(Seq(header), cb)

  override def receive(buffers: Seq[AddressLengthTag], cb: TransactionCallback): Transaction = {
    val tx = createTransaction
    buffers.foreach(alt => tx.registerForReceive(alt))
    tx.start(UCXTransactionType.Receive, buffers.size, cb)
    val ucxCallback = new UCXTagCallback {
      override def onError(alt: AddressLengthTag, ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Got an error... for tag: ${TransportUtils.formatTag(alt.tag)}, tx $tx")
        tx.handleTagError(alt.tag, errorMsg)
        tx.txCallback(TransactionStatus.Error)
      }

      override def onSuccess(alt: AddressLengthTag): Unit = {
        logDebug(s"Successful receive: ${TransportUtils.formatTag(alt.tag)}, tx $tx")
        tx.handleTagCompleted(alt)
        if (tx.decrementPendingAndGet <= 0) {
          logDebug(s"Receive done for tag: ${TransportUtils.formatTag(alt.tag)}, tx $tx")
          tx.txCallback(TransactionStatus.Success)
        }
      }

      override def onCancel(alt: AddressLengthTag): Unit = {
        tx.handleTagCancelled(alt.tag)
        tx.txCallback(TransactionStatus.Cancelled)
      }

      override def onMessageStarted(ucxMessage: UcpRequest): Unit = {
        tx.registerPendingMessage(ucxMessage)
      }
    }

    buffers.foreach(alt=> {
      logDebug(s"Receiving [tag=${TransportUtils.formatTag(alt.tag)}, size=${alt.length}]")
      ucx.receive(alt, ucxCallback)
      tx.incrementReceiveSize(alt.length)
    })
    tx
  }

  private[ucx] def cancel(msg: UcpRequest): Unit =
    ucx.cancel(msg)

  private[ucx] def createTransaction: UCXTransaction = {
    val thisTxId = ucx.getNextTransactionId

    val tx = new UCXTransaction(this, thisTxId)

    pendingTransactions.put(thisTxId, tx)
    logDebug(s"PENDING TRANSACTIONS AFTER ADD $pendingTransactions")
    tx
  }

  def removeTransaction(tx: UCXTransaction): Unit = {
    pendingTransactions.remove(tx.txId)
    logDebug(s"PENDING TRANSACTIONS AFTER REMOVE $pendingTransactions")
  }

  override def toString: String = {
    s"Connection(ucx=$ucx, peerExecutorId=$peerExecutorId) "
  }
}

object UCXConnection extends Logging {
  //
  // Handshake message code. This, I expect, could be folded into the [[BlockManagerId]],
  // but I have not tried this. If we did, it would eliminate the extra TCP connection
  // in this class.
  //
  private def readBytesFromStream(direct: Boolean,
    is: InputStream, lengthToRead: Int): ByteBuffer = {
    var bytesRead = 0
    var read = 0
    val buff = new Array[Byte](lengthToRead)
    while (read >= 0 && bytesRead < lengthToRead) {
      logTrace(s"Reading ${lengthToRead}. Currently at ${bytesRead}")
      read = is.read(buff, bytesRead, lengthToRead - bytesRead)
      if (read > 0) {
        bytesRead = bytesRead + read
      }
    }

    if (bytesRead < lengthToRead) {
      throw new IllegalStateException("Read less bytes than expected!")
    }

    val byteBuffer = ByteBuffer.wrap(buff)

    if (direct) {
      // a direct buffer does not allow .array() (not implemented).
      // therefore we received in the JVM heap, and now we copy to the native heap
      // using a .put on that native buffer
      val directCopy = ByteBuffer.allocateDirect(lengthToRead)
      TransportUtils.copyBuffer(byteBuffer, directCopy, lengthToRead)
      directCopy.rewind() // reset position
      directCopy
    } else {
      byteBuffer
    }
  }

  /**
    * Handles the input stream, using [[readBytesFromStream]] to read from the
    * stream: the length of the WorkerAddress + the WorkerAddress +
    * the remote ExecutorId (as an int)
    *
    * @param is - management port InputStream
    * @return - (WorkerAddress, remoteExecutorId)
    */
  def readHandshakeHeader(is: InputStream): (WorkerAddress, Int) = {
    val maxLen = 1024 * 1024

    // get the length from the stream, it's the first thing sent.
    val workerAddressLength = readBytesFromStream(false, is, 4).getInt()

    require(workerAddressLength <= maxLen,
      s"Received an abnormally large (>$maxLen Bytes) WorkerAddress " +
        s"(${workerAddressLength} Bytes), dropping.")

    val workerAddress = readBytesFromStream(true, is, workerAddressLength)

    // get the remote executor Id, that's the last part of the handshake
    val executorId = readBytesFromStream(false, is, 4).getInt()

    (WorkerAddress(workerAddress), executorId)
  }


  /**
    * Writes a header that is exchanged in the management port. The header contains:
    *  - UCP Worker address length (4 bytes)
    *  - UCP Worker address (variable length)
    *  - Local executor id (4 bytes)
    *
    * @param os              - OutputStream to write to
    * @param workerAddress   - ByteBuffer that holds
    * @param localExecutorId - The local executorId
    */
  def writeHandshakeHeader(os: OutputStream,
                           workerAddress: ByteBuffer,
                           localExecutorId: Int): Unit = {
    val headerSize = 4 + workerAddress.remaining() + 4

    val sizeBuff = ByteBuffer.allocate(headerSize)
    sizeBuff.putInt(workerAddress.capacity)
    sizeBuff.put(workerAddress)
    sizeBuff.putInt(localExecutorId)
    sizeBuff.flip()

    os.write(sizeBuff.array)
    os.flush()
  }
}
