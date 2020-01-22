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

package ai.rapids.spark.ucx

import java.io._
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

import ai.rapids.cudf.{Cuda, NvtxColor, NvtxRange}
import ai.rapids.spark.RapidsUCXShuffleIterator
import org.apache.spark.internal.Logging
import org.openucx.jucx._
import org.openucx.jucx.ucp._
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Public interface used when establishing a management port
  */
abstract class UCXConnectionCallback{
  def onConnection(connection: Connection): Unit
}

/**
  * This is a private api used within the ucx package.
  * It is used by [[Transaction]] to call into the UCX functions. It adds the tag
  * as we use that to track the message and for debugging.
  */
private[ucx] abstract class UCXTagCallback {
  def onError(tag: Long, ucsStatus: Int, errorMsg: String): Unit
  def onSuccess(tag: Long): Unit
}

object TransactionStatus extends Enumeration {
  val NotStarted, InProgress, Complete, Success, Error = Value
}

object RapidsUcxUtil {
  def formatTag(tag: Long): String = {
    f"0x$tag%016X"
  }

  def composeTag(baseTag: Long, msgTag: Long): Long = baseTag | msgTag

  def copyBuffer(src: ByteBuffer, dst: ByteBuffer, size: Int): Unit = {
    val copyMetaBuffer = new NvtxRange("UCX CopyBuffer", NvtxColor.RED)
    try {
      (0 until size).foreach(_ => dst.put(src.get()))
    } finally {
      copyMetaBuffer.close()
    }
  }
}

// TODO: add header and describe threading requirements
class Transaction(conn: Connection,
                  @volatile var status: TransactionStatus.Value,
                  var errorMessage: Option[String],
                  var total: Long,
                  @volatile var pending: AtomicLong,
                  txId: Long = 0) extends AutoCloseable with Logging {

  /**
    * Case class representing stats for the a transaction
    * @param txTimeMs - amount of time this [[Transaction]] took
    * @param sendSize - amount of bytes sent
    * @param receiveSize - amount of bytes received
    * @param sendThroughput - send throughput
    * @param recvThroughput - receive throughput
    */
  case class TransactionStats(txTimeMs: Double,
                              sendSize: Long,
                              receiveSize: Long,
                              sendThroughput: Double,
                              recvThroughput: Double)

  private val registeredByTag = mutable.HashMap[Long, Buffer]()

  // This holds all the registered buffers. It is how we know when are done
  // (when completed == registered)
  private val registered = new ArrayBuffer[Buffer]()
  private val completed = new ArrayBuffer[Buffer]()

  // This is for debugging purposes. With trace on, buffers will move from registered
  // to error if there was a UCX error handling them.
  private val errored = new ArrayBuffer[Buffer]()

  private var txCallback: (Transaction, Boolean) => Unit = _

  // Start and end times used for metrics
  private var start: Long = 0
  private var end: Long = 0

  // Track how much was sent and received/ also for metrics
  var sendSize = new AtomicLong(0L)
  var receiveSize = new AtomicLong(0L)

  // a range that would cover this transaction. Note, a metadata receive range is not going to be started,
  // in hopes of avoiding extra noise
  private var range: NvtxRange = _
  private var closed: Boolean = false

  // This is the condition variable used to determine when the transaction is complete.
  val lock = new ReentrantLock()
  private val notComplete = lock.newCondition()

  // TODO: This sets the name of the transaction, it is used when printing the transaction to string
  //   could be replaced by an enum (which would help with other things)
  var myName: String = ""

  private def formatRegistered: String = {
    registered.map(x => x.toString).mkString("\n")
  }

  private def formatCompleted: String = {
    completed.map(x => x.toString).mkString("\n")
  }

  private def formatErrored: String = {
    errored.map(x => x.toString).mkString("\n")
  }

  override def toString: String = toString(log.isTraceEnabled())

  private def toString(verbose: Boolean): String = {
    val msgPre = s"Transaction(" +
      s"txId=$txId, " +
      s"type=$myName, " +
      s"connection=${conn.toString}, " +
      s"status=$status, " +
      s"errorMessage=$errorMessage, " +
      s"totalMessages=$total, " +
      s"pending=$pending"

    if (verbose) {
      msgPre +
      s"\nregistered=\n$formatRegistered" +
      s"\ncompleted=\n$formatCompleted" +
      s"\nerrored=\n$formatErrored)"
    } else {
      msgPre + ")"
    }
  }

  /**
    * TODO: Note that this does not handle a timeout. In new UCX versions we should be able to
    *   cancel a message after a timeout, so this doesn't freeze the task forever.
    *
    * NOTE: I believe we could simplify this with a blocking queue, but I haven't tried the change yet.
    */
  def waitForCompletion(): Unit = {
    while (status != TransactionStatus.Complete &&
           status != TransactionStatus.Error) {
      logInfo(s"Waiting for status to become complete! ${this}")
      val condRange = new NvtxRange("Conditional wait", NvtxColor.PURPLE)
      lock.lock()
      if (status != TransactionStatus.Complete &&
          status != TransactionStatus.Error) {
        notComplete.await()
      }
      lock.unlock()
      condRange.close()
    }
    logInfo(s"Leaving waitForCompletion ${this}")
  }

  /**
    * Interal function to register a callback against the callback service
    * @param cb - callback function to call using the callbackService.
    */
  private def registerCb(cb: Transaction => Unit): Unit = {
    txCallback =
      (tx: Transaction, error: Boolean) => {
        conn.callbackService.execute(() =>
        try {
          if (!error){
            // Success is not a terminal state. It indicates that 
            // from the UCX perspective the transfer was successful and not an error. 
            // In the caller's callback, make sure that the transaction is marked as Success,
            // else handle it (retry, or let Spark know about it so it can re-schedule).
            //
            // This is set before we call the callback function. Once that callback is done,
            // we mark the request Complete (signaling the condition variable `notComplete`)
            // which will unblock a thread that has called `waitForCompletion`.
            tx.status = TransactionStatus.Success
            tx.stop()
          }
          cb(tx)
          if (error) {
            tx.closeError()
          } else {
            tx.closeSuccess()
          }
        } catch {
          case e: Throwable =>
            val sw = new StringWriter()
            e.printStackTrace(new PrintWriter(sw))
            logError(s"Detected an exception from user code. Dropping: " + sw.toString)
            tx.closeError()
        })
      }
  }

  // tag used for a unique response to a request initiated by [[Transaction.request]]
  def getMetadataTag: Long = conn.composeResponseTag(conn.assignMessageTag())

  /**
    * Register a buffer address/size for a send transaction
    * @param buffAddress - address of buffer
    * @param size - size of buffer
    * @return - tag that should be used to reference this transfer
    */
  def registerForSend(buffAddress: Long, size: Long): Long = {
    val sendMsgTag = conn.composeSendTag(conn.assignMessageTag())
    val buff = Buffer(sendMsgTag, buffAddress, size)
    registeredByTag.put(sendMsgTag, buff)
    registered += buff
    logTrace(s"Assigned tag for send ${RapidsUcxUtil.formatTag(sendMsgTag)} for message at " +
      s"buffer $buffAddress with size $size")
    sendMsgTag
  }

  /**
    * Register a buffer address/size for a receive transaction
    * @param msgTag - tag to be used to receive (this is communicated in metadata before the receive is posted)
    * @param buffAddress - address of buffer
    * @param size - size of buffer
    * @return - tag that should be used to reference this transfer
    */
  def registerForReceive(msgTag: Long, buffAddress: Long, size: Long): Unit = {
    val recvMsgTag = conn.composeReceiveTag(msgTag)
    val buff = Buffer(recvMsgTag, buffAddress, size)
    registered += buff
    registeredByTag.put(recvMsgTag, buff)
    logTrace(s"Assigned tag for receive ${RapidsUcxUtil.formatTag(recvMsgTag)} for message at " +
      s"buffer $buffAddress with size $size")
  }

  /**
    * This performs a request/response, where the request and response are read from/deposited from memory
    * This is used when an executor wants to request blocks from a remote executor, it sends a
    * [[ShuffleMetadataRequest]] in the [[reqAddress]] and expects a [[ShuffleMatadataResponse]] in the
    * [[respAddress]] at the [[responseTag]].
    * @param reqAddress - address where request should be read from
    * @param reqSize - request size
    * @param respAddress - address where response should be deposited
    * @param respSize - response size
    * @param responseTag - tag to use to listen for the response
    * @param cb - callback to call once the response is done
    * @return
    */
  def request(reqAddress: Long, reqSize: Long, respAddress: Long, respSize: Long, responseTag: Long,
              cb: Transaction => Unit): Transaction = {
    val tx = this

    start("UCX Request", 2, cb)

    val sendTransaction = conn.createTransaction
    logDebug(s"Performing header request on tag ${RapidsUcxUtil.formatTag(conn.metadataMsgTag)}")
    sendTransaction.send(reqAddress, reqSize, conn.metadataMsgTag, sendTx => {
      logTrace(s"UCX request send callback $sendTx")
      if (sendTx.status == TransactionStatus.Success) {
        tx.sendSize.addAndGet(reqSize)
        if (tx.pending.decrementAndGet() <= 0) {
          logDebug(s"Header request is done on send: $status, tag: ${RapidsUcxUtil.formatTag(conn.metadataMsgTag)}")
          tx.txCallback(tx, false)
        }
      }
    })

    val receiveTransaction = conn.createTransaction

    receiveTransaction.receive(respAddress, respSize, responseTag, receiveTx => {
      logTrace(s"UCX request receive callback $receiveTx")
      if (receiveTx.status == TransactionStatus.Success) {
        tx.receiveSize.addAndGet(respSize)
        if (tx.pending.decrementAndGet() <= 0) {
          logDebug(s"Header request is done on receive: ${this}, tag: ${RapidsUcxUtil.formatTag(responseTag)}")
          tx.txCallback(tx, false)
        }
      }
    })

    tx
  }

  /**
    * Issue a receive on the [[conn.metadataMsgTag]] metadata tag
    * @param address - address where to listen to
    * @param size - size of buffer
    * @param cb - callback
    * @return
    */
  def receive(address: Long, size: Long, cb: Transaction => Unit): Transaction =
    receive(address, size, conn.metadataMsgTag, cb)

  /**
    * Receive a single buffer at address/size. This is not for public consuption
    * @param address - address of buffer
    * @param size - size to send
    * @param tag - the tag to use to send
    * @param cb - callback to invoke when the [[Transaction]] is done/errored
    * @return - [[Transaction]] object
    */
  private def receive(address: Long, size: Long, tag: Long, cb: Transaction => Unit): Transaction = {
    start("UCX receive", 1, cb)

    val msgTag = conn.composeReceiveTag(tag)

    val tx = this
    val ucxCallback = new UCXTagCallback {
      override def onError(tag: Long, ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Receive error for tag: ${RapidsUcxUtil.formatTag(msgTag)}")
        tx.errorMessage = Some(errorMsg)
        tx.txCallback(tx, true)
      }

      override def onSuccess(tag: Long): Unit = {
        val res = pending.decrementAndGet()
        logTrace(s"Receive success for tag: ${RapidsUcxUtil.formatTag(tag)} and address $address, tx $tx")
        if (res <= 0) {
          logDebug(s"Receive done for tag: ${RapidsUcxUtil.formatTag(tag)} and address $address, tx $tx")
          tx.txCallback(tx, false)
        }
      }
    }

    conn.receive(msgTag, address, size, ucxCallback)
    logTrace(s"Receive at $msgTag size $size issued")
    tx.receiveSize.addAndGet(size)

    tx
  }

  /**
    * Given a set of [[registered]] buffers, issue a receive transaction, then call
    * callback [[cb]] when done/errored.
    * @param cb - callback to call when transaction is done/errored
    * @return - [[Transaction]] object that can be used to wait
    */
  def receive(cb: Transaction => Unit): Transaction = {
    start("UCX Receive Multi", registered.size, cb)
    val tx = this
    val ucxCallback = new UCXTagCallback {
      override def onError(tag: Long, ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Got an error... for tag: ${RapidsUcxUtil.formatTag(tag)}")
        if (registeredByTag.contains(tag)){
         val origBuff = registeredByTag(tag)
         errored += origBuff
        }
        tx.errorMessage = Some(errorMsg)
        tx.txCallback(tx, true)
      }

      override def onSuccess(tag: Long): Unit = {
        logTrace(s"Got a success receive for tag: ${RapidsUcxUtil.formatTag(tag)}, tx $tx")
        if (registeredByTag.contains(tag)){
          val origBuff = registeredByTag(tag)
          completed += origBuff
        }
        if (tx.pending.decrementAndGet() <= 0) {
          logDebug(s"Receive done for tag: ${RapidsUcxUtil.formatTag(tag)}, tx $tx")
          tx.txCallback(tx, false)
        }
      }
    }

    tx.registered.foreach(buffer => {
      logTrace(s"Receiving at tag: ${RapidsUcxUtil.formatTag(buffer.tag)} buffer of size ${buffer.size}")
      conn.receive(buffer.tag, buffer.address, buffer.size, ucxCallback)
      tx.receiveSize.addAndGet(buffer.size)
    })

    tx
  }

  /**
    * Send a header message (using the [[headerTag]]) + any accumulated messages
    * that are in the [[registered]] array buffer.
    * @param headerAddress - the address of the header to send
    * @param headerSize - the size of the header
    * @param headerTag - the tag to use for the header piece
    * @param cb - callback to call when the full transaction is done/errored (including pending messages)
    * @return - [[Transaction]] object that can be used to wait
    */
  def send(headerAddress: Long, headerSize: Long, headerTag: Long, cb: Transaction => Unit): Transaction = {
    start("UCX Send", registered.size + 1, cb)

    val tx = this

    val ucxCallback = new UCXTagCallback {
      override def onError(tag: Long, ucsStatus: Int, errorMsg: String): Unit = {
        tx.errorMessage = Some(errorMsg)
        if (registeredByTag.contains(tag)){
          val origBuff = registeredByTag(tag)
          errored += origBuff
        }
        logError(s"Error sending: ${errorMsg}, tx: ${tx}")
        tx.txCallback(tx, true)
      }

      override def onSuccess(tag: Long): Unit = {
        logTrace(s"Successful send for tag: ${RapidsUcxUtil.formatTag(tag)}, tx = ${tx}")
        if (registeredByTag.contains(tag)){
          val origBuff = registeredByTag(tag)
          completed += origBuff
        }
        if (tx.pending.decrementAndGet() <= 0) {
          logDebug(s"Calling tx.close for UCX Send: ${tx}")
          tx.txCallback(tx, false)
        }
      }
    }

    // send the header
    val metaTag = conn.composeSendTag(headerTag)

    logDebug(s"Sending header at ${RapidsUcxUtil.formatTag(metaTag)}")

    conn.send(metaTag, headerAddress, headerSize, ucxCallback)

    logInfo("Done issuing send for metadata request")

    tx.sendSize.addAndGet(headerSize)
    tx.registered.foreach(buffer => {
      logTrace(s"Sending at tag: ${RapidsUcxUtil.formatTag(buffer.tag)} buffer of size ${buffer.size}")
      conn.send(buffer.tag, buffer.address, buffer.size, ucxCallback)
      tx.sendSize.addAndGet(buffer.size)
    })

    tx
  }

  /**
    * Internal function to kick off a [[Transaction]]
    * @param name - optional name to use (can be null), used for ranges
    * @param numPending - number of messages we expect to see sent/received
    * @param cb - callback to call when done/errored
    */
  private def start(name: String, numPending: Long, cb: Transaction => Unit): Unit = {
    if (start != 0) {
      throw new IllegalStateException("Transaction can't be started twice!")
    }
    if (closed) {
      throw new IllegalStateException("Transaction already closed!!")
    }
    myName = name
    if (range != null) {
      throw new IllegalStateException("Range already initialized!!")
    }
    if (name != null) {
      range = new NvtxRange(name, NvtxColor.RED)
    }
    start = System.nanoTime
    registerCb(cb)
    status = TransactionStatus.InProgress
    total = numPending
    pending.set(numPending)
    if (numPending == 0) {
      logWarning(s"EMPTY TRANSACTION ${this}, setting it to done")
      status = TransactionStatus.Complete
    }
  }

  private def closeSuccess(): Unit = {
    lock.lock()
    status = TransactionStatus.Complete
    close()
    notComplete.signal()
    lock.unlock()
  }

  private def closeError(): Unit = {
    lock.lock()
    status = TransactionStatus.Error
    close()
    notComplete.signal()
    lock.unlock()
  }

  override def close(): Unit = {
    if (range != null) {
      range.close()
      range = null
    }
    closed = true
  }

  def stop(): Unit  = {
    end = System.nanoTime()
  }

  def getStats: TransactionStats = {
    if (end == 0) {
      throw new IllegalStateException("Transaction not stopped, can't get stats")
    }
    val diff: Double = (end - start)/1000000.0D
    val sendThroughput: Double = (sendSize.get()/1024.0D/1024.0D/1024.0D) / (diff / 1000.0D)
    val recvThroughput: Double = (receiveSize.get()/1024.0D/1024.0D/1024.0D) / (diff / 1000.0D)
    TransactionStats(diff, sendSize.get(), receiveSize.get(), sendThroughput, recvThroughput)
  }
}

case class Buffer(tag: Long, address: Long, size: Long) {
  override def toString: String = {
    s"Buffer(tag=${RapidsUcxUtil.formatTag(tag)}, address=$address, size=$size)"
  }
}

case class WorkerAddress(address: ByteBuffer)

/**
  * Connection object representing the UCX connection between two executors
  * @param ucx - [[UCX]] instance
  * @param peerExecutorId - Int Id for the remote executor
  * @param peerWorkerAddress - [[WorkerAddress]] received from peer.
  * @param baseReceiveTag - base tag to use for receive tags w.r.t. this Connection
  * @param baseSendTag - base tag to use for send tags w.r.t. this Connection
  * @param endpoint - UcpEndpoint we need to use to target the peer
  */
class Connection(ucx: UCX,
                 val peerExecutorId: Int,
                 peerWorkerAddress: WorkerAddress,
                 baseReceiveTag: Long,
                 baseSendTag: Long,
                 endpoint: UcpEndpoint) extends Logging {

  // monotonically increasing timer that holds the txId (for debug purposes, at this stage)
  val txId = new AtomicLong(0L)

  // TODO: This should go away and become a queue that the executor is pulling from, s.t. the
  //   executor task thread blocks
  private[ucx] val callbackService = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setNameFormat("callback-thread-%d").build)

  logInfo(s"Constructed connection [" +
    s"peerExecutorId=${peerExecutorId}, " +
    s"baseReceiveTag=${RapidsUcxUtil.formatTag(baseReceiveTag)}, " +
    s"baseSendTag=${RapidsUcxUtil.formatTag(baseSendTag)}")

  override def toString: String = {
    s"Connection(ucx=${ucx}, "+
    s"peerExecutorId=${peerExecutorId}, " +
    s"receiveTag=${RapidsUcxUtil.formatTag(baseReceiveTag)}, " +
    s"sendTag=${RapidsUcxUtil.formatTag(baseSendTag)}" +
    ")"
  }

  // this is used to signify a metadata request
  val metadataMsgTag: Long = 0x0000FFFF

  private val responseTag = 0x10000000

  private[ucx] def assignMessageTag(): Long = ucx.assignMessageTag()

  private[ucx] def composeSendTag(msgTag: Long): Long = RapidsUcxUtil.composeTag(baseSendTag, msgTag)

  private[ucx] def composeReceiveTag(msgTag: Long): Long = RapidsUcxUtil.composeTag(baseReceiveTag, msgTag)

  private[ucx] def composeResponseTag(msgTag: Long): Long = RapidsUcxUtil.composeTag(responseTag, msgTag)

  private[ucx] def send(tag: Long, address: Long, size: Long, ucxCallback: UCXTagCallback) =
    ucx.send(endpoint, tag, address, size, ucxCallback)

  private[ucx] def receive(tag: Long, address: Long, size: Long, ucxCallback: UCXTagCallback) =
    ucx.receive(tag, address, size, ucxCallback)

  def createTransaction: Transaction = {
    new Transaction(this,
          TransactionStatus.NotStarted,
          None,
          0,
          new AtomicLong(0),
          txId.incrementAndGet())
  }
}

/**
  * The UCX class holds a [[UcpContext]] and [[UcpWorker]]. It opens a TCP port for UCP's Worker Address
  * to be transmitted from executor to executor in order to create [[UcpEndpoint]]s.
  *
  * The current API supported from UCX is the tag based API. A tag is a long that identifies a "topic"
  * for a particular
  *
  * Send and receive methods are exposed here, but should not be used directly (as it deals with raw tags). Instead,
  * the [[Transaction]] interface should be used.
  *
  * Note that we currently use an extra TCP connection to communicate the WorkerAddress and other
  * pieces to other peers. It would be ideal if this could fit somewhere else (like the [[BlockManagerId]])
  *
  * @param executorId - unique id that is used as part of the tags from UCX messages
  * @param usingWakeupFeature - turn on if you want to use polling instead of a hot loop. Currently DOES NOT WORK.
  */
class UCX(executorId: Int = -1, usingWakeupFeature: Boolean = false) extends AutoCloseable with Logging {
  private var context: UcpContext = null
  private var worker: UcpWorker = null
  private val endpoints = new ArrayBuffer[UcpEndpoint]()
  private var initialized = false

  // a peer tag identifies an incoming connection uniquely
  private val peerTag = new AtomicLong(0) // peer tags

  // this is a monotonically increasing id for every message posted
  private val msgTag = new AtomicLong(0) // outgoing message tag

  // event loop calls [[UcpWorker.progress]] in a tight loop
  val progressThread = Executors.newFixedThreadPool(1,
    new ThreadFactoryBuilder().setNameFormat("progress-thread-%d").build)

  // management port socket
  private var serverSocket: ServerSocket = null
  private val serverService = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setNameFormat("ucx-mgmt-thread-%d").build)

  // Pending* case classes hold state for a message. Since we don't ask for thread safety behavior in UCX
  // we queue all sends and receives and these two case classes hold that state
  case class PendingSend(endpoint: UcpEndpoint, address: Long, size: Long, tag: Long, callback: UCXTagCallback)
  case class PendingReceive(address: Long, size: Long, tag: Long, callback: UCXTagCallback)

  // The pending queues are used to enqueue [[PendingReceive]] or [[PendingSend]], from executor task threads
  // and [[progressThread]] will hand them to the UcpWorker thread.
  private var pendingReceive = new ConcurrentLinkedQueue[PendingReceive]()
  private var pendingSend = new ConcurrentLinkedQueue[PendingSend]()

  // Multiple executor threads are going to try to call connect (from the [[RapidsUCXShuffleIterator]])
  // because they don't know what executors to connect to up until shuffle fetch time.
  //
  // This makes sure that all executor threads get the same [[Connection]] object for a specific management
  // (host, port) key.
  private val connectionCache = new mutable.HashMap[(String, Int), Connection]()

  /**
    * Initializes the UCX context and local worker and starts up the worker progress thread.
    * UCX worker/endpoint relationship.
    *
    * This is not thread safe, it is called from a lazy var initialization from the ShuffleManager => 1 thread.
    */
  def init(): Unit = {
    if (initialized) {
      throw new IllegalStateException("UCX already initialized")
    }

    logInfo(s"Creating UCX context.")
    var contextParams = new UcpParams().requestTagFeature()
    if (usingWakeupFeature) {
      contextParams = contextParams.requestWakeupFeature()
    }
    context = new UcpContext(contextParams)
    logInfo(s"UCX context created")

    logInfo(s"Creating UCX worker")
    var workerParams = new UcpWorkerParams()

    if (usingWakeupFeature) {
      workerParams = workerParams
        .requestWakeupRMA()
        .requestWakeupTagSend()
        .requestWakeupTagRecv()
    }

    worker = context.newWorker(workerParams)
    logInfo(s"UCX Worker created")

    initialized = true

    // The expectation is that the device is already set when Rmm is intialized
    val deviceId = Cuda.getDevice
    logInfo(s"Initializing UCX on deviceId=$deviceId")

    progressThread.execute(() => {
      Cuda.setDevice(deviceId) // get the 0th device for now, according to visible devices

      // utility function to make all the progress possible in each iteration
      // this could change in the future to 1 progress call per loop, or be used
      // entirely differently once polling is figured out
      def drainWorker(): Unit = {
        while (worker.progress() > 0) {}
      }

      while(initialized) {
        // TODO: condition variables
        // service the worker
        if(initialized) {
          drainWorker()
          // else worker.progress returned 0
          if (usingWakeupFeature) {
            drainWorker()
            worker.waitForEvents()
          }
        }

        while (!pendingSend.isEmpty) {
          val ps = pendingSend.poll()
          if (ps != null) {
            handlePendingSend(ps)
          }
        }

        while (!pendingReceive.isEmpty) {
          val pr = pendingReceive.poll()
          if (pr != null) {
            handlePendingReceive(pr)
          }
        }
      }
    })
  }

  /**
    * Starts a TCP server to listen for external clients, returning with
    * what port it used. This port needs to be communicated to external clients
    * via the [[BlockManagerId]]'s topologyInfo optional.
    *
    * @param mgmtHost - String the hostname to bind to
    * @param connectionCallback - a callback we can delegate new connections when they are received
    * @return port bound
    */
  def startManagementPort(mgmtHost: String, connectionCallback: UCXConnectionCallback): Int = {
    var portBindAttempts = 100
    var portBound = false
    while (!portBound && portBindAttempts > 0) {
      try {
        logInfo(s"Starting ephemeral UCX management port at host ${mgmtHost}")
        // TODO: use ucx listener for this
        serverSocket = new ServerSocket()
        // open a TCP/IP socket to connect to a client
        // send the worker address to the client who wants to talk to us
        // associate with [[onNewConnection]]
        try {
          serverSocket.bind(new InetSocketAddress(mgmtHost, 0))
        } catch {
          case ioe: IOException =>
            logError(s"Unable to bind using host [${mgmtHost}]", ioe)
            throw ioe
        }
        logInfo(s"Successfully bound to ${mgmtHost}:${serverSocket.getLocalPort}")
        portBound = true
        serverService.execute(() => {
          while (initialized) {
            logInfo(s"Accepting UCX management connections.")
            try {
              val s = serverSocket.accept()
              logInfo(s"Received a UCX connection: ${s}")
              // disable Nagle's algorithm, in hopes of data not buffered by TCP
              s.setTcpNoDelay(true)
              handleSocket(s, connectionCallback)
            } catch {
              case e: Throwable =>
                if (!initialized) {
                  logInfo(s"UCX management socket closing")
                } else {
                  logError(s"Got exception while waiting for a UCX management connection", e)
                }
            }
          }
        })
      } catch {
        case ioe: IOException => {
          portBindAttempts = portBindAttempts - 1
        }
      }
    }
    if (!portBound) {
      throw new IllegalStateException(s"Cannot bind UCX, tried ${portBindAttempts} times")
    }
    serverSocket.getLocalPort
  }

  //
  // Handshake message code. This, I expect, could be folded into the [[BlockManagerId]], but I have not
  // tried this. If we did, it would eliminate the extra TCP connection in this class.
  //

  /**
    * deserializes a handshake header as it appers in the ByteBuffer
    * @param buffer - buffer that will contain a (somtimes) partial header
    * @return - Option[(WorkerAddress, remotePeerTag, remoteExecutorId))]
    */
  private def deserializeHandshakeHeader(buffer: ByteBuffer): Option[(WorkerAddress, Long, Int)] = {
    if (buffer.remaining >= 4) {
      val workerAddressLength = buffer.getInt(0)
      //TODO: buffer.remaining appears to not return the correct thing
      //making this if succeed
      if (buffer.position() + buffer.remaining >= workerAddressLength + 4 + 8 + 4) {
        buffer.getInt // advance
        val workerAddressBuffer = ByteBuffer.allocateDirect(workerAddressLength)
        // copy from the byte buffer, into the WorkerAddress buffer
        RapidsUcxUtil.copyBuffer(buffer, workerAddressBuffer, workerAddressLength)
        val remotePeerTag = buffer.getLong
        val remoteExecutorId = buffer.getInt
        workerAddressBuffer.rewind
        Some((WorkerAddress(workerAddressBuffer), remotePeerTag, remoteExecutorId))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
    * Handles the input stream, delegating to [[deserializeHandshakeHeader]] to deserialize the
    * header.
    * @param is - management port InputStream
    * @return - (WorkerAddress, remotePeerTag, remoteExecutorId)
    */
  private def readHandshakeHeader(is: InputStream): (WorkerAddress, Long, Int) = {
    var totalRead = 0
    var read = 0
    var workerAddressTag: Option[(WorkerAddress, Long, Int)] = None
    val maxLen = 1024 * 1024
    val buff = new Array[Byte](maxLen)
    val bb = ByteBuffer.wrap(buff)
    var off = 0
    while (initialized && read >= 0 && workerAddressTag.isEmpty) {
      if (is.available() > 0) {
        read = is.read(buff, off, maxLen - off)
        if (read > 0) {
          totalRead = totalRead + read
          off = off + read
        }
        bb.limit(totalRead.toInt)
        workerAddressTag = deserializeHandshakeHeader(bb)
      }
    }
    if (initialized && workerAddressTag.isEmpty) {
      throw new IllegalStateException("Error while fetching worker address!")
    }
    workerAddressTag.get
  }

  /**
    * Writes a header that is exchanged in the management port. The header contains:
    *  - UCP Worker address length (4 bytes)
    *  - UCP Worker address (variable length)
    *  - Local peer tag (8 bytes)
    *  - Local executort id (4 bytes)
    * @param os - OutputStream to write to
    * @param workerAddress - ByteBuffer that holds
    * @param localPeerTag - The locally assigned tag for this connection
    * @param localExecutorId - The local executorId
    */
  private def writeHandshakeHeader(os: OutputStream,
                                   workerAddress: ByteBuffer,
                                   localPeerTag: Long,
                                   localExecutorId: Int): Unit = {
    val headerSize = 4 + workerAddress.remaining() + 8 + 4

    val sizeBuff = ByteBuffer.allocate(headerSize)
    sizeBuff.putInt(workerAddress.capacity)
    sizeBuff.put(workerAddress)
    sizeBuff.putLong(localPeerTag)
    sizeBuff.putInt(localExecutorId)
    sizeBuff.flip()

    os.write(sizeBuff.array)
    os.flush()
  }

  private def addPending(tag: Long, cb: UCXTagCallback, msg: UcxRequest): Unit = {
    if (msg.isCompleted) {
      logDebug("Pending UCX message finished early")
    }
    if (usingWakeupFeature) {
      worker.signal() // s.t. it unblocks and sees the pending op
    }
  }

  private def handlePendingSend(pendingSend: PendingSend, numTries: Int = 0): UcxRequest = {
    var pendingSendRange = new NvtxRange("UCX handlePendingSend", NvtxColor.PURPLE)

    val ucxCb = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        logError("error sending : " + ucsStatus + " " + errorMsg)
        if (pendingSendRange != null) {
          pendingSendRange.close()
          pendingSendRange = null
        }
        pendingSend.callback.onError(pendingSend.tag, ucsStatus, errorMsg)
      }

      override def onSuccess(request: UcxRequest): Unit = {
        if (pendingSendRange != null) {
          pendingSendRange.close()
          pendingSendRange = null
        }
        pendingSend.callback.onSuccess(pendingSend.tag)
      }
    }

    val ucxMsg = pendingSend.endpoint.sendTaggedNonBlocking(
      pendingSend.address, pendingSend.size, pendingSend.tag,
      ucxCb)

    addPending(pendingSend.tag, pendingSend.callback, ucxMsg)
    ucxMsg
  }

  private def handlePendingReceive(pendingReceive: PendingReceive): UcxRequest = {
    logTrace(s"Starting handlePendingReceive ${pendingReceive} for tag ${RapidsUcxUtil.formatTag(pendingReceive.tag)}")
    //val pendingReceiveRange = new NvtxRange("UCX handlePendingReceive", NvtxColor.PURPLE)

    var called = false
    val ucxCb = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        logError(s"Error receiving: ${ucsStatus} ${errorMsg}")
        pendingReceive.callback.onError(pendingReceive.tag, ucsStatus, errorMsg)
      }

      override def onSuccess(request: UcxRequest): Unit = {
        if (called) {
          logInfo(s"onSuccess already called for request... early case?")
        } else {
          called = true
          logTrace(s"Success receiving calling callback ${RapidsUcxUtil.formatTag(pendingReceive.tag)}")
          pendingReceive.callback.onSuccess(pendingReceive.tag)
        }
      }
    }

    val ucxMsg = worker.recvTaggedNonBlocking(pendingReceive.address, pendingReceive.size,
      pendingReceive.tag, Long.MaxValue, ucxCb)

    if (ucxMsg.isCompleted) {
      logInfo(s"Receive completed early!! ${pendingReceive}")
      ucxCb.onSuccess(ucxMsg)
    }

    addPending(pendingReceive.tag, pendingReceive.callback, ucxMsg)
    ucxMsg
  }

  // LOW LEVEL API
  def send(ep: UcpEndpoint, tag: Long, address: Long, size: Long, cb: UCXTagCallback) = {
    pendingSend.add(PendingSend(ep, address, size, tag, cb))
    if (usingWakeupFeature) {
      worker.signal() // s.t. it unblocks and sees the pending op
    }
  }

  def receive(tag: Long, address: Long, size: Long, cb: UCXTagCallback) = {
    pendingReceive.add(PendingReceive(address, size, tag, cb))
    if (usingWakeupFeature) {
      worker.signal() // s.t. it unblocks and sees the pending op
    }
  }

  def assignMessageTag(): Long = {
    val tag = msgTag.addAndGet(1)
    tag & 0x0000FFFF //. lower 32 bits
  }

  private def assignPeerTag(endpoint: UcpEndpoint): Long = {
    // TODO: add peer to a map of connections
    val tag = peerTag.addAndGet(1)
    // this is not ideal, could have collisions
    (((executorId << 16) | tag) << 32)  // move the tag to the upper 32 bits
  }

  /**
    * Handle an incoming connection on the TCP management port
    * This will fetch the [[WorkerAddress]] from the peer, and establish a UcpEndpoint
    * @param socket
    * @param connectionCallback
    */
  private def handleSocket(socket: Socket, connectionCallback: UCXConnectionCallback) = {
    var gotConnection = new NvtxRange(s"UCX Handle Connection from ${socket.getInetAddress}", NvtxColor.RED)
    try {
      logDebug(s"Reading worker address from: ${socket}")
      val is = socket.getInputStream
      val os = socket.getOutputStream

      // get the peer worker address and TODO: block manager id
      val (peerWorkerAddress: WorkerAddress, remotePeerTag: Long, peerExecutorId: Int) = readHandshakeHeader(is)
      logInfo(s"Got peer worker address size ${peerWorkerAddress.address.capacity} " +
        s"and remotePeerTag ${RapidsUcxUtil.formatTag(remotePeerTag)} from ${socket}")

      // establish a UCX endpoint local -> peer
      val peerEndpoint = setupEndpoint(peerWorkerAddress)

      val localPeerTag = assignPeerTag(peerEndpoint)

      // send the local worker address to the peer
      writeHandshakeHeader(os, worker.getAddress, localPeerTag, executorId)

      // peer would have established an endpoint peer -> local
      logInfo(s"Sent server workerAddress ${socket} and peerTag ${RapidsUcxUtil.formatTag(localPeerTag)}")

      // at this point we have handshaked, UCX is ready to go for this point-to-point connection.
      // assume that we get a list of block ids, tag tuples we want to transfer out
      socket.close()

      val connection = new Connection(
        this,
        peerExecutorId,
        peerWorkerAddress,
        localPeerTag,   // receiveTag (set by this executor)
        remotePeerTag,  // sendTag (sent by the peer)
        peerEndpoint)

      logInfo(s"NEW INCOMING UCX CONNECTION ${connection}")
      gotConnection.close()
      gotConnection = null
      connectionCallback.onConnection(connection)
    } finally {
      if (gotConnection != null) {
        gotConnection.close()
      }
    }
  }

  /**
    * Establish a new [[UcpEndpoint]] given a [[WorkerAddress]]. It also
    * caches them s.t. at [[close]] time we can release resources.
    * @param workerAddress
    * @return UcpEndpoint
    */
  private def setupEndpoint(workerAddress: WorkerAddress): UcpEndpoint = {
    logInfo(s"Starting an endpoint to ${workerAddress}")
    // create an UCX endpoint using workerAddress
    val endpoint = worker.newEndpoint(
            new UcpEndpointParams()
              .setUcpAddress(workerAddress.address))
    endpoints += endpoint
    endpoint
  }

  /**
    * Connect to a remote UCX management port. This is called from [[RapidsUCXShuffleIterator]]
    * @param peerMgmtHost management TCP host
    * @param peerMgmtPort management TCP port
    * @return Connection object representing this connection
    */
  def connect(peerMgmtHost: String, peerMgmtPort: Int): Connection = synchronized {
    if (connectionCache.contains((peerMgmtHost, peerMgmtPort))) {
      return connectionCache((peerMgmtHost, peerMgmtPort))
    }
    logInfo(s"Connecting to ${peerMgmtHost} to ${peerMgmtPort}")
    val nvtx = new NvtxRange(s"UCX Connect to ${peerMgmtHost}:${peerMgmtPort}", NvtxColor.RED)
    try {
      val socket = new Socket(peerMgmtHost, peerMgmtPort)
      socket.setTcpNoDelay(true)
      val os = socket.getOutputStream
      val is = socket.getInputStream

      val localPeerTag = assignPeerTag(null)
      writeHandshakeHeader(os, worker.getAddress, localPeerTag, executorId)

      logDebug("Waiting for peer worker address and tag")
      val (peerWorkerAddress, remotePeerTag: Long, remoteExecutorId: Int) = readHandshakeHeader(is)
      logInfo(s"Peer worker tag on connect ${RapidsUcxUtil.formatTag(remotePeerTag)} " +
      s"from remoteExecutorId ${remoteExecutorId}")

      val endpoint = setupEndpoint(peerWorkerAddress)

      socket.close()

      val connection = new Connection(
        this,
        remoteExecutorId,
        peerWorkerAddress,
        localPeerTag,    // receiveTag (set by this executor)
        remotePeerTag,   // sendTag (sent by the peer)
        endpoint)

      logInfo(s"NEW OUTGOING UCX CONNECTION ${connection}")
      connectionCache.put((peerMgmtHost, peerMgmtPort), connection) // memoize, for other tasks
      connection
    } finally {
      nvtx.close()
    }
  }

  override def close(): Unit = {
    initialized = false

    // TODO: flush?
    if (serverSocket != null) {
      serverSocket.close()
      serverSocket = null
    }

    if (usingWakeupFeature) {
      worker.signal()
    }

    serverService.shutdown()
    if (!serverService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX mgmt service failed to terminate correctly")
    }

    progressThread.shutdown()
    if (!progressThread.awaitTermination(500, TimeUnit.MICROSECONDS)) {
      logError("UCX progress thread failed to terminate correctly")
    }

    endpoints.foreach(ep => ep.close())

    if (worker != null) {
      worker.close()
    }

    if (context != null) {
      context.close()
    }
  }
}
