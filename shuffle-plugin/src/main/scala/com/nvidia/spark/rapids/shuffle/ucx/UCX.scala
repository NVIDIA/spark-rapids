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

package com.nvidia.spark.rapids.shuffle.ucx

import java.io._
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{MemoryBuffer, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.GpuDeviceManager
import com.nvidia.spark.rapids.shuffle.{AddressLengthTag, ClientConnection, MemoryRegistrationCallback, TransportUtils}
import org.openucx.jucx._
import org.openucx.jucx.ucp._

import org.apache.spark.internal.Logging

case class WorkerAddress(address: ByteBuffer)

/**
 * The UCX class wraps JUCX classes and handles all communication with UCX from other
 * parts of the shuffle code. It manages a `UcpContext` and `UcpWorker`, for the
 * local executor, and maintain a set of `UcpEndpoint` for peers.
 *
 * The current API supported from UCX is the tag based API. Tags are exposed in this class
 * via an `AddressLengthTag`.
 *
 * This class uses an extra TCP management connection to perform a handshake with remote peers,
 * this port should be distributed to peers by other means (e.g. via the `BlockManagerId`)
 *
 * @param executorId unique id (int) that identifies the local executor
 * @param usingWakeupFeature (true by default) set to false to use a hot loop, as opposed to
 *                           UCP provided signal/wait
 */
class UCX(executorId: Int, usingWakeupFeature: Boolean = true) extends AutoCloseable with Logging {
  private[this] val context = {
    val contextParams = new UcpParams().requestTagFeature()
    if (usingWakeupFeature) {
      contextParams.requestWakeupFeature()
    }
    new UcpContext(contextParams)
  }

  logInfo(s"UCX context created")

  // this object implements the transport-friendly interface for UCX
  private[this] val serverConnection = new UCXServerConnection(this)

  // monotonically increasing counter that holds the txId (for debug purposes, at this stage)
  private[this] val txId = new AtomicLong(0L)

  private var worker: UcpWorker = _
  private val endpoints = new ConcurrentHashMap[Long, UcpEndpoint]()
  @volatile private var initialized = false

  // a peer tag identifies an incoming connection uniquely
  private val peerTag = new AtomicLong(0) // peer tags

  // this is a monotonically increasing id for every response
  private val responseTag = new AtomicLong(0) // outgoing message tag

  // event loop, used to call [[UcpWorker.progress]], and perform all UCX work
  private val progressThread = Executors.newFixedThreadPool(1,
    GpuDeviceManager.wrapThreadFactory(
      new ThreadFactoryBuilder()
        .setNameFormat("progress-thread-%d")
        .setDaemon(true)
        .build))

  // management port socket
  private var serverSocket: ServerSocket = _
  private val acceptService = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setNameFormat("ucx-mgmt-thread-%d").build)

  private val serverService = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setNameFormat("ucx-connection-server-%d").build)

  // The pending queues are used to enqueue [[PendingReceive]] or [[PendingSend]], from executor
  // task threads and [[progressThread]] will hand them to the UcpWorker thread.
  private val workerTasks = new ConcurrentLinkedQueue[() => Unit]()

  // Multiple executor threads are going to try to call connect (from the
  // [[RapidsUCXShuffleIterator]]) because they don't know what executors to connect to up until
  // shuffle fetch time.
  //
  // This makes sure that all executor threads get the same [[Connection]] object for a specific
  // management (host, port) key.
  private val connectionCache = new ConcurrentHashMap[Long, ClientConnection]()
  private val executorIdToPeerTag = new ConcurrentHashMap[Long, Long]()

  // holds memory registered against UCX that should be de-register on exit (used for bounce
  // buffers)
  val registeredMemory = new ArrayBuffer[UcpMemory]

  /**
   * Initializes the UCX context and local worker and starts up the worker progress thread.
   * UCX worker/endpoint relationship.
   */
  def init(): Unit = {
    synchronized {
      if (initialized) {
        throw new IllegalStateException("UCX already initialized")
      }

      var workerParams = new UcpWorkerParams()

      if (usingWakeupFeature) {
        workerParams = workerParams
          .requestWakeupTagSend()
          .requestWakeupTagRecv()
      }

      worker = context.newWorker(workerParams)
      logInfo(s"UCX Worker created")
      initialized = true
    }

    progressThread.execute(() => {
      // utility function to make all the progress possible in each iteration
      // this could change in the future to 1 progress call per loop, or be used
      // entirely differently once polling is figured out
      def drainWorker(): Unit = {
        val nvtxRange = new NvtxRange("UCX Draining Worker", NvtxColor.RED)
        try {
          while (worker.progress() > 0) {}
        } finally {
          nvtxRange.close()
        }
      }

      while(initialized) {
        try {
          worker.progress()
          // else worker.progress returned 0
          if (usingWakeupFeature) {
            drainWorker()
            val sleepRange = new NvtxRange("UCX Sleeping", NvtxColor.PURPLE)
            try {
              worker.waitForEvents()
            } finally {
              sleepRange.close()
            }
          }

          while (!workerTasks.isEmpty) {
            val nvtxRange = new NvtxRange("UCX Handling Tasks", NvtxColor.CYAN)
            try {
              val wt = workerTasks.poll()
              if (wt != null) {
                wt()
              }
            } finally {
              nvtxRange.close()
            }
            worker.progress()
          }
        } catch {
          case t: Throwable =>
            logError("Exception caught in UCX progress thread. Continuing.", t)
        }
      }
    })
  }

  /**
   * Starts a TCP server to listen for external clients, returning with
   * what port it used.
   *
   * @param mgmtHost String the hostname to bind to
   * @return port bound
   */
  def startManagementPort(mgmtHost: String): Int = {
    var portBindAttempts = 100
    var portBound = false
    while (!portBound && portBindAttempts > 0) {
      try {
        logInfo(s"Starting ephemeral UCX management port at host $mgmtHost")
        // TODO: use ucx listener for this
        serverSocket = new ServerSocket()
        // open a TCP/IP socket to connect to a client
        // send the worker address to the client who wants to talk to us
        // associate with [[onNewConnection]]
        try {
          serverSocket.bind(new InetSocketAddress(mgmtHost, 0))
        } catch {
          case ioe: IOException =>
            logError(s"Unable to bind using host [$mgmtHost]", ioe)
            throw ioe
        }
        logInfo(s"Successfully bound to $mgmtHost:${serverSocket.getLocalPort}")
        portBound = true

        acceptService.execute(() => {
          while (initialized) {
            logInfo(s"Accepting UCX management connections.")
            try {
              val s = serverSocket.accept()
              // throw into a thread pool to actually handle the stream
              serverService.execute(() => {
                // disable Nagle's algorithm, in hopes of data not buffered by TCP
                s.setTcpNoDelay(true)
                handleSocket(s)
              })
            } catch {
              case e: Throwable if initialized =>
                // This will cause the `SparkUncaughtExceptionHandler` to get invoked
                // and it will shut down the executor (as it should).
                throw e
              case _: SocketException if !initialized =>
                // `initialized = false` means we are shutting down,
                // the socket will throw `SocketException` in this case
                // to unblock the accept, when `close()` is called.
                logWarning(s"UCX management socket closing")
              case ue: Throwable =>
                // a catch-all in case we get a non `SocketException` while closing (!initialized)
                logError(s"Unexpected exception while closing UCX management socket", ue)
            }
          }
        })
      } catch {
        case ioe: IOException =>
          logWarning(s"Retrying bind attempts $portBindAttempts", ioe)
          portBindAttempts = portBindAttempts - 1
      }
    }
    if (!portBound) {
      throw new IllegalStateException(s"Cannot bind UCX, tried $portBindAttempts times")
    }
    serverSocket.getLocalPort
  }

  // LOW LEVEL API
  def send(endpointId: Long, alt: AddressLengthTag, cb: UCXTagCallback): Unit = {
    val ucxCb = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        if (ucsStatus == UCX.UCS_ERR_CANCELED) {
          logWarning(
            s"Cancelled: tag=${TransportUtils.formatTag(alt.tag)}," +
              s" status=$ucsStatus, msg=$errorMsg")
          cb.onCancel(alt)
        } else {
          logError("error sending : " + ucsStatus + " " + errorMsg)
          cb.onError(alt, ucsStatus, errorMsg)
        }
      }

      override def onSuccess(request: UcpRequest): Unit = {
        cb.onSuccess(alt)
      }
    }

    onWorkerThreadAsync(() => {
      val ep = endpoints.get(endpointId)
      if (ep == null) {
        throw new IllegalStateException(s"I cant find endpoint $endpointId")
      }

      val request = ep.sendTaggedNonBlocking(alt.address, alt.length, alt.tag, ucxCb)
      cb.onMessageStarted(request)
    })
  }


  def getServerConnection: UCXServerConnection = serverConnection

  def receive(alt: AddressLengthTag, cb: UCXTagCallback): Unit = {
    val ucxCb = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        if (ucsStatus == UCX.UCS_ERR_CANCELED) {
          logWarning(
            s"Cancelled: tag=${TransportUtils.formatTag(alt.tag)}," +
              s" status=$ucsStatus, msg=$errorMsg")
          cb.onCancel(alt)
        } else {
          logError(s"Error receiving: $ucsStatus $errorMsg => $alt")
          cb.onError(alt, ucsStatus, errorMsg)
        }
      }

      override def onSuccess(request: UcpRequest): Unit = {
        logTrace(s"Success receiving calling callback ${TransportUtils.formatTag(alt.tag)}")
        cb.onSuccess(alt)
      }
    }

    onWorkerThreadAsync(() => {
      logTrace(s"Handling receive for tag ${TransportUtils.formatTag(alt.tag)}")
      val request = worker.recvTaggedNonBlocking(
        alt.address,
        alt.length,
        alt.tag,
        UCX.MATCH_FULL_TAG,
        ucxCb)
      cb.onMessageStarted(request)
    })
  }

  def cancel(request: UcpRequest): Unit = {
    onWorkerThreadAsync(() => {
      try {
        worker.cancelRequest(request)
        request.close()
      } catch {
        case e: Throwable =>
          logError("Error while cancelling UCX request: ", e)
      }
    })
  }

  private[ucx] def assignResponseTag(): Long = responseTag.incrementAndGet()

  private def ucxWorkerAddress: ByteBuffer = worker.getAddress

  /**
   * Establish a new [[UcpEndpoint]] given a [[WorkerAddress]]. It also
   * caches them s.t. at [[close]] time we can release resources.
   *
   * @param endpointId    presently an executorId, it is used to distinguish between endpoints
   *                      when routing messages outbound
   * @param workerAddress the worker address for the remote endpoint (ucx opaque object)
   * @return returns a [[UcpEndpoint]] that can later be used to send on (from the
   *         progress thread)
   */
  private[ucx] def setupEndpoint(endpointId: Long, workerAddress: WorkerAddress): UcpEndpoint = {
    logDebug(s"Starting/reusing an endpoint to $workerAddress with id $endpointId")
    // create an UCX endpoint using workerAddress
    endpoints.computeIfAbsent(endpointId,
      (_: Long) => {
        logInfo(s"No endpoint found for $endpointId. Adding it.")
        worker.newEndpoint(
          new UcpEndpointParams()
            .setUcpAddress(workerAddress.address))
      })
  }

  /**
   * Connect to a remote UCX management port.
   *
   * @param peerMgmtHost management TCP host
   * @param peerMgmtPort management TCP port
   * @return Connection object representing this connection
   */
  def getConnection(peerExecutorId: Int,
      peerMgmtHost: String,
      peerMgmtPort: Int): ClientConnection = {
    connectionCache.computeIfAbsent(peerExecutorId, _ => {
      val connection = new UCXClientConnection(peerExecutorId, peerTag.incrementAndGet(), this)
      startConnection(connection, peerMgmtHost, peerMgmtPort)
      connection
    })
  }

  private[ucx] def onWorkerThreadAsync(task: () => Unit): Unit = {
    workerTasks.add(task)
    if (usingWakeupFeature) {
      worker.signal()
    }
  }

  // client side
  private def startConnection(connection: UCXClientConnection,
      peerMgmtHost: String,
      peerMgmtPort: Int) = {
    logInfo(s"Connecting to $peerMgmtHost to $peerMgmtPort")
    val nvtx = new NvtxRange(s"UCX Connect to $peerMgmtHost:$peerMgmtPort", NvtxColor.RED)
    try {
      val socket = new Socket(peerMgmtHost, peerMgmtPort)
      try {
        socket.setTcpNoDelay(true)
        val os = socket.getOutputStream
        val is = socket.getInputStream

        // "this executor id will receive on tmpLocalReceiveTag for this Connection"
        UCXConnection.writeHandshakeHeader(os, ucxWorkerAddress, executorId)

        // "the remote executor will receive on remoteReceiveTag, and expects this executor to
        // receive on localReceiveTag"
        val (peerWorkerAddress, remoteExecutorId) = UCXConnection.readHandshakeHeader(is)

        val peerExecutorId = connection.getPeerExecutorId
        if (remoteExecutorId != peerExecutorId) {
          throw new IllegalStateException(s"Attempted to reach executor $peerExecutorId, but" +
            s" instead received reply from $remoteExecutorId")
        }

        onWorkerThreadAsync(() => {
          setupEndpoint(remoteExecutorId, peerWorkerAddress)
        })

        logInfo(s"NEW OUTGOING UCX CONNECTION $connection")
      } finally {
        socket.close()
      }
      connection
    } finally {
      nvtx.close()
    }
  }

  def assignPeerTag(peerExecutorId: Long): Long =
    executorIdToPeerTag.computeIfAbsent(peerExecutorId, _ => peerTag.incrementAndGet())

  /**
   * Handle an incoming connection on the TCP management port
   * This will fetch the [[WorkerAddress]] from the peer, and establish a UcpEndpoint
   *
   * @param socket an accepted socket to a remote client
   */
  private[ucx] def handleSocket(socket: Socket): Unit = {
    val connectionRange =
      new NvtxRange(s"UCX Handle Connection from ${socket.getInetAddress}", NvtxColor.RED)
    try {
      logDebug(s"Reading worker address from: $socket")
      try {
        val is = socket.getInputStream
        val os = socket.getOutputStream

        // get the peer worker address, we need to store this so we can send to this tag
        val (peerWorkerAddress: WorkerAddress, peerExecutorId: Int) =
          UCXConnection.readHandshakeHeader(is)

        logInfo(s"Got peer worker address from executor $peerExecutorId")

        // ack what we saw as the local and remote peer tags
        UCXConnection.writeHandshakeHeader(os, ucxWorkerAddress, executorId)

        onWorkerThreadAsync(() => {
          setupEndpoint(peerExecutorId, peerWorkerAddress)
        })

        // peer would have established an endpoint peer -> local
        logInfo(s"Sent server UCX worker address to executor $peerExecutorId")
      } finally {
        // at this point we have handshaked, UCX is ready to go for this point-to-point connection.
        // assume that we get a list of block ids, tag tuples we want to transfer out
        socket.close()
      }
    } finally {
      connectionRange.close()
    }
  }

  def register(rootBuffer: MemoryBuffer, mmapCallback: MemoryRegistrationCallback): Unit = {
    onWorkerThreadAsync(() => {
      val mmapParam = new UcpMemMapParams()
        .setAddress(rootBuffer.getAddress)
        .setLength(rootBuffer.getLength)

      //note that this can throw, lets call back and let caller figure out how to handle
      try {
        registeredMemory += context.memoryMap(mmapParam)
        mmapCallback(true)
      } catch {
        case t: Throwable =>
          logError(s"There was an issue registering $rootBuffer against UCX", t)
          mmapCallback(false)
      }
    })
  }

  def getNextTransactionId: Long = txId.incrementAndGet()

  override def close(): Unit = {
    onWorkerThreadAsync(() => {
      logInfo(s"De-registering UCX ${registeredMemory.size} memory buffers.")
      registeredMemory.foreach(_.deregister())
      registeredMemory.clear()
      synchronized {
        initialized = false
        notifyAll()
        // exit the loop
      }
    })

    synchronized {
      while (initialized) {
        wait(100)
      }
    }

    if (serverSocket != null) {
      serverSocket.close()
      serverSocket = null
    }

    if (usingWakeupFeature && worker != null) {
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

    endpoints.values().forEach(ep => ep.close())

    if (worker != null) {
      worker.close()
    }

    context.close()
  }
}

object UCX {
  // This is used to distinguish a cancelled request vs. other errors
  // as the callback is the same (onError)
  // from https://github.com/openucx/ucx/blob/master/src/ucs/type/status.h
  private val UCS_ERR_CANCELED = -16

  // We may consider matching tags partially for different request types
  private val MATCH_FULL_TAG: Long = 0xFFFFFFFFFFFFFFFFL
}
