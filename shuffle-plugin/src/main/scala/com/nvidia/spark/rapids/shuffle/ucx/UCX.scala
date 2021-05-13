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
import java.net._
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import ai.rapids.cudf.{MemoryBuffer, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.{Arm, GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.shuffle.{AddressLengthTag, ClientConnection, MemoryRegistrationCallback, TransportUtils}
import org.openucx.jucx._
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.storage.RapidsStorageUtils
import org.apache.spark.storage.BlockManagerId

case class WorkerAddress(address: ByteBuffer)

case class Rkeys(rkeys: Seq[ByteBuffer])

/**
 * A simple wrapper specifically for an Active Message Id and a small (Long) header
 * we are using per message.
 */
case class UCXActiveMessage(activeMessageId: Int,
                            header: Option[Long] = None,
                            cb: Option[UCXAmCallback] = None,
                            singleShotCallbackGen: Option[() => UCXAmCallback] = None) {
  /**
   * Request transactions need to be indepenent of each other. This function
   * creates callbacks (which in turn creates transactions), so that each request
   * can be handled independently.
   * @return a brand new callback instance
   */
  def getSingleShotCallback: () => UCXAmCallback = {
    require(singleShotCallbackGen.nonEmpty,
      s"Tried to generate a single shot callback with an invalid UCXActiveMessage $this")
    singleShotCallbackGen.get
  }

  override def toString: String = {
    val hdrString = if (header.isDefined) {
      TransportUtils.toHex(header.get)
    } else {
      "N/A"
    }
    s"[amId=${TransportUtils.toHex(activeMessageId)}, hdr=$hdrString]"
  }
}

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
 * @param transport transport instance for UCX
 * @param executor blockManagerId of the local executorId
 * @param rapidsConf rapids configuration
 */
class UCX(transport: UCXShuffleTransport, executor: BlockManagerId,
                       rapidsConf: RapidsConf)
    extends AutoCloseable with Logging with Arm {
  private[this] val context = {
    val contextParams = new UcpParams()
      .requestTagFeature()
      .requestAmFeature()
    if (rapidsConf.shuffleUcxUseWakeup) {
      contextParams.requestWakeupFeature()
    }
    new UcpContext(contextParams)
  }

  logInfo(s"UCX context created")

  def getExecutorId: Int = executor.executorId.toInt

  // this object implements the transport-friendly interface for UCX
  private[this] val serverConnection = new UCXServerConnection(this, transport)

  // monotonically increasing counter that holds the txId (for debug purposes, at this stage)
  private[this] val txId = new AtomicLong(0L)

  private var worker: UcpWorker = _
  private var listener: Option[UcpListener] = None
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

  // holds memory registered against UCX that should be de-register on exit (used for bounce
  // buffers)
  // NOTE: callers should hold the `registeredMemory` lock before modifying this array
  val registeredMemory = new ArrayBuffer[UcpMemory]

  // when this flag is set to true, an async call to `register` hasn't completed in
  // the worker thread. We need this to complete prior to getting the `rkeys`.
  private var pendingRegistration = false

  // Error handler that would be invoked on endpoint failure.
  private val epErrorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      withResource(ucpEndpoint) { _ =>
        if (errorCode != UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
          logError(s"Endpoint to $ucpEndpoint got error: $errorString")
        }
        endpoints.values().removeIf(ep => ep == ucpEndpoint)
      }
    }
  }

  // Common endpoint parameters.
  private def getEpParams = {
    val result = new UcpEndpointParams()
    if (rapidsConf.shuffleUcxUsePeerErrorHandler) {
      logDebug("Using peer error handling")
      result.setErrorHandler(epErrorHandler).setPeerErrorHandlingMode()
    }
    result
  }

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

      if (rapidsConf.shuffleUcxUseWakeup) {
        workerParams = workerParams
          .requestWakeupTagSend()
          .requestWakeupTagRecv()
      }

      worker = context.newWorker(workerParams)
      logInfo(s"UCX Worker created")
      if (rapidsConf.shuffleUcxUseSockaddr) {
        // For now backward endpoints are not used, but need to create
        // an endpoint from connectionHandler in order to use ucpListener connections.
        // With AM this endpoints would be used as replyEp.
        val backwardEpId = new AtomicInteger(0)
        val ucpListenerParams = new UcpListenerParams().setConnectionHandler(
          (connectionRequest: UcpConnectionRequest) => {
            logDebug(s"Got connection request from ${connectionRequest.getClientAddress}")
            endpoints.computeIfAbsent(backwardEpId.decrementAndGet(),
              _ => worker.newEndpoint(getEpParams.setConnectionRequest(connectionRequest)))
          })
        val maxRetries = SparkEnv.get.conf.getInt("spark.port.maxRetries", 16)
        val startPort = if (rapidsConf.shuffleUcxListenerStartPort != 0) {
          rapidsConf.shuffleUcxListenerStartPort
        } else {
          // TODO: remove this once ucx1.11 with random port selection would be released
          1024 + Random.nextInt(65535 - 1024)
        }
        for (i <- 0 until maxRetries) {
          val sockAddress = new InetSocketAddress(executor.host, startPort + i)
          try {
            ucpListenerParams.setSockAddr(sockAddress)
            listener = Option(worker.newListener(ucpListenerParams))
          } catch {
            case ex: UcxException =>
              logDebug(s"Failed to bind UcpListener on $sockAddress")
              listener = None
          }
        }
        if (listener.isEmpty) {
          throw new BindException(s"Couldn't start UcpListener " +
            s"on port range $startPort-${startPort + maxRetries}")
        }
        logInfo(s"Started UcpListener on ${listener.get.getAddress}")
      }
      initialized = true
    }

    progressThread.execute(() => {
      // utility function to make all the progress possible in each iteration
      // this could change in the future to 1 progress call per loop, or be used
      // entirely differently once polling is figured out
      def drainWorker(): Unit = {
        withResource(new NvtxRange("UCX Draining Worker", NvtxColor.RED)) { _ =>
          while (worker.progress() > 0) {}
        }
      }

      while(initialized) {
        try {
          worker.progress()
          // else worker.progress returned 0
          if (rapidsConf.shuffleUcxUseWakeup) {
            drainWorker()
            withResource(new NvtxRange("UCX Sleeping", NvtxColor.PURPLE)) { _ =>
              worker.waitForEvents()
            }
          }

          while (!workerTasks.isEmpty) {
            withResource(new NvtxRange("UCX Handling Tasks", NvtxColor.CYAN)) { _ =>
              val wt = workerTasks.poll()
              if (wt != null) {
                wt()
              }
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
            s"Cancelled: tag=${TransportUtils.toHex(alt.tag)}," +
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

  /**
   * Wraps interest in an Active Message Id, and potential callback(s) that should
   * be triggered when a message is received.
   *
   * Note that if a `singleCallback` is called, header is ignored, since this is a
   * request we are handling => the request echoes headers to the response (who do need
   * to pay attention to different callbacks)
   */
  private class ActiveMessageRegistration(activeMessageId: Int) {
    def getCallback(hdr: Option[Long]): UCXAmCallback = {
      if (singleCallback != null) {
        singleCallback()
      } else {
        require(hdr.isDefined, "Attempting to handle a response but the header was not valid!")
        // responses need a callback per header
        val cb = perHeaderCallbacks.get(hdr.get)
        require (cb != null,
          s"Failed to get an Active Message callback for $activeMessageId and header $hdr")
        cb
      }
    }

    def setSingleActiveMessageHandler(am: UCXActiveMessage): Unit = {
      require(perHeaderCallbacks.isEmpty)
      singleCallback = am.getSingleShotCallback
    }

    def addActiveMessageHandler(am: UCXActiveMessage): Unit = {
      // used to match an incoming header with a callback to trigger
      require(am.header.isDefined,
        "Attempting to set an Active Message response handler without a header")
      require(am.cb.isDefined,
        "Attempting to set an Active Message response handler without a callback")
      require(singleCallback == null)
      perHeaderCallbacks.put(am.header.get, am.cb.get)
    }

    private var singleCallback: () => UCXAmCallback = null
    private val perHeaderCallbacks = new ConcurrentHashMap[Long, UCXAmCallback]()
  }

  private val amRegistrations = new ConcurrentHashMap[Int, ActiveMessageRegistration]()

  /**
   * Register a response handler (clients will use this)
   * @note This function will be called for each client, with the same `am.activeMessageId`
   * @param activeMessage - An Active Message to register response interest for
   */
  def registerResponseHandler(activeMessage: UCXActiveMessage): Unit = {
    logDebug(s"Register Active Message $activeMessage response handler")

    val reg = amRegistrations.computeIfAbsent(activeMessage.activeMessageId,
      _ => {
        val reg = new ActiveMessageRegistration(activeMessage.activeMessageId)
        registerActiveMessage(activeMessage.activeMessageId, reg)
        reg
      })

    reg.addActiveMessageHandler(activeMessage)
  }

  /**
   * Register a request handler (the server will use this)
   * @note This function will be called once for the server for an `activeMessageId`
   * @param activeMessage
   */
  def registerRequestHandler(activeMessage: UCXActiveMessage): Unit = {
    logDebug(s"Register Active Message $activeMessage request handler")
    val activeMessageId = activeMessage.activeMessageId

    require(!amRegistrations.containsKey(activeMessageId),
      s"Tried to re-register a request handler for $activeMessageId")
    amRegistrations.computeIfAbsent(activeMessageId,
      _ => {
        val reg = new ActiveMessageRegistration(activeMessageId)
        reg.setSingleActiveMessageHandler(activeMessage)
        registerActiveMessage(activeMessageId, reg)
        reg
      })
  }

  private def registerActiveMessage(activeMessageId: Int, reg: ActiveMessageRegistration): Unit = {
    onWorkerThreadAsync(() => {
      worker.setAmRecvHandler(activeMessageId,
        (headerAddr, headerSize, amData: UcpAmData, _) => {
          val hdr = if (headerSize == 8) {
            Option(UcxUtils.getByteBufferView(headerAddr, headerSize).getLong)
          } else {
            None
          }

          val am = UCXActiveMessage(activeMessageId, hdr)

          logDebug(s"Active Message callback: $am")

          val cb = reg.getCallback(hdr)

          if (amData.isDataValid) {
            require(notForcingAmRndv,
              s"Handling an eager Active Message, but we are using " +
                s"'${rapidsConf.shuffleUcxActiveMessagesMode}' as our configured mode.")
            logDebug(s"Handling an EAGER active message receive ${amData}")
            val resp = UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength)

            // copy the data onto a buffer we own because it is going to be reused
            // in UCX
            val dbb = cb.onHostMessageReceived(amData.getLength)
            val bb = dbb.getBuffer()
            bb.put(resp)
            bb.rewind()
            cb.onSuccess(am, dbb)

            // we return OK telling UCX `amData` is ok to be closed, along with the eagerly
            // received data
            UcsConstants.STATUS.UCS_OK
          } else {
            // RNDV case: we get a direct buffer and UCX will fill it with data at `receive`
            // callback
            val resp = cb.onHostMessageReceived(amData.getLength)

            val receiveAm = amData.receive(UcxUtils.getAddress(resp.getBuffer()),
              new UcxCallback {
                override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                  withResource(resp) { _ =>
                    withResource(amData) { _ =>
                      logError(s"Error receiving: $ucsStatus $errorMsg => $activeMessageId ${hdr}")
                      if (ucsStatus == UCX.UCS_ERR_CANCELED) {
                        logWarning(
                          s"Cancelled Active Message " +
                            s"${TransportUtils.toHex(activeMessageId)}" +
                            s" status=$ucsStatus, msg=$errorMsg")
                        cb.onCancel(am)
                      } else {
                        cb.onError(am, ucsStatus, errorMsg)
                      }
                    }
                  }
                }
                override def onSuccess(request: UcpRequest): Unit = {
                  withResource(amData) { _ =>
                    cb.onSuccess(am, resp)
                  }
                }
              })

            cb.onMessageStarted(receiveAm)
            UcsConstants.STATUS.UCS_INPROGRESS
          }
        })
    })
  }

  // If we are not forcing RNDV (i.e. we are in auto or eager) other handling
  // can happen when we receive an Active Message message (it can contain
  // inline data that must be copied out in the callback).
  private def notForcingAmRndv: Boolean = {
    !rapidsConf.shuffleUcxActiveMessagesMode
      .equalsIgnoreCase("rndv")
  }

  private def activeMessageMode: Long = {
    rapidsConf.shuffleUcxActiveMessagesMode match {
      case "eager" =>
        UcpConstants.UCP_AM_SEND_FLAG_EAGER
      case "rndv" =>
        UcpConstants.UCP_AM_SEND_FLAG_RNDV
      case "auto" =>
        0L
    }
  }

  def sendActiveMessage(endpointId: Long, am: UCXActiveMessage,
                        dataAddress: Long, dataSize: Long, cb: UcxCallback): Unit = {
    require(am.header.isDefined,
      "An Active Message request must include a header so the response can find the " +
        "correct callback.")
    onWorkerThreadAsync(() => {
      val ep = endpoints.get(endpointId)
      if (ep == null) {
        throw new IllegalStateException(
          s"Trying to send a message to an endpoint that doesn't exist ${endpointId}")
      }
      logDebug(s"Sending $am msg of size $dataSize")

      // This isn't coming from the pool right now because it would be a bit of a
      // waste to get a larger hard-partitioned buffer just for 8 bytes.
      // TODO: since we no longer have metadata limits, the pool can be managed using the
      //   address-space allocator, so we should obtain this direct buffer from that pool
      val header = ByteBuffer.allocateDirect(8)
      header.putLong(am.header.get)
      header.rewind()

      ep.sendAmNonBlocking(
        am.activeMessageId,
        TransportUtils.getAddress(header),
        header.remaining(),
        dataAddress,
        dataSize,
        activeMessageMode,
        new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            cb.onSuccess(request)
            RapidsStorageUtils.dispose(header)
          }

          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            cb.onError(ucsStatus, errorMsg)
            RapidsStorageUtils.dispose(header)
          }
        })
    })
  }

  def getServerConnection: UCXServerConnection = serverConnection

  def receive(alt: AddressLengthTag, cb: UCXTagCallback): Unit = {
    val ucxCb = new UcxCallback {
      override def onError(ucsStatus: Int, errorMsg: String): Unit = {
        if (ucsStatus == UCX.UCS_ERR_CANCELED) {
          logWarning(
            s"Cancelled: tag=${TransportUtils.toHex(alt.tag)}," +
              s" status=$ucsStatus, msg=$errorMsg")
          cb.onCancel(alt)
        } else {
          logError(s"Error receiving: $ucsStatus $errorMsg => $alt")
          cb.onError(alt, ucsStatus, errorMsg)
        }
      }

      override def onSuccess(request: UcpRequest): Unit = {
        logTrace(s"Success receiving calling callback ${TransportUtils.toHex(alt.tag)}")
        cb.onSuccess(alt)
      }
    }

    onWorkerThreadAsync(() => {
      logTrace(s"Handling receive for tag ${TransportUtils.toHex(alt.tag)}")
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
      } catch {
        case e: Throwable =>
          logError("Error while cancelling UCX request: ", e)
      }
    })
  }

  def assignResponseTag(): Long = responseTag.incrementAndGet()

  private lazy val ucxAddress: ByteBuffer = if (rapidsConf.shuffleUcxUseSockaddr) {
    val listenerAddress = listener.get.getAddress
    val hostnameBytes = listenerAddress.getAddress.getAddress
    val result = ByteBuffer.allocateDirect(4 + hostnameBytes.length)
    result.putInt(listenerAddress.getPort)
    result.put(hostnameBytes)
    result.rewind()
    result
  } else {
    worker.getAddress
  }

  private def getUcxAddress: ByteBuffer = ucxAddress.asReadOnlyBuffer()

  /**
   * Establish a new [[UcpEndpoint]] given a [[WorkerAddress]]. It also
   * caches them s.t. at [[close]] time we can release resources.
   *
   * @param endpointId    presently an executorId, it is used to distinguish between endpoints
   *                      when routing messages outbound
   * @param workerAddress the worker address for the remote endpoint (ucx opaque object)
   * @param peerRkeys list of UCX rkeys that the peer has sent us for unpacking
   * @return returns a [[UcpEndpoint]] that can later be used to send on (from the
   *         progress thread)
   */
  def setupEndpoint(
      endpointId: Long, workerAddress: WorkerAddress, peerRkeys: Rkeys): UcpEndpoint = {
    logDebug(s"Starting/reusing an endpoint to $workerAddress with id $endpointId")
    // create an UCX endpoint using workerAddress or socket address
    val epParams = getEpParams
    endpoints.computeIfAbsent(endpointId,
      (_: Long) => {
        logInfo(s"No endpoint found for $endpointId. Adding it.")
        if (rapidsConf.shuffleUcxUseSockaddr) {
          val port = workerAddress.address.getInt
          val hostBytes = new Array[Byte](workerAddress.address.remaining())
          workerAddress.address.get(hostBytes)
          val hostAddress = InetAddress.getByAddress(hostBytes)
          val sockAddr = new InetSocketAddress(hostAddress, port)
          epParams.setSocketAddress(sockAddr)
        } else {
          epParams.setUcpAddress(workerAddress.address)
        }
        val ep = worker.newEndpoint(epParams)
        peerRkeys.rkeys.foreach(ep.unpackRemoteKey)
        ep
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
    val getConnectionStartTime = System.currentTimeMillis()
    val result = connectionCache.computeIfAbsent(peerExecutorId, _ => {
      val connection = new UCXClientConnection(
        peerExecutorId, peerTag.incrementAndGet(), this, transport)
      startConnection(connection, peerMgmtHost, peerMgmtPort)
      connection
    })
    logDebug(s"Got connection for executor ${peerExecutorId} in " +
      s"${System.currentTimeMillis() - getConnectionStartTime} ms")
    result
  }

  def onWorkerThreadAsync(task: () => Unit): Unit = {
    workerTasks.add(task)
    if (rapidsConf.shuffleUcxUseWakeup) {
      worker.signal()
    }
  }

  // client side
  private def startConnection(connection: UCXClientConnection,
      peerMgmtHost: String,
      peerMgmtPort: Int) = {
    logInfo(s"Connecting to $peerMgmtHost:$peerMgmtPort")
    withResource(new NvtxRange(s"UCX Connect to $peerMgmtHost:$peerMgmtPort", NvtxColor.RED)) { _ =>
      withResource(new Socket(peerMgmtHost, peerMgmtPort)) { socket =>
        socket.setTcpNoDelay(true)
        val os = socket.getOutputStream
        val is = socket.getInputStream

        // this executor id will receive on tmpLocalReceiveTag for this Connection
        UCXConnection.writeHandshakeHeader(os, getUcxAddress, getExecutorId, localRkeys)

        // the remote executor will receive on remoteReceiveTag, and expects this executor to
        // receive on localReceiveTag
        val (peerWorkerAddress, remoteExecutorId, peerRkeys) = UCXConnection.readHandshakeHeader(is)

        val peerExecutorId = connection.getPeerExecutorId
        if (remoteExecutorId != peerExecutorId) {
          throw new IllegalStateException(s"Attempted to reach executor $peerExecutorId, but" +
            s" instead received reply from $remoteExecutorId")
        }

        onWorkerThreadAsync(() => {
          setupEndpoint(remoteExecutorId, peerWorkerAddress, peerRkeys)
        })

        logInfo(s"NEW OUTGOING UCX CONNECTION $connection")
      }
      connection
    }
  }

  /**
   * Handle an incoming connection on the TCP management port
   * This will fetch the [[WorkerAddress]] from the peer, and establish a UcpEndpoint
   *
   * @param socket an accepted socket to a remote client
   */
  private def handleSocket(socket: Socket): Unit = {
    withResource(new NvtxRange(s"UCX Handle Connection from ${socket.getInetAddress}",
        NvtxColor.RED)) { _ =>
      logDebug(s"Reading worker address from: $socket")
      withResource(socket) { _ =>
        val is = socket.getInputStream
        val os = socket.getOutputStream

        // get the peer worker address, we need to store this so we can send to this tag
        val (peerWorkerAddress: WorkerAddress, peerExecutorId: Int, peerRkeys: Rkeys) =
          UCXConnection.readHandshakeHeader(is)

        logInfo(s"Got peer worker address from executor $peerExecutorId")

        // ack what we saw as the local and remote peer tags
        UCXConnection.writeHandshakeHeader(os, getUcxAddress, getExecutorId, localRkeys)

        onWorkerThreadAsync(() => {
          setupEndpoint(peerExecutorId, peerWorkerAddress, peerRkeys)
        })

        // peer would have established an endpoint peer -> local
        logInfo(s"Sent server UCX worker address to executor $peerExecutorId")
      }
    }
  }

  /**
   * Return rkeys (if we have registered memory)
   */
  private def localRkeys: Seq[ByteBuffer] = registeredMemory.synchronized {
    while (pendingRegistration) {
      registeredMemory.wait(100)
    }
    registeredMemory.map(_.getRemoteKeyBuffer)
  }

  /**
   * Register a set of `MemoryBuffers` against UCX.
   *
   * @param buffers to register
   * @param mmapCallback callback invoked when the memory map operation completes or fails
   */
  def register(buffers: Seq[MemoryBuffer], mmapCallback: MemoryRegistrationCallback): Unit =
    registeredMemory.synchronized {
      pendingRegistration = true

      onWorkerThreadAsync(() => {
        var error: Throwable = null
        registeredMemory.synchronized {
          try {
            buffers.foreach { buffer =>
              val mmapParam = new UcpMemMapParams()
                  .setAddress(buffer.getAddress)
                  .setLength(buffer.getLength)

              //note that this can throw, lets call back and let caller figure out how to handle
              try {
                val registered = context.memoryMap(mmapParam)
                registeredMemory += registered
              } catch {
                case t: Throwable =>
                  if (error == null) {
                    error = t
                  } else {
                    error.addSuppressed(t)
                  }
              }
            }
          } finally {
            mmapCallback(Option(error))
            pendingRegistration = false
            registeredMemory.notify()
          }
        }
      })
    }

  def getNextTransactionId: Long = txId.incrementAndGet()

  override def close(): Unit = {
    onWorkerThreadAsync(() => {
      amRegistrations.forEach { (activeMessageId, _) =>
        logInfo(s"Removing Active Message registration for " +
          s"${TransportUtils.toHex(activeMessageId)}")
        worker.removeAmRecvHandler(activeMessageId)
      }

      logInfo(s"De-registering UCX ${registeredMemory.size} memory buffers.")
      registeredMemory.synchronized {
        registeredMemory.foreach(_.deregister())
        registeredMemory.clear()
      }
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

    if (rapidsConf.shuffleUcxUseWakeup && worker != null) {
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
