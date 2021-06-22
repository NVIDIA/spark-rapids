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

import ai.rapids.cudf.{DeviceMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.{Arm, GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.shuffle.{ClientConnection, MemoryRegistrationCallback, MetadataTransportBuffer, TransportBuffer, TransportUtils}
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
 * A simple wrapper for an Active Message Id and a header. This pair
 * is used together when dealing with Active Messages, with `activeMessageId`
 * being a fire-and-forget registration with UCX, and `header` being a dynamic long
 * we continue to update (it contains the local executor id, and the transaction id).
 *
 * This allows us to send a request (with a header that the response handler knows about),
 * and for the request handler to echo back that header when it's done.
 */
case class UCXActiveMessage(activeMessageId: Int, header: Long, forceRndv: Boolean) {
  override def toString: String =
    UCX.formatAmIdAndHeader(activeMessageId, header)
}

/**
 * The UCX class wraps JUCX classes and handles all communication with UCX from other
 * parts of the shuffle code. It manages a `UcpContext` and `UcpWorker`, for the
 * local executor, and maintain a set of `UcpEndpoint` for peers.
 *
 * This class uses an extra TCP management connection to perform a handshake with remote peers,
 * this port should be distributed to peers by other means (e.g. via the `BlockManagerId`)
 *
 * @param transport transport instance for UCX
 * @param executor blockManagerId of the local executorId
 * @param rapidsConf rapids configuration
 */
class UCX(transport: UCXShuffleTransport, executor: BlockManagerId, rapidsConf: RapidsConf)
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

  // this is a monotonically increasing id, used for buffers
  private val uniqueIds = new AtomicLong(0)

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

  // There will be 1 entry in this map per UCX-registered Active Message. Presently
  // that means: 2 request active messages (Metadata and Transfer Request), and 2
  // response active messages (Metadata and Transfer response).
  private val amRegistrations = new ConcurrentHashMap[Int, ActiveMessageRegistration]()

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
        var attempt = 0
        while (listener.isEmpty && attempt < maxRetries) {
          val sockAddress = new InetSocketAddress(executor.host, startPort + attempt)
          attempt += 1
          try {
            ucpListenerParams.setSockAddr(sockAddress)
            listener = Option(worker.newListener(ucpListenerParams))
          } catch {
            case _: UcxException =>
              logDebug(s"Failed to bind UcpListener on $sockAddress. " +
                s"Attempt $attempt out of $maxRetries.")
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
            if (workerTasks.isEmpty) {
              withResource(new NvtxRange("UCX Sleeping", NvtxColor.PURPLE)) { _ =>
                // Note that `waitForEvents` checks any events that have occurred, and will
                // return early in those cases. Therefore, `waitForEvents` is safe to be
                // called after `worker.signal()`, as it will wake up right away.
                worker.waitForEvents()
              }
            }
          }

          withResource(new NvtxRange("UCX Handling Tasks", NvtxColor.CYAN)) { _ =>
            while (!workerTasks.isEmpty) {
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


  /**
   * This trait and next two implementations represent the mapping between an Active Message Id
   * and the callback that should be triggered when a message is received.
   *
   * There are three types of Active Messages we care about: requests, responses, and buffers.
   *
   * For requests:
   *   - `activeMessageId` for requests is the value of the `MessageType` enum, and it is
   *   set once when the `RapidsShuffleServer` is initialized, and no new request handlers
   *   are established.
   *
   *   - The Active Message header is handed to the request handler, via the transaction.
   *   The request handler needs to echo the header back for the response handler on the
   *   other side of the request.
   *
   *   - On a request, a callback is instantiated using `requestCallbackGen`, which creates
   *   a transaction each time. This is one way to handle several requests inbound to a server.
   *
   *   - useRndv: is set to false. We expect UCX to be able to use eager protocols or rndv
   *   at its leasure. `rapidsConf.shuffleUcxActiveMessagesForceRndv` can be used to force rndv.
   *
   * For responses:
   *   - `activeMessageId` for responses is the value of the `MessageType` enum with an extra
   *   bit flipped (see `UCXConnection.composeResponseAmId`). These are also set once, as requests
   *   are sent out.
   *
   *   - The Active Message header is used to pick the correct callback to call. In this case
   *   there could be several expected responses, for a single response `activeMessageId`, so the
   *   server echoes back our header so we can invoke the correct response callback.
   *
   *   - Each response received at the response activeMessageId, will be demuxed using the header:
   *   responseActiveMessageId1 -> [callbackForHeader1, callbackForHeader2, ..., callbackForHeaderN]
   *
   *   - useRndv: is set to false. We expect UCX to be able to use eager protocols or rndv
   *   at its leasure. `rapidsConf.shuffleUcxActiveMessagesForceRndv` can be used to force rndv.
   *
   * For buffers:
   *   - `activeMessageId` for buffers is the value of the `MessageType` enum (`MessageType.Buffer`)
   *
   *   - The Active Message header is used to pick the correct callback to call. Each header
   *   encodes the peer we expect to receive from and a monotonically inscreasing id per request.
   *
   *   - useRndv: is set to true. We prefer the zero-copy method since the buffer pointer
   *   provided to UCX could be on the GPU. An eager message would require us to issue an H2D in
   *   the handler.
   */
  trait ActiveMessageRegistration {
    val activeMessageId: Int
    def getCallback(header: Long): UCXAmCallback
    def useRndv: Boolean
  }

  class ReceiveActiveMessageRegistration(override val activeMessageId: Int, mask: Long)
      extends ActiveMessageRegistration {

    private[this] val handlers = new ConcurrentHashMap[Long, () => UCXAmCallback]()

    def addWildcardHeaderHandler(wildcardHdr: Long, cbGen: () => UCXAmCallback): Unit = {
      handlers.put(wildcardHdr, cbGen)
    }

    override def getCallback(header: Long): UCXAmCallback = {
      // callback is picked using a wildcard
      val cb = handlers.get(header & mask)
      if (cb == null) {
        throw new IllegalStateException(s"UCX Receive Active Message callback not found for " +
          s"${TransportUtils.toHex(header)} and mask ${TransportUtils.toHex(mask)}")
      }
      cb()
    }
    override def useRndv: Boolean = true
  }

  class RequestActiveMessageRegistration(override val activeMessageId: Int,
                                         requestCbGen: () => UCXAmCallback)
      extends ActiveMessageRegistration {

    def getCallback(header: Long): UCXAmCallback = requestCbGen()

    override def useRndv: Boolean = rapidsConf.shuffleUcxActiveMessagesForceRndv
  }

  class ResponseActiveMessageRegistration(override val activeMessageId: Int)
      extends ActiveMessageRegistration {
    private[this] val responseCallbacks = new ConcurrentHashMap[Long, UCXAmCallback]()

    def getCallback(header: Long): UCXAmCallback = {
      val cb = responseCallbacks.remove(header) // 1 callback per header
      require (cb != null,
        s"Failed to get a response Active Message callback for " +
          s"${UCX.formatAmIdAndHeader(activeMessageId, header)}")
      cb
    }

    def addResponseActiveMessageHandler(
        header: Long, responseCallback: UCXAmCallback): Unit = {
      val prior = responseCallbacks.putIfAbsent(header, responseCallback)
      require(prior == null,
        s"Invalid Active Message re-registration of response handler for " +
          s"${UCX.formatAmIdAndHeader(activeMessageId, header)}")
    }

    override def useRndv: Boolean = rapidsConf.shuffleUcxActiveMessagesForceRndv
  }

  /**
   * Register a response handler (clients will use this)
   *
   * @note This function will be called for each client, with the same `am.activeMessageId`
   * @param activeMessageId (up to 5 bits) used to register with UCX an Active Message
   * @param header a long used to demux responses arriving at `activeMessageId`
   * @param responseCallback callback to handle a particular response
   */
  def registerResponseHandler(
      activeMessageId: Int, header: Long, responseCallback: UCXAmCallback): Unit = {
    logDebug(s"Register Active Message " +
      s"${UCX.formatAmIdAndHeader(activeMessageId, header)} response handler")

    amRegistrations.computeIfAbsent(activeMessageId,
      _ => {
        val reg = new ResponseActiveMessageRegistration(activeMessageId)
        registerActiveMessage(reg)
        reg
      }) match {
      case reg: ResponseActiveMessageRegistration =>
        reg.addResponseActiveMessageHandler(header, responseCallback)
      case other =>
        throw new IllegalStateException(
          s"Attempted to add a response Active Message handler to existing registration $other " +
            s"for ${UCX.formatAmIdAndHeader(activeMessageId, header)}")
    }
  }

  /**
   * Register a request handler (the server will use this)
   * @note This function will be called once for the server for an `activeMessageId`
   * @param activeMessageId (up to 5 bits) used to register with UCX an Active Message
   * @param requestCallbackGen a function that instantiates a callback to handle
   *                           a particular request
   */
  def registerRequestHandler(activeMessageId: Int,
      requestCallbackGen: () => UCXAmCallback): Unit = {
    logDebug(s"Register Active Message $TransportUtils.request handler")
    val reg = new RequestActiveMessageRegistration(activeMessageId, requestCallbackGen)
    val oldReg = amRegistrations.putIfAbsent(activeMessageId, reg)
    require(oldReg == null,
      s"Tried to re-register a request handler for $activeMessageId")
    registerActiveMessage(reg)
  }

  /**
   * Register a receive (one way) message, this is used by the clients.
   * @param activeMessageId (up to 5 bits) used to register with UCX an Active Message
   * @param hdrMask - a long with bits set for those bits that should be masked to find
   *                  the proper callback.
   * @param hdrWildcard - a long with an id that should be associated with a specific callback.
   * @param receiveCallbackGen - a function that instantiates a callback to handle
   *                             a particular receive.
   */
  def registerReceiveHandler(activeMessageId: Int, hdrMask: Long, hdrWildcard: Long,
      receiveCallbackGen: () => UCXAmCallback): Unit = {
    logDebug(s"Register Active Message ${TransportUtils.toHex(activeMessageId)} " +
      s"mask ${TransportUtils.toHex(hdrMask)} " +
      s"wild ${TransportUtils.toHex(hdrWildcard)} request handler")
    amRegistrations.computeIfAbsent(activeMessageId,
      _ => {
        val reg = new ReceiveActiveMessageRegistration(activeMessageId, hdrMask)
        registerActiveMessage(reg)
        reg
      }) match {
      case reg: ReceiveActiveMessageRegistration =>
        reg.addWildcardHeaderHandler(hdrWildcard, receiveCallbackGen)
      case other =>
        throw new IllegalStateException(
          s"Attempted to add a receive Active Message handler to existing registration $other " +
            s"for ${UCX.formatAmIdAndHeader(activeMessageId, hdrWildcard)}")
    }
  }

  private def registerActiveMessage(reg: ActiveMessageRegistration): Unit = {
    onWorkerThreadAsync(() => {
      worker.setAmRecvHandler(reg.activeMessageId,
        (headerAddr, headerSize, amData: UcpAmData, _) => {
          if (headerSize != 8) {
            // this is a coding error, so I am just blowing up. It should never happen.
            throw new IllegalStateException(
              s"Received message with wrong header size $headerSize")
          } else {
            val header = UcxUtils.getByteBufferView(headerAddr, headerSize).getLong()
            val am = UCXActiveMessage(reg.activeMessageId, header, reg.useRndv)

            withResource(new NvtxRange("AM Receive", NvtxColor.YELLOW)) { _ =>
              logDebug(s"Active Message received: $am")
              val cb = reg.getCallback(header)

              if (amData.isDataValid) {
                require(!reg.useRndv,
                  s"Handling an eager Active Message, but expected rndv for: " +
                    s"amId ${TransportUtils.toHex(reg.activeMessageId)}")
                val resp = UcxUtils.getByteBufferView(amData.getDataAddress, amData.getLength)

                // copy the data onto a buffer we own because it is going to be reused
                // in UCX
                cb.onMessageReceived(amData.getLength, header, {
                  case mtb: MetadataTransportBuffer =>
                    mtb.copy(resp)
                    cb.onSuccess(am, mtb)
                  case _ =>
                    cb.onError(am, 0,
                      "Received an eager message for non-metadata message")
                })

                // we return OK telling UCX `amData` is ok to be closed, along with the eagerly
                // received data
                UcsConstants.STATUS.UCS_OK
              } else {
                // RNDV case: we get a direct buffer and UCX will fill it with data at `receive`
                // callback
                cb.onMessageReceived(amData.getLength, header, (resp: TransportBuffer) => {
                  logDebug(s"Receiving Active Message ${am} using data address " +
                    s"${TransportUtils.toHex(resp.getAddress())}")

                  // we must call `receive` on the `amData` object within the progress thread
                  onWorkerThreadAsync(() => {
                    val receiveAm = amData.receive(resp.getAddress(),
                      new UcxCallback {
                        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                          withResource(resp) { _ =>
                            withResource(amData) { _ =>
                              if (ucsStatus == UCX.UCS_ERR_CANCELED) {
                                logWarning(
                                  s"Cancelled Active Message " +
                                    s"${TransportUtils.toHex(reg.activeMessageId)}" +
                                    s" status=$ucsStatus, msg=$errorMsg")
                                cb.onCancel(am)
                              } else {
                                cb.onError(am, ucsStatus, errorMsg)
                              }
                            }
                          }
                        }

                        override def onSuccess(request: UcpRequest): Unit = {
                          withResource(new NvtxRange("AM Success", NvtxColor.ORANGE)) { _ =>
                            withResource(amData) { _ =>
                              logDebug(s"Success with Active Message ${am} using data address " +
                                s"${TransportUtils.toHex(resp.getAddress())}")
                              cb.onSuccess(am, resp)
                            }
                          }
                        }
                      })
                    cb.onMessageStarted(receiveAm)
                  })
                })
                UcsConstants.STATUS.UCS_INPROGRESS
              }
            }
          }
        })
    })
  }

  def sendActiveMessage(endpointId: Long, am: UCXActiveMessage,
      data: MemoryBuffer, cb: UcxCallback): Unit = {
    sendActiveMessage(
      endpointId,
      am,
      data.getAddress,
      data.getLength,
      cb,
      isGpu = data.isInstanceOf[DeviceMemoryBuffer])
  }

  def sendActiveMessage(endpointId: Long, am: UCXActiveMessage,
      data: ByteBuffer, cb: UcxCallback): Unit = {
    sendActiveMessage(
      endpointId,
      am,
      TransportUtils.getAddress(data),
      data.remaining(),
      cb,
      isGpu = false)
  }

  private def sendActiveMessage(endpointId: Long, am: UCXActiveMessage,
      dataAddress: Long, dataSize: Long,
      cb: UcxCallback, isGpu: Boolean): Unit = {
    onWorkerThreadAsync(() => {
      val useRndv = am.forceRndv || rapidsConf.shuffleUcxActiveMessagesForceRndv
      val ep = endpoints.get(endpointId)
      if (ep == null) {
        throw new IllegalStateException(
          s"Trying to send a message to an endpoint that doesn't exist ${endpointId}")
      }
      logDebug(s"Sending $am msg of size $dataSize to peer ${endpointId} data addr: " +
        s"${TransportUtils.toHex(dataAddress)}. Is gpu? ${isGpu}")

      // This isn't coming from the pool right now because it would be a bit of a
      // waste to get a larger hard-partitioned buffer just for 8 bytes.
      // TODO: since we no longer have metadata limits, the pool can be managed using the
      //   address-space allocator, so we should obtain this direct buffer from that pool
      val header = ByteBuffer.allocateDirect(UCX.ACTIVE_MESSAGE_HEADER_SIZE.toInt)
      header.putLong(am.header)
      header.rewind()

      val flags = if (useRndv) {
        UcpConstants.UCP_AM_SEND_FLAG_RNDV
      } else {
        0L /* AUTO */
      }

      val memType = if (isGpu) {
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_CUDA
      } else {
        UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST
      }

      withResource(new NvtxRange("AM Send", NvtxColor.GREEN)) { _ =>
        ep.sendAmNonBlocking(
          am.activeMessageId,
          TransportUtils.getAddress(header),
          UCX.ACTIVE_MESSAGE_HEADER_SIZE,
          dataAddress,
          dataSize,
          flags,
          new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              cb.onSuccess(request)
              RapidsStorageUtils.dispose(header)
            }

            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              cb.onError(ucsStatus, errorMsg)
              RapidsStorageUtils.dispose(header)
            }
          }, memType)
      }
    })
  }

  def getServerConnection: UCXServerConnection = serverConnection

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

  def assignUniqueId(): Long = uniqueIds.incrementAndGet()

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
      val connection = new UCXClientConnection(peerExecutorId, this, transport)
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
      withResource(new NvtxRange("UCX Signal", NvtxColor.RED)) { _ =>
        worker.signal()
      }
    }
  }

  // client side
  private def startConnection(connection: UCXClientConnection,
      peerMgmtHost: String,
      peerMgmtPort: Int) = {
    logInfo(s"Connecting to $peerMgmtHost:$peerMgmtPort")
    withResource(new NvtxRange(s"UCX Connect to $peerMgmtHost:$peerMgmtPort", NvtxColor.RED)) { _ =>
      withResource(new Socket()) { socket =>
        socket.setTcpNoDelay(true)
        socket.connect(new InetSocketAddress(peerMgmtHost, peerMgmtPort),
          rapidsConf.shuffleUcxMgmtConnTimeout)
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

        val (peerWorkerAddress: WorkerAddress, peerExecutorId: Int, peerRkeys: Rkeys) =
          UCXConnection.readHandshakeHeader(is)

        logInfo(s"Got peer worker address from executor $peerExecutorId")

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
        logDebug(s"Removing Active Message registration for " +
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

  // We include a header with this size in our active messages
  private val ACTIVE_MESSAGE_HEADER_SIZE = 8L

  def formatAmIdAndHeader(activeMessageId: Int, header: Long) =
    s"[amId=${TransportUtils.toHex(activeMessageId)}, hdr=${TransportUtils.toHex(header)}]"
}
