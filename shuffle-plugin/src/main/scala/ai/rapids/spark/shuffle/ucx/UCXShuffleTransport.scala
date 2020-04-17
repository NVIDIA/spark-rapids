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

import java.nio.ByteBuffer
import java.util.PriorityQueue
import java.util.concurrent._

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, NvtxColor, NvtxRange}
import ai.rapids.spark.shuffle._
import ai.rapids.spark.{GpuDeviceManager, RapidsConf}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.GpuShuffleEnv
import org.apache.spark.storage.BlockManagerId

import scala.collection.mutable

/**
  * This is the UCX implementation of [[RapidsShuffleTransport]]
  *
  * This class will create instances of [[RapidsShuffleClient]] and [[RapidsShuffleServer]]
  * that are UCX-specific, it also implements some facilities like bounce buffer pools, and the
  * java Executors used in the client and server.
  *
  * @param shuffleServerId - BlockManagerId for this executor
  * @param rapidsConf - plugin configuration
  */
class UCXShuffleTransport(shuffleServerId: BlockManagerId, rapidsConf: RapidsConf)
  extends RapidsShuffleTransport
    with Logging {

  private[this] var inflightSize = 0L
  private[this] val inflightLimit = rapidsConf.shuffleTransportMaxReceiveInflightBytes
  private[this] val inflightMonitor = new Object
  private[this] var inflightStarted = true

  private[this] val shuffleMetadataPool = new DirectByteBufferPool(rapidsConf.shuffleMaxMetadataSize)

  private[this] val bounceBufferSize = rapidsConf.shuffleUcxBounceBuffersSize
  private[this] val deviceNumBuffers = rapidsConf.shuffleUcxDeviceBounceBuffersCount
  private[this] val hostNumBuffers = rapidsConf.shuffleUcxHostBounceBuffersCount

  private[this] var deviceSendBuffMgr: BounceBufferManager[DeviceMemoryBuffer] = null
  private[this] var hostSendBuffMgr: BounceBufferManager[HostMemoryBuffer] = null
  private[this] var deviceReceiveBuffMgr: BounceBufferManager[DeviceMemoryBuffer] = null

  private[this] val executorId = shuffleServerId.executorId.toInt

  private[this] val clients = new ConcurrentHashMap[Long, RapidsShuffleClient]()

  private[this] lazy val ucx = {
    logWarning("UCX Shuffle Transport Enabled")
    val ucxImpl = new UCX(executorId, rapidsConf.shuffleUcxUseWakeup)
    ucxImpl.init()
    initBounceBufferPools(bounceBufferSize,
      deviceNumBuffers, hostNumBuffers)

    // Perform transport (potentially IB) registration early
    // NOTE: on error we log and close things, which should fail other parts of the job in a bad way
    // in reality we should take a stab at lowering the requirement, and registering a smaller buffer.
    ucxImpl.register(deviceSendBuffMgr.getRootBuffer(), success => {
      if (!success) {
        logError(s"Error registering device send buffer, of size: " +
          s"${deviceSendBuffMgr.getRootBuffer().getLength}")
        ucxImpl.close()
      }
    })
    ucxImpl.register(deviceReceiveBuffMgr.getRootBuffer(), success => {
      if (!success) {
        logError(s"Error registering device receive buffer, of size: " +
          s"${deviceReceiveBuffMgr.getRootBuffer().getLength}")
        ucxImpl.close()
      }
    })
    ucxImpl.register(hostSendBuffMgr.getRootBuffer(), success => {
      if (!success) {
        logError(s"Error registering device receive buffer, of size: " +
          s"${hostSendBuffMgr.getRootBuffer().getLength}")
        ucxImpl.close()
      }
    })
    ucxImpl
  }

  override def getMetaBuffer(size: Long): RefCountedDirectByteBuffer = {
    if (size > rapidsConf.shuffleMaxMetadataSize) {
      logWarning(s"Large metadata message size $size B, larger than ${rapidsConf.shuffleMaxMetadataSize} B. " +
        s"Consider setting ${RapidsConf.SHUFFLE_MAX_METADATA_SIZE.key} higher.")
      new RefCountedDirectByteBuffer(ByteBuffer.allocateDirect(size.toInt), None)
    } else {
      shuffleMetadataPool.getBuffer(size)
    }
  }

  /**
    * Initialize the bounce buffer pools that are to be used to send and receive data against UCX
    *
    * We have 2 pools for the send side, since buffers may come from spilled memory (host),
    * or device memory.
    *
    * We have 1 pool for the receive side, since all receives are targeted for the GPU.
    *
    * The size of buffers is the same for all pools, since send/receive sizes need to match. The count
    * can be set independently.
    *
    * @param bounceBufferSize - the size for a single bounce buffer
    * @param deviceNumBuffers - number of buffers to allocate for the device
    * @param hostNumBuffers - number of buffers to allocate for the host
    */
  def initBounceBufferPools(bounceBufferSize: Long, deviceNumBuffers: Int, hostNumBuffers: Int): Unit = {
    deviceSendBuffMgr =
      new BounceBufferManager[DeviceMemoryBuffer](
        "device-send",
        bounceBufferSize,
        deviceNumBuffers,
        (size: Long) => DeviceMemoryBuffer.allocate(size))

    deviceReceiveBuffMgr =
      new BounceBufferManager[DeviceMemoryBuffer](
        "device-receive",
        bounceBufferSize,
        deviceNumBuffers,
        (size: Long) => DeviceMemoryBuffer.allocate(size))

    hostSendBuffMgr =
      new BounceBufferManager[HostMemoryBuffer](
        "host-send",
        bounceBufferSize,
        hostNumBuffers,
        (size: Long) => HostMemoryBuffer.allocate(size))
  }

  def freeBounceBufferPools(): Unit = {
    Seq(deviceSendBuffMgr, deviceReceiveBuffMgr, hostSendBuffMgr).foreach(_.close())
  }

  def freeReceiveBounceBuffers(bounceBuffers: Seq[MemoryBuffer]): Unit = {
    bounceBuffers.foreach(bb => {
      deviceReceiveBuffMgr.freeBuffer(bb)
    })
  }

  def freeSendBounceBuffers(bounceBuffers: Seq[MemoryBuffer]): Unit = {
    bounceBuffers.foreach {
      case hb: HostMemoryBuffer => hostSendBuffMgr.freeBuffer(hb)
      case db: DeviceMemoryBuffer => deviceSendBuffMgr.freeBuffer(db)
    }
  }

  private def getNumBounceBuffers(remaining: Long, totalRequired: Int): Int = {
    val numBuffers = (remaining + bounceBufferSize - 1) / bounceBufferSize
    Math.min(numBuffers, totalRequired).toInt
  }

  override def getSendBounceBuffers(deviceMemory: Boolean, remaining: Long, totalRequired: Int): Seq[MemoryBuffer] = {
    val numBuffs = getNumBounceBuffers(remaining, totalRequired)
    if (!deviceMemory) {
      toBounceBuffersIfNecessary(hostSendBuffMgr, numBuffs)
    } else {
      toBounceBuffersIfNecessary(deviceSendBuffMgr, numBuffs)
    }
  }

  override def getReceiveBounceBuffers(remaining: Long, totalRequired: Int): Seq[MemoryBuffer] = {
    val numBuffs = getNumBounceBuffers(remaining, totalRequired)
    toBounceBuffersIfNecessary(deviceReceiveBuffMgr, numBuffs)
  }

  def toBounceBuffersIfNecessary[T <: MemoryBuffer](
      bounceBuffMgr: BounceBufferManager[T],
      numBuffs: Integer): Seq[MemoryBuffer] = bounceBuffMgr.synchronized {
    val nvtx = new NvtxRange("Getting bounce buffer", NvtxColor.YELLOW)
    try {
      // if the # of buffers requested is more than what the pool has, we would deadlock
      // this ensures we only get as many buffers as the pool could possibly give us.
      val possibleNumBuffers = Math.min(bounceBuffMgr.numBuffers, numBuffs)
      val bounceBuffers: Seq[MemoryBuffer] = (0 until possibleNumBuffers).map(_ => bounceBuffMgr.acquireBuffer())
      logDebug(s"Got $possibleNumBuffers bounce buffers from pool out of ${numBuffs} requested.")
      bounceBuffers
    } finally {
      nvtx.close()
    }
  }

  def connect(peerBlockManagerId: BlockManagerId): ClientConnection = {
    val topo = peerBlockManagerId.topologyInfo
    val connection: ClientConnection = if (topo.isDefined) {
      val topoParts = topo.get.split("=")
      if (topoParts.size == 2 &&
          topoParts(0).equalsIgnoreCase(RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX)) {
        val peerExecutorId = peerBlockManagerId.executorId.toInt
        ucx.getConnection(peerExecutorId, peerBlockManagerId.host, topoParts(1).toInt)
      } else {
        // in the future this may create connections in other transports
        throw new IllegalStateException(s"Invalid block manager id for the rapids shuffle $peerBlockManagerId")
      }
    } else {
      throw new IllegalStateException(s"Invalid block manager id for the rapids shuffle $peerBlockManagerId")
    }

    connection
  }

  class CallerRunsAndLogs extends ThreadPoolExecutor.CallerRunsPolicy {
    override def rejectedExecution(runnable: Runnable, threadPoolExecutor: ThreadPoolExecutor): Unit = {
      logWarning(s"Rejected execution for ${threadPoolExecutor}, running in caller's thread.")
      super.rejectedExecution(runnable, threadPoolExecutor)
    }
  }

  // NOTE: this pool, as is, will add a new thread per task. This will likely change.
  private[this] val clientExecutor = new ThreadPoolExecutor(1,
    rapidsConf.shuffleMaxClientThreads,
    rapidsConf.shuffleClientThreadKeepAliveTime,
    TimeUnit.SECONDS,
    new ArrayBlockingQueue[Runnable](1),
    GpuDeviceManager.wrapThreadFactory(
      new ThreadFactoryBuilder()
        .setNameFormat("shuffle-transport-client-exec-%d")
        .setDaemon(true)
        .build),
    // if we can't hand off because we are too busy, block the caller (in UCX's case, the progress thread)
    new CallerRunsAndLogs())

  override def makeClient(localExecutorId: Long,
                 blockManagerId: BlockManagerId): RapidsShuffleClient = {
    val peerExecutorId = blockManagerId.executorId.toLong
    val clientConnection = connect(blockManagerId)
    clients.computeIfAbsent(peerExecutorId, _ => {
      new RapidsShuffleClient(
        localExecutorId,
        clientConnection,
        this,
        clientExecutor,
        rapidsConf.shuffleMaxMetadataSize)
    })
  }

  private[this] val threadFactory = GpuDeviceManager.wrapThreadFactory(
    new ThreadFactoryBuilder()
      .setNameFormat(s"shuffle-server-conn-thread-${executorId}-%d")
      .setDaemon(true)
      .build)

  // NOTE: this is a single thread for the executor, nothing prevents us from having a pool here.
  // This will likely change.
  private[this] val serverExecutor = Executors.newSingleThreadExecutor(threadFactory)

  /**
    * Construct a server instance
    * @param requestHandler - used to get metadata info, and acquire tables used in the shuffle.
    * @return - the server instance
    */
  override def makeServer(requestHandler: RapidsShuffleRequestHandler): RapidsShuffleServer = {
    new RapidsShuffleServer(
      this,
      ucx.getServerConnection,
      shuffleServerId,
      requestHandler,
      serverExecutor,
      rapidsConf)
  }

  /**
    * Returns true if the transport allows for [[neededAmount]] + its inflight tally
    * to be inflight at this time.
    *
    * @param neededAmount - amount of bytes needed.
    * @return - true if amount of bytes needed was allowed into the inflight amount,
    *         false otherwise (caller should try again)
    */
  private def markBytesInFlight(neededAmount: Long): Boolean = inflightMonitor.synchronized {
    // if it would fit, or we are sending nothing (protects against the buffer that is bigger than limit),
    // go ahead and allow it
    var didFit = false
    if (wouldFit(neededAmount)) {
      didFit = true
      inflightSize = inflightSize + neededAmount
      logDebug(s"New inflight size ${inflightSize} after adding = ${neededAmount}")
    } else {
      logDebug(s"Did not update inflight size ${inflightSize}: ${neededAmount} + ${inflightSize} > ${inflightLimit}")
    }

    didFit
  }

  // NOTE: this function is called from within monitor.synchronized blocks always
  private def wouldFit(neededAmount: Long): Boolean = inflightSize + neededAmount <= inflightLimit || inflightSize == 0

  override def doneBytesInFlight(bytesCompleted: Long): Unit = inflightMonitor.synchronized {
    inflightSize = inflightSize - bytesCompleted
    logDebug(s"Done with ${bytesCompleted} bytes inflight, new inflightSize is ${inflightSize}")
    inflightMonitor.notifyAll()
  }

  private val altList = new PriorityQueue[PendingTransferRequest](1000, (t: PendingTransferRequest, t1: PendingTransferRequest) => {
    if (t.getLength < t1.getLength) {
      -1;
    } else if (t.getLength > t1.getLength) {
      1;
    } else {
      0
    }
  })

  private[this] val exec = Executors.newSingleThreadExecutor(
    GpuDeviceManager.wrapThreadFactory(
      new ThreadFactoryBuilder()
        .setNameFormat(s"shuffle-transport-throttle-monitor")
        .setDaemon(true)
        .build))

  exec.execute(() => {
    while (inflightStarted) {
      try {
        var perClientReq = mutable.Map[RapidsShuffleClient, Seq[PendingTransferRequest]]()
        var removed = false
        inflightMonitor.synchronized {
          var head = altList.peek()
          if (head == null) {
            val waitRange = new NvtxRange("Transport throttling", NvtxColor.RED)
            try {
              inflightMonitor.wait(100)
            } finally {
              waitRange.close()
            }
          } else {
            while (head != null && markBytesInFlight(head.getLength)) {
              altList.remove(head)
              removed = true
              perClientReq +=
                (head.client -> (perClientReq.getOrElse(head.client, Seq.empty) :+ head))
              head = altList.peek()
            }
          }
        }
        if (perClientReq.nonEmpty) {
          logDebug(s"Issuing client req ${perClientReq.size}")
        }
        perClientReq.foreach { case (client, reqs) => {
          client.issueBufferReceives(new BufferReceiveState(reqs, this))
        }}
      } catch {
        case t: Throwable =>
          logError("Error in the UCX throttle loop", t)
      }
    }
  })

  override def queuePending(reqs: Seq[PendingTransferRequest]): Unit = inflightMonitor.synchronized {
    import collection.JavaConverters._
    altList.addAll(reqs.asJava)
    logDebug(s"THROTTLING ${altList.size} queued requests")
    inflightMonitor.notifyAll()
  }

  override def close(): Unit = {
    exec.shutdown()
    clientExecutor.shutdown()
    serverExecutor.shutdown()

    inflightMonitor.synchronized {
      inflightStarted = false
      inflightMonitor.notify()
    }

    if (!exec.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Transport throttle failed to terminate correctly")
    }
    if (!clientExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Client failed to terminate correctly")
    }
    if (!serverExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
      logError("UCX Shuffle Server failed to terminate correctly")
    }

    ucx.close()
    freeBounceBufferPools()
  }
}
