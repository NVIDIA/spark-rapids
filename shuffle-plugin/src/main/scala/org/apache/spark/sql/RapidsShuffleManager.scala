/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, Executors, ExecutorService, Future}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.spark._
import ai.rapids.spark.format.{MetadataRequest, MetadataResponse}
import ai.rapids.spark.ucx._
import com.google.common.util.concurrent.AtomicDouble
import org.openucx.jucx.UcxUtils

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage._
import org.apache.spark.util.Utils

class GpuShuffleHandle[K, V](
    val wrapped: ShuffleHandle,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(wrapped.shuffleId, dependency) {

  override def toString: String = s"GPU SHUFFLE HANDLE $shuffleId"
}

class GpuShuffleBlockResolver(private val wrapped: ShuffleBlockResolver,
    private val blockManager: BlockManager,
    private val compressionCodec: CompressionCodec)
  extends ShuffleBlockResolver with Logging {
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startPart, endPart) = blockId match {
      case sbbid: ShuffleBlockBatchId =>
        (sbbid.shuffleId, sbbid.mapId, sbbid.startReduceId, sbbid.endReduceId)
      case sbid: ShuffleBlockId =>
        (sbid.shuffleId, sbid.mapId, sbid.reduceId, sbid.reduceId + 1)
      case _ => throw new IllegalArgumentException(s"${blockId.getClass} $blockId is not currently supported")
    }
    // Make sure that the data is spilled to disk so we can read it the slow way...
    RapidsShuffleManager.getCached(shuffleId, mapId).foreach(mapCache => {
      mapCache.spillSync(wrapped.asInstanceOf[IndexShuffleBlockResolver], blockManager, compressionCodec)
      mapCache.markRead(startPart, endPart)
    })
    wrapped.getBlockData(blockId)
  }

  override def stop(): Unit = wrapped.stop()
}

class PartitionCache(val shuffleId: Int, val mapId: Long, val partId: Int) extends AutoCloseable with Logging{
  val data = new ArrayBuffer[ColumnarBatch]()
  private var wasRead = false

  def hasBeenRead: Boolean = wasRead

  override def toString: String = {
    "PartitionCache(" +
    s"cb_cols=${data.map(cb => cb.numCols).mkString(", ")}" +
    ")"
  }

  def numBatches : Int = data.size

  def sizeUsed: Long =
    data.map(GpuResourceManager.deviceMemoryUsed).sum

  def writeTo(outputStream: OutputStream): Unit = {
    data.foreach(cb => {
      val table = GpuColumnVector.from(cb)
      try {
        // TODO this must be kept in sync with GpuColumnarBatchSerializer and as follow on work
        // should probably move there.
        JCudfSerialization.writeToStream(table, outputStream, 0, table.getRowCount)
      } finally {
        table.close()
      }
    })
  }

  def add(batch: ColumnarBatch): Unit = {
    GpuColumnVector.extractBases(batch).foreach(_.incRefCount())
    data += batch
  }

  def markAsRead(): Unit = wasRead = true

  def read: Iterator[ColumnarBatch] = {
    logDebug(s"PartitionCache read for [shuffle_id=$shuffleId, map_id=$mapId, part_id=$partId]")
    data.foreach(cb => GpuColumnVector.extractBases(cb).foreach(_.incRefCount()))
    data.iterator
  }

  override def close(): Unit = {
    logDebug(s"Closing PartitionCache [ref=${this}, shuffle_id=$shuffleId, map_id=$mapId, part_id=$partId]")
    data.foreach(cb => cb.close())
    data.clear()
  }
}

private class PartitionWriterStream (private val wrapped: OutputStream,
    private val partitionLengths: Array[Long],
    val partitionId: Int) extends OutputStream with Logging {
  private var count = 0
  private var isClosed = false

  def getCount: Int = count

  override def write(b: Int): Unit = {
    verifyNotClosed()
    wrapped.write(b)
    count += 1
  }

  override def write(buf: Array[Byte], pos: Int, length: Int): Unit = {
    verifyNotClosed()
    wrapped.write(buf, pos, length)
    count += length
  }

  override def close(): Unit = {
    isClosed = true
    partitionLengths(partitionId) = count
  }

  private def verifyNotClosed(): Unit = {
    if (isClosed) throw new IllegalStateException("Attempting to write to a closed block output stream.")
  }
}

class GpuMapCache(
    val shuffleId: Int,
    val mapId: Long,
    val numParts: Int) extends AutoCloseable with Logging {
  private val data = new ConcurrentHashMap[Int, PartitionCache]()
  // In the worst case we cannot fit all of this maps output in memory while it
  // is being produced.  In that case we need to spill while we are working on it.
  private var complete = false
  private var spilled = false
  private var spilling: Future[_] = _

  def releaseIf(targetSize: Long, onlyIfRead: Boolean, blocking: Boolean): Long = {
    //TODO release right now iff it has been read from gpu despite spilling, until we mmap files for ucx
    logInfo(s"At releaseIf [target_size=$targetSize, only_if_read=$onlyIfRead, blocking=$blocking]")
    var total = 0L
    if (blocking && spilling != null) {
      spilling.get()
    }
    if (targetSize > 0 && spilling != null && spilling.isDone) {
      val it = data.entrySet().iterator()
      while (it.hasNext) {
        val entry = it.next()
        val partCache: PartitionCache = entry.getValue
        if (!onlyIfRead && partCache.hasBeenRead) {
          val size = partCache.sizeUsed
          total = total + size
          it.remove()

          logInfo(s"Closing PartitionCache [key=${entry.getKey}, cache=$partCache]")
          partCache.close()
        }
      }
    }
    total
  }

  def get(partId: Int): Option[PartitionCache] = {
    logDebug(s"getting sid = $shuffleId, partId=$partId, partCache=${data.get(partId)}")
    Option(data.get(partId))
  }

  def cache(partId: Int, batch: ColumnarBatch): Unit = synchronized {
    if (spilling != null) {
      throw new IllegalStateException("NOT IMPLEMENTED YET...")
    }
    var partCache = data.get(partId)
    if (partCache == null) {
      partCache = data.computeIfAbsent(partId, _ => new PartitionCache(shuffleId, mapId, partId))
    }
    logDebug(s"caching sid = $shuffleId, partId=$partId, numCols=${batch.numCols}")
    partCache.add(batch)
  }

  def markRead(startPart: Int, endPart: Int): Unit = {
    assert(complete)
    (startPart until endPart).foreach(part => {
      get(part).foreach(p => p.markAsRead())
    })
  }

  def doneCaching(
      conf: SparkConf,
      blockResolver: IndexShuffleBlockResolver,
      blockManager: BlockManager,
      compressionCodec: CompressionCodec): Unit = synchronized {
    complete = true
    if (spilled) {
      throw new IllegalStateException("NOT IMPLEMENTED YET")
    } else if (spilling == null) {
      spilling = RapidsShuffleManager.spillAsync(conf, this, blockResolver, blockManager, compressionCodec)
    } else {
      throw new IllegalStateException("SPILLING HAS ALREADY STARTED!?!?")
    }
  }

  def spillSync(blockResolver: IndexShuffleBlockResolver,
      blockManager: BlockManager,
      compressionCodec: CompressionCodec): Unit = synchronized {
    assert(complete)
    // TODO what we really need is spilled, not spilling.  Spilling probably should
    // just be replaced with synchronization of some kind.
    if (!spilled) {
      val range = new NvtxRange("CACHED SPILL", NvtxColor.DARK_GREEN)
      val outputFile = blockResolver.getDataFile(shuffleId, mapId)
      val outputTempFile = Utils.tempFileWith(outputFile)
      try {
        val lengths = new Array[Long](numParts)
        // TODO might be able to use a channel or something else in the future...
        val outputStream = new BufferedOutputStream(new FileOutputStream(outputTempFile))
        try {
          (0 until numParts).foreach(partId => {
            val pc = data.get(partId)
            if (pc != null) {
              val parWrite = new PartitionWriterStream(outputStream, lengths, partId)
              var os = blockManager.serializerManager.wrapForEncryption(parWrite)
              try {
                if (compressionCodec != null) {
                  os = compressionCodec.compressedOutputStream(os)
                }
                pc.writeTo(os)
              } finally {
                os.close()
              }
            }
          })
        } finally {
          outputStream.close()
        }
        blockResolver.writeIndexFileAndCommit(shuffleId, mapId, lengths, outputTempFile)
        spilled = true
      } finally {
        if (outputTempFile.exists() && !outputTempFile.delete()) {
          logError(s"Could not delete $outputTempFile")
        }
        range.close()
      }
    }
  }

  override def close(): Unit = synchronized {
    if (spilling != null) {
      spilling.cancel(false)
    }
    val it = data.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val v: PartitionCache = entry.getValue
      it.remove()
      v.close()
    }
  }
}

class GpuShuffleCache(shuffleId: Int) extends AutoCloseable with Logging {
  private val data = new ConcurrentHashMap[Long, GpuMapCache]()

  def releaseIfRead(target: Long): Long = {
    var total = 0L
    if (target > 0) {
      data.values().forEach(mapCache => total += mapCache.releaseIf(target - total,
        onlyIfRead = true, blocking = false))
    }
    total
  }

  def releaseIfSpilled(target: Long, blocking: Boolean): Long = {
    var total = 0L
    if (target > 0) {
      data.values().forEach(mapCache => total += mapCache.releaseIf(target - total,
        onlyIfRead = false, blocking))
    }
    total
  }

  def get(mapId: Long): Option[GpuMapCache] = Option(data.get(mapId))

  def getOrCreate(mapId: Long, numParts: Int): GpuMapCache = {
    var ret = data.get(mapId)
    if (ret == null) {
      ret = data.computeIfAbsent(mapId, _ => new GpuMapCache(shuffleId, mapId, numParts))
    }
    ret
  }

  override def close(): Unit = {
    logDebug(s"Closing GpuShuffleCache $shuffleId")
    val it = data.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val v = entry.getValue
      it.remove()
      v.close()
    }
  }
}

object RapidsShuffleManager extends GpuSpillable with Logging {
  private val data = new ConcurrentHashMap[Int, GpuShuffleCache]()
  private var threadPool: ExecutorService = _

  private def getExecService(conf: SparkConf): ExecutorService = synchronized {
    if (threadPool == null) {
      val rapidsConf = new RapidsConf(conf)
      // TODO it looks like the the desired configs are not set in local mode.  Not sure what we
      // can do about it though.  Need some follow on work
//      val execCores = conf.get(config.EXECUTOR_CORES)
//      val coresPerTask = math.max(conf.get(config.CPUS_PER_TASK), 1) // avoid a 0
//      numThreads = execCores/coresPerTask
      val numThreads = rapidsConf.shuffleSpillThreads
      val al = new AtomicLong(0)
      threadPool = Executors.newFixedThreadPool(numThreads, (runnable: Runnable) => {
        val t = new Thread(runnable, "SPILL_THREAD_" + al.getAndIncrement())
        t.setDaemon(true)
        t
      })
    }
    threadPool
  }

  def spillAsync(conf: SparkConf,
      gpuMapCache: GpuMapCache,
      blockResolver: IndexShuffleBlockResolver,
      blockManager: BlockManager,
      compressionCodec: CompressionCodec): Future[_] = {
    val tp = getExecService(conf)
    tp.submit(new Runnable {
      override def run(): Unit = gpuMapCache.spillSync(blockResolver, blockManager, compressionCodec)
    })
  }

  def unwrapHandle(handle: ShuffleHandle): ShuffleHandle = handle match {
    case gh: GpuShuffleHandle[_, _] => gh.wrapped
    case other => other
  }

  def registerGpuShuffle(shuffleId: Int): Unit = {
    // Note that in local mode this can be called multiple times.
    data.putIfAbsent(shuffleId, new GpuShuffleCache(shuffleId))
  }

  def unregisterGpuShuffle(shuffleId: Int): Unit = {
    val shuffleCache = data.remove(shuffleId)
    if (shuffleCache != null) {
      shuffleCache.close()
    }
  }

  def getCached(shuffleId: Int, mapId: Long): Option[GpuMapCache] = {
    val shuffleCache = Option(data.get(shuffleId))
    // TODO do we need extra thread safety here?  If we are shutting down do we care if we close in
    //  the middle of something else???
    shuffleCache.flatMap(_.get(mapId))
  }

  def getOrCreate(
      shuffleId: Int,
      mapId: Long,
      numParts: Int): GpuMapCache = {
    val shuffleCache = data.get(shuffleId)
    // If there is not shuffleCache we are shutting down and an exception is probably okay.
    shuffleCache.getOrCreate(mapId, numParts)
  }

  def closeAll(): Unit = {
    // In local mode with unit tests we may call closeAll multiple times
    // so we don't want to stop the threadpool, unless we also have a way to start it up again
    // on demand, but it would be good to know that we finished writing before we shut down
    // other code.

    val set = new java.util.HashSet(data.keySet())
    set.forEach(id => unregisterGpuShuffle(id))
  }

  /**
   * Spill GPU memory if possible
   *
   * @param target the amount of memory that we want to try and spill.
   */
  override def spill(target: Long): Unit = {
    val range = new NvtxRange("SHUFFLE CACHE SPILL", NvtxColor.ORANGE)
    try {
      var total = 0L
      logDebug(s"RELEASE UNUSED SHUFFLE CACHE TARGET $target")
      val readSpillRange = new NvtxRange("RELEASE READ", NvtxColor.PURPLE)
      try {
        data.values().forEach(shuffleCache => total += shuffleCache.releaseIfRead(target - total))
      } finally {
        readSpillRange.close()
      }
      logInfo(s"SHUFFLE RELEASED $total bytes ALREADY READ")
      if (target > total) {
        logDebug(s"TRY TO RELEASE MEMORY THAT HAS FINISHED SPILLING")
        val finishSpillRange = new NvtxRange("RELEASE FINISHED", NvtxColor.YELLOW)
        try {
          data.values().forEach(shuffleCache =>
            total += shuffleCache.releaseIfSpilled(target - total, blocking = false))
        } finally {
          finishSpillRange.close()
        }
        logInfo(s"SHUFFLE RELEASED $total bytes ALREADY SPILLED")
      }
      if (target > total) {
        logDebug(s"TRY TO RELEASE MEMORY THAT HAS FINISHED BEING WRITTEN")
        val finishSpillRange = new NvtxRange("SYNC RELEASE FINISHED", NvtxColor.YELLOW)
        try {
          data.values().forEach(shuffleCache =>
            total += shuffleCache.releaseIfSpilled(target - total, blocking = true))
        } finally {
          finishSpillRange.close()
        }
        logInfo(s"SHUFFLE RELEASED $total bytes WITH WAITING FOR SPILL")
      }
    } finally {
      range.close()
    }
  }
}

class RapidsCachingWriter[K, V](
    conf: SparkConf,
    blockManager: BlockManager,
    blockResolver: IndexShuffleBlockResolver,
    // Never keep a reference to the ShuffleHandle in the cache as it being GCed triggers
    // the data being released
    handle: GpuShuffleHandle[K, V],
    mapId: Long,
    compressionCodec: CompressionCodec,
    metrics: ShuffleWriteMetricsReporter,
    rapidsShufflePort: Int) extends ShuffleWriter[K, V] with Logging {

  private val numParts = handle.dependency.partitioner.numPartitions
  private val sizes = new Array[Long](numParts)
  private val forMap = RapidsShuffleManager.getOrCreate(handle.shuffleId, mapId, numParts)

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.write", NvtxColor.CYAN)
    try {
      var bytesWritten: Long = 0L
      var recordsWritten: Long = 0L
      records.foreach(p => {
        val partId = p._1.asInstanceOf[Int]
        val batch = p._2.asInstanceOf[ColumnarBatch]
        logDebug(s"Caching partId=$partId, batch=[num_cols=${batch.numCols()}, num_rows=${batch.numRows()}]")
        val partSize = GpuColumnVector.extractBases(batch).map(_.getDeviceMemorySize).sum
        recordsWritten = recordsWritten + 1
        bytesWritten = bytesWritten + partSize
        sizes(partId) += partSize
        // The size of the data is really only used to tell if the data should be shuffled or not
        // a 0 indicates that we should not shuffle anything.  This is here for the special case
        // where we have no columns, because of predicate push down, but we have a row count as
        // metadata.  We still want to shuffle it. The 100 is an arbitrary number and can be really
        // any non-zero number that is not too large
        if (batch.numRows() > 0) {
          sizes(partId) += 100
        } else {
          //println(s"SHOULD? Not caching 0 row batch ${batch} for partId: ${partId}")
        }
        forMap.cache(partId, batch)
      })
      metrics.incBytesWritten(bytesWritten)
      metrics.incRecordsWritten(recordsWritten)
    } finally {
      nvtxRange.close()
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.close", NvtxColor.CYAN)
    try {
      if (success) {
        forMap.doneCaching(conf, blockResolver, blockManager, compressionCodec)
      }

      // upon seeing this port, the other side will try to connect to the port
      // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
      val shuffleServerId = if (rapidsShufflePort > 0) {
        BlockManagerId(
          blockManager.shuffleServerId.executorId,
          blockManager.shuffleServerId.host,
          blockManager.shuffleServerId.port,
          Some(s"rapids=$rapidsShufflePort")
        )
      } else {
        blockManager.shuffleServerId
      }
      logInfo(s"Done caching shuffle success=$success, server_id=$shuffleServerId")
      Some(MapStatus(shuffleServerId, sizes, mapId))
    } finally {
      nvtxRange.close()
    }
  }
}

/**
 * A shuffle manager optimized for the Rapids Plugin For Apache Spark.
 */
class RapidsShuffleManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager with Logging {
  import RapidsShuffleManager._

  GpuResourceManager.register(RapidsShuffleManager)
  private val wrapped = new SortShuffleManager(conf)
  GpuShuffleEnv.setRapidsShuffleManagerInitialized(true)
  logWarning("Rapids Shuffle Plugin Enabled")

  //Many of these values like blockManager are not initialized when the constructor is called,
  // so they all need to be lazy values that are executed when things are first called
  private lazy val env = SparkEnv.get
  private lazy val blockManager = env.blockManager
  private lazy val shouldFallThroughOnEverything = {
    val fallThrough = !GpuShuffleEnv.isRapidsShuffleEnabled
    if (fallThrough) {
      logWarning("Rapids Shuffle Plugin is falling back to SortShuffleManager because " +
        "external shuffle is enabled")
    }
    fallThrough
  }
  val inflightRequests = new AtomicBoolean(false)
  var invalidRequestsNum = new AtomicLong(1)
  private lazy val localBlockManagerId = blockManager.blockManagerId
  private lazy val compressionEnabled: Boolean = conf.get(config.SHUFFLE_COMPRESS)
  private lazy val compressionCodec: CompressionCodec =
    if (compressionEnabled) {
      CompressionCodec.createCodec(conf)
    } else {
      null
    }
  private lazy val resolver = if (shouldFallThroughOnEverything) {
    wrapped.shuffleBlockResolver
  } else {
    new GpuShuffleBlockResolver(wrapped.shuffleBlockResolver, blockManager, compressionCodec)
  }

  val handleNewConnection: UCXConnectionCallback = new UCXConnectionCallback {
    override def onConnection(connection: Connection): Unit = {
      doHandleConnection(connection)
    }
  }

  def doHandleConnection(connection: Connection): Unit = {
    logInfo(s"Waiting for a new connection. Posting receive.")
    val metaRequest = ByteBuffer.allocateDirect(ShuffleMetadata.contentMetaSize)
    val tx = connection.createTransaction
    tx.receive(UcxUtils.getAddress(metaRequest), ShuffleMetadata.contentMetaSize, tx => {
      // immediately issue a new receive
      doHandleConnection(connection)
      logTrace(s"Received metadata request!! $tx")
      if (tx.status != TransactionStatus.Error) {
        if (rapidsConf.shuffleUcxThrottleEnable) {
          try {
            val request = ShuffleMetadata.materializeRequest(metaRequest)
            if (inflightRequests.compareAndSet(false, true)) {
              logDebug("pending blockid but is served =" + request.blockIds(0).mapId() + "," + request.blockIds(0).startReduceId())
              handleMetadataRequest(connection, request)
            } else {
              logInfo("Send Invalid for metadata request = " + request.responseTag())
              sendInvalid(connection, request)
            }
          } catch {
            case e: Throwable => {
              val sw = new StringWriter()
              e.printStackTrace(new PrintWriter(sw))
              logError("exception while handling metadata request" + sw.toString)
            }
          }
        } else {
          try {
            val request = ShuffleMetadata.materializeRequest(metaRequest)
            handleMetadataRequest(connection, request)
          } catch {
            case e: Throwable => {
              val sw = new StringWriter()
              e.printStackTrace(new PrintWriter(sw))
              logError("exception while handling metadata request" + sw.toString)
            }
          }
        }
      } else if (tx.status == TransactionStatus.Error) {
        logError("error getting metadata request: " + tx)
        //TODO handle error
      }
    })
  }

  var ucxPort: Int = -1

  private val rapidsConf = new RapidsConf(conf)
  private val updateWaitPeriod = rapidsConf.shuffleUcxUpdateWaitPeriod
  var waitPeriod = new AtomicDouble(rapidsConf.shuffleUcxWaitPeriod)
  lazy val ucx: Option[UCX] = if (rapidsConf.shuffleUcxEnable && !isDriver) {
    logWarning("UCX Shuffle Transport Enabled")
    val ucx = new UCX(blockManager.shuffleServerId.executorId.toInt, rapidsConf.shuffleUcxUseWakeup)
    ucx.init()
    ucxPort = ucx.startManagementPort(blockManager.shuffleServerId.host, handleNewConnection)
    Some(ucx)
  } else {
    None
  }

  def sendInvalid(connection: Connection, req : MetadataRequest) : Unit = {
    //send push back metadata response
    invalidRequestsNum.incrementAndGet()
    val metadataReponse: ByteBuffer = ShuffleMetadata.getShuffleMetadataResponse(isValid = false, null,
      System.currentTimeMillis() + invalidRequestsNum.get()*waitPeriod.get().toLong)
    val bb = ByteBuffer.allocateDirect(ShuffleMetadata.contentMetaSize) // other side expects 1 MB
    RapidsUcxUtil.copyBuffer(metadataReponse, bb, metadataReponse.remaining())
    bb.flip()
    val sz = bb.remaining()
    val tx = connection.createTransaction
    tx.send(UcxUtils.getAddress(bb), ShuffleMetadata.contentMetaSize, req.responseTag(), tx => {
      logInfo(s"Done sending an INVALID metadata response of: $sz size , status" + tx.status)
    })
    tx.waitForCompletion
  }

  def handleMetadataRequest(connection: Connection, req: MetadataRequest): Unit = {
    invalidRequestsNum.set(1)
    logDebug(s"HandleMetadataRequest for peerExecutorId ${connection.peerExecutorId} and " +
      s"responseTag ${RapidsUcxUtil.formatTag(req.responseTag())}")
    val handleMetadataRequest =
      new NvtxRange(s"Handle Metadata Request peerExecutorId ${connection.peerExecutorId}", NvtxColor.RED)
    // get the tables to get the buff meta response going - send back response
    var responseTables = new ArrayBuffer[Table]() //Tables in iterator order
    var pcs = new ArrayBuffer[PartitionCache]() //PartitionCache's to mark as read
    for (i <- 0 until req.blockIdsLength()) {
      val blockId = req.blockIds(i)
      val mapCache = RapidsShuffleManager.getCached(blockId.shuffleId(), blockId.mapId())
      logDebug(s"Getting cached blockId [shuffle_id=${blockId.shuffleId()}, map_id=${blockId.mapId()}, start_rid=${blockId.startReduceId()}, end_rid=${blockId.endReduceId()}]")
      blockId.startReduceId().to(blockId.endReduceId()).foreach(rId => {
        mapCache.get.get(rId) match {
          case Some(pc: PartitionCache) =>
            val cbIter = pc.read
            val tableIter: Iterator[Table] = cbIter.filter(cb => cb.numRows() > 0).map(cb => {
              GpuColumnVector.from(cb)
            })
            // TODO: need to signal around here that we need those blocks static wherever they reside
            // 1) tell the other side about what's coming
            val tableArr: Array[Table] = tableIter.toArray
            responseTables ++= tableArr
            pcs += pc

          case None =>
            logInfo(" not gpu map cache" + mapCache.get.get(blockId.startReduceId()).getClass)
        }
      })
    }

    val metadataReponse: ByteBuffer = ShuffleMetadata.getShuffleMetadataResponse(isValid = true,
      responseTables.toArray, System.currentTimeMillis() + waitPeriod.get().toLong)
    val resp: MetadataResponse = ShuffleMetadata.materializeResponse(metadataReponse)

    val sendRange = new NvtxRange("UCX Send", NvtxColor.RED)
    val tx = connection.createTransaction

    // 2) send each individual buffer over
    for (i <- 0 until resp.tableMetaLength()) {
      for (j <- 0 until resp.tableMeta(i).columnMetasLength()) {
        // where are these buffers allocated
        val column = responseTables(i).getColumn(j)
        val columnMeta = resp.tableMeta(i).columnMetas(j)

        val cvDataAddr: BaseDeviceMemoryBuffer = column.getDeviceBufferFor(BufferType.DATA)
        val cvValidityAddr: BaseDeviceMemoryBuffer = column.getDeviceBufferFor(BufferType.VALIDITY)

        //check device buffer pointer address here - typecast may not be the answer
        val data = columnMeta.data()

        val tagData = tx.registerForSend(CudfColumnVector.getAddress(cvDataAddr), data.len())
        data.mutateTag(tagData)

        if (column.getType == DType.STRING) {
          val offsets = column.getDeviceBufferFor(BufferType.OFFSET)
          val offsetTag = tx.registerForSend(CudfColumnVector.getAddress(offsets), offsets.getLength)

          // offsets
          columnMeta.offsets().mutateTag(offsetTag)
          columnMeta.offsets().mutateLen(offsets.getLength)
        }

        if (cvValidityAddr != null && columnMeta.validity().len() > 0) {
          val validity = columnMeta.validity()
          val tagValidity = tx.registerForSend(CudfColumnVector.getAddress(cvValidityAddr), validity.len())
          validity.mutateTag(tagValidity)
        }
      }
    }

    //ShuffleMetadata.printResponse("responding", resp)

    val bb = ByteBuffer.allocateDirect(ShuffleMetadata.contentMetaSize) // other side expects 1 MB
    RapidsUcxUtil.copyBuffer(metadataReponse, bb, metadataReponse.remaining())
    bb.flip()

    tx.send(UcxUtils.getAddress(bb), ShuffleMetadata.contentMetaSize, req.responseTag(), tx => {
      val stats = tx.getStats
      if (updateWaitPeriod) {
        waitPeriod.set(stats.txTimeMs)
      }
      logInfo(s"Sent table ${stats.sendSize} in ${stats.txTimeMs} ms @ bw: [send: ${stats.sendThroughput}] GB/sec")
      inflightRequests.set(false)
      pcs.foreach(_.markAsRead()) // yes, this is better
    })

    logInfo(s"Waiting for SEND tx $tx to complete")
    tx.waitForCompletion
    logInfo(s"DONE waiting for SEND tx $tx to complete")
    sendRange.close()
    handleMetadataRequest.close()
  }

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Always register with the wrapped handler so we can write to it ourselves if needed
    val orig = wrapped.registerShuffle(shuffleId, dependency)
    if (!shouldFallThroughOnEverything && dependency.isInstanceOf[GpuShuffleDependency[K, V, C]]) {
      val handle = new GpuShuffleHandle(orig, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
      handle
    } else {
      orig
    }
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext, metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val thePort = if (ucx.isDefined && ucx != None) { ucxPort } else { -1 }
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        registerGpuShuffle(handle.shuffleId)
        new RapidsCachingWriter(conf,
          env.blockManager,
          wrapped.shuffleBlockResolver,
          gpu.asInstanceOf[GpuShuffleHandle[K,V]],
          mapId,
          compressionCodec,
          metrics,
          thePort)
      case other =>
        wrapped.getWriter(other, mapId, context, metrics)
    }
  }

  override def getReaderForRange[K, C](handle:  ShuffleHandle,
       startMapIndex:  Int, endMapIndex:  Int, startPartition:  Int, endPartition:  Int,
       context:  TaskContext, metrics:  ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    /*
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        logInfo(s"Asking map output tracker for map range output sizes for: ${gpu.shuffleId}, " +
          s"mapIndices=$startMapIndex-$endMapIndex, parts=$startPartition-$endPartition")
        val nvtxRange = new NvtxRange("getMapSizesByExecId", NvtxColor.CYAN)
        val blocksByAddress = env.mapOutputTracker.getMapSizesByRange(gpu.shuffleId, startMapIndex, endPartition,
          startPartition, endPartition)
        nvtxRange.close()
        new RapidsCachingReader(rapidsConf, localBlockManagerId,
          blocksByAddress,
          gpu,
          context,
          metrics,
          ucx)
      case other => {
     */
    //TODO: not enabling the code above yet until we understand the new shuffle protocol
    wrapped.getReaderForRange(unwrapHandle(handle), startMapIndex, endMapIndex,
      startPartition, endPartition, context, metrics)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int,
       context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        logInfo(s"Asking map output tracker for map output sizes for: ${gpu.shuffleId}, parts=$startPartition-$endPartition")
        val nvtxRange = new NvtxRange("getMapSizesByExecId", NvtxColor.CYAN)
        val blocksByAddress = env.mapOutputTracker.getMapSizesByExecutorId(gpu.shuffleId, startPartition, endPartition)
        nvtxRange.close()
        new RapidsCachingReader(rapidsConf, localBlockManagerId,
          blocksByAddress,
          gpu,
          context,
          metrics,
          ucx)
      case other => {
        wrapped.getReader(unwrapHandle(other), startPartition, endPartition, context, metrics)
      }
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    unregisterGpuShuffle(shuffleId)
    wrapped.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = {
    closeAll()
    wrapped.stop()
  }
}
