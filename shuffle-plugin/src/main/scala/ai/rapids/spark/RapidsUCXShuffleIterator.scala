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

package ai.rapids.spark

import java.nio.ByteBuffer
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, PriorityBlockingQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, CudfColumnVector, DType, DeviceMemoryBuffer, NvtxColor, NvtxRange}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId}
import ai.rapids.spark.ShuffleMetadata.materializeResponse
import ai.rapids.spark.format.MetadataResponse
import ai.rapids.spark.ucx.{Connection, Transaction, TransactionStatus, UCX}
import org.apache.spark.internal.Logging
import org.openucx.jucx.UcxUtils


class ToFetch(var waitPeriod: Long, val localId: BlockManagerId, val remoteId: BlockManagerId, val connection: Connection, val blockIds: Seq[BlockId]) {
  override def toString : String = {
    "ToFetch[waitPeriod = " + waitPeriod + ", localId = "+localId + ", remoteId = " + remoteId + ", blockIds.size = " + blockIds.size
  }
}
/**
  * A Iterator over columnar batches that fetches UCX blocks during a RAPIDS enabled shuffle.
  * It keeps track of the blockIds that need to be fetched and the columnar batches that will be returned
  * as part of the next() call to this iterator. A read call from RapidsCachingReader that is UCX enabled will use
  * this along with any other iterators it needs to fulfill the fetch requests.
  * @param rapidsConf - instance of [[RapidsConf]]
  * @param localId - local [[BlockManagerId]]
  */
class RapidsUCXShuffleIterator(rapidsConf: RapidsConf, localId: BlockManagerId) extends Iterator[ColumnarBatch] with Logging {

  class FetchSync(val toFetch: LinkedBlockingQueue[ToFetch], val resolved : BlockingQueue[ColumnarBatch]) {
    var countFetches = new AtomicInteger(0)
    var countRetries = new AtomicInteger(0)
    var retries: PriorityBlockingQueue[ToFetch] = new PriorityBlockingQueue[ToFetch](100,
      (t: ToFetch, t1: ToFetch) => t.waitPeriod.toInt - t1.waitPeriod.toInt)
    val origFetches = new AtomicInteger(0)
    val reallyCompletedFetches = new AtomicInteger(0)
    val pendingCBs = new AtomicInteger(0)
    val completedCBs = new AtomicInteger(0)

    def addToResolved(columnarBatch: ColumnarBatch): Unit = {
      logDebug(s"[FetchSync] addToResolved cb.rows : ${columnarBatch.numRows()}")
      resolved.add(columnarBatch)
    }

    def addToCbsAndCompletedFetch(meta : MetadataResponse) : Unit = synchronized {
      fetchSync.pendingCBs.addAndGet(meta.tableMetaLength())
      fetchSync.reallyCompletedFetches.incrementAndGet()
    }

    def addToFetch(req: ToFetch): Unit = synchronized {
      origFetches.incrementAndGet()
      toFetch.put(req)
    }


    def addToRetries(waitPeriod: Long,req: ToFetch): Unit = {
      req.waitPeriod = waitPeriod
      retries.put(req)
    }

    def finished() : Boolean = {
      logDebug(s"[FetchSync] pendingCBs : ${pendingCBs.get} completedCBs = ${completedCBs.get}")
      (origFetches.get == reallyCompletedFetches.get) && (pendingCBs.get == completedCBs.get)
    }
  }

  val fetchSync = new FetchSync(new LinkedBlockingQueue[ToFetch](), new LinkedBlockingQueue[ColumnarBatch]())

  override def hasNext: Boolean = {
    !fetchSync.finished()
  }

  var veryFirst = true
  var steps = 1

  def getWaitTimes: Long = {
    if (veryFirst) {
      veryFirst = false
      0l
    } else {
      /* All but first fetch requests are made with some delay between different fetcher executors to alleviate
         some of the collision on the sender GPU. It grows with increements of 1 multiplied by it's own executor ID + 1.
         Eg. Executor 0 will send fetch requests at t + 0, t + 1, t + 2, t + 3.. milliseconds.
       */
      val waitTime = Math.max(rapidsConf.shuffleUcxMaxFetchWaitPeriod, steps * (localId.executorId.toInt + 1))
      steps = steps + 1
      waitTime
    }
  }

  val fetcherRunnable: Runnable = () => {
    while (fetchSync.origFetches.get != fetchSync.reallyCompletedFetches.get) {
      if (!fetchSync.toFetch.isEmpty) {
        logDebug(s"Fetch Original ask : ${fetchSync.countFetches.incrementAndGet()}")
        val r = fetchSync.toFetch.take()
        val period = getWaitTimes
        logDebug("Fetch original = " + r.toString + " sleeping.. " + period)
        Thread.sleep(period)
        doFetch(r)
      } else if (!fetchSync.retries.isEmpty) {
        logDebug(s"Fetch RETRIES ${fetchSync.countRetries.incrementAndGet()}")
        val ret = fetchSync.retries.take()
        val ts: Long = System.currentTimeMillis()
        if (ts < ret.waitPeriod) {
          logDebug("Fetch Retries = " + ret.toString)
          Thread.sleep(ret.waitPeriod - ts)
        }
        logDebug(s"fetcher sleeping.. ${ret.waitPeriod - ts}")
        doFetch(ret)
      }
    }
    logDebug(s"Fetcher thread has finished fetching ${fetchSync.origFetches}")
  }

  val fetcherThread = new Thread(fetcherRunnable)
  override def next(): ColumnarBatch = {
    var cb : ColumnarBatch = null
    val range = new NvtxRange("RapidsUCXShuffleIterator.next", NvtxColor.RED)
    try {
      cb = fetchSync.resolved.take()
      fetchSync.completedCBs.incrementAndGet()
      /*
      1) we can issue all fetches at once
      2) we can issue each fetch in lockstep with a .next()
      3) we can issue fetches lazily in another thread, an next may or may not block (prefer this one?)
     */
    } finally {
      if (cb == null) {
        throw new IllegalStateException("Called next but Iterator is empty")
      }
      range.close()
    }
    cb
  }

  def doFetch(nextToFetch: ToFetch) : Unit = {
    val allocMetaReq = new NvtxRange("Allocating Meta Req local="+nextToFetch.localId.port+ " remote="+nextToFetch.remoteId.port, NvtxColor.ORANGE)
    //TODO: ask ucx here to fetch on this endpoint the block
    //for now.. removes the block from those blocks we'd ask the regular block manager
    var metaReq: ByteBuffer = null
    var tx: Transaction = null
    var responseTag : Long = 0l
    try {
      logDebug(s"Requesting block_ids=[${nextToFetch.blockIds}]")
      if (nextToFetch.blockIds.isEmpty) {
        logDebug("Sending empty blockIds in the MetadataRequest?")
      }
      tx = nextToFetch.connection.createTransaction
      responseTag = tx.getMetadataTag
      metaReq =
        ShuffleMetadata.getShuffleMetadataRequest(responseTag, nextToFetch.blockIds.toArray)
    } finally {
      allocMetaReq.close()
    }
    val allocateResponse = new NvtxRange("Allocate Meta Resp", NvtxColor.ORANGE)
    var resp : ByteBuffer = null
    try {
      resp = ByteBuffer.allocateDirect(ShuffleMetadata.contentMetaSize)
    } finally {
      allocateResponse.close()
    }
    tx.request(
      UcxUtils.getAddress(metaReq),
      metaReq.remaining(),
      UcxUtils.getAddress(resp),
      ShuffleMetadata.contentMetaSize,
      responseTag,
      tx => {
        if (tx.status != TransactionStatus.Error) {
          // start the receives
          val handleMetaRange = new NvtxRange("Handle meta", NvtxColor.ORANGE)
          try {
            val metadataResponse: MetadataResponse = materializeResponse(resp)
            logDebug(s"Response valid bit = ${metadataResponse.isValid} waitPeriod = ${metadataResponse.waitPeriod()}")
            if (!metadataResponse.isValid) {
              fetchSync.addToRetries(metadataResponse.waitPeriod(), nextToFetch)
            } else {
              handleMeta(nextToFetch.connection, metadataResponse)
            }
          } finally {
            handleMetaRange.close()
          }
        } else {
          logError("Error in doFetch " + tx)
          throw new IllegalStateException(s"Error in metadata request transaction ${tx.errorMessage.get}")
        }
      })

    if (!rapidsConf.shuffleUcxRecvAsync) {
      tx.waitForCompletion () // so do these in lockstep
    }
  }

  case class IncomingColumn(dtype: DType, rowCount: Long, nullCount: Long,
                            dataBuff: DeviceMemoryBuffer,
                            validityBuff: DeviceMemoryBuffer,
                            offsetBuff: DeviceMemoryBuffer)

  case class IncomingColumnBatch(columns: Seq[IncomingColumn], rowCount: Long)

  def handleMeta(connection: Connection, meta: MetadataResponse) : Unit = {
    // resp has metadata at this point
    logDebug(s"Calling materializeResponse with $meta")
    // in the regular case, we would be allocating buffers as we issue these requests
    val tx = connection.createTransaction
    fetchSync.addToCbsAndCompletedFetch(meta)
    val incomingBatches = new Array[IncomingColumnBatch](meta.tableMetaLength())

    for (i <- 0 until meta.tableMetaLength()) {
      var rowCount = 0
      val table = meta.tableMeta(i)
      val incomingColumns = new Array[IncomingColumn](table.columnMetasLength())
      for (j <- 0 until table.columnMetasLength()) {
        val columnMeta = table.columnMetas(j)
        rowCount = columnMeta.rowCount().toInt
        var dLen = 0L
        var dTag = 0L
        dLen = columnMeta.data().len()
        dTag = columnMeta.data().tag()

        var vLen = 0L
        var vTag = 0L
        if (columnMeta.validity() != null) {
          vLen = columnMeta.validity().len()
          vTag = columnMeta.validity().tag()
        }

        var oLen = 0L
        var oTag = 0L
        if (columnMeta.offsets() != null) {
          oLen = columnMeta.offsets().len()
          oTag = columnMeta.offsets().tag()
        }

        var dBuff:BaseDeviceMemoryBuffer = null
        var vBuff:BaseDeviceMemoryBuffer = null
        var oBuff:BaseDeviceMemoryBuffer = null

        if (dLen > 0) {
          dBuff = DeviceMemoryBuffer.allocate(dLen)
          tx.registerForReceive(dTag, CudfColumnVector.getAddress(dBuff), dLen)
        }
        if (vLen > 0) {
          vBuff = DeviceMemoryBuffer.allocate(vLen)
          tx.registerForReceive(vTag, CudfColumnVector.getAddress(vBuff), vLen)
        }
        if (oLen > 0) {
          oBuff = DeviceMemoryBuffer.allocate(oLen)
          tx.registerForReceive(oTag, CudfColumnVector.getAddress(oBuff), oLen)
        }

        incomingColumns(j) = IncomingColumn(
          DType.values()(columnMeta.dType()),
          columnMeta.rowCount(),
          columnMeta.nullCount(),
          dBuff.asInstanceOf[DeviceMemoryBuffer],
          vBuff.asInstanceOf[DeviceMemoryBuffer],
          oBuff.asInstanceOf[DeviceMemoryBuffer])
      }
      incomingBatches(i) = IncomingColumnBatch(incomingColumns, rowCount)

    }

    val receiveTx = tx.receive(tx => {
      val stats = tx.getStats
      incomingBatches.map(ib => {
        logDebug(s"incoming batch $ib")
          new ColumnarBatch(
            ib.columns.map(ic => {
              GpuColumnVector.from(
                CudfColumnVector.createColumnVector(
                  ic.dtype,
                  ic.rowCount,
                  ic.nullCount,
                  ic.dataBuff,
                  ic.validityBuff,
                  ic.offsetBuff,
                  false))}).toArray, ib.rowCount.toInt)
      }).foreach(cb=> fetchSync.addToResolved(cb))
      logInfo(s"Read table size ${stats.receiveSize} in" +
        s" ${stats.txTimeMs} ms @ bw: [recv: ${stats.recvThroughput}] GB/sec")
      //received this 'j' column
    })

    logDebug(s"Waiting for RECEIVE tx $receiveTx to complete")
    //receiveTx.waitForCompletion
    //logDebug(s"[RapidsCachingReader] DONE waiting for RECEIVE tx
    // ${receiveTx} incoming batches has size ${incomingBatches.size}, and cbArray has size ${cbArray.size}, and resolved has size ${resolved.size}")

    logDebug(s"Incoming batches in ${this} from tx $receiveTx")
  }

  var threadStarted = new AtomicBoolean(false)
  def fetch(localId: BlockManagerId, ucx: UCX,
      blockManagerId: BlockManagerId, blockInfos: Seq[(BlockId, Long, Int)]) : Unit = {
    val sid = blockInfos.head._1.asInstanceOf[org.apache.spark.storage.ShuffleBlockId].shuffleId
    val start = System.nanoTime()
    logInfo(s"UCX BLOCK! [block_infos=$blockInfos, block_manager_id=$blockManagerId]")
    val ucxPort: Int = blockManagerId.topologyInfo.get.split("=")(1).toInt
    val connection: Connection = ucx.connect(blockManagerId.host, ucxPort)
    val connectionTime = (System.nanoTime() - start) / 1000000
    logInfo(s"Got a connection $connection in $connectionTime ms")
    val blockIds = blockInfos.map(_._1)
    val needToFetch = new ToFetch(0, localId, blockManagerId, connection, blockIds)
    fetchSync.addToFetch(needToFetch)
    if (!threadStarted.getAndSet(true)) {
      fetcherThread.start()
    }
  }
}
