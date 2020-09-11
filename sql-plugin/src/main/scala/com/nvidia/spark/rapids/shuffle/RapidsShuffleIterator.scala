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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.{GpuSemaphore, RapidsBuffer, RapidsConf, ShuffleReceivedBufferCatalog, ShuffleReceivedBufferId}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{RapidsShuffleFetchFailedException, RapidsShuffleTimeoutException}
import org.apache.spark.sql.rapids.{GpuShuffleEnv, ShuffleMetricsUpdater}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}

/**
 * An Iterator over columnar batches that fetches blocks using [[RapidsShuffleClient]]s.
 *
 * A `transport` instance is used to make [[RapidsShuffleClient]]s that are able to fetch
 * blocks.
 *
 * @param localBlockManagerId the `BlockManagerId` for the local executor
 * @param rapidsConf plugin configuration
 * @param transport transport to use to fetch blocks
 * @param blocksByAddress blocks to fetch
 * @param metricsUpdater instance of `ShuffleMetricsUpdater` to update the Spark
 *                       shuffle metrics
 * @param timeoutSeconds a timeout in seconds, that the iterator will wait while polling
 *                       for batches
 */
class RapidsShuffleIterator(
    localBlockManagerId: BlockManagerId,
    rapidsConf: RapidsConf,
    transport: RapidsShuffleTransport,
    blocksByAddress: Array[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    metricsUpdater: ShuffleMetricsUpdater,
    catalog: ShuffleReceivedBufferCatalog = GpuShuffleEnv.getReceivedCatalog,
    timeoutSeconds: Long = GpuShuffleEnv.shuffleFetchTimeoutSeconds)
  extends Iterator[ColumnarBatch]
    with Logging {

  /**
   * General trait encapsulating either a buffer or an error. Used to hand off batches
   * to tasks (in the good case), or exceptions (in the bad case)
   */
  trait ShuffleClientResult

  /**
   * A result for a successful buffer received
   * @param bufferId - the shuffle received buffer id as tracked in the catalog
   */
  case class BufferReceived(
      bufferId: ShuffleReceivedBufferId) extends ShuffleClientResult

  /**
   * A result for a failed attempt at receiving block metadata, or corresponding batches.
   * @param blockManagerId - the offending peer block manager id
   * @param blockId - shuffle block id that we were fetching
   * @param mapIndex - the mapIndex (as returned by the `MapOutputTracker` in
   *                 `blocksByAddress`
   * @param errorMessage - a human-friendly error to report
   */
  case class TransferError(
      blockManagerId: BlockManagerId,
      blockId: ShuffleBlockBatchId,
      mapIndex: Int,
      errorMessage: String) extends ShuffleClientResult

  // when batches (or errors) arrive from the transport, the are pushed
  // to the `resolvedBatches` queue.
  private[this] val resolvedBatches = new LinkedBlockingQueue[ShuffleClientResult]()

  // Used to track requests that are pending where the number of [[ColumnarBatch]] results is
  // not known yet
  private[this] val pendingFetchesByAddress = mutable.Map[BlockManagerId, Long]()

  // These are batches that are expected, but that have not been resolved yet.
  private[this] var batchesInFlight: Long = 0L

  // Batches expected is a moving target of batches this iterator will see. The target is moved, by
  // [[RapidsShuffleFetchHandler]]'s start method, as metadata responses are received from
  // [[RapidsShuffleServer]]s.
  private[this] var totalBatchesExpected: Long = 0L

  // As batches are received this counter is updated. When all is said and done,
  // [[totalBathcesExpected]] must equal [[totalBatchesResolved]], as long as
  // [[pendingFetchesByAddress]] is empty, and there are no [[batchesInFlight]].
  private[this] var totalBatchesResolved: Long = 0L

  blocksByAddress.foreach(bba => {
    // expected blocks per address
    if (pendingFetchesByAddress.put(bba._1, bba._2.size).nonEmpty){
      throw new IllegalStateException(
        s"Repeated block managers asked to be fetched: $blocksByAddress")
    }
  })

  private[this] var markedAsDone: Boolean = false

  override def hasNext: Boolean = resolvedBatches.synchronized {
    val hasMoreBatches =
      pendingFetchesByAddress.nonEmpty || batchesInFlight > 0 || !resolvedBatches.isEmpty
    logDebug(s"$taskContext hasNext: batches expected = $totalBatchesExpected, batches " +
      s"resolved = $totalBatchesResolved, pending = ${pendingFetchesByAddress.size}, " +
      s"batches in flight = $batchesInFlight, resolved ${resolvedBatches.size}, " +
      s"hasNext = $hasMoreBatches")
    if (!hasMoreBatches) {
      markedAsDone = true
    }
    if (markedAsDone && totalBatchesExpected != totalBatchesResolved) {
      throw new IllegalStateException(
        s"This iterator had $totalBatchesResolved but $totalBatchesExpected were expected.")
    }
    hasMoreBatches
  }

  private val localHost = localBlockManagerId.host

  private val localExecutorId = localBlockManagerId.executorId.toLong

  private var started: Boolean = false

  // NOTE: `mapIndex` is utilized by the `FetchFailedException` to reference
  // a map output by index from the statuses collection in `MapOutputTracker`.
  //
  // This is different than the `mapId` in the `ShuffleBlockBatchId`, because
  // as of Spark 3.x the default shuffle protocol overloads `mapId` to be
  // `taskAttemptId`.
  case class BlockIdMapIndex(id: ShuffleBlockBatchId, mapIndex: Int)

  def start(): Unit = {
    logInfo(s"Fetching ${blocksByAddress.size} blocks.")

    // issue local fetches first
    val (local, remote) = blocksByAddress.partition(ba => ba._1.host == localHost)

    var clients = Seq[RapidsShuffleClient]()

    (local ++ remote).foreach {
      case (blockManagerId: BlockManagerId, blockIds: Seq[(BlockId, Long, Int)]) => {
        val shuffleRequestsMapIndex: Seq[BlockIdMapIndex] =
          blockIds.map { case (blockId, _, mapIndex) =>
            /**
             * [[ShuffleBlockBatchId]] is an internal optimization in Spark, which will likely
             * never see it unless explicitly enabled.
             *
             * There are other things that can turn it off, but we really don't care too much
             * about it.
             */
            blockId match {
              case sbbid: ShuffleBlockBatchId => BlockIdMapIndex(sbbid, mapIndex)
              case sbid: ShuffleBlockId =>
                BlockIdMapIndex(
                  ShuffleBlockBatchId(sbid.shuffleId, sbid.mapId, sbid.reduceId, sbid.reduceId),
                    mapIndex)
              case _ =>
                throw new IllegalArgumentException(
                  s"${blockId.getClass} $blockId is not currently supported")
            }
          }

        val client = try {
          transport.makeClient(localExecutorId, blockManagerId)
        } catch {
          case t: Throwable => {
            val errorMsg = s"Error getting client to fetch ${blockIds} from ${blockManagerId}: ${t}"
            logError(errorMsg, t)
            val BlockIdMapIndex(firstId, firstMapIndex) = shuffleRequestsMapIndex.head
            throw new RapidsShuffleFetchFailedException(
              blockManagerId,
              firstId.shuffleId,
              firstId.mapId,
              firstMapIndex,
              firstId.startReduceId,
              errorMsg)
          }
        }

        val handler = new RapidsShuffleFetchHandler {
          private[this] var clientExpectedBatches = 0L
          private[this] var clientResolvedBatches = 0L
          def start(expectedBatches: Int): Unit = resolvedBatches.synchronized {
            if (expectedBatches == 0) {
              throw new IllegalStateException(
                s"Received an invalid response from shuffle server: " +
                  s"0 expected batches for $shuffleRequestsMapIndex")
            }
            pendingFetchesByAddress.remove(blockManagerId)
            batchesInFlight = batchesInFlight + expectedBatches
            totalBatchesExpected = totalBatchesExpected + expectedBatches
            clientExpectedBatches = expectedBatches
            logDebug(s"Task: $taskAttemptId Client $blockManagerId " +
              s"Expecting $expectedBatches batches, $batchesInFlight batches currently in " +
              s"flight, total expected by this client: $clientExpectedBatches, total resolved by " +
              s"this client: $clientResolvedBatches")
          }

          def batchReceived(bufferId: ShuffleReceivedBufferId): Unit =
            resolvedBatches.synchronized {
              batchesInFlight = batchesInFlight - 1
              val nvtxRange = new NvtxRange(s"BATCH RECEIVED", NvtxColor.DARK_GREEN)
              try {
                if (markedAsDone) {
                  throw new IllegalStateException(
                    "This iterator was marked done, but a batched showed up after!!")
                }
                totalBatchesResolved = totalBatchesResolved + 1
                clientResolvedBatches = clientResolvedBatches + 1
                resolvedBatches.offer(BufferReceived(bufferId))

                if (clientExpectedBatches == clientResolvedBatches) {
                  logDebug(s"Task: $taskAttemptId Client $blockManagerId is " +
                    s"done fetching batches. Total batches expected $clientExpectedBatches, " +
                    s"total batches resolved $clientResolvedBatches.")
                } else {
                  logDebug(s"Task: $taskAttemptId Client $blockManagerId is " +
                    s"NOT done fetching batches. Total batches expected $clientExpectedBatches, " +
                    s"total batches resolved $clientResolvedBatches.")
                }
              } finally {
                nvtxRange.close()
              }
            }

          override def transferError(errorMessage: String): Unit = resolvedBatches.synchronized {
            // If Spark detects a single fetch failure, the whole task has failed
            // as per `FetchFailedException`. In the future `mapIndex` will come from the
            // error callback.
            shuffleRequestsMapIndex.map { case BlockIdMapIndex(id, mapIndex) =>
              resolvedBatches.offer(TransferError(
              blockManagerId, id, mapIndex, errorMessage))
            }
          }
        }

        logInfo(s"Client $blockManagerId triggered, for ${shuffleRequestsMapIndex.size} blocks")
        client.doFetch(shuffleRequestsMapIndex.map(_.id), handler)
        clients = clients :+ client
      }
    }

    logInfo(s"RapidsShuffleIterator for ${Thread.currentThread()} started with " +
      s"${clients.size} clients.")
  }

  private[this] def receiveBufferCleaner(): Unit = {
    if (hasNext) {
      logWarning(s"Iterator for task ${taskAttemptId} closing, " +
          s"but it is not done. Closing ${resolvedBatches.size()} resolved batches!!")
      resolvedBatches.forEach {
        case BufferReceived(bufferId) =>
          GpuShuffleEnv.getReceivedCatalog.removeBuffer(bufferId)
        case _ =>
      }
    }
  }

  // Used to print log messages, defaulting to a value for unit tests
  private[this] lazy val taskAttemptId: String =
    taskContext.map(_.taskAttemptId().toString)
        .getOrElse("testTaskAttempt")

  private[this] val taskContext: Option[TaskContext] = Option(TaskContext.get())

  //TODO: on task completion we currently don't ask clients to stop/clean resources
  taskContext.foreach(_.addTaskCompletionListener[Unit](_ => receiveBufferCleaner()))

  def pollForResult(timeoutSeconds: Long): Option[ShuffleClientResult] = {
    Option(resolvedBatches.poll(timeoutSeconds, TimeUnit.SECONDS))
  }

  override def next(): ColumnarBatch = {
    var cb: ColumnarBatch = null
    var sb: RapidsBuffer = null
    val range = new NvtxRange(s"RapidshuffleIterator.next", NvtxColor.RED)

    // If N tasks downstream are accumulating memory we run the risk OOM
    // On the other hand, if wait here we may not start processing batches that are ready.
    // Not entirely clear what the right answer is, at this time.
    //
    // It is worth noting that we can get in the situation where spilled buffers are acquired
    // (since the scheduling does not take into account the state of the buffer in the catalog),
    // which in turn could spill other device buffers that could have been handed off downstream
    // without a copy. In other words, more investigation is needed to find if some heuristics
    // could be applied to pipeline the copies back to device at acquire time.
    //
    // For now: picking to acquire the semaphore now. The batch fetches for *this* task are about to
    // get started, a few lines below. Assuming that any task that is in the semaphore, has active
    // fetches and so it could produce device memory. Note this is not allowing for some external
    // thread to schedule the fetches for us, it may be something we consider in the future, given
    // memory pressure.
    taskContext.foreach(GpuSemaphore.acquireIfNecessary)

    if (!started) {
      // kick off if we haven't already
      start()
      started = true
    }

    val blockedStart = System.currentTimeMillis()
    var result: Option[ShuffleClientResult] = None

    result = pollForResult(timeoutSeconds)
    val blockedTime = System.currentTimeMillis() - blockedStart
    result match {
      case Some(BufferReceived(bufferId)) =>
        val nvtxRangeAfterGettingBatch = new NvtxRange("RapidsShuffleIterator.gotBatch",
          NvtxColor.PURPLE)
        try {
          sb = catalog.acquireBuffer(bufferId)
          cb = sb.getColumnarBatch
          metricsUpdater.update(blockedTime, 1, sb.size, cb.numRows())
        } finally {
          nvtxRangeAfterGettingBatch.close()
          range.close()
          if (sb != null) {
            sb.close()
          }
          catalog.removeBuffer(bufferId)
        }
      case Some(TransferError(blockManagerId, shuffleBlockBatchId, mapIndex, errorMessage)) =>
        taskContext.foreach(GpuSemaphore.releaseIfNecessary)
        metricsUpdater.update(blockedTime, 0, 0, 0)
        val errorMsg = s"Transfer error detected by shuffle iterator, failing task. ${errorMessage}"
        logError(errorMsg)
        throw new RapidsShuffleFetchFailedException(
          blockManagerId,
          shuffleBlockBatchId.shuffleId,
          shuffleBlockBatchId.mapId,
          mapIndex,
          shuffleBlockBatchId.startReduceId,
          errorMsg)
      case None =>
        // NOTE: this isn't perfect, since what we really want is the transport to
        // bubble this error, but for now we'll make this a fatal exception.
        taskContext.foreach(GpuSemaphore.releaseIfNecessary)
        metricsUpdater.update(blockedTime, 0, 0, 0)
        val errMsg = s"Timed out after ${timeoutSeconds} seconds while waiting for a shuffle batch."
        logError(errMsg)
        throw new RapidsShuffleTimeoutException(errMsg)
      case _ =>
        throw new IllegalStateException(s"Invalid result type $result")
    }
    cb
  }
}
