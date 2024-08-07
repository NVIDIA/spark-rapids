/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.{File, FileInputStream}
import java.util.Optional
import java.util.concurrent.{Callable, ConcurrentHashMap, ExecutionException, Executors, Future, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleRequestHandler, RapidsShuffleServer, RapidsShuffleTransport}

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleWriter, _}
import org.apache.spark.shuffle.api._
import org.apache.spark.shuffle.sort.{BypassMergeSortShuffleHandle, SortShuffleManager}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.shims.{GpuShuffleBlockResolver, RapidsShuffleThreadedReader, RapidsShuffleThreadedWriter}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{RapidsShuffleBlockFetcherIterator, _}
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.util.collection.{ExternalSorter, OpenHashSet}

class GpuShuffleHandle[K, V](
    val wrapped: ShuffleHandle,
    override val dependency: GpuShuffleDependency[K, V, V])
  extends BaseShuffleHandle(wrapped.shuffleId, dependency) {

  override def toString: String = s"GPU SHUFFLE HANDLE $shuffleId"
}

class ShuffleHandleWithMetrics[K, V, C](
    shuffleId: Int,
    val metrics: Map[String, SQLMetric],
    override val dependency: GpuShuffleDependency[K, V, C])
    extends BaseShuffleHandle(shuffleId, dependency) {
}

abstract class GpuShuffleBlockResolverBase(
    protected val wrapped: ShuffleBlockResolver,
    catalog: ShuffleBufferCatalog)
  extends ShuffleBlockResolver with Logging {
  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val hasActiveShuffle: Boolean = blockId match {
      case sbbid: ShuffleBlockBatchId =>
        catalog.hasActiveShuffle(sbbid.shuffleId)
      case sbid: ShuffleBlockId =>
        catalog.hasActiveShuffle(sbid.shuffleId)
      case _ => throw new IllegalArgumentException(s"${blockId.getClass} $blockId "
          + "is not currently supported")
    }
    if (hasActiveShuffle) {
      throw new IllegalStateException(s"The block $blockId is being managed by the catalog")
    }
    wrapped.getBlockData(blockId, dirs)
  }

  override def stop(): Unit = wrapped.stop()
}

/**
 * The `ShuffleWriteMetricsReporter` is based on accumulators, which are not thread safe.
 * This class is a thin wrapper that adds synchronization, since these metrics will be written
 * by multiple threads.
 * @param wrapped
 */
class ThreadSafeShuffleWriteMetricsReporter(val wrapped: ShuffleWriteMetricsReporter)
  extends ShuffleWriteMetrics {

  def getWriteTime: Long = synchronized {
    TaskContext.get.taskMetrics().shuffleWriteMetrics.writeTime
  }

  override private[spark] def incBytesWritten(v: Long): Unit = synchronized {
    wrapped.incBytesWritten(v)
  }
  override private[spark] def incRecordsWritten(v: Long): Unit = synchronized {
    wrapped.incRecordsWritten(v)
  }
  override private[spark] def incWriteTime(v: Long): Unit = synchronized {
    wrapped.incWriteTime(v)
  }
  override private[spark] def decBytesWritten(v: Long): Unit = synchronized {
    wrapped.decBytesWritten(v)
  }
  override private[spark] def decRecordsWritten(v: Long): Unit = synchronized {
    wrapped.decRecordsWritten(v)
  }
}

object RapidsShuffleInternalManagerBase extends Logging {
  def unwrapHandle(handle: ShuffleHandle): ShuffleHandle = handle match {
    case gh: GpuShuffleHandle[_, _] => gh.wrapped
    case other => other
  }

  /**
   * "slots" are a thread + queue thin wrapper that is used
   * to execute tasks that need to be done in sequentially.
   * This is done such that the threaded shuffle posts
   * tasks that are for writer_i, or reader_i, which are
   * guaranteed to be processed sequentially for that writer or reader.
   * Writers/readers that land in a different slot are working independently
   * and could perform their work in parallel.
   * @param slotNum this slot's unique number only used to name its executor
   */
  private class Slot(slotNum: Int, slotType: String) {
    private val p = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat(s"rapids-shuffle-$slotType-$slotNum")
        .setDaemon(true)
        .build())

    def offer[T](task: Callable[T]): Future[T] = {
      p.submit(task)
    }

    def shutdownNow(): Unit = p.shutdownNow()
  }

  // this is set by the executor on startup, when the MULTITHREADED
  // shuffle mode is utilized, as per these configs:
  //   spark.rapids.shuffle.multiThreaded.writer.threads
  //   spark.rapids.shuffle.multiThreaded.reader.threads
  private var numWriterSlots: Int = 0
  private var numReaderSlots: Int = 0
  private lazy val writerSlots = new mutable.HashMap[Int, Slot]()
  private lazy val readerSlots = new mutable.HashMap[Int, Slot]()

  // used by callers to obtain a unique slot
  private val writerSlotNumber = new AtomicInteger(0)
  private val readerSlotNumber= new AtomicInteger(0)

  private var mtShuffleInitialized: Boolean = false

  /**
   * Send a task to a specific write slot.
   * @param slotNum the slot to submit to
   * @param task a task to execute
   * @note there must not be an uncaught exception while calling
   *      `task`.
   */
  def queueWriteTask[T](slotNum: Int, task: Callable[T]): Future[T] = {
    writerSlots(slotNum % numWriterSlots).offer(task)
  }

  /**
   * Send a task to a specific read slot.
   * @param slotNum the slot to submit to
   * @param task a task to execute
   * @note there must not be an uncaught exception while calling
   *      `task`.
   */
  def queueReadTask[T](slotNum: Int, task: Callable[T]): Future[T] = {
    readerSlots(slotNum % numReaderSlots).offer(task)
  }

  def startThreadPoolIfNeeded(
      numWriterThreads: Int,
      numReaderThreads: Int): Unit = synchronized {
    if (!mtShuffleInitialized) {
      mtShuffleInitialized = true
      numWriterSlots = numWriterThreads
      numReaderSlots = numReaderThreads
      if (writerSlots.isEmpty) {
        (0 until numWriterSlots).foreach { slotNum =>
          writerSlots.put(slotNum, new Slot(slotNum, "writer"))
        }
      }
      if (readerSlots.isEmpty) {
        (0 until numReaderSlots).foreach { slotNum =>
          readerSlots.put(slotNum, new Slot(slotNum, "reader"))
        }
      }
    }
  }

  def stopThreadPool(): Unit = synchronized {
    mtShuffleInitialized = false
    writerSlots.values.foreach(_.shutdownNow())
    writerSlots.clear()

    readerSlots.values.foreach(_.shutdownNow())
    readerSlots.clear()
  }

  def getNextWriterSlot: Int = Math.abs(writerSlotNumber.incrementAndGet())
  def getNextReaderSlot: Int = Math.abs(readerSlotNumber.incrementAndGet())
}

trait RapidsShuffleWriterShimHelper {
  def setChecksumIfNeeded(writer: DiskBlockObjectWriter, partition: Int): Unit = {
    // noop until Spark 3.2.0+
  }

  // Partition lengths, used for MapStatus, but also exposed in Spark 3.2.0+
  private var myPartitionLengths: Array[Long] = null

  // This is a Spark 3.2.0+ function, adding a default here for testing purposes
  def getPartitionLengths: Array[Long] = myPartitionLengths

  def commitAllPartitions(writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long] = {
    myPartitionLengths = doCommitAllPartitions(writer, emptyChecksums)
    myPartitionLengths
  }

  def doCommitAllPartitions(writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long]
}

abstract class RapidsShuffleThreadedWriterBase[K, V](
    blockManager: BlockManager,
    handle: ShuffleHandleWithMetrics[K, V, V],
    mapId: Long,
    sparkConf: SparkConf,
    writeMetrics: ShuffleWriteMetricsReporter,
    maxBytesInFlight: Long,
    shuffleExecutorComponents: ShuffleExecutorComponents,
    numWriterThreads: Int)
      extends ShuffleWriter[K, V]
        with RapidsShuffleWriterShimHelper
        with Logging {
  private var myMapStatus: Option[MapStatus] = None
  private val metrics = handle.metrics
  private val serializationTimeMetric =
    metrics.get("rapidsShuffleSerializationTime")
  private val shuffleWriteTimeMetric =
    metrics.get("rapidsShuffleWriteTime")
  private val shuffleCombineTimeMetric =
    metrics.get("rapidsShuffleCombineTime")
  private val ioTimeMetric =
    metrics.get("rapidsShuffleWriteIoTime")
  private val dep: ShuffleDependency[K, V, V] = handle.dependency
  private val shuffleId = dep.shuffleId
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val serializer = dep.serializer.newInstance()
  private val transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true)
  private val fileBufferSize = sparkConf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024
  private val limiter = new BytesInFlightLimiter(maxBytesInFlight)
  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private var stopping = false

  private val diskBlockObjectWriters = new mutable.HashMap[Int, (Int, DiskBlockObjectWriter)]()

  /**
   * Simple wrapper that tracks the time spent iterating the given iterator.
   */
  private class TimeTrackingIterator(delegate: Iterator[Product2[K, V]])
    extends Iterator[Product2[K, V]] {

    private var iterateTimeNs: Long = 0L

    override def hasNext: Boolean = {
      val start = System.nanoTime()
      val ret = delegate.hasNext
      iterateTimeNs += System.nanoTime() - start
      ret
    }

    override def next(): Product2[K, V] = {
      val start = System.nanoTime()
      val ret = delegate.next
      iterateTimeNs += System.nanoTime() - start
      ret
    }

    def getIterateTimeNs: Long = iterateTimeNs
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // Iterating the `records` may involve some heavy computations.
    // TimeTrackingIterator is used to track how much time we spend for such computations.
    write(new TimeTrackingIterator(records))
  }

  private def write(records: TimeTrackingIterator): Unit = {
    withResource(new NvtxRange("ThreadedWriter.write", NvtxColor.RED)) { _ =>
      withResource(new NvtxRange("compute", NvtxColor.GREEN)) { _ =>
        val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
          shuffleId,
          mapId,
          numPartitions)
        try {
          var openTimeNs = 0L
          val partLengths = if (!records.hasNext) {
            commitAllPartitions(mapOutputWriter, true /*empty checksum*/)
          } else {
            // per reduce partition id
            // open all the writers ahead of time (Spark does this already)
            val openStartTime = System.nanoTime()
            (0 until numPartitions).map { i =>
              val (blockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
              val writer: DiskBlockObjectWriter = blockManager.getDiskWriter(
                blockId, file, serializer, fileBufferSize, writeMetrics)
              setChecksumIfNeeded(writer, i) // spark3.2.0+

              // Places writer objects at round robin slot numbers apriori
              // this choice is for simplicity but likely needs to change so that
              // we can handle skew better
              val slotNum = RapidsShuffleInternalManagerBase.getNextWriterSlot
              diskBlockObjectWriters.put(i, (slotNum, writer))
            }
            openTimeNs = System.nanoTime() - openStartTime

            // we call write on every writer for every record in parallel
            val writeFutures = new mutable.Queue[Future[Unit]]
            // Accumulated record write time as if they were sequential
            val recordWriteTime: AtomicLong = new AtomicLong(0L)
            // Time spent waiting on the limiter
            var waitTimeOnLimiterNs: Long = 0L
            // Time spent computing ColumnarBatch sizes
            var batchSizeComputeTimeNs: Long = 0L
            // Timestamp when the main processing begins
            val processingStart: Long = System.nanoTime()
            try {
              while (records.hasNext) {
                // get the record
                val record = records.next()
                val key = record._1
                val value = record._2
                val reducePartitionId: Int = partitioner.getPartition(key)
                val (slotNum, myWriter) = diskBlockObjectWriters(reducePartitionId)

                if (numWriterThreads == 1) {
                  val recordWriteTimeStart = System.nanoTime()
                  myWriter.write(key, value)
                  recordWriteTime.getAndAdd(System.nanoTime() - recordWriteTimeStart)
                } else {
                  // we close batches actively in the `records` iterator as we get the next batch
                  // this makes sure it is kept alive while a task is able to handle it.
                  val sizeComputeStart = System.nanoTime()
                  val (cb, size) = value match {
                    case columnarBatch: ColumnarBatch =>
                      (SlicedGpuColumnVector.incRefCount(columnarBatch),
                        SlicedGpuColumnVector.getTotalHostMemoryUsed(columnarBatch))
                    case _ =>
                      (null, 0L)
                  }
                  val waitOnLimiterStart = System.nanoTime()
                  batchSizeComputeTimeNs += waitOnLimiterStart - sizeComputeStart
                  limiter.acquireOrBlock(size)
                  waitTimeOnLimiterNs += System.nanoTime() - waitOnLimiterStart
                  writeFutures += RapidsShuffleInternalManagerBase.queueWriteTask(slotNum, () => {
                    withResource(cb) { _ =>
                      try {
                        val recordWriteTimeStart = System.nanoTime()
                        myWriter.write(key, value)
                        recordWriteTime.getAndAdd(System.nanoTime() - recordWriteTimeStart)
                      } finally {
                        limiter.release(size)
                      }
                    }
                  })
                }
              }
            } finally {
              // This is in a finally block so that if there is an exception queueing
              // futures, that we will have waited for any queued write future before we call
              // .abort on the map output writer (we had test failures otherwise)
              withResource(new NvtxRange("WaitingForWrites", NvtxColor.PURPLE)) { _ =>
                try {
                  while (writeFutures.nonEmpty) {
                    try {
                      writeFutures.dequeue().get()
                    } catch {
                      case ee: ExecutionException =>
                        // this exception is a wrapper for the underlying exception
                        // i.e. `IOException`. The ShuffleWriter.write interface says
                        // it can throw these.
                        throw ee.getCause
                    }
                  }
                } finally {
                  // cancel all pending futures (only in case of error will we cancel)
                  writeFutures.foreach(_.cancel(true /*ok to interrupt*/))
                }
              }
            }

            // writeTimeNs is an approximation of the amount of time we spent in
            // DiskBlockObjectWriter.write, which involves serializing records and writing them
            // on disk. As we use multiple threads for writing, writeTimeNs is
            // estimated by 'the total amount of time it took to finish processing the entire logic
            // above' minus 'the amount of time it took to do anything expensive other than the
            // serialization and the write. The latter involves computations in upstream execs,
            // ColumnarBatch size estimation, and the time blocked on the limiter.
            val writeTimeNs = (System.nanoTime() - processingStart) -
              records.getIterateTimeNs - batchSizeComputeTimeNs - waitTimeOnLimiterNs

            val combineTimeStart = System.nanoTime()
            val pl = writePartitionedData(mapOutputWriter)
            val combineTimeNs = System.nanoTime() - combineTimeStart

            // add openTime which is also done by Spark, and we are counting
            // in the ioTime later
            writeMetrics.incWriteTime(openTimeNs)

            // At this point, Spark has timed the amount of time it took to write
            // to disk (the IO, per write). But note that when we look at the
            // multi threaded case, this metric is now no longer task-time.
            // Users need to look at "rs. shuffle write time" (shuffleWriteTimeMetric),
            // which does its own calculation at the task-thread level.
            // We use ioTimeNs, however, to get an approximation of serialization time.
            val ioTimeNs =
              writeMetrics.asInstanceOf[ThreadSafeShuffleWriteMetricsReporter].getWriteTime

            // serializationTime is the time spent compressing/encoding batches that wasn't
            // counted in the ioTime
            val totalPerRecordWriteTime = recordWriteTime.get() + ioTimeNs
            val ioRatio = (ioTimeNs.toDouble/totalPerRecordWriteTime)
            val serializationRatio = 1.0 - ioRatio

            // update metrics, note that we expect them to be relative to the task
            ioTimeMetric.foreach(_ += (ioRatio * writeTimeNs).toLong)
            serializationTimeMetric.foreach(_ += (serializationRatio * writeTimeNs).toLong)
            // we add all three here because this metric is meant to show the time
            // we are blocked on writes
            shuffleWriteTimeMetric.foreach(_ += (openTimeNs + writeTimeNs + combineTimeNs))
            shuffleCombineTimeMetric.foreach(_ += combineTimeNs)
            pl
          }
          myMapStatus = Some(MapStatus(blockManager.shuffleServerId, partLengths, mapId))
        } catch {
          // taken directly from BypassMergeSortShuffleWriter
          case e: Exception =>
            try {
              mapOutputWriter.abort(e)
            } catch {
              case e2: Exception =>
                logError("Failed to abort the writer after failing to write map output.", e2);
                e.addSuppressed(e2);
            }
            throw e
        }
      }
    }
  }

  def writePartitionedData(mapOutputWriter: ShuffleMapOutputWriter): Array[Long] = {
    // after all temporary shuffle writes are done, we need to produce a single
    // file (shuffle_[map_id]_0) which is done during this commit phase
    withResource(new NvtxRange("CommitShuffle", NvtxColor.RED)) { _ =>
      // per reduce partition
      val segments = (0 until numPartitions).map {
        reducePartitionId =>
          withResource(diskBlockObjectWriters(reducePartitionId)._2) { writer =>
            val segment = writer.commitAndGet()
            (reducePartitionId, segment.file)
          }
      }

      val writeStartTime = System.nanoTime()
      segments.foreach { case (reducePartitionId, file) =>
        val partWriter = mapOutputWriter.getPartitionWriter(reducePartitionId)
        if (file.exists()) {
          if (transferToEnabled) {
            val maybeOutputChannel: Optional[WritableByteChannelWrapper] =
              partWriter.openChannelWrapper()
            if (maybeOutputChannel.isPresent) {
              writePartitionedDataWithChannel(file, maybeOutputChannel.get())
            } else {
              writePartitionedDataWithStream(file, partWriter)
            }
          } else {
            writePartitionedDataWithStream(file, partWriter)
          }
          file.delete()
        }
      }
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
    }
    commitAllPartitions(mapOutputWriter, false /*non-empty checksums*/)
  }

  // taken from BypassMergeSortShuffleWriter
  // this code originally called into guava.Closeables.close
  // and had logic to silence exceptions thrown while copying
  // I am ignoring this for now.
  def writePartitionedDataWithStream(file: java.io.File, writer: ShufflePartitionWriter): Unit = {
    withResource(new FileInputStream(file)) { in =>
      withResource(writer.openStream()) { os =>
        Utils.copyStream(in, os, false, false)
      }
    }
  }

  // taken from BypassMergeSortShuffleWriter
  // this code originally called into guava.Closeables.close
  // and had logic to silence exceptions thrown while copying
  // I am ignoring this for now.
  def writePartitionedDataWithChannel(
    file: File,
    outputChannel: WritableByteChannelWrapper): Unit = {
    // note outputChannel.close() doesn't actually close it.
    // The call is there to record keep the partition lengths
    // after the serialization completes.
    withResource(outputChannel) { _ =>
      withResource(new FileInputStream(file)) { in =>
        withResource(in.getChannel) { inputChannel =>
          Utils.copyFileStreamNIO(
            inputChannel, outputChannel.channel, 0L, inputChannel.size)
        }
      }
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      None
    } else {
      stopping = true
      if (success) {
        if (myMapStatus.isEmpty) {
          // should not happen, but adding it just in case (this differs from Spark)
          cleanupTempData()
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        myMapStatus
      } else {
        cleanupTempData()
        None
      }
    }
  }

  private def cleanupTempData(): Unit = {
    // The map task failed, so delete our output data.
    try {
      diskBlockObjectWriters.values.foreach { case (_, writer) =>
        val file = writer.revertPartialWritesAndClose()
        if (!file.delete()) logError(s"Error while deleting file ${file.getAbsolutePath()}")
      }
    } finally {
      diskBlockObjectWriters.clear()
    }
  }

  def getBytesInFlight: Long = limiter.getBytesInFlight
}

class BytesInFlightLimiter(maxBytesInFlight: Long) {
  private var inFlight: Long = 0L

  def acquire(sz: Long): Boolean = {
    if (sz == 0) {
      true
    } else {
      synchronized {
        if (inFlight == 0 || sz + inFlight < maxBytesInFlight) {
          inFlight += sz
          true
        } else {
          false
        }
      }
    }
  }

  def acquireOrBlock(sz: Long): Unit = {
    var acquired = acquire(sz)
    if (!acquired) {
      synchronized {
        while (!acquired) {
          acquired = acquire(sz)
          if (!acquired) {
            wait()
          }
        }
      }
    }
  }

  def release(sz: Long): Unit = synchronized {
    inFlight -= sz
    notifyAll()
  }

  def getBytesInFlight: Long = inFlight
}

abstract class RapidsShuffleThreadedReaderBase[K, C](
    handle: ShuffleHandleWithMetrics[K, C, C],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    maxBytesInFlight: Long,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    canUseBatchFetch: Boolean = false,
    numReaderThreads: Int = 0)
  extends ShuffleReader[K, C] with Logging {

  case class GetMapSizesResult(
      blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      canEnableBatchFetch: Boolean)

  protected def getMapSizes: GetMapSizesResult

  private val GetMapSizesResult(blocksByAddress, canEnableBatchFetch) = getMapSizes

  // For spark versions 3.2.0+ `canEnableBatchFetch` can be false given merged
  // map output
  private val shouldBatchFetch = canUseBatchFetch && canEnableBatchFetch

  private val sqlMetrics = handle.metrics
  private val dep = handle.dependency
  private val deserializationTimeNs = sqlMetrics.get("rapidsShuffleDeserializationTime")
  private val shuffleReadTimeNs = sqlMetrics.get("rapidsShuffleReadTime")
  private val dataReadSize = sqlMetrics.get("dataReadSize")

  private var shuffleReadRange: NvtxRange =
    new NvtxRange("ThreadedReader.read", NvtxColor.PURPLE)

  private def closeShuffleReadRange(): Unit = {
    if (shuffleReadRange != null) {
      shuffleReadRange.close()
      shuffleReadRange = null
    }
  }

  onTaskCompletion(context) {
    // should not be needed, but just in case
    closeShuffleReadRange()
  }

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: Fetching continuous blocks in batch is incompatible with io encryption.
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }


  class RapidsShuffleThreadedBlockIterator(
      fetcherIterator: RapidsShuffleBlockFetcherIterator,
      serializer: GpuColumnarBatchSerializer)
    extends Iterator[(Any, Any)] {
    private val queued = new LinkedBlockingQueue[(Any, Any)]
    private val futures = new mutable.Queue[Future[Option[BlockState]]]()
    private val serializerInstance = serializer.newInstance()
    private val limiter = new BytesInFlightLimiter(maxBytesInFlight)
    private val fallbackIter: Iterator[(Any, Any)] with AutoCloseable =
      if (numReaderThreads == 1) {
        // this is the non-optimized case, where we add metrics to capture the blocked
        // time and the deserialization time as part of the shuffle read time.
        new Iterator[(Any, Any)]() with AutoCloseable {
          private var currentIter: Iterator[(Any, Any)] = _
          private var currentStream: AutoCloseable = _
          override def hasNext: Boolean = fetcherIterator.hasNext || (
              currentIter != null && currentIter.hasNext)

          override def close(): Unit = {
            if (currentStream != null) {
              currentStream.close()
              currentStream = null
            }
          }

          override def next(): (Any, Any) = {
            val fetchTimeStart = System.nanoTime()
            var readBlockedTime = 0L
            if (currentIter == null || !currentIter.hasNext) {
              val readBlockedStart = System.nanoTime()
              val (_, stream) = fetcherIterator.next()
              readBlockedTime = System.nanoTime() - readBlockedStart
              // this is stored only to call close on it
              currentStream = stream
              currentIter = serializerInstance.deserializeStream(stream).asKeyValueIterator
            }
            val res = currentIter.next()
            val fetchTime = System.nanoTime() - fetchTimeStart
            deserializationTimeNs.foreach(_ += (fetchTime - readBlockedTime))
            shuffleReadTimeNs.foreach(_ += fetchTime)
            res
          }
        }
      } else {
        null
      }

    // Register a completion handler to close any queued cbs,
    // pending iterators, or futures
    onTaskCompletion(context) {
      // remove any materialized batches
      queued.forEach {
        case (_, cb:ColumnarBatch) => cb.close()
      }
      queued.clear()

      // close any materialized BlockState objects that are holding onto netty buffers or
      // file descriptors
      pendingIts.safeClose()
      pendingIts.clear()

      // we could have futures left that are either done or in flight
      // we need to cancel them and then close out any `BlockState`
      // objects that were created (to remove netty buffers or file descriptors)
      val futuresAndCancellations = futures.map { f =>
        val didCancel = f.cancel(true)
        (f, didCancel)
      }

      // if we weren't able to cancel, we are going to make a best attempt at getting the future
      // and we are going to close it. The timeout is to prevent an (unlikely) infinite wait.
      // If we do timeout then this handler is going to throw.
      var failedFuture: Option[Throwable] = None
      futuresAndCancellations
        .filter { case (_, didCancel) => !didCancel }
        .foreach { case (future, _) =>
          try {
            // this could either be a successful future, or it finished with exception
            // the case when it will fail with exception is when the underlying stream is closed
            // as part of the shutdown process of the task.
            future.get(10, TimeUnit.MILLISECONDS)
              .foreach(_.close())
          } catch {
            case t: Throwable =>
              // this is going to capture the first exception and not worry about others
              // because we probably don't want to spam the UI or log with an exception per
              // block we are fetching
              if (failedFuture.isEmpty) {
                failedFuture = Some(t)
              }
          }
        }
      futures.clear()
      try { 
        if (fallbackIter != null) {
          fallbackIter.close()
        }
      } catch {
        case t: Throwable => 
          if (failedFuture.isEmpty) {
            failedFuture = Some(t)
          } else {
            failedFuture.get.addSuppressed(t)
          }
      } finally {
        failedFuture.foreach { e =>
          throw e
        }
      }
    }

    override def hasNext: Boolean = {
      if (fallbackIter != null) {
        fallbackIter.hasNext
      } else {
        pendingIts.nonEmpty ||
          fetcherIterator.hasNext || futures.nonEmpty || queued.size() > 0
      }
    }

    case class BlockState(
        blockId: BlockId,
        batchIter: SerializedBatchIterator,
        origStream: AutoCloseable)
      extends Iterator[(Any, Any)] with AutoCloseable {

      private var nextBatchSize = {
        var success = false
        try {
          val res = batchIter.tryReadNextHeader().getOrElse(0L)
          success = true
          res
        } finally {
          if (!success) {
            // we tried to read from a stream, but something happened
            // lets close it
            close()
          }
        }
      }

      def getNextBatchSize: Long = nextBatchSize

      override def hasNext: Boolean = batchIter.hasNext

      override def next(): (Any, Any) = {
        val nextBatch = batchIter.next()
        var success = false
        try {
          nextBatchSize = batchIter.tryReadNextHeader().getOrElse(0L)
          success = true
          nextBatch
        } finally {
          if (!success) {
            // the call to get a next header threw. We need to close `nextBatch`.
            nextBatch match {
              case (_, cb: ColumnarBatch) => cb.close()
            }
          }
        }
      }

      override def close(): Unit = {
        origStream.close() // make sure we call this on error
      }
    }

    private val pendingIts = new mutable.Queue[BlockState]()

    override def next(): (Any, Any) = {
      require(hasNext, "called next on an empty iterator")
      withResource(new NvtxRange("ParallelDeserializerIterator.next", NvtxColor.CYAN)) { _ =>
        val result = if (fallbackIter != null) {
          fallbackIter.next()
        } else {
          var waitTime: Long = 0L
          var waitTimeStart: Long = 0L
          popFetchedIfAvailable()
          waitTime = 0L
          if (futures.nonEmpty) {
            withResource(new NvtxRange("BatchWait", NvtxColor.CYAN)) { _ =>
              waitTimeStart = System.nanoTime()
              val pending = futures.dequeue().get // wait for one future
              waitTime += System.nanoTime() - waitTimeStart
              // if the future returned a block state, we have more work to do
              pending match {
                case Some(leftOver@BlockState(_, _, _)) =>
                  pendingIts.enqueue(leftOver)
                case _ => // done
              }
            }
          }

          if (pendingIts.nonEmpty) {
            // if we had pending iterators, we should try to see if now one can be handled
            popFetchedIfAvailable()
          }

          // We either have added futures and so will have items queued
          // or we already exhausted the fetchIterator and are just waiting
          // for our futures to finish. Either way, it's safe to block
          // here while we wait.
          waitTimeStart = System.nanoTime()
          val res = queued.take()
          res match {
            case (_, cb: ColumnarBatch) =>
              limiter.release(SerializedTableColumn.getMemoryUsed(cb))
              popFetchedIfAvailable()
            case _ => 0 // TODO: do we need to handle other types here?
          }
          waitTime += System.nanoTime() - waitTimeStart
          deserializationTimeNs.foreach(_ += waitTime)
          shuffleReadTimeNs.foreach(_ += waitTime)
          res
        }

        val uncompressedSize = result match {
          case (_, cb: ColumnarBatch) => SerializedTableColumn.getMemoryUsed(cb)
          case _ => 0 // TODO: do we need to handle other types here?
        }

        dataReadSize.foreach(_ += uncompressedSize)

        // if this is the last call, close our range
        if (!hasNext) {
          closeShuffleReadRange()
        }
        result
      }
    }

    private def deserializeTask(blockState: BlockState): Unit = {
      val slot = RapidsShuffleInternalManagerBase.getNextReaderSlot
      futures += RapidsShuffleInternalManagerBase.queueReadTask(slot, () => {
        var success = false
        try {
          var currentBatchSize = blockState.getNextBatchSize
          var didFit = true
          while (blockState.hasNext && didFit) {
            val batch = blockState.next()
            queued.offer(batch)
            // peek at the next batch
            currentBatchSize = blockState.getNextBatchSize
            didFit = limiter.acquire(currentBatchSize)
          }
          success = true
          if (!didFit) {
            Some(blockState)
          } else {
            None // no further batches
          }
        } finally {
          if (!success) {
            blockState.close()
          }
        }
      })
    }

    private def popFetchedIfAvailable(): Unit = {
      // If fetcherIterator is not exhausted, we try and get as many
      // ready results.
      if (pendingIts.nonEmpty) {
        var continue = true
        while(pendingIts.nonEmpty && continue) {
          val blockState = pendingIts.head
          // check if we can handle the head batch now
          if (limiter.acquire(blockState.getNextBatchSize)) {
            // kick off deserialization task
            pendingIts.dequeue()
            deserializeTask(blockState)
          } else {
            continue = false
          }
        }
      } else {
        if (fetcherIterator.hasNext) {
          withResource(new NvtxRange("queueFetched", NvtxColor.YELLOW)) { _ =>
            // `resultCount` is exposed from the fetcher iterator and if non-zero,
            // it means that there are pending results that need to be handled.
            // We max with 1, because there could be a race condition where
            // we are trying to get a batch and we haven't received any results
            // yet, we need to block on the fetch for this case so we have
            // something to return.
            var amountToDrain = Math.max(fetcherIterator.resultCount, 1)
            val fetchTimeStart = System.nanoTime()

            // We drain fetched results. That is, we push decode tasks
            // onto our queue until the results in the fetcher iterator
            // are all dequeued (the ones that were completed up until now).
            var readBlockedTime = 0L
            var didFit = true
            while (amountToDrain > 0 && fetcherIterator.hasNext && didFit) {
              amountToDrain -= 1
              // fetch block time accounts for time spent waiting for streams.next()
              val readBlockedStart = System.nanoTime()
              val (blockId: BlockId, inputStream) = fetcherIterator.next()
              readBlockedTime += System.nanoTime() - readBlockedStart

              val deserStream = serializerInstance.deserializeStream(inputStream)
              val batchIter = deserStream.asKeyValueIterator.asInstanceOf[SerializedBatchIterator]
              val blockState = BlockState(blockId, batchIter, inputStream)
              // get the next known batch size (there could be multiple batches)
              if (limiter.acquire(blockState.getNextBatchSize)) {
                // we can fit at least the first batch in this block
                // kick off a deserialization task
                deserializeTask(blockState)
              } else {
                // first batch didn't fit, put iterator aside and stop asking for results
                // from the fetcher
                pendingIts.enqueue(blockState)
                didFit = false
              }
            }
            // keep track of the overall metric which includes blocked time
            val fetchTime = System.nanoTime() - fetchTimeStart
            deserializationTimeNs.foreach(_ += (fetchTime - readBlockedTime))
            shuffleReadTimeNs.foreach(_ += fetchTime)
          }
        }
      }
    }
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val wrappedStreams = RapidsShuffleBlockFetcherIterator.makeIterator(
      context,
      blockManager,
      SparkEnv.get,
      blocksByAddress,
      serializerManager,
      readMetrics,
      fetchContinuousBlocksInBatch)

    val recordIter = new RapidsShuffleThreadedBlockIterator(
      wrappedStreams,
      dep.serializer.asInstanceOf[GpuColumnarBatchSerializer])

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      }, context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        onTaskCompletion(context) {
          sorter.stop()
        }
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}

class RapidsCachingWriter[K, V](
    blockManager: BlockManager,
    // Never keep a reference to the ShuffleHandle in the cache as it being GCed triggers
    // the data being released
    handle: GpuShuffleHandle[K, V],
    mapId: Long,
    metricsReporter: ShuffleWriteMetricsReporter,
    catalog: ShuffleBufferCatalog,
    rapidsShuffleServer: Option[RapidsShuffleServer],
    metrics: Map[String, SQLMetric])
  extends ShuffleWriter[K, V]
    with Logging {
  private val numParts = handle.dependency.partitioner.numPartitions
  private val sizes = new Array[Long](numParts)

  private val uncompressedMetric: SQLMetric = metrics("dataSize")

  // This is here for the special case where we have no columns like with the .count
  // case or when we have 0-byte columns. We pick 100 as an arbitrary number so that
  // we can shuffle these degenerate batches, which have valid metadata and should be
  // used on the reducer side for computation.
  private val DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT: Long = 100L

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // NOTE: This MUST NOT CLOSE the incoming batches because they are
    //       closed by the input iterator generated by GpuShuffleExchangeExec
    val nvtxRange = new NvtxRange("RapidsCachingWriter.write", NvtxColor.CYAN)
    try {
      var bytesWritten: Long = 0L
      var recordsWritten: Long = 0L
      records.foreach { p =>
        val partId = p._1.asInstanceOf[Int]
        val batch = p._2.asInstanceOf[ColumnarBatch]
        logDebug(s"Caching shuffle_id=${handle.shuffleId} map_id=$mapId, partId=$partId, "
            + s"batch=[num_cols=${batch.numCols()}, num_rows=${batch.numRows()}]")
        recordsWritten = recordsWritten + batch.numRows()
        var partSize: Long = 0
        val blockId = ShuffleBlockId(handle.shuffleId, mapId, partId)
        if (batch.numRows > 0 && batch.numCols > 0) {
          // Add the table to the shuffle store
          val handle = batch.column(0) match {
            case c: GpuPackedTableColumn =>
              val contigTable = c.getContiguousTable
              partSize = c.getTableBuffer.getLength
              uncompressedMetric += partSize
              catalog.addContiguousTable(
                blockId,
                contigTable,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_PRIORITY,
                // we don't need to sync here, because we sync on the cuda
                // stream after sliceInternalOnGpu (contiguous_split)
                needsSync = false)
            case c: GpuCompressedColumnVector =>
              val buffer = c.getTableBuffer
              partSize = buffer.getLength
              val tableMeta = c.getTableMeta
              uncompressedMetric += tableMeta.bufferMeta().uncompressedSize()
              catalog.addBuffer(
                blockId,
                buffer,
                tableMeta,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_PRIORITY,
                // we don't need to sync here, because we sync on the cuda
                // stream after compression.
                needsSync = false)
            case c =>
              throw new IllegalStateException(s"Unexpected column type: ${c.getClass}")
          }
          bytesWritten += partSize
          // if the size is 0 and we have rows, we are in a case where there are columns
          // but the type is such that there isn't a buffer in the GPU backing it.
          // For example, a Struct column without any members. We treat such a case as if it
          // were a degenerate table.
          if (partSize == 0 && batch.numRows() > 0) {
            sizes(partId) += DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT
          } else {
            sizes(partId) += partSize
          }
          handle
        } else {
          // no device data, tracking only metadata
          val tableMeta = MetaUtils.buildDegenerateTableMeta(batch)
          val handle =
            catalog.addDegenerateRapidsBuffer(
              blockId,
              tableMeta)

          // ensure that we set the partition size to the default in this case if
          // we have non-zero rows, so this degenerate batch is shuffled.
          if (batch.numRows > 0) {
            sizes(partId) += DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT
          }
          handle
        }
      }
      metricsReporter.incBytesWritten(bytesWritten)
      metricsReporter.incRecordsWritten(recordsWritten)
    } finally {
      nvtxRange.close()
    }
  }

  /**
   * Used to remove shuffle buffers when the writing task detects an error, calling `stop(false)`
   */
  private def cleanStorage(): Unit = {
    catalog.removeCachedHandles()
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    val nvtxRange = new NvtxRange("RapidsCachingWriter.close", NvtxColor.CYAN)
    try {
      if (!success) {
        cleanStorage()
        None
      } else {
        // upon seeing this port, the other side will try to connect to the port
        // in order to establish an UCX endpoint (on demand), if the topology has "rapids" in it.
        val shuffleServerId = if (rapidsShuffleServer.isDefined) {
          val originalShuffleServerId = rapidsShuffleServer.get.originalShuffleServerId
          val server = rapidsShuffleServer.get
          BlockManagerId(
            originalShuffleServerId.executorId,
            originalShuffleServerId.host,
            originalShuffleServerId.port,
            Some(s"${RapidsShuffleTransport.BLOCK_MANAGER_ID_TOPO_PREFIX}=${server.getPort}"))
        } else {
          blockManager.shuffleServerId
        }
        logInfo(s"Done caching shuffle success=$success, server_id=$shuffleServerId, "
            + s"map_id=$mapId, sizes=${sizes.mkString(",")}")
        Some(MapStatus(shuffleServerId, sizes, mapId))
      }
    } finally {
      nvtxRange.close()
    }
  }

  def getPartitionLengths(): Array[Long] = {
    throw new UnsupportedOperationException("TODO")
  }
}

/**
 * A shuffle manager optimized for the RAPIDS Plugin For Apache Spark.
 * @note This is an internal class to obtain access to the private
 *       `ShuffleManager` and `SortShuffleManager` classes. When configuring
 *       Apache Spark to use the RAPIDS shuffle manager,
 */
class RapidsShuffleInternalManagerBase(conf: SparkConf, val isDriver: Boolean)
    extends ShuffleManager with RapidsShuffleHeartbeatHandler with Logging {

  def getServerId: BlockManagerId = server.fold(blockManager.blockManagerId)(_.getId)

  override def addPeer(peer: BlockManagerId): Unit = {
    transport.foreach { t =>
      try {
        t.connect(peer)
      } catch {
        case ex: Exception =>
          // We ignore the exception after logging in this instance because
          // we may have a peer that doesn't exist anymore by the time `addPeer` is invoked
          // due to a heartbeat response from the driver, or the peer may have a temporary network
          // issue.
          //
          // This is safe because `addPeer` is only invoked due to a heartbeat that is used to
          // opportunistically hide cost of initializing transport connections. The transport
          // will re-try if it must fetch from this executor at a later time, in that case
          // a connection failure causes the tasks to fail.
          logWarning(s"Unable to connect to peer $peer, ignoring!", ex)
      }
    }
  }

  private val rapidsConf = new RapidsConf(conf)

  if (!isDriver && rapidsConf.isMultiThreadedShuffleManagerMode) {
    RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
      rapidsConf.shuffleMultiThreadedWriterThreads,
      rapidsConf.shuffleMultiThreadedReaderThreads)
  }

  protected val wrapped = new SortShuffleManager(conf)

  private[this] val transportEnabledMessage =
    if (!rapidsConf.isUCXShuffleManagerMode) {
      if (rapidsConf.isCacheOnlyShuffleManagerMode) {
        "Transport disabled (local cached blocks only)"
      } else {
        val numWriteThreads = rapidsConf.shuffleMultiThreadedWriterThreads
        val numReadThreads = rapidsConf.shuffleMultiThreadedReaderThreads
        s"Multi-threaded shuffle mode " +
          s"(write threads=$numWriteThreads, read threads=$numReadThreads)"
      }
    } else {
      s"Transport enabled (remote fetches will use ${rapidsConf.shuffleTransportClassName}"
    }

  logWarning(s"Rapids Shuffle Plugin enabled. ${transportEnabledMessage}. To disable the " +
      s"RAPIDS Shuffle Manager set `${RapidsConf.SHUFFLE_MANAGER_ENABLED}` to false")

  //Many of these values like blockManager are not initialized when the constructor is called,
  // so they all need to be lazy values that are executed when things are first called

  // NOTE: this can be null in the driver side.
  protected lazy val env = SparkEnv.get
  protected lazy val blockManager = env.blockManager
  protected lazy val shouldFallThroughOnEverything = {
    val fallThroughReasons = new ListBuffer[String]()
    if (!rapidsConf.isMultiThreadedShuffleManagerMode) {
      if (GpuShuffleEnv.isExternalShuffleEnabled) {
        fallThroughReasons += "External Shuffle Service is enabled"
      }
      if (GpuShuffleEnv.isSparkAuthenticateEnabled) {
        fallThroughReasons += "Spark authentication is enabled"
      }
    }
    if (rapidsConf.isSqlExplainOnlyEnabled) {
      fallThroughReasons += "Plugin is in explain only mode"
    }
    if (fallThroughReasons.nonEmpty) {
      logWarning(s"Rapids Shuffle Plugin is falling back to SortShuffleManager " +
          s"because: ${fallThroughReasons.mkString(", ")}")
    }
    fallThroughReasons.nonEmpty
  }

  private lazy val localBlockManagerId = blockManager.blockManagerId

  // Used to prevent stopping multiple times RAPIDS Shuffle Manager internals.
  // see the `stop` method
  private var stopped: Boolean = false

  // Code that expects the shuffle catalog to be initialized gets it this way,
  // with error checking in case we are in a bad state.
  protected def getCatalogOrThrow: ShuffleBufferCatalog =
    Option(GpuShuffleEnv.getCatalog).getOrElse(
      throw new IllegalStateException("The ShuffleBufferCatalog is not initialized but the " +
          "RapidsShuffleManager is configured"))

  protected lazy val resolver =
    if (shouldFallThroughOnEverything || rapidsConf.isMultiThreadedShuffleManagerMode) {
      wrapped.shuffleBlockResolver
    } else { // we didn't fallback && we are using the UCX shuffle
      val catalog = GpuShuffleEnv.getCatalog
      if (catalog == null) {
        if (isDriver) {
          // this is an OK state to be in. It means we didn't fall back
          // (`shouldFallbackThroughOnEverything` is false) and this is just the driver
          // in a job with RapidsShuffleManager enabled. We want to just use the regular
          // shuffle block resolver here, since we don't do anything on the driver.
          wrapped.shuffleBlockResolver
        } else {
          // this would be bad: if we are an executor, didn't fallback, and RapidsShuffleManager
          // is enabled, we need to fail.
          throw new IllegalStateException(
            "An executor with RapidsShuffleManager is trying to use a ShuffleBufferCatalog " +
                "that isn't initialized."
          )
        }
      } else {
        // A driver in local mode with the RapidsShuffleManager enabled would go through this
        // else statement, because the "executor" is the driver, and isDriver=true, or
        // The regular case where the executor has RapidsShuffleManager enabled.
        // What these cases have in common is that `catalog` is defined.
        new GpuShuffleBlockResolver(wrapped.shuffleBlockResolver, catalog)
      }
    }

  private[this] lazy val transport: Option[RapidsShuffleTransport] = {
    if (rapidsConf.isUCXShuffleManagerMode && !isDriver) {
      Some(RapidsShuffleTransport.makeTransport(blockManager.shuffleServerId, rapidsConf))
    } else {
      None
    }
  }

  private[this] lazy val server: Option[RapidsShuffleServer] = {
    if (rapidsConf.isGPUShuffle && !isDriver) {
      val catalog = getCatalogOrThrow
      val requestHandler = new RapidsShuffleRequestHandler() {
        override def acquireShuffleBuffer(tableId: Int): RapidsBuffer = {
          val handle = catalog.getShuffleBufferHandle(tableId)
          catalog.acquireBuffer(handle)
        }

        override def getShuffleBufferMetas(sbbId: ShuffleBlockBatchId): Seq[TableMeta] = {
          (sbbId.startReduceId to sbbId.endReduceId).flatMap(rid => {
            catalog.blockIdToMetas(ShuffleBlockId(sbbId.shuffleId, sbbId.mapId, rid))
          })
        }
      }
      val server = transport.get.makeServer(requestHandler)
      server.start()
      Some(server)
    } else {
      None
    }
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Always register with the wrapped handler so we can write to it ourselves if needed
    val orig = wrapped.registerShuffle(shuffleId, dependency)

    dependency match {
      case _ if shouldFallThroughOnEverything ||
        rapidsConf.isMultiThreadedShuffleManagerMode => orig
      case gpuDependency: GpuShuffleDependency[K, V, C] if gpuDependency.useGPUShuffle =>
        new GpuShuffleHandle(orig,
          dependency.asInstanceOf[GpuShuffleDependency[K, V, V]])
      case _ => orig
    }
  }

  lazy val execComponents: Option[ShuffleExecutorComponents] = {
    import scala.collection.JavaConverters._
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    Some(executorComponents)
  }

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  protected val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  private def trackMapTaskForCleanup(shuffleId: Int, mapId: Long): Unit = {
    // this uses OpenHashSet as it is copied from Spark
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized {
      mapTaskIds.add(mapId)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle, mapId: Long, context: TaskContext,
    metricsReporter: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case gpu: GpuShuffleHandle[_, _] =>
        registerGpuShuffle(handle.shuffleId)
        new RapidsCachingWriter(
          env.blockManager,
          gpu.asInstanceOf[GpuShuffleHandle[K, V]],
          mapId,
          metricsReporter,
          getCatalogOrThrow,
          server,
          gpu.dependency.metrics)
      case bmssh: BypassMergeSortShuffleHandle[_, _] =>
        bmssh.dependency match {
          case gpuDep: GpuShuffleDependency[_, _, _]
              if gpuDep.useMultiThreadedShuffle &&
                  rapidsConf.shuffleMultiThreadedWriterThreads > 0 =>
            // use the threaded writer if the number of threads specified is 1 or above,
            // with 0 threads we fallback to the Spark-provided writer.
            val handleWithMetrics = new ShuffleHandleWithMetrics(
              bmssh.shuffleId,
              gpuDep.metrics,
              // cast the handle with specific generic types due to type-erasure
              gpuDep.asInstanceOf[GpuShuffleDependency[K, V, V]])
            // we need to track this mapId so we can clean it up later on unregisterShuffle
            trackMapTaskForCleanup(handle.shuffleId, context.taskAttemptId())
            // in most scenarios, the pools have already started, except for local mode
            // here we try to start them if we see they haven't
            RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
              rapidsConf.shuffleMultiThreadedWriterThreads,
              rapidsConf.shuffleMultiThreadedReaderThreads)
            new RapidsShuffleThreadedWriter[K, V](
              blockManager,
              handleWithMetrics,
              mapId,
              conf,
              new ThreadSafeShuffleWriteMetricsReporter(metricsReporter),
              rapidsConf.shuffleMultiThreadedMaxBytesInFlight,
              execComponents.get,
              rapidsConf.shuffleMultiThreadedWriterThreads)
          case _ =>
            wrapped.getWriter(handle, mapId, context, metricsReporter)
        }
      case _ =>
        wrapped.getWriter(handle, mapId, context, metricsReporter)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case gpuHandle: GpuShuffleHandle[_, _] =>
        logInfo(s"Asking map output tracker for dependency ${gpuHandle.dependency}, " +
            s"map output sizes for: ${gpuHandle.shuffleId}, parts=$startPartition-$endPartition")
        if (gpuHandle.dependency.keyOrdering.isDefined) {
          // very unlikely, but just in case
          throw new IllegalStateException("A key ordering was requested for a gpu shuffle "
              + s"dependency ${gpuHandle.dependency.keyOrdering.get}, this is not supported.")
        }

        val nvtxRange = new NvtxRange("getMapSizesByExecId", NvtxColor.CYAN)
        val blocksByAddress = try {
          SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(gpuHandle.shuffleId,
            startMapIndex, endMapIndex, startPartition, endPartition)
        } finally {
          nvtxRange.close()
        }

        new RapidsCachingReader(rapidsConf, localBlockManagerId,
          blocksByAddress,
          context,
          metrics,
          transport,
          getCatalogOrThrow,
          gpuHandle.dependency.sparkTypes)
      case other: ShuffleHandle if
          rapidsConf.isMultiThreadedShuffleManagerMode
            && rapidsConf.shuffleMultiThreadedReaderThreads > 0 =>
        // we enable a multi-threaded reader in the case where we have 1 or
        // more threads and we have enbled the MULTITHREADED shuffle mode.
        // We special case the threads=1 case in the reader to behave like regular
        // spark, but this allows us to add extra metrics that Spark normally
        // doesn't look at while materializing blocks.
        val baseHandle = other.asInstanceOf[BaseShuffleHandle[K, C, C]]

        // we check that the dependency is a `GpuShuffleDependency` and if not
        // we go back to the regular path (e.g. is a GpuColumnarExchange?)
        // TODO: it may make sense to expand this code (and the writer code) to include
        //   regular Exchange nodes. For now this is being conservative and a few changes
        //   would need to be made to deal with missing metrics, for example, for a regular
        //   Exchange node.
        baseHandle.dependency match {
          case gpuDep: GpuShuffleDependency[K, C, C] if gpuDep.useMultiThreadedShuffle =>
            // We want to use batch fetch in the non-push shuffle case. Spark
            // checks for a config to see if batch fetch is enabled (this check), and
            // it also checks when getting (potentially merged) map status from
            // the MapOutputTracker.
            val canUseBatchFetch =
              SortShuffleManager.canUseBatchFetch(startPartition, endPartition, context)

            val shuffleHandleWithMetrics = new ShuffleHandleWithMetrics(
              baseHandle.shuffleId, gpuDep.metrics, gpuDep)
            // in most scenarios, the pools have already started, except for local mode
            // here we try to start them if we see they haven't
            RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(
              rapidsConf.shuffleMultiThreadedWriterThreads,
              rapidsConf.shuffleMultiThreadedReaderThreads)
            new RapidsShuffleThreadedReader(
              startMapIndex,
              endMapIndex,
              startPartition,
              endPartition,
              shuffleHandleWithMetrics,
              context,
              metrics,
              rapidsConf.shuffleMultiThreadedMaxBytesInFlight,
              canUseBatchFetch = canUseBatchFetch,
              numReaderThreads = rapidsConf.shuffleMultiThreadedReaderThreads)
          case _ =>
            val shuffleHandle = RapidsShuffleInternalManagerBase.unwrapHandle(other)
            wrapped.getReader(shuffleHandle, startMapIndex, endMapIndex, startPartition,
              endPartition, context, metrics)
        }
      case other =>
        val shuffleHandle = RapidsShuffleInternalManagerBase.unwrapHandle(other)
        wrapped.getReader(shuffleHandle, startMapIndex, endMapIndex, startPartition,
          endPartition, context, metrics)
    }
  }

  def registerGpuShuffle(shuffleId: Int): Unit = {
    val catalog = GpuShuffleEnv.getCatalog
    if (catalog != null) {
      // Note that in local mode this can be called multiple times.
      logInfo(s"Registering shuffle $shuffleId")
      catalog.registerShuffle(shuffleId)
    }
  }

  def unregisterGpuShuffle(shuffleId: Int): Unit = {
    val catalog = GpuShuffleEnv.getCatalog
    if (catalog != null) {
      logInfo(s"Unregistering shuffle $shuffleId from shuffle buffer catalog")
      catalog.unregisterShuffle(shuffleId)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    unregisterGpuShuffle(shuffleId)
    shuffleBlockResolver match {
      case isbr: IndexShuffleBlockResolver =>
        Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
          mapTaskIds.iterator.foreach { mapTaskId =>
            isbr.removeDataByMap(shuffleId, mapTaskId)
          }
        }
      case _: GpuShuffleBlockResolver => // noop
      case _ =>
        throw new IllegalStateException(
          "unregisterShuffle called with unexpected resolver " +
            s"$shuffleBlockResolver and blocks left to be cleaned")
    }
    wrapped.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = resolver

  override def stop(): Unit = synchronized {
    wrapped.stop()
    if (!stopped) {
      stopped = true
      server.foreach(_.close())
      transport.foreach(_.close())
      if (rapidsConf.isMultiThreadedShuffleManagerMode) {
        RapidsShuffleInternalManagerBase.stopThreadPool()
      }
    }
  }
}
