/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import java.io.{File, FileInputStream, IOException, OutputStream}
import java.util.Optional
import java.util.concurrent.{Callable, ConcurrentHashMap, ExecutionException, Executors, Future, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.NvtxRegistry
import com.nvidia.spark.rapids.RapidsConf
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.kudo.OpenByteArrayOutputStream
import com.nvidia.spark.rapids.shuffle.{RapidsShuffleRequestHandler, RapidsShuffleServer, RapidsShuffleTransport}

import org.apache.spark.{InterruptibleIterator, MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{ShuffleWriter, _}
import org.apache.spark.shuffle.api._
import org.apache.spark.shuffle.sort.{BypassMergeSortShuffleHandle, SortShuffleManager}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.{METRIC_DATA_READ_SIZE, METRIC_DATA_SIZE, METRIC_SHUFFLE_DESERIALIZATION_TIME, METRIC_SHUFFLE_READ_TIME}
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
      extends RapidsShuffleWriter[K, V]
        with RapidsShuffleWriterShimHelper {
  private val dep: ShuffleDependency[K, V, V] = handle.dependency
  private val shuffleId = dep.shuffleId
  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val serializer = dep.serializer.newInstance()
  private val transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true)
  private val fileBufferSize = 64 << 10
  private val limiter = new BytesInFlightLimiter(maxBytesInFlight)

  private var shuffleWriteRange: NvtxId = NvtxRegistry.THREADED_WRITER_WRITE.push()

  // Case class for tracking partial sorted files in multi-batch scenario
  private case class PartialFile(
    file: File,
    partitionLengths: Array[Long],
    mapOutputWriter: ShuffleMapOutputWriter)
  
  // Encapsulates all state for processing one GPU batch
  private case class BatchState(
    batchId: Int,
    mapOutputWriter: ShuffleMapOutputWriter,
    partitionBuffers: ConcurrentHashMap[Int, OpenByteArrayOutputStream],
    partitionFutures: ConcurrentHashMap[Int, 
      java.util.concurrent.CopyOnWriteArrayList[Future[(Long, Long)]]],
    partitionBytesProgress: ConcurrentHashMap[Int, Long],
    partitionFuturesProgress: ConcurrentHashMap[Int, Int],
    maxPartitionSeen: java.util.concurrent.atomic.AtomicInteger,
    processingComplete: java.util.concurrent.atomic.AtomicBoolean,
    writerCondition: Object,
    writerExecutor: java.util.concurrent.ExecutorService,
    writerFuture: Future[_])

  /**
   * Increment the reference count and get the memory size for a value.
   * This method handles ColumnarBatch values with SlicedGpuColumnVector or
   * SlicedSerializedColumnVector columns.
   *
   * @param value the value to process (typically a ColumnarBatch)
   * @return a tuple of (ColumnarBatch with incremented ref count, memory size)
   */
  private def incRefCountAndGetSize(value: Any): (ColumnarBatch, Long) = {
    value match {
      case columnarBatch: ColumnarBatch =>
        if (columnarBatch.numCols() > 0) {
          columnarBatch.column(0) match {
            case _: SlicedGpuColumnVector =>
              (SlicedGpuColumnVector.incRefCount(columnarBatch),
                SlicedGpuColumnVector.getTotalHostMemoryUsed(columnarBatch))
            case _: SlicedSerializedColumnVector =>
              (SlicedSerializedColumnVector.incRefCount(columnarBatch),
                SlicedSerializedColumnVector.getTotalHostMemoryUsed(
                  columnarBatch))
            case _ =>
              (null, 0L)
          }
        } else {
          (columnarBatch, 0L)
        }
      case _ =>
        (null, 0L)
    }
  }

  /**
   * Create independent state for processing one GPU batch.
   * This allows multiple batches to be processed in pipeline without blocking.
   */
  private def createBatchState(
      batchId: Int,
      writer: ShuffleMapOutputWriter): BatchState = {
    import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
    import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}
    
    val partitionBuffers = new ConcurrentHashMap[Int, OpenByteArrayOutputStream]()
    val partitionFutures = new ConcurrentHashMap[Int, 
      java.util.concurrent.CopyOnWriteArrayList[Future[(Long, Long)]]]()
    val partitionBytesProgress = new ConcurrentHashMap[Int, Long]()
    val partitionFuturesProgress = new ConcurrentHashMap[Int, Int]()
    val maxPartitionSeen = new AtomicInteger(-1)
    val processingComplete = new AtomicBoolean(false)
    val writerCondition = new Object()

    // Create dedicated writer thread for this batch
    val writerThreadFactory = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, 
          s"rapids-shuffle-writer-${shuffleId}-${mapId}-batch${batchId}")
        thread.setDaemon(true)
        thread
      }
    }
    val writerExecutor = Executors.newSingleThreadExecutor(writerThreadFactory)

    var unfinishedStream: Option[OutputStream] = None
    
    // Helper to write partition buffer
    def writePartitionBuffer(
        partitionId: Int,
        start: Long,
        end: Long,
        doCleanUp: Boolean): Unit = {
      Option(partitionBuffers.get(partitionId)) match {
        case Some(buffer) =>
          if (unfinishedStream.isEmpty) {
            unfinishedStream = Some(writer.getPartitionWriter(partitionId).openStream())
          }
          if (end - start > 0) {
            unfinishedStream.get.write(buffer.getBuf, start.toInt, (end - start).toInt)
          }
          if (doCleanUp) {
            buffer.close()
            partitionBuffers.remove(partitionId)
            unfinishedStream.get.close()
            unfinishedStream = None
            partitionFutures.remove(partitionId)
            partitionFuturesProgress.remove(partitionId)
            partitionBytesProgress.remove(partitionId)
          }
        case None =>
          throw new IllegalStateException(
            s"No buffer found for partition $partitionId in batch $batchId")
      }
    }

    // Writer task for this batch
    val writerTask = new Runnable {
      override def run(): Unit = {
        var currentPartitionToWrite = 0
        while (!processingComplete.get() || 
            currentPartitionToWrite != maxPartitionSeen.get()) {
          if (currentPartitionToWrite <= maxPartitionSeen.get()) {
            var containsLastForThisPartition = false
            var futures: java.util.concurrent.CopyOnWriteArrayList[Future[(Long, Long)]] = null
            futures = partitionFutures.get(currentPartitionToWrite)
            if (currentPartitionToWrite < maxPartitionSeen.get()) {
              containsLastForThisPartition = true
            }

            if (futures != null) {
              import scala.collection.JavaConverters._
              var newFutureTouched = false
              val futuresProgress =
                 partitionFuturesProgress.getOrDefault(currentPartitionToWrite, 0)
              futures.asScala.zipWithIndex.filter(pair => {
                pair._2 >= futuresProgress
              }).foreach { future =>
                newFutureTouched = true
                val (recordSize, compressedSize) = future._1.get()

                val bytesProgress = 
                  partitionBytesProgress.getOrDefault(currentPartitionToWrite, 0L)
                writePartitionBuffer(currentPartitionToWrite,
                  bytesProgress,
                  bytesProgress + compressedSize,
                  doCleanUp = false)

                partitionBytesProgress.put(
                  currentPartitionToWrite, bytesProgress + compressedSize)
                partitionFuturesProgress.compute(currentPartitionToWrite, 
                  (key, value) => { value + 1 })

                limiter.release(recordSize)
              }

              if (containsLastForThisPartition) {
                writePartitionBuffer(currentPartitionToWrite, 0, 0, doCleanUp = true)
                currentPartitionToWrite += 1
              } else {
                if (!newFutureTouched) {
                  writerCondition.synchronized {
                    writerCondition.wait(1)
                  }
                }
              }
            } else {
              val partWriter = writer.getPartitionWriter(currentPartitionToWrite)
              partWriter.openStream().close()
              currentPartitionToWrite += 1
            }
          } else {
            writerCondition.synchronized {
              writerCondition.wait(1)
            }
          }
        }
      }
    }
    
    val writerFuture = writerExecutor.submit(writerTask)

    BatchState(
      batchId,
      writer,
      partitionBuffers,
      partitionFutures,
      partitionBytesProgress,
      partitionFuturesProgress,
      maxPartitionSeen,
      processingComplete,
      writerCondition,
      writerExecutor,
      writerFuture)
  }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      shuffleId,
      mapId,
      numPartitions)
    mapOutputWriters += mapOutputWriter  // Track for cleanup
    
    val partLengths = if (!records.hasNext) {
      commitAllPartitions(mapOutputWriter, true)
    } else {
      writePartitionedBatch(records, mapOutputWriter)
    }

    myMapStatus = Some(getMapStatus(blockManager.shuffleServerId, partLengths, mapId))

    if (shuffleWriteRange != null) {
      shuffleWriteRange.pop()
      shuffleWriteRange = null
    }
  }

  /**
   * Unified write path that handles both single batch and multi-batch tasks.
   * Leverages streaming parallel processing with pipelined partition writing.
   * 
   * For single batch: Main thread processes all records without blocking, while a dedicated
   * background writer thread waits for each partition to complete and writes them in order.
   * 
   * For multi-batch: Detects partition ID decreasing (indicates new batch), creates
   * partial sorted files for each batch, then merges them in the final output.
   * Each batch has independent state for true pipeline processing.
   */
  private def writePartitionedBatch(
      records: Iterator[Product2[Any, Any]],
      mapOutputWriter: ShuffleMapOutputWriter): Array[Long] = {
    
    import java.util.concurrent.atomic.AtomicLong
    import java.util.concurrent.ConcurrentHashMap
    import scala.collection.mutable.ArrayBuffer
    
    val serializerInstance = serializer
    var recordsWritten: Long = 0L
    
    // Track timing for metrics
    val writeStartTime = System.nanoTime()
    // Track total written size (compressed size)
    val totalCompressedSize = new AtomicLong(0L)
    var waitTimeOnLimiterNs: Long = 0L

    // Multi-batch tracking
    val batchStates = new ArrayBuffer[BatchState]()
    val partialFiles = new ArrayBuffer[PartialFile]()
    var currentBatchId: Int = 0
    var previousMaxPartition: Int = -1
    var isMultiBatch: Boolean = false

    // Assign a slot number to each partition for consistent serialization
    val partitionSlots = new ConcurrentHashMap[Int, Int]()
    
    // Create initial batch state
    var currentBatch = createBatchState(currentBatchId, mapOutputWriter)
    
    try {
      while (records.hasNext) {
        val record = records.next()
        val key = record._1
        val value = record._2
        val reducePartitionId: Int = partitioner.getPartition(key)
        
        // Detect multi-batch: partition ID decreased means new batch started
        if (reducePartitionId < previousMaxPartition) {
          if (!isMultiBatch) {
            isMultiBatch = true
            logInfo(s"Detected multi-batch scenario for shuffle $shuffleId, " +
              s"transitioning to pipeline mode")
          }
          
          // Signal current batch is complete (but don't block next batch!)
          currentBatch.maxPartitionSeen.set(numPartitions)
          currentBatch.processingComplete.set(true)
          currentBatch.writerCondition.synchronized { 
            currentBatch.writerCondition.notifyAll() 
          }
          
          // Add to list for later finalization
          batchStates += currentBatch
          
          // Immediately create new batch and continue processing (pipeline!)
          currentBatchId += 1
          val newWriter = shuffleExecutorComponents.createMapOutputWriter(
            shuffleId,
            mapId,
            numPartitions)
          mapOutputWriters += newWriter  // Track for cleanup
          currentBatch = createBatchState(currentBatchId, newWriter)
          
          previousMaxPartition = -1
        }
        
        recordsWritten += 1
        previousMaxPartition = math.max(previousMaxPartition, reducePartitionId)

        // Get or create futures queue for this partition in current batch
        val futures = currentBatch.partitionFutures.computeIfAbsent(reducePartitionId, 
          _ => new java.util.concurrent.CopyOnWriteArrayList[Future[(Long, Long)]]())

        val (cb, recordSize) = incRefCountAndGetSize(value)

        // Acquire limiter and process compression task immediately
        val waitOnLimiterStart = System.nanoTime()
        limiter.acquireOrBlock(recordSize)
        waitTimeOnLimiterNs += System.nanoTime() - waitOnLimiterStart

        // Get or assign a slot number for this partition to ensure
        // all tasks for the same partition run serially in the same slot
        val slotNum = partitionSlots.computeIfAbsent(reducePartitionId,
          _ => RapidsShuffleInternalManagerBase.getNextWriterSlot)
        val future = RapidsShuffleInternalManagerBase.queueWriteTask(slotNum, () => {
          try {
            withResource(cb) { _ =>
              // Get or create buffer for this partition in current batch
              val buffer = currentBatch.partitionBuffers.computeIfAbsent(
                reducePartitionId, _ => new OpenByteArrayOutputStream())
              val originLength = buffer.getCount

              // Serialize + compress to memory buffer
              val compressedOutputStream = blockManager.serializerManager.wrapStream(
                ShuffleBlockId(shuffleId, mapId, reducePartitionId), buffer)

              val serializationStream = serializerInstance.serializeStream(
                compressedOutputStream)
              withResource(serializationStream) { serializer =>
                serializer.writeKey(key.asInstanceOf[Any])
                serializer.writeValue(value.asInstanceOf[Any])
              }

              // Track total written data size (compressed size)
              val compressedSize = (buffer.getCount - originLength).toLong
              totalCompressedSize.addAndGet(compressedSize)
              (recordSize, compressedSize)
            }
          } catch {
            case e: Exception => {
              logError(s"Exception in compression task for shuffle $shuffleId", e)
              throw e
            }
          }
        })

        futures.add(future)
        currentBatch.maxPartitionSeen.set(
          math.max(currentBatch.maxPartitionSeen.get(), reducePartitionId))

        // Wake up writer thread for current batch
        currentBatch.writerCondition.synchronized { 
          currentBatch.writerCondition.notifyAll() 
        }
      }

      // Mark end of last batch - ensure all partitions are processed
      currentBatch.maxPartitionSeen.set(numPartitions)
      currentBatch.processingComplete.set(true)
      currentBatch.writerCondition.synchronized { 
        currentBatch.writerCondition.notifyAll() 
      }
      
      // Add last batch to list
      batchStates += currentBatch

      // Wait for all batches to complete (now they can finish in parallel!)
      batchStates.foreach { batch =>
        try {
          batch.writerFuture.get()
        } catch {
          case ee: ExecutionException => throw ee.getCause
        }

        // CRITICAL: For multi-batch, preserve outputTempFile before any commit
        // commitAllPartitions() would rename/move the file, so we extract first
        if (isMultiBatch) {
          val (tempFile, partLengths) = extractFileAndLengthsFromWriter(
            batch.mapOutputWriter)
          partialFiles += PartialFile(tempFile, partLengths, batch.mapOutputWriter)
        } else {
          // Single batch: commit normally
          val partLengths = commitAllPartitions(batch.mapOutputWriter, true)
          partialFiles += PartialFile(null, partLengths, batch.mapOutputWriter)
        }
      }

      // Update write metrics
      val totalWriteTime = System.nanoTime() - writeStartTime
      writeMetrics.incWriteTime(totalWriteTime - waitTimeOnLimiterNs)
      writeMetrics.incRecordsWritten(recordsWritten)
      writeMetrics.incBytesWritten(totalCompressedSize.get())

    } finally {
      // Cleanup all batch states
      import scala.collection.JavaConverters._
      batchStates.foreach { batch =>
        try {
          batch.writerExecutor.shutdown()
          if (!batch.writerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
            batch.writerExecutor.shutdownNow()
          }
        } catch {
          case _: InterruptedException =>
            batch.writerExecutor.shutdownNow()
            Thread.currentThread().interrupt()
        }

        // Cancel pending futures
        batch.partitionFutures.values().asScala.foreach { futuresQueue =>
          futuresQueue.asScala.foreach(_.cancel(true))
          futuresQueue.clear()
        }

        // Clean up buffers
        val iter = batch.partitionBuffers.values().iterator()
        while (iter.hasNext()) {
          try {
            iter.next().close()
          } catch {
            case _: Exception => /* ignore */
          }
        }
      }
    }

    // Handle final output
    if (isMultiBatch) {
      // Multi-batch: create NEW writer for final merge
      // CRITICAL: Cannot reuse mapOutputWriter as it would write to same outputTempFile
      val finalMergeWriter = shuffleExecutorComponents.createMapOutputWriter(
      shuffleId,
      mapId,
      numPartitions)
      mapOutputWriters += finalMergeWriter  // Track for cleanup
      
      mergePartialFiles(partialFiles.toSeq, finalMergeWriter)
    } else {
      // Single batch: already committed, just return lengths
      getPartitionLengths
    }
  }
  
  /**
   * Merge multiple partial sorted files into final output.
   * Each partial file contains data for all partitions (0 to N) from one GPU batch.
   * The merged file will have: partition 0 from all batches, partition 1 from all batches, etc.
   * 
   * Layout of merged file:
   *   partition 0 data from partial file 0
   *   partition 0 data from partial file 1
   *   ...
   *   partition 0 data from partial file M
   *   partition 1 data from partial file 0
   *   partition 1 data from partial file 1
   *   ...
   */
  private def mergePartialFiles(
      partialFiles: Seq[PartialFile],
      finalWriter: ShuffleMapOutputWriter): Array[Long] = {

    val mergeStartTime = System.nanoTime()

    // Open all partial files once, reuse across all partitions
    // Since partial files are sorted by partition, we read sequentially
    val partialFileInputs = partialFiles.map { pf =>
      new FileInputStream(pf.file)
    }

    try {
      // For each partition, copy data from all partial files in order
      (0 until numPartitions).foreach { partitionId =>
        val partWriter = finalWriter.getPartitionWriter(partitionId)
        val maybeOutputChannel: Optional[WritableByteChannelWrapper] =
          partWriter.openChannelWrapper()

        if (maybeOutputChannel.isPresent && transferToEnabled) {
          // Use channel-based fast copy with NIO
          val outputChannel = maybeOutputChannel.get()
          withResource(outputChannel) { _ =>
            partialFiles.zip(partialFileInputs).foreach {
              case (partialFile, inputStream) =>
                val partitionLength = partialFile.partitionLengths(partitionId)
                if (partitionLength > 0) {
                  val inputChannel = inputStream.getChannel
                  val currentPos = inputChannel.position()
                  Utils.copyFileStreamNIO(
                    inputChannel,
                    outputChannel.channel,
                    currentPos,
                    partitionLength)
                  // Ensure position advances (copyFileStreamNIO should do this)
                  inputChannel.position(currentPos + partitionLength)
                }
            }
          }
        } else {
          // Use stream-based copy (no channels, pure streams)
          withResource(partWriter.openStream()) { os =>
            partialFiles.zip(partialFileInputs).foreach {
              case (partialFile, inputStream) =>
                val partitionLength = partialFile.partitionLengths(partitionId)
                if (partitionLength > 0) {
                  // Read sequentially from current stream position
                  // No skip/offset needed - stream position auto-advances!
                  val buffer = new Array[Byte](fileBufferSize)
                  var remainingBytes = partitionLength
                  while (remainingBytes > 0) {
                    val bytesToRead = math.min(remainingBytes, buffer.length).toInt
                    val bytesRead = inputStream.read(buffer, 0, bytesToRead)
                    if (bytesRead > 0) {
                      os.write(buffer, 0, bytesRead)
                      remainingBytes -= bytesRead
                    } else {
                      throw new IOException(
                        s"Unexpected end of file reading partition $partitionId")
                    }
                  }
                }
            }
          }
        }
      }
    } finally {
      // Close all partial file streams
      partialFileInputs.foreach { in =>
        try {
          in.close()
        } catch {
          case _: Exception => /* ignore */
        }
      }
      partialFiles.foreach { pf =>
        try {
          pf.file.delete()
        } catch {
          case _: Exception => /* ignore */
        }
      }
    }

    writeMetrics.incWriteTime(System.nanoTime() - mergeStartTime)

    // Clean up partial files
    partialFiles.foreach { partialFile =>
      if (partialFile.file != null && partialFile.file.exists()) {
        partialFile.file.delete()
      }
    }

    // Commit final merged output
    commitAllPartitions(finalWriter, true)
  }
  
  /**
   * Extract outputTempFile and partitionLengths from ShuffleMapOutputWriter.
   * 
   * IMPORTANT: For partial batches in multi-batch scenario, we should NOT call
   * commitAllPartitions() as it would rename/move outputTempFile. Instead, we just
   * close the streams and extract the file reference for later merge.
   * 
   * NOTE: Uses reflection to access LocalDiskShuffleMapOutputWriter internals.
   */
  private def extractFileAndLengthsFromWriter(
      writer: ShuffleMapOutputWriter): (File, Array[Long]) = {
    try {
      val clazz = writer.getClass
      
      // First, ensure all streams are closed by calling cleanUp
      val cleanUpMethod = clazz.getDeclaredMethod("cleanUp")
      cleanUpMethod.setAccessible(true)
      cleanUpMethod.invoke(writer)
      
      // Access outputTempFile field
      val fileField = clazz.getDeclaredField("outputTempFile")
      fileField.setAccessible(true)
      val file = fileField.get(writer).asInstanceOf[File]
      
      // Access partitionLengths field
      val lengthsField = clazz.getDeclaredField("partitionLengths")
      lengthsField.setAccessible(true)
      val lengths = lengthsField.get(writer).asInstanceOf[Array[Long]]
      
      if (file == null) {
        throw new IllegalStateException(
          "outputTempFile is null - writer may not have written any data yet")
      }
      
      (file, lengths)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          "Failed to extract outputTempFile and partitionLengths from " +
          "ShuffleMapOutputWriter. Consider implementing " +
          "RapidsLocalDiskShuffleMapOutputWriter.", e)
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
  private val deserializationTimeNs = sqlMetrics.get(METRIC_SHUFFLE_DESERIALIZATION_TIME)
  private val shuffleReadTimeNs = sqlMetrics.get(METRIC_SHUFFLE_READ_TIME)
  private val dataReadSize = sqlMetrics.get(METRIC_DATA_READ_SIZE)

  private var shuffleReadRange: NvtxId = NvtxRegistry.THREADED_READER_READ.push()

  private def closeShuffleReadRange(): Unit = {
    if (shuffleReadRange != null) {
      shuffleReadRange.pop()
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
        pendingIts.nonEmpty || futures.nonEmpty || queued.size() > 0 ||
          fetcherIterator.hasNext 
      }
    }

    case class BlockState(
        blockId: BlockId,
        batchIter: BaseSerializedTableIterator,
        origStream: AutoCloseable)
      extends Iterator[(Any, Any)] with AutoCloseable {

      private var nextBatchSize = {
        var success = false
        try {
          val res = batchIter.peekNextBatchSize().getOrElse(0L)
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
          nextBatchSize = batchIter.peekNextBatchSize().getOrElse(0L)
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
      val res = NvtxRegistry.PARALLEL_DESERIALIZER_ITERATOR_NEXT {
        val result = if (fallbackIter != null) {
          fallbackIter.next()
        } else {
          var waitTime: Long = 0L
          var waitTimeStart: Long = 0L
          popFetchedIfAvailable()
          waitTime = 0L
          if (futures.nonEmpty) {
            NvtxRegistry.BATCH_WAIT {
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
        result
      }

      // if this is the last call, close our range
      if (!hasNext) {
        closeShuffleReadRange()
      }

      res
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
          NvtxRegistry.QUEUE_FETCHED {
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
              val batchIter = deserStream.asKeyValueIterator
                .asInstanceOf[BaseSerializedTableIterator]
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
  extends RapidsCachingWriterBase[K, V](blockManager, handle, mapId, rapidsShuffleServer, catalog) {

  private val uncompressedMetric: SQLMetric = metrics(METRIC_DATA_SIZE)

  // This is here for the special case where we have no columns like with the .count
  // case or when we have 0-byte columns. We pick 100 as an arbitrary number so that
  // we can shuffle these degenerate batches, which have valid metadata and should be
  // used on the reducer side for computation.
  private val DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT: Long = 100L

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // NOTE: This MUST NOT CLOSE the incoming batches because they are
    //       closed by the input iterator generated by GpuShuffleExchangeExec
    NvtxRegistry.RAPIDS_CACHING_WRITER_WRITE {
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
          batch.column(0) match {
            case c: GpuPackedTableColumn =>
              val contigTable = c.getContiguousTable
              partSize = c.getTableBuffer.getLength
              uncompressedMetric += partSize
              catalog.addContiguousTable(
                blockId,
                contigTable,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_TASK_PRIORITY)
            case c: GpuCompressedColumnVector =>
              partSize = c.getTableBuffer.getLength
              uncompressedMetric += c.getTableMeta.bufferMeta().uncompressedSize()
              catalog.addCompressedBatch(
                blockId,
                batch,
                SpillPriorities.OUTPUT_FOR_SHUFFLE_INITIAL_TASK_PRIORITY)
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
        } else {
          // no device data, tracking only metadata
          val tableMeta = MetaUtils.buildDegenerateTableMeta(batch)
          catalog.addDegenerateRapidsBuffer(
            blockId,
            tableMeta)

          // ensure that we set the partition size to the default in this case if
          // we have non-zero rows, so this degenerate batch is shuffled.
          if (batch.numRows > 0) {
            sizes(partId) += DEGENERATE_PARTITION_BYTE_SIZE_DEFAULT
          }
        }
      }
      metricsReporter.incBytesWritten(bytesWritten)
      metricsReporter.incRecordsWritten(recordsWritten)
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
        override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
          catalog.getShuffleBufferHandle(tableId)
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

        val blocksByAddress = NvtxRegistry.GET_MAP_SIZES_BY_EXEC_ID {
          SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(gpuHandle.shuffleId,
            startMapIndex, endMapIndex, startPartition, endPartition)
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
