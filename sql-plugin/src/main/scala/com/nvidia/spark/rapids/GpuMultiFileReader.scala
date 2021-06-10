/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.Queue

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.nvidia.spark.rapids.GpuMetric.PEAK_DEVICE_MEMORY
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The base HostMemoryBuffer information read from a single file.
 */
trait HostMemoryBuffersWithMetaDataBase {
  // PartitionedFile to be read
  def partitionedFile: PartitionedFile
  // An array of BlockChunk(HostMemoryBuffer and its data size) read from PartitionedFile
  def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)]
  // Total bytes read
  def bytesRead: Long
}

// This is a common trait for all kind of file formats
trait MultiFileReaderFunctions extends Arm {

  // Add partitioned columns into the batch
  protected def addPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: InternalRow,
      partitionSchema: StructType): Option[ColumnarBatch] = {
    if (partitionSchema.nonEmpty) {
      batch.map { cb =>
        val partitionValues = inPartitionValues.toSeq(partitionSchema)
        val partitionScalars = ColumnarPartitionReaderWithPartitionValues
          .createPartitionValues(partitionValues, partitionSchema)
        withResource(partitionScalars) { scalars =>
          ColumnarPartitionReaderWithPartitionValues.addPartitionValues(cb, scalars,
            GpuColumnVector.extractTypes(partitionSchema))
        }
      }
    } else {
      batch
    }
  }

  protected def fileSystemBytesRead(): Long = {
    FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
  }
}

// A tool to create a ThreadPoolExecutor
// Please note that the TaskContext is not set in these threads and should not be used.
object MultiFileThreadPoolUtil {

  def createThreadPool(
      threadTag: String,
      maxThreads: Int = 20,
      keepAliveSeconds: Long = 60): ThreadPoolExecutor = {
    val threadFactory = new ThreadFactoryBuilder()
      .setNameFormat(threadTag + " reader worker-%d")
      .setDaemon(true)
      .build()

    val threadPoolExecutor = new ThreadPoolExecutor(
      maxThreads, // corePoolSize: max number of threads to create before queuing the tasks
      maxThreads, // maximumPoolSize: because we use LinkedBlockingDeque, this is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPoolExecutor.allowCoreThreadTimeOut(true)
    threadPoolExecutor
  }
}

/**
 * The Abstract multi-file cloud reading framework
 *
 * The data driven:
 * next() -> if (first time) initAndStartReaders -> submit tasks (getBatchRunner)
 *        -> wait tasks done sequentially -> decode in GPU (readBatch)
 *
 * @param conf Configuration parameters
 * @param files PartitionFiles to be read
 * @param numThreads the number of threads to read files parallelly.
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param filters push down filters
 * @param execMetrics the metrics
 */
abstract class MultiFileCloudPartitionReaderBase(
    conf: Configuration,
    files: Array[PartitionedFile],
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ScanWithMetrics with Arm {

  metrics = execMetrics

  protected var maxDeviceMemory: Long = 0

  protected var batch: Option[ColumnarBatch] = None
  protected var isDone: Boolean = false

  private var filesToRead = 0
  protected var currentFileHostBuffers: Option[HostMemoryBuffersWithMetaDataBase] = None
  private var isInitted = false
  private val tasks = new ConcurrentLinkedQueue[Future[HostMemoryBuffersWithMetaDataBase]]()
  private val tasksToRun = new Queue[Callable[HostMemoryBuffersWithMetaDataBase]]()
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  private def initAndStartReaders(): Unit = {
    // limit the number we submit at once according to the config if set
    val limit = math.min(maxNumFileProcessed, files.length)
    for (i <- 0 until limit) {
      val file = files(i)
      // Add these in the order as we got them so that we can make sure
      // we process them in the same order as CPU would.
      tasks.add(getThreadPool(numThreads).submit(getBatchRunner(file, conf, filters)))
    }
    // queue up any left to add once others finish
    for (i <- limit until files.length) {
      val file = files(i)
      tasksToRun.enqueue(getBatchRunner(file, conf, filters))
    }
    isInitted = true
    filesToRead = files.length
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param file file to be read
   * @param conf the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  def getBatchRunner(
    file: PartitionedFile,
    conf: Configuration,
    filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase]

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * There're two rules:
   * 1. Same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. Different file formats have different ThreadPoolExecutors
   *
   * @param numThreads  max number of threads to create
   * @return A ThreadPoolExecutors to used
   */
  def getThreadPool(numThreads: Int): ThreadPoolExecutor

  /**
   * Decode HostMemoryBuffers in GPU
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return Option[ColumnarBatch] which has been decoded by GPU
   */
  def readBatch(
    fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Option[ColumnarBatch]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  override def next(): Boolean = {
    withResource(new NvtxRange(getFileFormatShortName + " readBatch", NvtxColor.GREEN)) { _ =>
      if (isInitted == false) {
        initAndStartReaders()
      }
      batch.foreach(_.close())
      batch = None
      // if we have batch left from the last file read return it
      if (currentFileHostBuffers.isDefined) {
        if (getSizeOfHostBuffers(currentFileHostBuffers.get) == 0) {
          next()
        }
        batch = readBatch(currentFileHostBuffers.get)
      } else {
        currentFileHostBuffers = None
        if (filesToRead > 0 && !isDone) {
          val fileBufsAndMeta = tasks.poll.get()
          filesToRead -= 1
          TrampolineUtil.incBytesRead(inputMetrics, fileBufsAndMeta.bytesRead)
          InputFileUtils.setInputFileBlock(
            fileBufsAndMeta.partitionedFile.filePath,
            fileBufsAndMeta.partitionedFile.start,
            fileBufsAndMeta.partitionedFile.length)

          if (getSizeOfHostBuffers(fileBufsAndMeta) == 0) {
            // if sizes are 0 means no rows and no data so skip to next file
            // file data was empty so submit another task if any were waiting
            addNextTaskIfNeeded()
            next()
          } else {
            batch = readBatch(fileBufsAndMeta)
            // the data is copied to GPU so submit another task if we were limited
            addNextTaskIfNeeded()
          }
        } else {
          isDone = true
          metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
        }
      }
    }

    // this shouldn't happen but if somehow the batch is None and we still
    // have work left skip to the next file
    if (batch.isEmpty && filesToRead > 0 && !isDone) {
      next()
    }

    // This is odd, but some operators return data even when there is no input so we need to
    // be sure that we grab the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  private def getSizeOfHostBuffers(fileInfo: HostMemoryBuffersWithMetaDataBase): Long = {
    fileInfo.memBuffersAndSizes.map(_._2).sum
  }

  private def addNextTaskIfNeeded(): Unit = {
    if (tasksToRun.nonEmpty && !isDone) {
      val runner = tasksToRun.dequeue()
      tasks.add(getThreadPool(numThreads).submit(runner))
    }
  }

  override def close(): Unit = {
    // this is more complicated because threads might still be processing files
    // in cases close got called early for like limit() calls
    isDone = true
    currentFileHostBuffers.foreach { current =>
      current.memBuffersAndSizes.foreach { case (buf, _) =>
        if (buf != null) {
          buf.close()
        }
      }
    }
    currentFileHostBuffers = None
    batch.foreach(_.close())
    batch = None
    tasks.asScala.foreach { task =>
      if (task.isDone()) {
        task.get.memBuffersAndSizes.foreach { case (buf, _) =>
          if (buf != null) {
            buf.close()
          }
        }
      } else {
        // Note we are not interrupting thread here so it
        // will finish reading and then just discard. If we
        // interrupt HDFS logs warnings about being interrupted.
        task.cancel(false)
      }
    }
  }

}
