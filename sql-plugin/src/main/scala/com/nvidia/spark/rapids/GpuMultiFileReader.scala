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

import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Future}

import scala.collection.JavaConverters._
import scala.collection.mutable.Queue

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange}
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
  // An array of BlockChunk (HostMemoryBuffer and its size)
  def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)]
  def bytesRead: Long
}

trait MultiFileReaderFunctions extends Arm {
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

/**
 * The Abstract base class working as multi-file cloud reading framework
 * @param conf
 * @param files PartitionFiles to be read
 * @param numThreads the number of threads to parallelly read files.
 * @param maxNumFileProcessed
 * @param filters push down filters
 * @param execMetrics
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
      tasks.add(MultiFileThreadPoolFactory.submitToThreadPool(
        getBatchRunner(file, conf, filters), numThreads))
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
   * file reading logic in a Callable which will be running in a thread pool
   *
   * @param file file to be read
   * @param conf
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  def getBatchRunner(
    file: PartitionedFile,
    conf: Configuration,
    filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase]

  /**
   * decode HostMemoryBuffers by GPU
   * @param fileBufsAndMeta
   * @return
   */
  def readBatch(
    fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Option[ColumnarBatch]

  def getLogTag: String

  override def next(): Boolean = {
    withResource(new NvtxRange(getLogTag + " readBatch", NvtxColor.GREEN)) { _ =>
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
    if (!batch.isDefined && filesToRead > 0 && !isDone) {
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
    if (tasksToRun.size > 0 && !isDone) {
      val runner = tasksToRun.dequeue()
      tasks.add(MultiFileThreadPoolFactory.submitToThreadPool(runner, numThreads))
    }
  }

  override def close(): Unit = {
    // this is more complicated because threads might still be processing files
    // in cases close got called early for like limit() calls
    isDone = true
    currentFileHostBuffers.foreach { current =>
      current.memBuffersAndSizes.foreach { case (buf, size) =>
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
        task.get.memBuffersAndSizes.foreach { case (buf, size) =>
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
