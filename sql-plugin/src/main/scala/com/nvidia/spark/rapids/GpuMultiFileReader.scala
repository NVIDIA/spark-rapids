/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, ExecutorCompletionService, Future, ThreadPoolExecutor, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Queue}
import scala.collection.mutable
import scala.language.implicitConversions

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric.{BUFFER_TIME, FILTER_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.InputFileUtils
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * This contains a single HostMemoryBuffer along with other metadata needed
 * for combining the buffers before sending to GPU.
 */
case class SingleHMBAndMeta(hmb: HostMemoryBuffer, bytes: Long, numRows: Long,
    blockMeta: Seq[DataBlockBase])

object SingleHMBAndMeta {
  // Contains no data but could have number of rows for things like count().
  def empty(numRows: Long = 0): SingleHMBAndMeta = {
    SingleHMBAndMeta(null.asInstanceOf[HostMemoryBuffer], 0, numRows, Seq.empty)
  }
}

/**
 * The base HostMemoryBuffer information read from a single file.
 */
trait HostMemoryBuffersWithMetaDataBase {
  // PartitionedFile to be read
  def partitionedFile: PartitionedFile
  // Original PartitionedFile if path was replaced with Alluxio
  def origPartitionedFile: Option[PartitionedFile] = None
  // An array of BlockChunk(HostMemoryBuffer and its data size) read from PartitionedFile
  def memBuffersAndSizes: Array[SingleHMBAndMeta]
  // Total bytes read
  def bytesRead: Long

  // Time spent on filtering
  private var _filterTime: Long = 0L
  // Time spent on buffering
  private var _bufferTime: Long = 0L

  // The partition values which are needed if combining host memory buffers
  // after read by the multithreaded reader but before sending to GPU.
  def allPartValues: Option[Array[(Long, InternalRow)]] = None

  // Called by parquet/orc/avro scanners to set the amount of time (in nanoseconds)
  // that filtering and buffering incurred in one of the scan runners.
  def setMetrics(filterTime: Long, bufferTime: Long): Unit = {
    _bufferTime = bufferTime
    _filterTime = filterTime
  }

  def getBufferTime: Long = _bufferTime
  def getFilterTime: Long = _filterTime

  def getBufferTimePct: Double = {
    val totalTime = _filterTime + _bufferTime
    _bufferTime.toDouble / totalTime
  }

  def getFilterTimePct: Double = {
    val totalTime = _filterTime + _bufferTime
    _filterTime.toDouble / totalTime
  }
}

// This is a common trait for all kind of file formats
trait MultiFileReaderFunctions {
  @scala.annotation.nowarn(
    "msg=method getAllStatistics in class FileSystem is deprecated"
  )
  protected def fileSystemBytesRead(): Long = {
    FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
  }
}

// Singleton thread pool used across all tasks for multifile reading.
// Please note that the TaskContext is not set in these threads and should not be used.
object MultiFileReaderThreadPool extends Logging {
  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      maxThreads: Int,
      keepAliveSeconds: Int = 60): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      val threadPoolExecutor =
        TrampolineUtil.newDaemonCachedThreadPool("multithreaded file reader worker", maxThreads,
          keepAliveSeconds)
      threadPoolExecutor.allowCoreThreadTimeOut(true)
      logDebug(s"Using $maxThreads for the multithreaded reader thread pool")
      threadPool = Some(threadPoolExecutor)
    }
    threadPool.get
  }

  /**
   * Get the existing thread pool or create one with the given thread count if it does not exist.
   * @note The thread number will be ignored if the thread pool is already created, or modified
   *       if it is not the right size compared to the number of cores available.
   */
  def getOrCreateThreadPool(numThreadsFromConf: Int): ThreadPoolExecutor = {
    threadPool.getOrElse {
      val numThreads = Math.max(numThreadsFromConf, GpuDeviceManager.getNumCores)

      if (numThreadsFromConf != numThreads) {
        logWarning(s"Configuring the file reader thread pool with a max of $numThreads " +
            s"threads instead of ${RapidsConf.MULTITHREAD_READ_NUM_THREADS} = $numThreadsFromConf")
      }
      initThreadPool(numThreads)
    }
  }
}

object MultiFileReaderUtils {

  private implicit def toURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme != null) {
        return uri
      }
    } catch {
      case _: URISyntaxException =>
    }
    new File(path).getAbsoluteFile.toURI
  }

  private def hasPathInCloud(filePaths: Array[String], cloudSchemes: Set[String]): Boolean = {
    // Assume the `filePath` always has a scheme, if not try using the local filesystem.
    // If that doesn't work for some reasons, users need to configure it directly.
    filePaths.exists(fp => cloudSchemes.contains(fp.getScheme))
  }

  // If Alluxio is enabled and we do task time replacement we have to take that
  // into account here so we use the Coalescing reader instead of the MultiThreaded reader.
  def useMultiThreadReader(
      coalescingEnabled: Boolean,
      multiThreadEnabled: Boolean,
      files: Array[String],
      cloudSchemes: Set[String],
      anyAlluxioPathsReplaced: Boolean = false): Boolean =
    !coalescingEnabled || (multiThreadEnabled &&
      (!anyAlluxioPathsReplaced && hasPathInCloud(files, cloudSchemes)))
}

/**
 * The base multi-file partition reader factory to create the cloud reading or
 * coalescing reading respectively.
 *
 * @param sqlConf         the SQLConf
 * @param broadcastedConf the Hadoop configuration
 * @param rapidsConf      the Rapids configuration
 * @param alluxioPathReplacementMap Optional map containing mapping of DFS
 *                                  scheme to Alluxio scheme
 */
abstract class MultiFilePartitionReaderFactoryBase(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    @transient rapidsConf: RapidsConf,
    alluxioPathReplacementMap: Option[Map[String, String]] = None)
  extends PartitionReaderFactory with Logging {

  protected val maxReadBatchSizeRows: Int = rapidsConf.maxReadBatchSizeRows
  protected val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes
  protected val targetBatchSizeBytes: Long = rapidsConf.gpuTargetBatchSizeBytes
  protected val maxGpuColumnSizeBytes: Long = rapidsConf.maxGpuColumnSizeBytes
  protected val useChunkedReader: Boolean = rapidsConf.chunkedReaderEnabled
  protected val maxChunkedReaderMemoryUsageSizeBytes: Long =
    if(rapidsConf.limitChunkedReaderMemoryUsage) {
      (rapidsConf.chunkedReaderMemoryUsageRatio * targetBatchSizeBytes).toLong
    } else {
      0L
    }
  private val allCloudSchemes = rapidsConf.getCloudSchemes.toSet

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  /**
   * An abstract method to indicate if coalescing reading can be used
   */
  protected def canUseCoalesceFilesReader: Boolean

  /**
   * An abstract method to indicate if cloud reading can be used
   */
  protected def canUseMultiThreadReader: Boolean

  /**
   * Build the PartitionReader for cloud reading
   *
   * @param files files to be read
   * @param conf configuration
   * @return cloud reading PartitionReader
   */
  protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch]

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  protected def getFileFormatShortName: String

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val files = filePartition.files
    val filePaths = files.map(_.filePath.toString())
    val conf = broadcastedConf.value.value

    if (useMultiThread(filePaths)) {
      logInfo("Using the multi-threaded multi-file " + getFileFormatShortName + " reader, " +
        s"files: ${filePaths.mkString(",")} task attemptid: ${TaskContext.get.taskAttemptId()}")
      buildBaseColumnarReaderForCloud(files, conf)
    } else {
      logInfo("Using the coalesce multi-file " + getFileFormatShortName + " reader, files: " +
        s"${filePaths.mkString(",")} task attemptid: ${TaskContext.get.taskAttemptId()}")
      buildBaseColumnarReaderForCoalescing(files, conf)
    }
  }

  /** for testing */
  private[rapids] def useMultiThread(filePaths: Array[String]): Boolean =
    MultiFileReaderUtils.useMultiThreadReader(canUseCoalesceFilesReader,
      canUseMultiThreadReader, filePaths, allCloudSchemes, alluxioPathReplacementMap.isDefined)
}

/**
 * The base class for PartitionReader
 *
 * @param conf        Configuration
 * @param execMetrics metrics
 */
abstract class FilePartitionReaderBase(conf: Configuration, execMetrics: Map[String, GpuMetric])
    extends PartitionReader[ColumnarBatch] with Logging with ScanWithMetrics {

  metrics = execMetrics

  protected var isDone: Boolean = false
  protected var batchIter: Iterator[ColumnarBatch] = EmptyGpuColumnarBatchIterator

  override def get(): ColumnarBatch = {
    batchIter.next()
  }

  override def close(): Unit = {
    batchIter = EmptyGpuColumnarBatchIterator
    isDone = true
  }
}

// Contains the actual file path to read from and then an optional original path if its read from
// Alluxio. To make it transparent to the user, we return the original non-Alluxio path
// for input_file_name.
case class PartitionedFileInfoOptAlluxio(toRead: PartitionedFile, original: Option[PartitionedFile])

case class CombineConf(
    combineThresholdSize: Long, // The size to combine to when combining small files
    combineWaitTime: Int) // The amount of time to wait for other files ready for combination.

/**
 * The Abstract multi-file cloud reading framework
 *
 * The data driven:
 * next() -> if (first time) initAndStartReaders -> submit tasks (getBatchRunner)
 *        -> wait tasks done sequentially -> decode in GPU (readBatch)
 *
 * @param conf Configuration parameters
 * @param inputFiles PartitionFiles to be read
 * @param numThreads the number of threads to read files in parallel.
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param filters push down filters
 * @param execMetrics the metrics
 * @param ignoreCorruptFiles Whether to ignore corrupt files when GPU failed to decode the files
 * @param alluxioPathReplacementMap Map containing mapping of DFS scheme to Alluxio scheme
 * @param alluxioReplacementTaskTime Whether the Alluxio replacement algorithm is set to task time
 * @param keepReadsInOrder Whether to require the files to be read in the same order as Spark.
 *                         Defaults to true for formats that don't explicitly handle this.
 * @param combineConf configs relevant to combination
 */
abstract class MultiFileCloudPartitionReaderBase(
    conf: Configuration,
    inputFiles: Array[PartitionedFile],
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric],
    maxReadBatchSizeRows: Int,
    maxReadBatchSizeBytes: Long,
    ignoreCorruptFiles: Boolean = false,
    alluxioPathReplacementMap: Map[String, String] = Map.empty,
    alluxioReplacementTaskTime: Boolean = false,
    keepReadsInOrder: Boolean = true,
    combineConf: CombineConf = CombineConf(-1, -1))
  extends FilePartitionReaderBase(conf, execMetrics) {

  private var filesToRead = 0
  protected var currentFileHostBuffers: Option[HostMemoryBuffersWithMetaDataBase] = None
  protected var combineLeftOverFiles: Option[Array[HostMemoryBuffersWithMetaDataBase]] = None
  private var isInitted = false
  private val tasks = new ConcurrentLinkedQueue[Future[HostMemoryBuffersWithMetaDataBase]]()
  private val tasksToRun = new Queue[Callable[HostMemoryBuffersWithMetaDataBase]]()
  private[this] val inputMetrics = Option(TaskContext.get).map(_.taskMetrics().inputMetrics)
    .getOrElse(TrampolineUtil.newInputMetrics())
  // this is used when the read order doesn't matter and in that case, the tasks queue
  // above is used to track any left being processed that need to be cleaned up if we exit early
  // like in the case of a limit call and we don't read all files
  private var fcs: ExecutorCompletionService[HostMemoryBuffersWithMetaDataBase] = null

  private val files: Array[PartitionedFileInfoOptAlluxio] = {
    if (alluxioPathReplacementMap.nonEmpty) {
      if (alluxioReplacementTaskTime) {
        AlluxioUtils.updateFilesTaskTimeIfAlluxio(inputFiles, Some(alluxioPathReplacementMap))
      } else {
        // was done at CONVERT_TIME, need to recalculate the original path to set for
        // input_file_name
        AlluxioUtils.getOrigPathFromReplaced(inputFiles, alluxioPathReplacementMap)
      }
    } else {
      inputFiles.map(PartitionedFileInfoOptAlluxio(_, None))
    }
  }

  private def initAndStartReaders(): Unit = {
    // limit the number we submit at once according to the config if set
    val limit = math.min(maxNumFileProcessed, files.length)
    val tc = TaskContext.get
    if (!keepReadsInOrder) {
      logDebug("Not keeping reads in order")
      synchronized {
        if (fcs == null) {
          val threadPool =
            MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
          fcs = new ExecutorCompletionService[HostMemoryBuffersWithMetaDataBase](threadPool)
        }
      }
    } else {
      logDebug("Keeping reads in same order")
    }

    // Currently just add the files in order, we may consider doing something with the size of
    // the files in the future. ie try to start some of the larger files but we may not want
    // them all to be large
    for (i <- 0 until limit) {
      val file = files(i)
      logDebug(s"MultiFile reader using file ${file.toRead}, orig file is ${file.original}")
      if (!keepReadsInOrder) {
        val futureRunner = fcs.submit(getBatchRunner(tc, file.toRead, file.original, conf, filters))
        tasks.add(futureRunner)
      } else {
        // Add these in the order as we got them so that we can make sure
        // we process them in the same order as CPU would.
        val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
        tasks.add(threadPool.submit(getBatchRunner(tc, file.toRead, file.original, conf, filters)))
      }
    }
    // queue up any left to add once others finish
    for (i <- limit until files.length) {
      val file = files(i)
      tasksToRun.enqueue(getBatchRunner(tc, file.toRead, file.original, conf, filters))
    }
    isInitted = true
    filesToRead = files.length
  }

  // Each format should implement combineHMBs and canUseCombine if they support combining
  def combineHMBs(
      results: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    require(results.size == 1,
        "Expected results to only have 1 element, this format doesn't support combining!")
    results.head
  }

  def canUseCombine: Boolean = false

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc task context to use
   * @param file file to be read
   * @param origFile optional original unmodified file if replaced with Alluxio
   * @param conf the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      origFile: Option[PartitionedFile],
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase]

  /**
   * Decode HostMemoryBuffers in GPU
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return an iterator of batches that were decoded
   */
  def readBatches(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  /**
   * Given a set of host buffers actually read have the GPU read them and update the
   * batchIter with the returned Columnar batches.
   */
  private def readBuffersToBatch(currentFileHostBuffers: HostMemoryBuffersWithMetaDataBase,
      addTaskIfNeeded: Boolean): Unit = {
    if (getNumRowsInHostBuffers(currentFileHostBuffers) == 0) {
      closeCurrentFileHostBuffers()
      if (addTaskIfNeeded) addNextTaskIfNeeded()
      next()
    } else {
      val file = currentFileHostBuffers.partitionedFile.filePath
      batchIter = try {
        readBatches(currentFileHostBuffers)
      } catch {
        case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(s"Skipped the corrupted file: $file", e)
          EmptyGpuColumnarBatchIterator
      }
      // the data is copied to GPU so submit another task if we were limited
      if (addTaskIfNeeded) addNextTaskIfNeeded()
    }
  }

  // we have to check both the combine threshold and the batch size limits
  private def hasMetCombineThreshold(sizeInBytes: Long, numRows: Long): Boolean = {
    sizeInBytes >= combineConf.combineThresholdSize || sizeInBytes >= maxReadBatchSizeBytes ||
      numRows >= maxReadBatchSizeRows
  }

  /**
   * While there are files already read into host memory buffers, take up to
   * threshold size and append to the results ArrayBuffer.
   */
  private def readReadyFiles(
      initSize: Long,
      initNumRows: Long,
      results: ArrayBuffer[HostMemoryBuffersWithMetaDataBase]): Unit = {
    var takeMore = true
    var currSize = initSize
    var currNumRows = initNumRows
    while (takeMore && !hasMetCombineThreshold(currSize, currNumRows) && filesToRead > 0) {
      val hmbFuture = if (keepReadsInOrder) {
        tasks.poll()
      } else {
        val fut = fcs.poll()
        if (fut != null) {
          tasks.remove(fut)
        }
        fut
      }
      if (hmbFuture == null) {
        if (combineConf.combineWaitTime > 0) {
          // no more are ready, wait to see if any finish within wait time
          val hmbAndMeta = if (keepReadsInOrder) {
            tasks.poll().get(combineConf.combineWaitTime, TimeUnit.MILLISECONDS)
          } else {
            val fut = fcs.poll(combineConf.combineWaitTime, TimeUnit.MILLISECONDS)
            if (fut == null) {
              null
            } else {
              tasks.remove(fut)
              fut.get()
            }
          }
          if (hmbAndMeta != null) {
            results.append(hmbAndMeta)
            currSize += hmbAndMeta.memBuffersAndSizes.map(_.bytes).sum
            filesToRead -= 1
          } else {
            // no more ready after waiting
            takeMore = false
          }
        } else {
          // wait time is <= 0
          takeMore = false
        }
      } else {
        results.append(hmbFuture.get())
        currSize += hmbFuture.get().memBuffersAndSizes.map(_.bytes).sum
        currNumRows += hmbFuture.get().memBuffersAndSizes.map(_.numRows).sum
        filesToRead -= 1
      }
    }
  }

  private def getNextBuffersAndMetaAndCombine(): HostMemoryBuffersWithMetaDataBase = {
    var sizeRead = 0L
    var numRowsRead = 0L
    val results = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    readReadyFiles(0, 0, results)
    if (results.isEmpty) {
      // none were ready yet so wait as long as need for first one
      val hostBuffersWithMeta = if (keepReadsInOrder) {
        tasks.poll().get()
      } else {
        val bufMetaFut = fcs.take()
        tasks.remove(bufMetaFut)
        bufMetaFut.get()
      }
      sizeRead += hostBuffersWithMeta.memBuffersAndSizes.map(_.bytes).sum
      numRowsRead += hostBuffersWithMeta.memBuffersAndSizes.map(_.numRows).sum

      filesToRead -= 1
      results.append(hostBuffersWithMeta)
      // since we had to wait for one to be ready,
      // check if more are ready as well
      readReadyFiles(sizeRead, numRowsRead, results)
    }
    combineHMBs(results.toArray)
  }

  private def getNextBuffersAndMetaSingleFile(): HostMemoryBuffersWithMetaDataBase = {
    val hmbAndMetaInfo = if (keepReadsInOrder) {
      tasks.poll().get()
    } else {
      val bufMetaFut = fcs.take()
      tasks.remove(bufMetaFut)
      bufMetaFut.get()
    }
    filesToRead -= 1
    hmbAndMetaInfo
  }

  private def getNextBuffersAndMeta(): HostMemoryBuffersWithMetaDataBase = {
    if (canUseCombine) {
      getNextBuffersAndMetaAndCombine()
    } else {
      getNextBuffersAndMetaSingleFile()
    }
  }

  /**
   * If we were combining host memory buffers and ran into the case we had to split them,
   * then we handle the left over ones first.
   */
  private def handleLeftOverCombineFiles(): HostMemoryBuffersWithMetaDataBase = {
    val leftOvers = combineLeftOverFiles.get
    // unset leftOverFiles because it will get reset in combineHMBs again if needed
    combineLeftOverFiles = None
    val results = ArrayBuffer[HostMemoryBuffersWithMetaDataBase]()
    val curSize = leftOvers.map(_.memBuffersAndSizes.map(_.bytes).sum).sum
    val curNumRows = leftOvers.map(_.memBuffersAndSizes.map(_.numRows).sum).sum
    readReadyFiles(curSize, curNumRows, results)
    val allReady = leftOvers ++ results
    val fileBufsAndMeta = combineHMBs(allReady)

    TrampolineUtil.incBytesRead(inputMetrics, fileBufsAndMeta.bytesRead)
    // this is combine mode so input file shouldn't be used at all but update to
    // what would be closest so we at least don't have same file as last batch
    val inputFileToSet =
    fileBufsAndMeta.origPartitionedFile.getOrElse(fileBufsAndMeta.partitionedFile)
    InputFileUtils.setInputFileBlock(
      inputFileToSet.filePath.toString(),
      inputFileToSet.start,
      inputFileToSet.length)
    fileBufsAndMeta
  }

  override def next(): Boolean = {
    withResource(new NvtxRange(getFileFormatShortName + " readBatch", NvtxColor.GREEN)) { _ =>
      if (!isInitted) {
        initAndStartReaders()
      }
      if (batchIter.hasNext) {
        // leave early we have something more to be read
        return true
      }

      // Temporary until we get more to read
      batchIter = EmptyGpuColumnarBatchIterator
      // if we have batch left from the last file read return it
      if (currentFileHostBuffers.isDefined) {
        readBuffersToBatch(currentFileHostBuffers.get, false)
      } else if (combineLeftOverFiles.isDefined) {
        // this means we already grabbed some while combining but something between
        // files was incompatible and couldn't be combined.
        val fileBufsAndMeta = handleLeftOverCombineFiles()
        readBuffersToBatch(fileBufsAndMeta, true)
      } else {
        if (filesToRead > 0 && !isDone) {
          // Filter time here includes the buffer time as well since those
          // happen in the same background threads. This is as close to wall
          // clock as we can get right now without further work.
          val startTime = System.nanoTime()
          val fileBufsAndMeta = getNextBuffersAndMeta()
          val blockedTime = System.nanoTime() - startTime
          metrics.get(FILTER_TIME).foreach {
            _ += (blockedTime * fileBufsAndMeta.getFilterTimePct).toLong
          }
          metrics.get(BUFFER_TIME).foreach {
            _ += (blockedTime * fileBufsAndMeta.getBufferTimePct).toLong
          }

          TrampolineUtil.incBytesRead(inputMetrics, fileBufsAndMeta.bytesRead)
          // if we replaced the path with Alluxio, set it to the original filesystem file
          // since Alluxio replacement is supposed to be transparent to the user
          // Note that combine mode would have fallen back to not use combine mode if
          // the inputFile was required.
          val inputFileToSet =
          fileBufsAndMeta.origPartitionedFile.getOrElse(fileBufsAndMeta.partitionedFile)
          InputFileUtils.setInputFileBlock(
            inputFileToSet.filePath.toString(),
            inputFileToSet.start,
            inputFileToSet.length)
          readBuffersToBatch(fileBufsAndMeta, true)
        } else {
          isDone = true
        }
      }
    }

    // this shouldn't happen but if somehow the batch is None and we still
    // have work left skip to the next file
    if (batchIter.isEmpty && filesToRead > 0 && !isDone) {
      next()
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batchReader` is
    // `EmptyBatchReader`. We are not acquiring the semaphore here since this next() is getting
    // called from the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  private def getNumRowsInHostBuffers(fileInfo: HostMemoryBuffersWithMetaDataBase): Long = {
    fileInfo.memBuffersAndSizes.map(_.numRows).sum
  }

  private def addNextTaskIfNeeded(): Unit = {
    if (tasksToRun.nonEmpty && !isDone) {
      val runner = tasksToRun.dequeue()
      if (!keepReadsInOrder) {
        val futureRunner = fcs.submit(runner)
        tasks.add(futureRunner)
      } else {
        val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
        tasks.add(threadPool.submit(runner))
      }
    }
  }

  private def closeCurrentFileHostBuffers(): Unit = {
    currentFileHostBuffers.foreach { current =>
      current.memBuffersAndSizes.foreach { hbInfo =>
        if (hbInfo.hmb != null) {
          hbInfo.hmb.close()
        }
      }
    }
    currentFileHostBuffers = None
  }

  override def close(): Unit = {
    // this is more complicated because threads might still be processing files
    // in cases close got called early for like limit() calls
    isDone = true
    closeCurrentFileHostBuffers()
    batchIter = EmptyGpuColumnarBatchIterator
    tasks.asScala.foreach { task =>
      if (task.isDone()) {
        task.get.memBuffersAndSizes.foreach { hmbInfo =>
          if (hmbInfo.hmb != null) {
            hmbInfo.hmb.close()
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

// A trait to describe a data block info
trait DataBlockBase {
  // get the row number of this data block
  def getRowCount: Long
  // the data read size
  def getReadDataSize: Long
  // the block size to be used to slice the whole HostMemoryBuffer
  def getBlockSize: Long
}

/**
 * A common trait for different schema in the MultiFileCoalescingPartitionReaderBase.
 *
 * The sub-class should wrap the real schema for the specific file format
 */
trait SchemaBase {
  def isEmpty: Boolean
}

/**
 * A common trait for the extra information for different file format
 */
trait ExtraInfo

/**
 * A single block info of a file,
 * Eg, A parquet file has 3 RowGroup, then it will produce 3 SingleBlockInfoWithMeta
 */
trait SingleDataBlockInfo {
  def filePath: Path // file path info
  def partitionValues: InternalRow // partition value
  def dataBlock: DataBlockBase // a single block info of a single file
  def schema: SchemaBase // schema information
  def readSchema: StructType // read schema information
  def extraInfo: ExtraInfo // extra information
}

/**
 * A context lives during the whole process of reading partitioned files
 * to a batch buffer (aka HostMemoryBuffer) to build a memory file.
 * Children can extend this to add more necessary fields.
 * @param origChunkedBlocks mapping of file path to data blocks
 * @param schema schema info
 */
class BatchContext(
  val origChunkedBlocks: mutable.LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
  val schema: SchemaBase) {}

/**
 * The abstracted multi-file coalescing reading class, which tries to coalesce small
 * ColumnarBatch into a bigger ColumnarBatch according to maxReadBatchSizeRows,
 * maxReadBatchSizeBytes and the [[checkIfNeedToSplitDataBlock]].
 *
 * Please be note, this class is applied to below similar file format
 *
 * | HEADER |   -> optional
 * | block  |   -> repeated
 * | FOOTER |   -> optional
 *
 * The data driven:
 *
 * next() -> populateCurrentBlockChunk (try the best to coalesce ColumnarBatch)
 *        -> allocate a bigger HostMemoryBuffer for HEADER + the populated block chunks + FOOTER
 *        -> write header to HostMemoryBuffer
 *        -> launch tasks to copy the blocks to the HostMemoryBuffer
 *        -> wait all tasks finished
 *        -> write footer to HostMemoryBuffer
 *        -> decode the HostMemoryBuffer in the GPU
 *
 * @param conf                  Configuration
 * @param clippedBlocks         the block metadata from the original file that has been
 *                              clipped to only contain the column chunks to be read
 * @param partitionSchema       schema of partitions
 * @param maxReadBatchSizeRows  soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param maxGpuColumnSizeBytes maximum number of bytes for a GPU column
 * @param numThreads            the size of the threadpool
 * @param execMetrics           metrics
 */
abstract class MultiFileCoalescingPartitionReaderBase(
    conf: Configuration,
    clippedBlocks: Seq[SingleDataBlockInfo],
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    numThreads: Int,
    execMetrics: Map[String, GpuMetric]) extends FilePartitionReaderBase(conf, execMetrics)
    with MultiFileReaderFunctions {

  private val blockIterator: BufferedIterator[SingleDataBlockInfo] =
    clippedBlocks.iterator.buffered
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  private case class CurrentChunkMeta(
    clippedSchema: SchemaBase,
    readSchema: StructType,
    currentChunk: Seq[(Path, DataBlockBase)],
    numTotalRows: Long,
    rowsPerPartition: Array[Long],
    allPartValues: Array[InternalRow],
    extraInfo: ExtraInfo)

  /**
   * To check if the next block will be split into another ColumnarBatch
   *
   * @param currentBlockInfo current SingleDataBlockInfo
   * @param nextBlockInfo    next SingleDataBlockInfo
   * @return true: split the next block into another ColumnarBatch and vice versa
   */
  def checkIfNeedToSplitDataBlock(
      currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean

  /**
   * Calculate the output size according to the block chunks and the schema, and the
   * estimated output size will be used as the initialized size of allocating HostMemoryBuffer
   *
   * Please be note, the estimated size should be at least equal to size of HEAD + Blocks + FOOTER
   *
   * @param batchContext the batch building context
   * @return Long, the estimated output size
   */
  def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long

  /**
   * Calculate the final block output size which will be used to decide
   * if re-allocate HostMemoryBuffer
   *
   * There is no need to re-calculate the block size, just calculate the footer size and
   * plus footerOffset.
   *
   * If the size calculated by this function is bigger than the one calculated
   * by calculateEstimatedBlocksOutputSize, then it will cause HostMemoryBuffer re-allocating, and
   * cause the performance issue.
   *
   * @param footerOffset footer offset
   * @param blocks       blocks to be evaluated
   * @param batchContext the batch building context
   * @return the output size
   */
  def calculateFinalBlocksOutputSize(footerOffset: Long, blocks: collection.Seq[DataBlockBase],
      batchContext: BatchContext): Long

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc task context to use
   * @param file file to be read
   * @param outhmb the sliced HostMemoryBuffer to hold the blocks, and the implementation
   *               is in charge of closing it in sub-class
   * @param blocks blocks meta info to specify which blocks to be read
   * @param offset used as the offset adjustment
   * @param batchContext the batch building context
   * @return Callable[(Seq[DataBlockBase], Long)], which will be submitted to a
   *         ThreadPoolExecutor, and the Callable will return a tuple result and
   *         result._1 is block meta info with the offset adjusted
   *         result._2 is the bytes read
   */
  def getBatchRunner(
      tc: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): Callable[(Seq[DataBlockBase], Long)]

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  /**
   * Sent host memory to GPU to decode
   *
   * @param dataBuffer the data which can be decoded in GPU
   * @param dataSize data size
   * @param clippedSchema the clipped schema
   * @param readSchema the expected schema
   * @param extraInfo the extra information for specific file format
   * @return Table
   */
  def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      clippedSchema: SchemaBase,
      readSchema: StructType,
      extraInfo: ExtraInfo): GpuDataProducer[Table]

  /**
   * Write a header for a specific file format. If there is no header for the file format,
   * just ignore it and return 0
   *
   * @param buffer where the header will be written
   * @param batchContext the batch building context
   * @return how many bytes written
   */
  def writeFileHeader(buffer: HostMemoryBuffer, batchContext: BatchContext): Long

  /**
   * Writer a footer for a specific file format. If there is no footer for the file format,
   * just return (hmb, offset)
   *
   * Please be note, some file formats may re-allocate the HostMemoryBuffer because of the
   * estimated initialized buffer size may be a little smaller than the actual size. So in
   * this case, the hmb should be closed in the implementation.
   *
   * @param buffer         The buffer holding (header + data blocks)
   * @param bufferSize     The total buffer size which equals to size of (header + blocks + footer)
   * @param footerOffset   Where begin to write the footer
   * @param blocks         The data block meta info
   * @param batchContext   The batch building context
   * @return the buffer and the buffer size
   */
  def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], batchContext: BatchContext): (HostMemoryBuffer, Long)

  /**
   * Return a batch context which will be shared during the process of building a memory file,
   * aka with the following APIs.
   *   - calculateEstimatedBlocksOutputSize
   *   - writeFileHeader
   *   - getBatchRunner
   *   - calculateFinalBlocksOutputSize
   *   - writeFileFooter
   * It is useful when something is needed by some or all of the above APIs.
   * Children can override this to return a customized batch context.
   * @param chunkedBlocks mapping of file path to data blocks
   * @param clippedSchema schema info
   */
  protected def createBatchContext(
      chunkedBlocks: mutable.LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      clippedSchema: SchemaBase): BatchContext = {
    new BatchContext(chunkedBlocks, clippedSchema)
  }

  /**
   * A callback to finalize the output batch. The batch returned will be the final
   * output batch of the reader's "get" method.
   *
   * @param batch the batch after decoding, adding partitioned columns.
   * @param extraInfo the corresponding extra information of the input batch.
   * @return the finalized columnar batch.
   */
  protected def finalizeOutputBatch(
      batch: ColumnarBatch,
      extraInfo: ExtraInfo): ColumnarBatch = {
    // Equivalent to returning the input batch directly.
    GpuColumnVector.incRefCounts(batch)
  }

  override def next(): Boolean = {
    if (batchIter.hasNext) {
      return true
    }
    batchIter = EmptyGpuColumnarBatchIterator
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
      } else {
        batchIter = readBatch()
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batchReader` is
    // `EmptyColumnarBatchReader`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchIter.hasNext
  }

  /**
   * You can reset the target batch size if needed for splits...
   */
  def startNewBufferRetry: Unit = ()

  private def readBatch(): Iterator[ColumnarBatch] = {
    withResource(new NvtxRange(s"$getFileFormatShortName readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkMeta = populateCurrentBlockChunk()
      val batchIter = if (currentChunkMeta.clippedSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        if (currentChunkMeta.numTotalRows == 0) {
          EmptyGpuColumnarBatchIterator
        } else {
          val rows = currentChunkMeta.numTotalRows.toInt
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val nullColumns = currentChunkMeta.readSchema.safeMap(f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
          val emptyBatch = new ColumnarBatch(nullColumns.toArray, rows)
          new SingleGpuColumnarBatchIterator(emptyBatch)
        }
      } else {
        val colTypes = currentChunkMeta.readSchema.fields.map(f => f.dataType)
        if (currentChunkMeta.currentChunk.isEmpty) {
          CachedGpuBatchIterator(EmptyTableReader, colTypes)
        } else {
          val (dataBuffer, dataSize) = readPartFiles(currentChunkMeta.currentChunk,
            currentChunkMeta.clippedSchema)
          if (dataSize == 0) {
            dataBuffer.close()
            CachedGpuBatchIterator(EmptyTableReader, colTypes)
          } else {
            startNewBufferRetry
            RmmRapidsRetryIterator.withRetryNoSplit(dataBuffer) { _ =>
              // We don't want to actually close the host buffer until we know that we don't
              // want to retry more, so offset the close for now.
              dataBuffer.incRefCount()
              val tableReader = readBufferToTablesAndClose(dataBuffer,
                dataSize, currentChunkMeta.clippedSchema, currentChunkMeta.readSchema,
                currentChunkMeta.extraInfo)
              CachedGpuBatchIterator(tableReader, colTypes)
            }
          }
        }
      }
      new GpuColumnarBatchWithPartitionValuesIterator(batchIter, currentChunkMeta.allPartValues,
        currentChunkMeta.rowsPerPartition, partitionSchema,
        maxGpuColumnSizeBytes).map { withParts =>
        withResource(withParts) { _ =>
          finalizeOutputBatch(withParts, currentChunkMeta.extraInfo)
        }
      }
    }
  }

  /**
   * Read all data blocks into HostMemoryBuffer
   * @param blocks a sequence of data blocks to be read
   * @param clippedSchema the clipped schema is used to calculate the estimated output size
   * @return (HostMemoryBuffer, Long)
   *         the HostMemoryBuffer and its data size
   */
  private def readPartFiles(
      blocks: Seq[(Path, DataBlockBase)],
      clippedSchema: SchemaBase): (HostMemoryBuffer, Long) = {

    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      // ugly but we want to keep the order
      val filesAndBlocks = LinkedHashMap[Path, ArrayBuffer[DataBlockBase]]()
      blocks.foreach { case (path, block) =>
        filesAndBlocks.getOrElseUpdate(path, new ArrayBuffer[DataBlockBase]) += block
      }
      val tasks = new java.util.ArrayList[Future[(Seq[DataBlockBase], Long)]]()

      val batchContext = createBatchContext(filesAndBlocks, clippedSchema)
      // First, estimate the output file size for the initial allocating.
      //   the estimated size should be >= size of HEAD + Blocks + FOOTER
      val initTotalSize = calculateEstimatedBlocksOutputSize(batchContext)
      val (buffer, bufferSize, footerOffset, outBlocks) =
        closeOnExcept(HostMemoryBuffer.allocate(initTotalSize)) { hmb =>
          // Second, write header
          var offset = writeFileHeader(hmb, batchContext)

          val allOutputBlocks = scala.collection.mutable.ArrayBuffer[DataBlockBase]()
          val tc = TaskContext.get
          val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
          filesAndBlocks.foreach { case (file, blocks) =>
            val fileBlockSize = blocks.map(_.getBlockSize).sum
            // use a single buffer and slice it up for different files if we need
            val outLocal = hmb.slice(offset, fileBlockSize)
            // Third, copy the blocks for each file in parallel using background threads
            tasks.add(threadPool.submit(
              getBatchRunner(tc, file, outLocal, blocks, offset, batchContext)))
            offset += fileBlockSize
          }

          for (future <- tasks.asScala) {
            val (blocks, bytesRead) = future.get()
            allOutputBlocks ++= blocks
            TrampolineUtil.incBytesRead(inputMetrics, bytesRead)
          }

          // Fourth, calculate the final buffer size
          val finalBufferSize = calculateFinalBlocksOutputSize(offset, allOutputBlocks.toSeq,
            batchContext)

          (hmb, finalBufferSize, offset, allOutputBlocks.toSeq)
        }

      // The footer size can change vs the initial estimated because we are combining more
      // blocks and offsets are larger, check to make sure we allocated enough memory before
      // writing. Not sure how expensive this is, we could throw exception instead if the
      // written size comes out > then the estimated size.
      var buf: HostMemoryBuffer = buffer
      val totalBufferSize = if (bufferSize > initTotalSize) {
        // Just ensure to close buffer when there is an exception
        closeOnExcept(buffer) { _ =>
          logWarning(s"The original estimated size $initTotalSize is too small, " +
            s"reallocating and copying data to bigger buffer size: $bufferSize")
        }
        // Copy the old buffer to a new allocated bigger buffer and close the old buffer
        buf = withResource(buffer) { _ =>
          withResource(new HostMemoryInputStream(buffer, footerOffset)) { in =>
            // realloc memory and copy
            closeOnExcept(HostMemoryBuffer.allocate(bufferSize)) { newhmb =>
              withResource(new HostMemoryOutputStream(newhmb)) { out =>
                IOUtils.copy(in, out)
              }
              newhmb
            }
          }
        }
        bufferSize
      } else {
        initTotalSize
      }

      // Fifth, write footer,
      // The implementation should be in charge of closing buf when there is an exception thrown,
      // Closing the original buf and returning a new allocated buffer is allowed, but there is no
      // reason to do that.
      // If you have to do this, please think about to add other abstract methods first.
      val (finalBuffer, finalBufferSize) = writeFileFooter(buf, totalBufferSize, footerOffset,
        outBlocks, batchContext)

      closeOnExcept(finalBuffer) { _ =>
        // triple check we didn't go over memory
        if (finalBufferSize > totalBufferSize) {
          throw new QueryExecutionException(s"Calculated buffer size $totalBufferSize is to " +
            s"small, actual written: ${finalBufferSize}")
        }
      }
      logDebug(s"$getFileFormatShortName Coalescing reading estimates the initTotalSize:" +
        s" $initTotalSize, and the true size: $finalBufferSize")
      (finalBuffer, finalBufferSize)
    }
  }

  /**
   * Populate block chunk with meta info according to maxReadBatchSizeRows
   * and maxReadBatchSizeBytes
   *
   * @return [[CurrentChunkMeta]]
   */
  private def populateCurrentBlockChunk(): CurrentChunkMeta = {
    val currentChunk = new ArrayBuffer[(Path, DataBlockBase)]
    var numRows: Long = 0
    var numBytes: Long = 0
    var numChunkBytes: Long = 0
    var currentFile: Path = null
    var currentPartitionValues: InternalRow = null
    var currentClippedSchema: SchemaBase = null
    var currentReadSchema: StructType = null
    val rowsPerPartition = new ArrayBuffer[Long]()
    var lastPartRows: Long = 0
    val allPartValues = new ArrayBuffer[InternalRow]()
    var currentDataBlock: SingleDataBlockInfo = null
    var extraInfo: ExtraInfo = null

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        if (currentFile == null) {
          // first time of readNextBatch
          currentDataBlock = blockIterator.head
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partitionValues
          allPartValues += currentPartitionValues
          currentClippedSchema = blockIterator.head.schema
          currentReadSchema = blockIterator.head.readSchema
          extraInfo = blockIterator.head.extraInfo
        }

        val peekedRowCount = blockIterator.head.dataBlock.getRowCount
        if (peekedRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }

        if (numRows == 0 || numRows + peekedRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(currentReadSchema, peekedRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            // only care to check if we are actually adding in the next chunk
            if (currentFile != blockIterator.head.filePath) {
              // check if need to split next data block into another ColumnarBatch
              if (checkIfNeedToSplitDataBlock(currentDataBlock, blockIterator.head)) {
                logInfo(s"splitting ${blockIterator.head.filePath} into another batch!")
                return
              }
              // If the partition values are different we have to track the number of rows that get
              // each partition value and the partition values themselves so that we can build
              // the full columns with different partition values later.
              if (blockIterator.head.partitionValues != currentPartitionValues) {
                rowsPerPartition += (numRows - lastPartRows)
                lastPartRows = numRows
                // we add the actual partition values here but then
                // the number of rows in that partition at the end or
                // when the partition changes
                allPartValues += blockIterator.head.partitionValues
              }
              currentFile = blockIterator.head.filePath
              currentPartitionValues = blockIterator.head.partitionValues
              currentClippedSchema = blockIterator.head.schema
              currentReadSchema = blockIterator.head.readSchema
              currentDataBlock = blockIterator.head
            }

            val nextBlock = blockIterator.next()
            val nextTuple = (nextBlock.filePath, nextBlock.dataBlock)
            currentChunk += nextTuple
            numRows += currentChunk.last._2.getRowCount
            numChunkBytes += currentChunk.last._2.getReadDataSize
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }
    readNextBatch()
    rowsPerPartition += (numRows - lastPartRows)
    logDebug(s"Loaded $numRows rows from ${getFileFormatShortName}. " +
      s"${getFileFormatShortName} bytes read: $numChunkBytes. Estimated GPU bytes: $numBytes. " +
      s"Number of different partitions: ${allPartValues.size}")
    CurrentChunkMeta(currentClippedSchema, currentReadSchema, currentChunk.toSeq,
      numRows, rowsPerPartition.toArray, allPartValues.toArray, extraInfo)
  }
}
