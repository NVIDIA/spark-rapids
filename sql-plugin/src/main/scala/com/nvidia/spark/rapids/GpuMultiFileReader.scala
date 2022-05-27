/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Queue}
import scala.math.max

import ai.rapids.cudf.{ColumnVector, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.GpuMetric.{NUM_OUTPUT_BATCHES, PEAK_DEVICE_MEMORY, SEMAPHORE_WAIT_TIME}
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

  @scala.annotation.nowarn(
    "msg=method getAllStatistics in class FileSystem is deprecated"
  )
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

/** A thread pool for multi-file reading */
class MultiFileReaderThreadPool {
  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      threadTag: String,
      numThreads: Int): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      threadPool = Some(MultiFileThreadPoolUtil.createThreadPool(threadTag, numThreads))
    }
    threadPool.get
  }

  /**
   * Get the existing internal thread pool or create one with the given tag and thread
   * number if it does not exist.
   * Note: The tag and thread number will be ignored if the thread pool is already created.
   */
  def getOrCreateThreadPool(threadTag: String, numThreads: Int): ThreadPoolExecutor = {
    threadPool.getOrElse(initThreadPool(threadTag, numThreads))
  }
}

/**
 * The base multi-file partition reader factory to create the cloud reading or
 * coalescing reading respectively.
 *
 * @param sqlConf         the SQLConf
 * @param broadcastedConf the Hadoop configuration
 * @param rapidsConf      the Rapids configuration
 */
abstract class MultiFilePartitionReaderFactoryBase(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    @transient rapidsConf: RapidsConf) extends PartitionReaderFactory with Arm with Logging {

  protected val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  protected val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
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
    val filePaths = files.map(_.filePath)
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
  private[rapids] def useMultiThread(filePaths: Array[String]): Boolean = {
    !canUseCoalesceFilesReader || (canUseMultiThreadReader && arePathsInCloud(filePaths))
  }

  private def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
    } catch {
      case _: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  // We expect the filePath here to always have a scheme on it,
  // if it doesn't we try using the local filesystem. If that
  // doesn't work for some reason user would need to configure
  // it directly.
  private def isCloudFileSystem(filePath: String): Boolean = {
    val uri = resolveURI(filePath)
    val scheme = uri.getScheme
    if (allCloudSchemes.contains(scheme)) {
      true
    } else {
      false
    }
  }

  private def arePathsInCloud(filePaths: Array[String]): Boolean = {
    filePaths.exists(isCloudFileSystem)
  }
}

/**
 * The base class for PartitionReader
 *
 * @param conf        Configuration
 * @param execMetrics metrics
 */
abstract class FilePartitionReaderBase(conf: Configuration, execMetrics: Map[String, GpuMetric])
    extends PartitionReader[ColumnarBatch] with Logging with ScanWithMetrics with Arm {

  metrics = execMetrics

  protected var isDone: Boolean = false
  protected var maxDeviceMemory: Long = 0
  protected var batch: Option[ColumnarBatch] = None

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    isDone = true
  }

  /**
   * Dump the data from HostMemoryBuffer to a file named by debugDumpPrefix + random + format
   *
   * @param hmb             host data to be dumped
   * @param dataLength      data size
   * @param splits          PartitionedFile to be handled
   * @param debugDumpPrefix file name prefix, if it is None, will not dump
   * @param format          file name suffix, if it is None, will not dump
   */
  protected def dumpDataToFile(
      hmb: HostMemoryBuffer,
      dataLength: Long,
      splits: Array[PartitionedFile],
      debugDumpPrefix: Option[String] = None,
      format: Option[String] = None): Unit = {
    if (debugDumpPrefix.isDefined && format.isDefined) {
      val (out, path) = FileUtils.createTempFile(conf, debugDumpPrefix.get, s".${format.get}")

      withResource(out) { _ =>
        withResource(new HostMemoryInputStream(hmb, dataLength)) { in =>
          logInfo(s"Writing split data for ${splits.mkString(", ")} to $path")
          IOUtils.copy(in, out)
        }
      }
    }
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
 * @param ignoreCorruptFiles Whether to ignore corrupt files when GPU failed to decode the files
 */
abstract class MultiFileCloudPartitionReaderBase(
    conf: Configuration,
    files: Array[PartitionedFile],
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric],
    ignoreCorruptFiles: Boolean = false) extends FilePartitionReaderBase(conf, execMetrics) {

  private var filesToRead = 0
  protected var currentFileHostBuffers: Option[HostMemoryBuffersWithMetaDataBase] = None
  private var isInitted = false
  private val tasks = new ConcurrentLinkedQueue[Future[HostMemoryBuffersWithMetaDataBase]]()
  private val tasksToRun = new Queue[Callable[HostMemoryBuffersWithMetaDataBase]]()
  private[this] val inputMetrics = Option(TaskContext.get).map(_.taskMetrics().inputMetrics)
      .getOrElse(TrampolineUtil.newInputMetrics())

  private def initAndStartReaders(): Unit = {
    // limit the number we submit at once according to the config if set
    val limit = math.min(maxNumFileProcessed, files.length)
    val tc = TaskContext.get
    for (i <- 0 until limit) {
      val file = files(i)
      // Add these in the order as we got them so that we can make sure
      // we process them in the same order as CPU would.
      tasks.add(getThreadPool(numThreads).submit(getBatchRunner(tc, file, conf, filters)))
    }
    // queue up any left to add once others finish
    for (i <- limit until files.length) {
      val file = files(i)
      tasksToRun.enqueue(getBatchRunner(tc, file, conf, filters))
    }
    isInitted = true
    filesToRead = files.length
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc   task context to use
   * @param file file to be read
   * @param conf the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase]

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The requirements:
   * 1. Same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. Different file formats have different ThreadPoolExecutors
   *
   * @param numThreads  max number of threads to create
   * @return ThreadPoolExecutor
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
          closeCurrentFileHostBuffers()
          next()
        } else {
          val file = currentFileHostBuffers.get.partitionedFile.filePath
          batch = try {
            readBatch(currentFileHostBuffers.get)
          } catch {
            case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
              logWarning(
                s"Skipped the corrupted file: ${file}", e)
              None
          }
        }
      } else {
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
            closeCurrentFileHostBuffers()
            addNextTaskIfNeeded()
            next()
          } else {
            val file = fileBufsAndMeta.partitionedFile.filePath
            batch = try {
              readBatch(fileBufsAndMeta)
            } catch {
              case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                logWarning(
                  s"Skipped the corrupted file: ${file}", e)
                None
            }

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

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
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

  private def closeCurrentFileHostBuffers(): Unit = {
    currentFileHostBuffers.foreach { current =>
      current.memBuffersAndSizes.foreach { case (buf, _) =>
        if (buf != null) {
          buf.close()
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
  def fieldNames: Array[String]
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
  val origChunkedBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
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
 *                                clipped to only contain the column chunks to be read
 * @param readDataSchema        the Spark schema describing what will be read
 * @param partitionSchema       schema of partitions
 * @param maxReadBatchSizeRows  soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param numThreads            the size of the threadpool
 * @param execMetrics           metrics
 */
abstract class MultiFileCoalescingPartitionReaderBase(
    conf: Configuration,
    clippedBlocks: Seq[SingleDataBlockInfo],
    readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    numThreads: Int,
    execMetrics: Map[String, GpuMetric]) extends FilePartitionReaderBase(conf, execMetrics)
    with MultiFileReaderFunctions {

  private val blockIterator: BufferedIterator[SingleDataBlockInfo] =
    clippedBlocks.iterator.buffered
  private[this] val inputMetrics = TaskContext.get.taskMetrics().inputMetrics

  private case class CurrentChunkMeta(
    clippedSchema: SchemaBase,
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
   * @param footerOffset  footer offset
   * @param blocks        blocks to be evaluated
   * @param batchContext  the batch building context
   * @return the output size
   */
  def calculateFinalBlocksOutputSize(footerOffset: Long, blocks: Seq[DataBlockBase],
    batchContext: BatchContext): Long

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The rules:
   * 1. same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. different file formats have different ThreadPoolExecutors
   *
   * @return ThreadPoolExecutor
   */
  def getThreadPool(numThreads: Int): ThreadPoolExecutor

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc     task context to use
   * @param file   file to be read
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
   * @param dataBuffer  the data which can be decoded in GPU
   * @param dataSize    data size
   * @param clippedSchema the clipped schema
   * @param extraInfo the extra information for specific file format
   * @return Table
   */
  def readBufferToTable(dataBuffer: HostMemoryBuffer, dataSize: Long, clippedSchema: SchemaBase,
    extraInfo: ExtraInfo): Table

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
      chunkedBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      clippedSchema: SchemaBase): BatchContext = {
    new BatchContext(chunkedBlocks, clippedSchema)
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
        metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
      } else {
        batch = readBatch()
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange(s"$getFileFormatShortName readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkMeta = populateCurrentBlockChunk()
      if (currentChunkMeta.clippedSchema.fieldNames.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        if (currentChunkMeta.numTotalRows == 0) {
          None
        } else {
          val rows = currentChunkMeta.numTotalRows.toInt
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = readDataSchema.safeMap { f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector]
          }
          val emptyBatch = new ColumnarBatch(nullColumns.toArray, rows)
          addAllPartitionValues(Some(emptyBatch), currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        }
      } else {
        val table = readToTable(currentChunkMeta.currentChunk, currentChunkMeta.clippedSchema,
          currentChunkMeta.extraInfo)
        try {
          val colTypes = readDataSchema.fields.map(f => f.dataType)
          val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          addAllPartitionValues(maybeBatch, currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(
      currentChunkedBlocks: Seq[(Path, DataBlockBase)],
      clippedSchema: SchemaBase,
      extraInfo: ExtraInfo): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    val (dataBuffer, dataSize) = readPartFiles(currentChunkedBlocks, clippedSchema)
    withResource(dataBuffer) { _ =>
      if (dataSize == 0) {
        None
      } else {
        val table = readBufferToTable(dataBuffer, dataSize, clippedSchema, extraInfo)
        closeOnExcept(table) { _ =>
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $currentChunkedBlocks")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(table)
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
          filesAndBlocks.foreach { case (file, blocks) =>
            val fileBlockSize = blocks.map(_.getBlockSize).sum
            // use a single buffer and slice it up for different files if we need
            val outLocal = hmb.slice(offset, fileBlockSize)
            // Third, copy the blocks for each file in parallel using background threads
            tasks.add(getThreadPool(numThreads).submit(
              getBatchRunner(tc, file, outLocal, blocks, offset, batchContext)))
            offset += fileBlockSize
          }

          for (future <- tasks.asScala) {
            val (blocks, bytesRead) = future.get()
            allOutputBlocks ++= blocks
            TrampolineUtil.incBytesRead(inputMetrics, bytesRead)
          }

          // Fourth, calculate the final buffer size
          val finalBufferSize = calculateFinalBlocksOutputSize(offset, allOutputBlocks,
            batchContext)

          (hmb, finalBufferSize, offset, allOutputBlocks)
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
            s"reallocing and copying data to bigger buffer size: $bufferSize")
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
    val rowsPerPartition = new ArrayBuffer[Long]()
    var lastPartRows: Long = 0
    val allPartValues = new ArrayBuffer[InternalRow]()
    var currrentDataBlock: SingleDataBlockInfo = null
    var extraInfo: ExtraInfo = null

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        if (currentFile == null) {
          // first time of readNextBatch
          currrentDataBlock = blockIterator.head
          currentFile = blockIterator.head.filePath
          currentPartitionValues = blockIterator.head.partitionValues
          allPartValues += currentPartitionValues
          currentClippedSchema = blockIterator.head.schema
          extraInfo = blockIterator.head.extraInfo
        }

        val peekedRowCount = blockIterator.head.dataBlock.getRowCount
        if (peekedRowCount > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }

        if (numRows == 0 || numRows + peekedRowCount <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema, peekedRowCount)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            // only care to check if we are actually adding in the next chunk
            if (currentFile != blockIterator.head.filePath) {
              // check if need to split next data block into another ColumnarBatch
              if (checkIfNeedToSplitDataBlock(currrentDataBlock, blockIterator.head)) {
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
              currrentDataBlock = blockIterator.head
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
    CurrentChunkMeta(currentClippedSchema, currentChunk,
      numRows, rowsPerPartition.toArray, allPartValues.toArray, extraInfo)
  }

  /**
   * Add all partition values found to the batch. There could be more then one partition
   * value in the batch so we have to build up columns with the correct number of rows
   * for each partition value.
   *
   * @param batch - columnar batch to append partition values to
   * @param inPartitionValues - array of partition values
   * @param rowsPerPartition - the number of rows that require each partition value
   * @param partitionSchema - schema of the partitions
   * @return
   */
  private def addAllPartitionValues(
      batch: Option[ColumnarBatch],
      inPartitionValues: Array[InternalRow],
      rowsPerPartition: Array[Long],
      partitionSchema: StructType): Option[ColumnarBatch] = {
    assert(rowsPerPartition.length == inPartitionValues.length)
    if (partitionSchema.nonEmpty) {
      batch.map { cb =>
        val numPartitions = inPartitionValues.length
        if (numPartitions > 1) {
          concatAndAddPartitionColsToBatch(cb, rowsPerPartition, inPartitionValues)
        } else {
          // single partition, add like other readers
          addPartitionValues(Some(cb), inPartitionValues.head, partitionSchema).get
        }
      }
    } else {
      batch
    }
  }

  private def concatAndAddPartitionColsToBatch(
      cb: ColumnarBatch,
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): ColumnarBatch = {
    withResource(cb) { _ =>
      closeOnExcept(buildAndConcatPartitionColumns(rowsPerPartition, inPartitionValues)) {
        allPartCols =>
          ColumnarPartitionReaderWithPartitionValues.addGpuColumVectorsToBatch(cb, allPartCols)
      }
    }
  }

  private def buildAndConcatPartitionColumns(
      rowsPerPartition: Array[Long],
      inPartitionValues: Array[InternalRow]): Array[GpuColumnVector] = {
    val numCols = partitionSchema.fields.length
    val allPartCols = new Array[GpuColumnVector](numCols)
    // build the partitions vectors for all partitions within each column
    // and concatenate those together then go to the next column
    for ((field, colIndex) <- partitionSchema.fields.zipWithIndex) {
      val dataType = field.dataType
      withResource(new Array[GpuColumnVector](inPartitionValues.length)) {
        partitionColumns =>
          for ((rowsInPart, partIndex) <- rowsPerPartition.zipWithIndex) {
            val partInternalRow = inPartitionValues(partIndex)
            val partValueForCol = partInternalRow.get(colIndex, dataType)
            val partitionScalar = GpuScalar.from(partValueForCol, dataType)
            withResource(partitionScalar) { scalar =>
              partitionColumns(partIndex) = GpuColumnVector.from(
                ai.rapids.cudf.ColumnVector.fromScalar(scalar, rowsInPart.toInt),
                dataType)
            }
          }
          val baseOfCols = partitionColumns.map(_.getBase)
          allPartCols(colIndex) = GpuColumnVector.from(
            ColumnVector.concatenate(baseOfCols: _*), field.dataType)
      }
    }
    allPartCols
  }

}
