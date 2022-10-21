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
import scala.collection.mutable
import scala.language.implicitConversions

import ai.rapids.cudf.{ColumnVector, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.GpuMetric.{makeSpillCallback, BUFFER_TIME, FILTER_TIME, PEAK_DEVICE_MEMORY, SEMAPHORE_WAIT_TIME}
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
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * The base HostMemoryBuffer information read from a single file.
 */
trait HostMemoryBuffersWithMetaDataBase {
  // PartitionedFile to be read
  def partitionedFile: PartitionedFile
  // Original PartitionedFile if path was replaced with Alluxio
  def origPartitionedFile: Option[PartitionedFile] = None
  // An array of BlockChunk(HostMemoryBuffer and its data size) read from PartitionedFile
  def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)]
  // Total bytes read
  def bytesRead: Long
  // Percentage of time spent on filtering
  private var _filterTimePct: Double = 0L
  // Percentage of time spent on buffering
  private var _bufferTimePct: Double = 0L

  // Called by parquet/orc/avro scanners to set the amount of time (in nanoseconds)
  // that filtering and buffering incurred in one of the scan runners.
  def setMetrics(filterTime: Long, bufferTime: Long): Unit = {
    val totalTime = filterTime + bufferTime
    _filterTimePct = filterTime.toDouble / totalTime
    _bufferTimePct = bufferTime.toDouble / totalTime
  }

  def getBufferTimePct: Double = _bufferTimePct
  def getFilterTimePct: Double = _filterTimePct
}

// This is a common trait for all kind of file formats
trait MultiFileReaderFunctions extends Arm {

  // Add partitioned columns into the batch
  protected def addPartitionValues(
      batch: ColumnarBatch,
      inPartitionValues: InternalRow,
      partitionSchema: StructType): ColumnarBatch = {
    if (partitionSchema.nonEmpty) {
      val partitionValues = inPartitionValues.toSeq(partitionSchema)
      val partitionScalars = ColumnarPartitionReaderWithPartitionValues
          .createPartitionValues(partitionValues, partitionSchema)
      withResource(partitionScalars) { scalars =>
        ColumnarPartitionReaderWithPartitionValues.addPartitionValues(batch, scalars,
          GpuColumnVector.extractTypes(partitionSchema))
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

// Singleton thread pool used across all tasks for multifile reading.
// Please note that the TaskContext is not set in these threads and should not be used.
object MultiFileReaderThreadPool extends Logging {
  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      maxThreads: Int,
      keepAliveSeconds: Long = 60): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      val threadFactory = new ThreadFactoryBuilder()
          .setNameFormat("multithreaded file reader worker-%d")
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
      logDebug(s"Using $maxThreads for the multithreaded reader thread pool")
      threadPool = Some(threadPoolExecutor)
    }

    threadPool.get
  }

  /**
   * Get the existing thread pool or create one with the given thread count if it does not exist.
   * @note The thread number will be ignored if the thread pool is already created.
   */
  def getOrCreateThreadPool(numThreads: Int): ThreadPoolExecutor = {
    threadPool.getOrElse(initThreadPool(numThreads))
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
 *                                   scheme to Alluxio scheme
 */
abstract class MultiFilePartitionReaderFactoryBase(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    @transient rapidsConf: RapidsConf,
    alluxioPathReplacementMap: Option[Map[String, String]] = None)
  extends PartitionReaderFactory with Arm with Logging {

  protected val maxReadBatchSizeRows: Int = rapidsConf.maxReadBatchSizeRows
  protected val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes
  protected val targetBatchSizeBytes: Long = rapidsConf.gpuTargetBatchSizeBytes
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
  private[rapids] def useMultiThread(filePaths: Array[String]): Boolean =
    MultiFileReaderUtils.useMultiThreadReader(canUseCoalesceFilesReader,
      canUseMultiThreadReader, filePaths, allCloudSchemes, alluxioPathReplacementMap.isDefined)
}

abstract class ColumnarBatchReader extends Iterator[ColumnarBatch] with AutoCloseable
object EmptyColumnarBatchReader extends ColumnarBatchReader {
  override def hasNext: Boolean = false

  override def next(): ColumnarBatch = {
    throw new NoSuchElementException()
  }

  override def close(): Unit = {}
}

case class SingleColumnarBatchReader(batch: ColumnarBatch) extends ColumnarBatchReader {
  private var isRead: Boolean = false

  override def hasNext: Boolean = !isRead

  override def next(): ColumnarBatch = {
    isRead = true
    batch
  }

  override def close(): Unit = {
    if (!isRead) {
      batch.close()
    }
  }
}

class CachingBatchReader(reader: TableReader,
    dataTypes: Array[DataType],
    spillCallback: SpillCallback) extends ColumnarBatchReader with Arm {

  private[this] var notSpillableBatch: Option[ColumnarBatch] = None
  private[this] val pending: mutable.Queue[SpillableColumnarBatch] = mutable.Queue.empty

  private[this] def makeSpillableAndClose(table: Table): SpillableColumnarBatch = {
    withResource(table) { _ =>
      SpillableColumnarBatch(GpuColumnVector.from(table, dataTypes),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
        spillCallback)
    }
  }

  { // Constructor...
    withResource(reader) { _ =>
      if (reader.hasNext) {
        // Special case for the first one.
        closeOnExcept(reader.next()) { firstTable =>
          if (!reader.hasNext) {
            notSpillableBatch = Some(GpuColumnVector.from(firstTable, dataTypes))
            firstTable.close()
          } else {
            pending += makeSpillableAndClose(firstTable)
            reader.foreach { t =>
              pending += makeSpillableAndClose(t)
            }
          }
        }
      }
    }
  }

  override def hasNext: Boolean = notSpillableBatch.nonEmpty || pending.nonEmpty

  override def next(): ColumnarBatch = {
    if (notSpillableBatch.isDefined) {
      val ret = notSpillableBatch.get
      notSpillableBatch = None
      ret
    } else {
      if (pending.isEmpty) {
        throw new NoSuchElementException()
      }
      withResource(pending.dequeue()) { spillable =>
        spillable.getColumnarBatch()
      }
    }
  }

  override def close(): Unit = {
    notSpillableBatch.foreach(_.close())
    pending.foreach(_.close())
  }
}

case class WrappedColumnarBatchReader(reader: ColumnarBatchReader,
    modifyAndClose: ColumnarBatch => ColumnarBatch) extends ColumnarBatchReader {
  override def hasNext: Boolean = reader.hasNext

  override def next(): ColumnarBatch = modifyAndClose(reader.next())

  override def close(): Unit = reader.close()
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
  protected var batchReader: ColumnarBatchReader = EmptyColumnarBatchReader
  protected lazy val spillCallback: SpillCallback = makeSpillCallback(execMetrics)

  override def get(): ColumnarBatch = {
    batchReader.next()
  }

  override def close(): Unit = {
    batchReader.close()
    batchReader = EmptyColumnarBatchReader
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

// Contains the actual file path to read from and then an optional original path if its read from
// Alluxio. To make it transparent to the user, we return the original non-Alluxio path
// for input_file_name.
case class PartitionedFileInfoOptAlluxio(toRead: PartitionedFile, original: Option[PartitionedFile])

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
 */
abstract class MultiFileCloudPartitionReaderBase(
    conf: Configuration,
    inputFiles: Array[PartitionedFile],
    numThreads: Int,
    maxNumFileProcessed: Int,
    filters: Array[Filter],
    execMetrics: Map[String, GpuMetric],
    ignoreCorruptFiles: Boolean = false,
    alluxioPathReplacementMap: Map[String, String] = Map.empty,
    alluxioReplacementTaskTime: Boolean = false)
  extends FilePartitionReaderBase(conf, execMetrics) {

  private var filesToRead = 0
  protected var currentFileHostBuffers: Option[HostMemoryBuffersWithMetaDataBase] = None
  private var isInitted = false
  private val tasks = new ConcurrentLinkedQueue[Future[HostMemoryBuffersWithMetaDataBase]]()
  private val tasksToRun = new Queue[Callable[HostMemoryBuffersWithMetaDataBase]]()
  private[this] val inputMetrics = Option(TaskContext.get).map(_.taskMetrics().inputMetrics)
      .getOrElse(TrampolineUtil.newInputMetrics())

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
    for (i <- 0 until limit) {
      val file = files(i)
      logDebug(s"MultiFile reader using file ${file.toRead}, orig file is ${file.original}")
      // Add these in the order as we got them so that we can make sure
      // we process them in the same order as CPU would.
      val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
      tasks.add(threadPool.submit(getBatchRunner(tc, file.toRead, file.original, conf, filters)))
    }
    // queue up any left to add once others finish
    for (i <- limit until files.length) {
      val file = files(i)
      tasksToRun.enqueue(getBatchRunner(tc, file.toRead, file.original, conf, filters))
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
   * @return Option[ColumnarBatch] which has been decoded by GPU
   */
  def readBatches(
    fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): ColumnarBatchReader

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  def getFileFormatShortName: String

  override def next(): Boolean = {
    withResource(new NvtxRange(getFileFormatShortName + " readBatch", NvtxColor.GREEN)) { _ =>
      if (!isInitted) {
        initAndStartReaders()
      }
      if (batchReader.hasNext) {
        // leave early we have something more to be read
        return true
      }

      batchReader.close()
      // Temporary until we get more to read
      batchReader = EmptyColumnarBatchReader
      // if we have batch left from the last file read return it
      if (currentFileHostBuffers.isDefined) {
        if (getSizeOfHostBuffers(currentFileHostBuffers.get) == 0) {
          closeCurrentFileHostBuffers()
          next()
        } else {
          val file = currentFileHostBuffers.get.partitionedFile.filePath
          batchReader = try {
            readBatches(currentFileHostBuffers.get)
          } catch {
            case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
              logWarning(s"Skipped the corrupted file: $file", e)
              EmptyColumnarBatchReader
          }
        }
      } else {
        if (filesToRead > 0 && !isDone) {
          // Filter time here includes the buffer time as well since those
          // happen in the same background threads. This is as close to wall
          // clock as we can get right now without further work.
          val startTime = System.nanoTime()
          val fileBufsAndMeta = tasks.poll.get()
          val blockedTime = System.nanoTime() - startTime
          metrics.get(FILTER_TIME).foreach {
            _ += (blockedTime * fileBufsAndMeta.getFilterTimePct).toLong
          }
          metrics.get(BUFFER_TIME).foreach {
            _ += (blockedTime * fileBufsAndMeta.getBufferTimePct).toLong
          }

          filesToRead -= 1
          TrampolineUtil.incBytesRead(inputMetrics, fileBufsAndMeta.bytesRead)
          // if we replaced the path with Alluxio, set it to the original filesystem file
          // since Alluxio replacement is supposed to be transparent to the user
          val inputFileToSet =
            fileBufsAndMeta.origPartitionedFile.getOrElse(fileBufsAndMeta.partitionedFile)
          InputFileUtils.setInputFileBlock(
            inputFileToSet.filePath,
            inputFileToSet.start,
            inputFileToSet.length)

          if (getSizeOfHostBuffers(fileBufsAndMeta) == 0) {
            // if sizes are 0 means no rows and no data so skip to next file
            // file data was empty so submit another task if any were waiting
            closeCurrentFileHostBuffers()
            addNextTaskIfNeeded()
            next()
          } else {
            val file = fileBufsAndMeta.partitionedFile.filePath
            batchReader = try {
              readBatches(fileBufsAndMeta)
            } catch {
              case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                logWarning(s"Skipped the corrupted file: $file", e)
                EmptyColumnarBatchReader
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
    if (batchReader.isEmpty && filesToRead > 0 && !isDone) {
      next()
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batchReader` is
    // `EmptyBatchReader`. We are not acquiring the semaphore here since this next() is getting
    // called from the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchReader.hasNext
  }

  private def getSizeOfHostBuffers(fileInfo: HostMemoryBuffersWithMetaDataBase): Long = {
    fileInfo.memBuffersAndSizes.map(_._2).sum
  }

  private def addNextTaskIfNeeded(): Unit = {
    if (tasksToRun.nonEmpty && !isDone) {
      val runner = tasksToRun.dequeue()
      val threadPool = MultiFileReaderThreadPool.getOrCreateThreadPool(numThreads)
      tasks.add(threadPool.submit(runner))
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
    batchReader.close()
    batchReader = EmptyColumnarBatchReader
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

abstract class TableReader extends Iterator[Table] with AutoCloseable
object EmptyTableReader extends TableReader {
  override def hasNext: Boolean = false

  override def next(): Table = throw new NoSuchElementException()

  override def close(): Unit = {}
}

class SingleTableReader(t: Table) extends TableReader {
  private var isRead = false

  override def hasNext: Boolean = !isRead

  override def next(): Table = {
    isRead = true
    t
  }

  override def close(): Unit = {
    if (!isRead) {
      t.close()
    }
  }
}

case class WrappedTableReader(reader: TableReader,
    modifyAndClose: Table => Table) extends TableReader {
  override def hasNext: Boolean = reader.hasNext

  override def next(): Table = modifyAndClose(reader.next())

  override def close(): Unit = reader.close()
}

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
 * @param numThreads            the size of the threadpool
 * @param execMetrics           metrics
 */
abstract class MultiFileCoalescingPartitionReaderBase(
    conf: Configuration,
    clippedBlocks: Seq[SingleDataBlockInfo],
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
   * @param footerOffset  footer offset
   * @param blocks        blocks to be evaluated
   * @param batchContext  the batch building context
   * @return the output size
   */
  def calculateFinalBlocksOutputSize(footerOffset: Long, blocks: Seq[DataBlockBase],
    batchContext: BatchContext): Long

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
   * @param readSchema the expected schema
   * @param extraInfo the extra information for specific file format
   * @return Table
   */
  def readBufferToTablesAndClose(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, readSchema: StructType, extraInfo: ExtraInfo): TableReader

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
    if (batchReader.hasNext) {
      return true
    }
    batchReader.close()
    batchReader = EmptyColumnarBatchReader
    if (!isDone) {
      if (!blockIterator.hasNext) {
        isDone = true
        metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
      } else {
        batchReader = readBatch()
      }
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batchReader` is
    // `EmptyColumnarBatchReader`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batchReader.hasNext
  }

  private def readBatch(): ColumnarBatchReader = {
    withResource(new NvtxRange(s"$getFileFormatShortName readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkMeta = populateCurrentBlockChunk()
      val batchReader = if (currentChunkMeta.clippedSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        if (currentChunkMeta.numTotalRows == 0) {
          EmptyColumnarBatchReader
        } else {
          val rows = currentChunkMeta.numTotalRows.toInt
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = currentChunkMeta.readSchema.safeMap(f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
          val emptyBatch = new ColumnarBatch(nullColumns.toArray, rows)
          SingleColumnarBatchReader(emptyBatch)
        }
      } else {
        val colTypes = currentChunkMeta.readSchema.fields.map(f => f.dataType)
        val tableReader = readToTable(currentChunkMeta.currentChunk, currentChunkMeta.clippedSchema,
          currentChunkMeta.readSchema, currentChunkMeta.extraInfo)
        new CachingBatchReader(tableReader, colTypes, spillCallback)
      }
      WrappedColumnarBatchReader(batchReader,
        cb => {
          // we have to add partition values here for this batch, we already verified that
          // its not different for all the blocks in this batch
          withResource(addAllPartitionValues(cb, currentChunkMeta.allPartValues,
            currentChunkMeta.rowsPerPartition, partitionSchema)) { withParts =>
            finalizeOutputBatch(withParts, currentChunkMeta.extraInfo)
          }
        }
      )
    }
  }

  private def readToTable(
      currentChunkedBlocks: Seq[(Path, DataBlockBase)],
      clippedSchema: SchemaBase,
      readDataSchema: StructType,
      extraInfo: ExtraInfo): TableReader = {
    if (currentChunkedBlocks.isEmpty) {
      return EmptyTableReader
    }
    val (dataBuffer, dataSize) = readPartFiles(currentChunkedBlocks, clippedSchema)
    closeOnExcept(dataBuffer) { _ =>
      if (dataSize == 0) {
        EmptyTableReader
      } else {
        readBufferToTablesAndClose(dataBuffer, dataSize, clippedSchema, readDataSchema, extraInfo)
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
    CurrentChunkMeta(currentClippedSchema, currentReadSchema, currentChunk,
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
      batch: ColumnarBatch,
      inPartitionValues: Array[InternalRow],
      rowsPerPartition: Array[Long],
      partitionSchema: StructType): ColumnarBatch = {
    assert(rowsPerPartition.length == inPartitionValues.length)
    if (partitionSchema.nonEmpty) {
      val numPartitions = inPartitionValues.length
      if (numPartitions > 1) {
        concatAndAddPartitionColsToBatch(batch, rowsPerPartition, inPartitionValues)
      } else {
        // single partition, add like other readers
        addPartitionValues(batch, inPartitionValues.head, partitionSchema)
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
