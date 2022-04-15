/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.io.{FileNotFoundException, IOException, OutputStream}
import java.net.URI
import java.util.concurrent.{Callable, ThreadPoolExecutor}

import scala.annotation.tailrec
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.math.max

import ai.rapids.cudf.{AvroOptions => CudfAvroOptions, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric.{GPU_DECODE_TIME, NUM_OUTPUT_BATCHES, PEAK_DEVICE_MEMORY, READ_FS_TIME, SEMAPHORE_WAIT_TIME, WRITE_BUFFER_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.avro.file.DataFileConstants.SYNC_SIZE
import org.apache.avro.mapred.FsInput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.AvroOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.shims.AvroUtils
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.avro.AvroScan
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuAvroScan {

  def tagSupport(scanMeta: ScanMeta[AvroScan]) : Unit = {
    val scan = scanMeta.wrapped
    tagSupport(
      scan.sparkSession,
      scan.readDataSchema,
      scan.options.asScala.toMap,
      scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      readSchema: StructType,
      options: Map[String, String],
      meta: RapidsMeta[_, _, _]): Unit = {

    if (!meta.conf.isAvroEnabled) {
      meta.willNotWorkOnGpu("Avro input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO} to true")
    }

    if (!meta.conf.isAvroReadEnabled) {
      meta.willNotWorkOnGpu("Avro input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO_READ} to true")
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new AvroOptions(options, hadoopConf)
    AvroUtils.tagSupport(parsedOptions, meta)

    FileFormatChecks.tag(meta, readSchema, AvroFormatType, ReadFileOp)
  }

}

case class GpuAvroScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    rapidsConf: RapidsConf,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    queryUsesInputFile: Boolean = false) extends FileScan with ScanWithMetrics {
  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val parsedOptions = new AvroOptions(caseSensitiveMap, hadoopConf)
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    if (rapidsConf.isAvroPerFileReadEnabled) {
      GpuAvroPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, rapidsConf, parsedOptions, metrics)
    } else {
      GpuAvroMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf,
        parsedOptions, metrics, queryUsesInputFile)
    }
  }

  // overrides nothing in 330
  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

}

/** Avro partition reader factory to build columnar reader */
case class GpuAvroPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    options: AvroOptions,
    metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory with Logging {

  private val debugDumpPrefix = Option(rapidsConf.avroDebugDumpPrefix)
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val blockMeta = AvroFileFilterHandler(conf, options).filterBlocks(partFile)
    val reader = new PartitionReaderWithBytesRead(new GpuAvroPartitionReader(conf, partFile,
      blockMeta, readDataSchema, debugDumpPrefix, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}

/**
 * The multi-file partition reader factory for cloud or coalescing reading of avro file format.
 */
case class GpuAvroMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    options: AvroOptions,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val debugDumpPrefix = Option(rapidsConf.avroDebugDumpPrefix)
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  private val numThreads = rapidsConf.avroMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumAvroFilesParallel

  // Disable coalescing reading until it is supported.
  override val canUseCoalesceFilesReader: Boolean = false

  override val canUseMultiThreadReader: Boolean = rapidsConf.isAvroMultiThreadReadEnabled

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   */
  override final def getFileFormatShortName: String = "AVRO"

  /**
   * Build the PartitionReader for cloud reading
   */
  override def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val filterHandler = AvroFileFilterHandler(conf, options)
    new GpuMultiFileCloudAvroPartitionReader(conf, files, readDataSchema, partitionSchema,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads, maxNumFileProcessed,
      debugDumpPrefix, filters, filterHandler, metrics, ignoreMissingFiles, ignoreCorruptFiles)
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return a PartitionReader of coalescing reading
   */
  override def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException
  }

}

/** A trait collecting common methods across the 3 kinds of avro readers */
trait GpuAvroReaderBase extends Arm with Logging { self: FilePartitionReaderBase =>
  private val avroFormat = Some("avro")

  def debugDumpPrefix: Option[String]

  def readDataSchema: StructType

  /**
   * Send a host buffer to GPU for decoding.
   * The input hostBuf will be closed after returning, please do not use it anymore.
   * 'splits' is used only for debugging.
   */
  protected def sendToGpu(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      splits: Array[PartitionedFile]): Option[ColumnarBatch] = {
    withResource(hostBuf) { _ =>
      if (bufSize == 0) {
        None
      } else {
        // Dump buffer for debugging when required
        dumpDataToFile(hostBuf, bufSize, splits, debugDumpPrefix, avroFormat)

        val readOpts = CudfAvroOptions.builder()
          .includeColumn(readDataSchema.fieldNames.toSeq: _*)
          .build()
        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

        val table = withResource(new NvtxWithMetrics("Avro decode",
            NvtxColor.DARK_GREEN, metrics(GPU_DECODE_TIME))) { _ =>
          Table.readAvro(readOpts, hostBuf, 0, bufSize)
        }
        withResource(table) { t =>
          val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(t)
          logDebug(s"GPU batch size: $batchSizeBytes bytes")
          maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
          metrics(NUM_OUTPUT_BATCHES) += 1
          // convert to batch
          Some(GpuColumnVector.from(t, GpuColumnVector.extractTypes(readDataSchema)))
        }
      } // end of else
    }
  }

  /**
   * Get the block chunk according to the max batch size and max rows.
   *
   * @param blockIter blocks to be evaluated
   * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader
   *                             reads per batch
   * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader
   *                              reads per batch
   * @return
   */
  protected def populateCurrentBlockChunk(
      blockIter: BufferedIterator[BlockInfo],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long): Seq[BlockInfo] = {

    val currentChunk = new ArrayBuffer[BlockInfo]
    var numRows, numBytes, numAvroBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.count > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.count <= maxReadBatchSizeRows) {
          val estBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema, peekedRowGroup.count)
          if (numBytes == 0 || numBytes + estBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.count
            numAvroBytes += currentChunk.last.blockDataSize
            numBytes += estBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()
    logDebug(s"Loaded $numRows rows from Avro. bytes read: $numAvroBytes. " +
      s"Estimated GPU bytes: $numBytes")
    currentChunk
  }

  /** Read a split into a host buffer, preparing for sending to GPU */
  protected def readPartFile(
      blockMeta: AvroBlockMeta,
      partFilePath: Path,
      conf: Configuration): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Avro buffer file split", NvtxColor.YELLOW,
        metrics("bufferTime"))) { _ =>
      if (blockMeta.blocks.isEmpty) {
        return (null, 0L)
      }
      val estOutSize = estimateOutputSize(blockMeta)
      withResource(partFilePath.getFileSystem(conf).open(partFilePath)) { in =>
        closeOnExcept(HostMemoryBuffer.allocate(estOutSize)) { hmb =>
          withResource(new HostMemoryOutputStream(hmb)) { out =>
            copyBlocks(blockMeta, in, out)
            // check we didn't go over memory
            if (out.getPos > estOutSize) {
              throw new QueryExecutionException(s"Calculated buffer size $estOutSize is" +
                s" too small, actual written: ${out.getPos}")
            }
            (hmb, out.getPos)
          }
        }
      }
    }
  }

  /** Estimate the total size from the given block meta */
  private def estimateOutputSize(blockMeta: AvroBlockMeta): Long = {
    // For simplicity, we just copy the whole header of AVRO
    var totalSize: Long = blockMeta.header.getFirstBlockStart
    // Add all blocks
    totalSize += blockMeta.blocks.map(_.blockLength).sum
    totalSize
  }

  /** Copy the data specified by the block meta from `in` to `out` */
  private def copyBlocks(
      blockMeta: AvroBlockMeta,
      in: FSDataInputStream,
      out: OutputStream): Unit = {
    val copyRanges = computeCopyRanges(blockMeta)
    // copy cache: 8MB
    val copyCache = new Array[Byte](8 * 1024 * 1024)
    var readTime, writeTime = 0L

    copyRanges.foreach { range =>
      if (in.getPos != range.offset) {
        in.seek(range.offset)
      }
      var bytesLeft = range.length
      while (bytesLeft > 0) {
        // downcast is safe because copyBuffer.length is an int
        val readLength = Math.min(bytesLeft, copyCache.length).toInt
        val start = System.nanoTime()
        in.readFully(copyCache, 0, readLength)
        val mid = System.nanoTime()
        out.write(copyCache, 0, readLength)
        val end = System.nanoTime()
        readTime += (mid - start)
        writeTime += (end - mid)
        bytesLeft -= readLength
      }
    }
    metrics.get(READ_FS_TIME).foreach(_.add(readTime))
    metrics.get(WRITE_BUFFER_TIME).foreach(_.add(writeTime))
  }

  /**
   * Calculate the copy ranges from block meta.
   * And it will try to combine the sequential blocks
   */
  private def computeCopyRanges(blockMeta: AvroBlockMeta): Array[CopyRange] = {
    var currentCopyStart, currentCopyEnd = 0L
    val copyRanges = new ArrayBuffer[CopyRange]()

    // Combine the meta and blocks into a seq to get the copy range
    val metaAndBlocks =
      Seq(BlockInfo(0, blockMeta.header.getFirstBlockStart, 0, 0)) ++ blockMeta.blocks

    metaAndBlocks.foreach { block =>
      if (currentCopyEnd != block.blockStart) {
        if (currentCopyEnd != 0) {
          copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
        }
        currentCopyStart = block.blockStart
        currentCopyEnd = currentCopyStart
      }
      currentCopyEnd += block.blockLength
    }

    if (currentCopyEnd != currentCopyStart) {
      copyRanges.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
    }
    copyRanges.toArray
  }

}

/** A PartitionReader that reads an AVRO file split on the GPU. */
class GpuAvroPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    blockMeta: AvroBlockMeta,
    override val readDataSchema: StructType,
    override val debugDumpPrefix: Option[String],
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric])
  extends FilePartitionReaderBase(conf, execMetrics) with GpuAvroReaderBase {

  private val partFilePath = new Path(new URI(partFile.filePath))
  private val blockIterator: BufferedIterator[BlockInfo] = blockMeta.blocks.iterator.buffered

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
    withResource(new NvtxRange("Avro readBatch", NvtxColor.GREEN)) { _ =>
      val currentChunkedBlocks = populateCurrentBlockChunk(blockIterator,
        maxReadBatchSizeRows, maxReadBatchSizeBytes)
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentChunkedBlocks.map(_.count).sum.toInt
        if (numRows == 0) {
          None
        } else {
          Some(new ColumnarBatch(Array.empty, numRows.toInt))
        }
      } else {
        if (currentChunkedBlocks.isEmpty) {
          None
        } else {
          val (dataBuffer, dataSize) = readPartFile(
            AvroBlockMeta(blockMeta.header, currentChunkedBlocks), partFilePath, conf)
          sendToGpu(dataBuffer, dataSize, Array(partFile))
        }
      }
    }
  }

}

/**
 * A PartitionReader that can read multiple AVRO files in parallel.
 * This is most efficient running in a cloud environment where the I/O of reading is slow.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param readDataSchema the Spark schema describing what will be read
 * @param partitionSchema Schema of partitions.
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows to be read per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes to be read per batch
 * @param numThreads the size of the threadpool
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated AVRO data or null
 * @param filters filters passed into the filterHandler
 * @param filterHandler used to filter the AVRO blocks
 * @param execMetrics the metrics
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
 */
class GpuMultiFileCloudAvroPartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    numThreads: Int,
    maxNumFileProcessed: Int,
    override val debugDumpPrefix: Option[String],
    filters: Array[Filter],
    filterHandler: AvroFileFilterHandler,
    execMetrics: Map[String, GpuMetric],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, filters,
    execMetrics, ignoreCorruptFiles) with MultiFileReaderFunctions with GpuAvroReaderBase {

  override def readBatch(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
      Option[ColumnarBatch] = fileBufsAndMeta match {
    case buffer: AvroHostBuffersWithMeta =>
      val bufsAndSizes = buffer.memBuffersAndSizes
      if (bufsAndSizes.length == 0) {
        currentFileHostBuffers = None
        return None
      }
      // At least one buffer, try to send the first one to GPU
      val (dataBuf, dataSize) = bufsAndSizes.head
      val partitionValues = buffer.partitionedFile.partitionValues
      val optBatch = if (dataBuf == null) {
        // Not reading any data, but add in partition data if needed
        // Someone is going to process this data, even if it is just a row count
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
        val emptyBatch = new ColumnarBatch(Array.empty, dataSize.toInt)
        addPartitionValues(Some(emptyBatch), partitionValues, partitionSchema)
      } else {
        val maybeBatch = sendToGpu(dataBuf, dataSize, files)
        // we have to add partition values here for this batch, we already verified that
        // it's not different for all the blocks in this batch
        addPartitionValues(maybeBatch, partitionValues, partitionSchema)
      }
       // Update the current buffers
      closeOnExcept(optBatch) { _ =>
        if (bufsAndSizes.length > 1) {
          val updatedBuffers = bufsAndSizes.drop(1)
          currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
        } else {
          currentFileHostBuffers = None
        }
      }
      optBatch
    case t =>
      throw new RuntimeException(s"Unknown avro buffer type: ${t.getClass.getSimpleName}")
  }

  override def getThreadPool(numThreads: Int): ThreadPoolExecutor =
    AvroMultiFileThreadPool.getOrCreateThreadPool(getFileFormatShortName, numThreads)

  override final def getFileFormatShortName: String = "AVRO"

  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] =
    new ReadBatchRunner(tc, file, conf, filters)

  /** Two utils classes */
  private case class AvroHostBuffersWithMeta(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
    override val bytesRead: Long) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      taskContext: TaskContext,
      partFile: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends Callable[HostMemoryBuffersWithMetaDataBase] with Logging {

    override def call(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${partFile.filePath}", e)
          createBufferAndMeta(Array((null, 0)), 0)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${partFile.filePath}", e)
          createBufferAndMeta(Array((null, 0)), 0)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def createBufferAndMeta(arrayBufSize: Array[(HostMemoryBuffer, Long)],
        bytesRead: Long): HostMemoryBuffersWithMetaDataBase =
      AvroHostBuffersWithMeta(partFile, arrayBufSize, bytesRead)

    private val partFilePath = new Path(new URI(partFile.filePath))
    private var blockChunkIter: BufferedIterator[BlockInfo] = null

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]

      val blockMeta = filterHandler.filterBlocks(partFile)
      try {
        if (blockMeta.blocks.isEmpty || isDone) {
          // No blocks or got close before finishing, return null buffer and zero size
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          return createBufferAndMeta(Array((null, 0)), bytesRead)
        }
        blockChunkIter = blockMeta.blocks.iterator.buffered
        if (readDataSchema.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          val numRows = blockChunkIter.map(_.count).sum.toInt
          // Overload the size to be the number of rows with null buffer
          createBufferAndMeta(Array((null, numRows)), bytesRead)
        } else {
          while (blockChunkIter.hasNext) {
            val blocksToRead = populateCurrentBlockChunk(blockChunkIter,
              maxReadBatchSizeRows, maxReadBatchSizeBytes)
            hostBuffers += readPartFile(
              AvroBlockMeta(blockMeta.header, blocksToRead), partFilePath, conf)
          }
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          val bufArray = if (isDone) {
            // got close before finishing
            hostBuffers.foreach(_._1.safeClose())
            Array((null, 0))
          } else {
            hostBuffers.toArray
          }
          createBufferAndMeta(hostBuffers.toArray, bytesRead)
        }
      } catch {
        case e: Throwable =>
          hostBuffers.foreach(_._1.safeClose(e))
          throw e
      }
    }
  }

}

/** Singleton threadpool that is used across all the tasks. */
object AvroMultiFileThreadPool extends MultiFileReaderThreadPool

/** A tool to filter Avro blocks */
case class AvroFileFilterHandler(
    hadoopConf: Configuration,
    @transient options: AvroOptions) extends Arm with Logging {

  @scala.annotation.nowarn(
    "msg=value ignoreExtension in class AvroOptions is deprecated*"
  )
  val ignoreExtension = options.ignoreExtension

  private def passSync(blockStart: Long, position: Long): Boolean = {
    blockStart >= position + SYNC_SIZE
  }

  def filterBlocks(partFile: PartitionedFile): AvroBlockMeta = {
    if (ignoreExtension || partFile.filePath.endsWith(".avro")) {
      val in = new FsInput(new Path(new URI(partFile.filePath)), hadoopConf)
      closeOnExcept(in) { _ =>
        withResource(AvroDataFileReader.openReader(in)) { reader =>
          val blocks = reader.getBlocks()
          val filteredBlocks = new ArrayBuffer[BlockInfo]()
          blocks.foreach(block => {
            if (partFile.start <= block.blockStart - SYNC_SIZE &&
              !passSync(block.blockStart, partFile.start + partFile.length)) {
              filteredBlocks.append(block)
            }
          })
          AvroBlockMeta(reader.getHeader(), filteredBlocks)
        }
      }
    } else {
      AvroBlockMeta(new Header(), Seq.empty)
    }
  }
}

/**
 * Avro block meta info
 *
 * @param header the header of avro file
 * @param blocks the total block info of avro file
 */
case class AvroBlockMeta(header: Header, blocks: Seq[BlockInfo])

/**
 * CopyRange to indicate from where to copy.
 *
 * @param offset from where to copy
 * @param length how many bytes to copy
 */
private case class CopyRange(offset: Long, length: Long)

