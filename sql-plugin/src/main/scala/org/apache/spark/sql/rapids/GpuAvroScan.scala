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
import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.language.implicitConversions
import scala.math.max

import ai.rapids.cudf.{AvroOptions => CudfAvroOptions, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric.{GPU_DECODE_TIME, NUM_OUTPUT_BATCHES, PEAK_DEVICE_MEMORY, READ_FS_TIME, SEMAPHORE_WAIT_TIME, WRITE_BUFFER_TIME}
import org.apache.avro.Schema
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
      GpuAvroPartitionReaderFactory(sparkSession.sessionState.conf, rapidsConf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, parsedOptions, metrics)
    } else {
      val f = GpuAvroMultiFilePartitionReaderFactory(sparkSession.sessionState.conf,
        rapidsConf, broadcastedConf, dataSchema, readDataSchema, readPartitionSchema,
        parsedOptions, metrics, pushedFilters, queryUsesInputFile)
      // Now only coalescing is supported, so need to check it can be used for the final choice.
      if (f.canUseCoalesceFilesReader){
        f
      } else {
        // Fall back to PerFile reading
        GpuAvroPartitionReaderFactory(sparkSession.sessionState.conf, rapidsConf, broadcastedConf,
          dataSchema, readDataSchema, readPartitionSchema, parsedOptions, metrics)
      }
    }
  }

  // overrides nothing in 330
  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  override def equals(obj: Any): Boolean = obj match {
    case a: GpuAvroScan =>
      super.equals(a) && dataSchema == a.dataSchema && options == a.options &&
          equivalentFilters(pushedFilters, a.pushedFilters) && rapidsConf == a.rapidsConf &&
          queryUsesInputFile == a.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }
}

/** Avro partition reader factory to build columnar reader */
case class GpuAvroPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    @transient rapidsConf: RapidsConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
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
    @transient rapidsConf: RapidsConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: AvroOptions,
    metrics: Map[String, GpuMetric],
    filters: Array[Filter],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val debugDumpPrefix = Option(rapidsConf.avroDebugDumpPrefix)
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  private val numThreads = rapidsConf.avroMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumAvroFilesParallel

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isAvroCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

  // disbale multi-threaded until it is supported.
  override val canUseMultiThreadReader: Boolean = false

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
    throw new UnsupportedOperationException()
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
    val clippedBlocks = ArrayBuffer[AvroSingleDataBlockInfo]()
    val mapPathHeader = LinkedHashMap[Path, Header]()
    val filterHandler = AvroFileFilterHandler(conf, options)
    files.foreach { file =>
      val singleFileInfo = try {
        filterHandler.filterBlocks(file)
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${file.filePath}", e)
          AvroBlockMeta(null, Seq.empty)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${file.filePath}", e)
          AvroBlockMeta(null, Seq.empty)
      }
      val fPath = new Path(new URI(file.filePath))
      clippedBlocks ++= singleFileInfo.blocks.map(block =>
        AvroSingleDataBlockInfo(
            fPath,
            AvroDataBlock(block),
            file.partitionValues,
            AvroSchemaWrapper(singleFileInfo.header.schema),
            AvroExtraInfo()))
      if (singleFileInfo.blocks.nonEmpty) {
        // No need to check the header since it can not be null when blocks is not empty here.
        mapPathHeader.put(fPath, singleFileInfo.header)
      }
    }
    new GpuMultiFileAvroPartitionReader(conf, files, clippedBlocks, readDataSchema,
      partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads,
      debugDumpPrefix, metrics, mapPathHeader.toMap)
  }

}

/** A trait collecting common methods across the 3 kinds of avro readers */
trait GpuAvroReaderBase extends Arm with Logging { self: FilePartitionReaderBase =>
  private val avroFormat = Some("avro")

  def debugDumpPrefix: Option[String]

  def readDataSchema: StructType

  /**
   * Read the host data to GPU for decoding, and return it as a cuDF Table.
   * The input host buffer should contain valid data, otherwise the behavior is
   * undefined.
   * 'splits' is used only for debugging.
   */
  protected final def sendToGpuUnchecked(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      splits: Array[PartitionedFile]): Table = {
    // Dump buffer for debugging when required
    dumpDataToFile(hostBuf, bufSize, splits, debugDumpPrefix, avroFormat)

    val readOpts = CudfAvroOptions.builder()
      .includeColumn(readDataSchema.fieldNames.toSeq: _*)
      .build()
    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

    withResource(new NvtxWithMetrics("Avro decode",
        NvtxColor.DARK_GREEN, metrics(GPU_DECODE_TIME))) { _ =>
      Table.readAvro(readOpts, hostBuf, 0, bufSize)
    }
  }

  /**
   * Send a host buffer to GPU for decoding, and return it as a ColumnarBatch.
   * The input hostBuf will be closed after returning, please do not use it anymore.
   * 'splits' is used only for debugging.
   */
  protected final def sendToGpu(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      splits: Array[PartitionedFile]): Option[ColumnarBatch] = {
    withResource(hostBuf) { _ =>
      if (bufSize == 0) {
        None
      } else {
        withResource(sendToGpuUnchecked(hostBuf, bufSize, splits)) { t =>
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
  protected final def populateCurrentBlockChunk(
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
  protected final def readPartFile(
      partFilePath: Path,
      blocks: Seq[BlockInfo],
      header: Header,
      conf: Configuration): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Avro buffer file split", NvtxColor.YELLOW,
        metrics("bufferTime"))) { _ =>
      if (blocks.isEmpty) {
        // No need to check the header here since it can not be null when blocks is not empty.
        return (null, 0L)
      }
      val estOutSize = estimateOutputSize(blocks, header)
      withResource(partFilePath.getFileSystem(conf).open(partFilePath)) { in =>
        closeOnExcept(HostMemoryBuffer.allocate(estOutSize)) { hmb =>
          withResource(new HostMemoryOutputStream(hmb)) { out =>
            val headerAndBlocks = BlockInfo(0, header.firstBlockStart, 0, 0) +: blocks
            copyBlocksData(headerAndBlocks, in, out)
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

  /** Estimate the total size from the given blocks and header */
  protected final def estimateOutputSize(blocks: Seq[BlockInfo], header: Header): Long = {
    // Start from the Header
    var totalSize: Long = header.firstBlockStart
    // Add all blocks
    totalSize += blocks.map(_.blockLength).sum
    totalSize
  }

  /** Copy the data specified by the blocks from `in` to `out` */
  protected final def copyBlocksData(
      blocks: Seq[BlockInfo],
      in: FSDataInputStream,
      out: OutputStream): Seq[BlockInfo] = {
    val copyRanges = computeCopyRanges(blocks)
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
    blocks
  }

  /**
   * Calculate the copy ranges from blocks.
   * And it will try to combine the sequential blocks.
   */
  private def computeCopyRanges(blocks: Seq[BlockInfo]): Array[CopyRange] = {
    var currentCopyStart, currentCopyEnd = 0L
    val copyRanges = new ArrayBuffer[CopyRange]()

    blocks.foreach { block =>
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
          val (dataBuffer, dataSize) = readPartFile(partFilePath, currentChunkedBlocks,
            blockMeta.header, conf)
          sendToGpu(dataBuffer, dataSize, Array(partFile))
        }
      }
    }
  }

}

/**
 * A PartitionReader that can read multiple AVRO files up to the certain size. It will
 * coalesce small files together and copy the block data in a separate thread pool to speed
 * up processing the small files before sending down to the GPU.
 */
class GpuMultiFileAvroPartitionReader(
    conf: Configuration,
    splits: Array[PartitionedFile],
    clippedBlocks: Seq[AvroSingleDataBlockInfo],
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    numThreads: Int,
    override val debugDumpPrefix: Option[String],
    execMetrics: Map[String, GpuMetric],
    mapPathHeader: Map[Path, Header])
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedBlocks, readDataSchema,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads,
    execMetrics) with GpuAvroReaderBase {

  override def checkIfNeedToSplitDataBlock(
      currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    val nextHeader = mapPathHeader.get(nextBlockInfo.filePath).get
    val curHeader = mapPathHeader.get(currentBlockInfo.filePath).get
    // Split into another block when
    //   1) the sync markers are different, or
    //   2) a key exists in both of the two headers' metadata, and maps to different values.
    //if (!Header.hasSameSync(nextHeader, curHeader)) {
    //  logInfo(s"Avro sync marker in the next file ${nextBlockInfo.filePath}" +
    //    s" differs from the current one in file ${currentBlockInfo.filePath}," +
    //    s" splitting it into a new batch!")
    //  return true
    //}
    if (Header.hasConflictInMetadata(nextHeader, curHeader)) {
      logInfo(s"Avro metadata in the next file ${nextBlockInfo.filePath}" +
        s" conflicts with the current one in file ${currentBlockInfo.filePath}," +
        s" splitting it into a new batch!")
      return true
    }

    false
  }

  override def calculateEstimatedBlocksOutputSize(
      blocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      schema: SchemaBase): Long = {
    // Get headers according to the input paths.
    val headers = blocks.keys.map(mapPathHeader.get(_).get)
    // Merge the meta, it is safe because the compatibility has been verifed
    // in 'checkIfNeedToSplitDataBlock'
    Header.mergeMetadata(headers.toSeq).map { mergedHeader =>
      val allBlocks = blocks.values.flatten.toSeq
      estimateOutputSize(allBlocks, mergedHeader)
    } getOrElse 0L
  }

  override def writeFileHeader(paths: Seq[Path], buffer: HostMemoryBuffer): Long = {
    // Get headers according to the input paths.
    val headers = paths.map(mapPathHeader.get(_).get)
    // Merge the meta, it is safe because the compatibility has been verifed
    // in 'checkIfNeedToSplitDataBlock'
    Header.mergeMetadata(headers).map { mergedHeader =>
      withResource(new HostMemoryOutputStream(buffer)) { out =>
        AvroFileWriter(out).writeHeader(mergedHeader)
        out.getPos
      }
    } getOrElse 0L
  }

  override def calculateFinalBlocksOutputSize(footerOffset: Long,
      blocks: Seq[DataBlockBase], schema: SchemaBase): Long = {
    // In 'calculateEstimatedBlocksOutputSize', we have got the true size for
    // Header + All Blocks.
    footerOffset
  }

  override def writeFileFooter(buffer: HostMemoryBuffer, bufferSize: Long, footerOffset: Long,
      blocks: Seq[DataBlockBase], clippedSchema: SchemaBase): (HostMemoryBuffer, Long) = {
    // AVRO files have no footer, do nothing
    (buffer, bufferSize)
  }

  override def readBufferToTable(dataBuffer: HostMemoryBuffer, dataSize: Long,
      clippedSchema: SchemaBase, extraInfo: ExtraInfo): Table = {
    sendToGpuUnchecked(dataBuffer, dataSize, splits)
  }

  override def getThreadPool(numThreads: Int): ThreadPoolExecutor =
    AvroMultiFileThreadPool.getOrCreateThreadPool(getFileFormatShortName, numThreads)

  override final def getFileFormatShortName: String = "AVRO"

  override def getBatchRunner(
      tc: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long): Callable[(Seq[DataBlockBase], Long)] =
    new AvroCopyBlocksRunner(tc, file, outhmb, blocks, offset)

  // The runner to copy blocks to offset of HostMemoryBuffer
  class AvroCopyBlocksRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long)
    extends Callable[(Seq[DataBlockBase], Long)] {

    override def call(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        val startBytesRead = fileSystemBytesRead()
        val res = withResource(outhmb) { _ =>
          withResource(file.getFileSystem(conf).open(file)) { in =>
            withResource(new HostMemoryOutputStream(outhmb)) { out =>
              copyBlocksData(blocks, in, out)
            }
          }
        }
        val bytesRead = fileSystemBytesRead() - startBytesRead
        (res, bytesRead)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  // Some implicits for conversions between the base class to the sub-class
  implicit def toAvroSchema(schema: SchemaBase): Schema =
    schema.asInstanceOf[AvroSchemaWrapper].schema

  implicit def toBlockInfo(block: DataBlockBase): BlockInfo =
    block.asInstanceOf[AvroDataBlock].blockInfo

  implicit def toBlockInfos(blocks: Seq[DataBlockBase]): Seq[BlockInfo] =
    blocks.map(toBlockInfo(_))

  implicit def toBlockBases(blocks: Seq[BlockInfo]): Seq[DataBlockBase] =
    blocks.map(AvroDataBlock(_))

  implicit def toAvroExtraInfo(in: ExtraInfo): AvroExtraInfo =
    in.asInstanceOf[AvroExtraInfo]

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
      AvroBlockMeta(null, Seq.empty)
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

/** Extra information: codec */
case class AvroExtraInfo() extends ExtraInfo

/** avro schema wrapper */
case class AvroSchemaWrapper(schema: Schema) extends SchemaBase

/** avro BlockInfo wrapper */
case class AvroDataBlock(blockInfo: BlockInfo) extends DataBlockBase {
  override def getRowCount: Long = blockInfo.count
  override def getReadDataSize: Long = blockInfo.blockDataSize
  override def getBlockSize: Long = blockInfo.blockLength
}

case class AvroSingleDataBlockInfo(
  filePath: Path,
  dataBlock: AvroDataBlock,
  partitionValues: InternalRow,
  schema: AvroSchemaWrapper,
  extraInfo: AvroExtraInfo) extends SingleDataBlockInfo
