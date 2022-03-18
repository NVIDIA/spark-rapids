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

import java.io.OutputStream
import java.net.URI

import scala.annotation.tailrec
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.math.max

import ai.rapids.cudf.{AvroOptions => CudfAvroOptions, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.{Arm, AvroDataFileReader, AvroFormatType, BlockInfo, ColumnarPartitionReaderWithPartitionValues, FileFormatChecks, FilePartitionReaderBase, GpuBatchUtils, GpuColumnVector, GpuMetric, GpuSemaphore, Header, HostMemoryOutputStream, NvtxWithMetrics, PartitionReaderWithBytesRead, RapidsConf, RapidsMeta, ReadFileOp, ScanMeta, ScanWithMetrics}
import com.nvidia.spark.rapids.GpuMetric.{GPU_DECODE_TIME, NUM_OUTPUT_BATCHES, PEAK_DEVICE_MEMORY, READ_FS_TIME, SEMAPHORE_WAIT_TIME, WRITE_BUFFER_TIME}
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
import org.apache.spark.sql.rapids.shims.AvroUtils
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

    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    val parsedOptions = new AvroOptions(options, hadoopConf)

    if (!meta.conf.isAvroEnabled) {
      meta.willNotWorkOnGpu("Avro input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO} to true")
    }

    if (!meta.conf.isAvroReadEnabled) {
      meta.willNotWorkOnGpu("Avro input has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_AVRO_READ} to true")
    }

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
    rapidsConf: RapidsConf,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty) extends FileScan with ScanWithMetrics {
  override def isSplitable(path: Path): Boolean = true

  @scala.annotation.nowarn(
    "msg=value ignoreExtension in class AvroOptions is deprecated*"
  )
  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val parsedOptions = new AvroOptions(caseSensitiveMap, hadoopConf)
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    GpuAvroPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      rapidsConf,
      parsedOptions.ignoreExtension,
      metrics)
  }

  // overrides nothing in 330
  def withFilters(
    partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

}

/** Avro partition reader factory to build columnar reader */
case class GpuAvroPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    ignoreExtension: Boolean,
    metrics: Map[String, GpuMetric]) extends FilePartitionReaderFactory with Logging {

  private val debugDumpPrefix = rapidsConf.parquetDebugDumpPrefix
  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val blockMeta = GpuAvroFileFilterHandler(sqlConf, broadcastedConf,
      ignoreExtension, broadcastedConf.value.value).filterBlocks(partFile)
    val reader = new PartitionReaderWithBytesRead(new AvroPartitionReader(conf, partFile, blockMeta,
      readDataSchema, debugDumpPrefix, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}

/**
 * A tool to filter Avro blocks
 *
 * @param sqlConf         SQLConf
 * @param broadcastedConf the Hadoop configuration
 */
private case class GpuAvroFileFilterHandler(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    ignoreExtension: Boolean,
    hadoopConf: Configuration) extends Arm with Logging {

  def filterBlocks(partFile: PartitionedFile): AvroBlockMeta = {

    def passSync(blockStart: Long, position: Long): Boolean = {
      blockStart >= position + SYNC_SIZE
    }

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
case class CopyRange(offset: Long, length: Long)

/**
 *
 * @param conf the Hadoop configuration
 * @param partFile the partitioned files to read
 * @param blockMeta the block meta info of partFile
 * @param readDataSchema the Spark schema describing what will be read
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated avro data or null
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param execMetrics metrics
 */
class AvroPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    blockMeta: AvroBlockMeta,
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric]) extends FilePartitionReaderBase(conf, execMetrics) {

  val filePath = new Path(new URI(partFile.filePath))
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
        val table = readToTable(currentChunkedBlocks)
        try {
          val colTypes = readDataSchema.fields.map(f => f.dataType)
          val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
          maybeBatch.foreach { batch =>
            logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
          }
          maybeBatch
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(currentChunkedBlocks: Seq[BlockInfo]): Option[Table] = {
    if (currentChunkedBlocks.isEmpty) {
      return None
    }
    val (dataBuffer, dataSize) = readPartFile(currentChunkedBlocks, filePath)
    try {
      if (dataSize == 0) {
        None
      } else {

        // Dump data into a file
        dumpDataToFile(dataBuffer, dataSize, Array(partFile), Option(debugDumpPrefix), Some("avro"))

        val includeColumns = readDataSchema.fieldNames.toSeq

        val parseOpts = CudfAvroOptions.builder()
          .includeColumn(includeColumns: _*).build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

        val table = withResource(new NvtxWithMetrics("Avro decode", NvtxColor.DARK_GREEN,
          metrics(GPU_DECODE_TIME))) { _ =>
          Table.readAvro(parseOpts, dataBuffer, 0, dataSize)
        }
        closeOnExcept(table) { _ =>
          maxDeviceMemory = max(GpuColumnVector.getTotalDeviceMemoryUsed(table), maxDeviceMemory)
          if (readDataSchema.length < table.getNumberOfColumns) {
            throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $filePath")
          }
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(table)
      }
    } finally {
      dataBuffer.close()
    }
  }

  /** Copy the data into HMB */
  protected def copyDataRange(
      range: CopyRange,
      in: FSDataInputStream,
      out: OutputStream,
      copyBuffer: Array[Byte]): Unit = {
    var readTime = 0L
    var writeTime = 0L
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    var bytesLeft = range.length
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      val start = System.nanoTime()
      in.readFully(copyBuffer, 0, readLength)
      val mid = System.nanoTime()
      out.write(copyBuffer, 0, readLength)
      val end = System.nanoTime()
      readTime += (mid - start)
      writeTime += (end - mid)
      bytesLeft -= readLength
    }
    execMetrics.get(READ_FS_TIME).foreach(_.add(readTime))
    execMetrics.get(WRITE_BUFFER_TIME).foreach(_.add(writeTime))
  }

  /**
   * Tried to combine the sequential blocks
   * @param blocks blocks to be combined
   * @param blocksRange the list of combined ranges
   */
  private def combineBlocks(blocks: Seq[BlockInfo],
      blocksRange: ArrayBuffer[CopyRange]) = {
    var currentCopyStart = 0L
    var currentCopyEnd = 0L

    // Combine the meta and blocks into a seq to get the copy range
    val metaAndBlocks: Seq[BlockInfo] =
      Seq(BlockInfo(0, blockMeta.header.getFirstBlockStart, 0, 0)) ++ blocks

    metaAndBlocks.foreach { block =>
      if (currentCopyEnd != block.blockStart) {
        if (currentCopyEnd != 0) {
          blocksRange.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
        }
        currentCopyStart = block.blockStart
        currentCopyEnd = currentCopyStart
      }
      currentCopyEnd += block.blockLength
    }

    if (currentCopyEnd != currentCopyStart) {
      blocksRange.append(CopyRange(currentCopyStart, currentCopyEnd - currentCopyStart))
    }
  }

  protected def readPartFile(
    blocks: Seq[BlockInfo],
    filePath: Path): (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Avro buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      withResource(filePath.getFileSystem(conf).open(filePath)) { in =>
        val estTotalSize = calculateOutputSize(blocks)
        closeOnExcept(HostMemoryBuffer.allocate(estTotalSize)) { hmb =>
          val out = new HostMemoryOutputStream(hmb)
          val copyRanges = new ArrayBuffer[CopyRange]()
          combineBlocks(blocks, copyRanges)
          val copyBuffer = new Array[Byte](8 * 1024 * 1024)
          copyRanges.foreach(copyRange => copyDataRange(copyRange, in, out, copyBuffer))
          // check we didn't go over memory
          if (out.getPos > estTotalSize) {
            throw new QueryExecutionException(s"Calculated buffer size $estTotalSize is to " +
              s"small, actual written: ${out.getPos}")
          }
          (hmb, out.getPos)
        }
      }
    }
  }

  /**
   * Calculate the combined size
   * @param currentChunkedBlocks the blocks to calculated
   * @return the total size of blocks + header
   */
  protected def calculateOutputSize(currentChunkedBlocks: Seq[BlockInfo]): Long = {
    var totalSize: Long = 0;
    // For simplicity, we just copy the whole meta of AVRO
    totalSize += blockMeta.header.getFirstBlockStart
    // Add all blocks
    totalSize += currentChunkedBlocks.map(_.blockLength).sum
    totalSize
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
    var numRows: Long = 0
    var numBytes: Long = 0
    var numAvroBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIter.hasNext) {
        val peekedRowGroup = blockIter.head
        if (peekedRowGroup.count > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedRowGroup.count <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedRowGroup.count)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIter.next()
            numRows += currentChunk.last.count
            numAvroBytes += currentChunk.last.count
            numBytes += estimatedBytes
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
}
