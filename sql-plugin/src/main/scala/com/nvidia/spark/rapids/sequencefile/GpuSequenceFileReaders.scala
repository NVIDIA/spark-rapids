/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.sequencefile

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.util
import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.io.async.{AsyncRunner, UnboundedAsyncRunner}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DataOutputBuffer, SequenceFile}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

private[sequencefile] object GpuSequenceFileReaders {
  final val KEY_FIELD: String = "key"
  final val VALUE_FIELD: String = "value"

  /**
   * Extracts the payload from a serialized BytesWritable record
   * (4-byte big-endian length prefix + payload bytes) and appends
   * it to the host buffer.
   *
   * Records with a missing or inconsistent length prefix produce a
   * zero-length payload. This is not an error: BytesWritable allows
   * empty payloads, and truly corrupt records never reach this method
   * because SequenceFile.Reader.nextRaw() throws IOException first.
   */
  def addBytesWritablePayload(
      bufferer: HostBinaryListBufferer,
      bytes: Array[Byte],
      offset: Int,
      totalLen: Int): Unit = {
    if (totalLen < 4) {
      bufferer.addBytes(bytes, offset, 0)
    } else {
      val payloadLen = ((bytes(offset) & 0xFF) << 24) |
        ((bytes(offset + 1) & 0xFF) << 16) |
        ((bytes(offset + 2) & 0xFF) << 8) |
        (bytes(offset + 3) & 0xFF)
      if (payloadLen > 0 && payloadLen <= totalLen - 4) {
        bufferer.addBytes(bytes, offset + 4, payloadLen)
      } else {
        bufferer.addBytes(bytes, offset, 0)
      }
    }
  }
}

private[sequencefile] final class UnsupportedSequenceFileCompressionException(msg: String)
  extends Exception(msg)

/**
 * Buffers binary values into one contiguous bytes buffer with an INT32 offsets buffer, and then
 * materializes a cuDF LIST<UINT8> device column using `makeListFromOffsets`.
 *
 * This class uses pinned memory (via HostAlloc) when available for better H2D transfer
 * performance. Pinned memory allows for faster and potentially asynchronous copies to the GPU.
 */
private[sequencefile] final class HostBinaryListBufferer(
    initialSizeBytes: Long,
    initialRows: Int) extends AutoCloseable with Logging {
  // Use HostAlloc which prefers pinned memory for better H2D transfer performance
  private var dataBuffer: HostMemoryBuffer =
    HostAlloc.alloc(math.max(initialSizeBytes, 1L), preferPinned = true)
  private var dataLocation: Long = 0L

  private var rowsAllocated: Int = math.max(initialRows, 1)
  private var offsetsBuffer: HostMemoryBuffer =
    HostAlloc.alloc((rowsAllocated.toLong + 1L) * DType.INT32.getSizeInBytes, preferPinned = true)
  private var numRows: Int = 0

  logDebug(s"HostBinaryListBufferer allocated: data=${dataBuffer.getLength} bytes, " +
    s"offsets=${offsetsBuffer.getLength} bytes")

  def rows: Int = numRows

  def usedBytes: Long = dataLocation

  private def growOffsetsIfNeeded(): Unit = {
    if (numRows + 1 > rowsAllocated) {
      // Use Int.MaxValue - 2 to ensure (rowsAllocated + 1) * 4 doesn't overflow
      val newRowsAllocated = math.min(rowsAllocated.toLong * 2, Int.MaxValue.toLong - 2L).toInt
      val newSize = (newRowsAllocated.toLong + 1L) * DType.INT32.getSizeInBytes
      // Use HostAlloc for pinned memory preference
      closeOnExcept(HostAlloc.alloc(newSize, preferPinned = true)) { tmpBuffer =>
        tmpBuffer.copyFromHostBuffer(0, offsetsBuffer, 0, offsetsBuffer.getLength)
        offsetsBuffer.close()
        offsetsBuffer = tmpBuffer
        rowsAllocated = newRowsAllocated
        logDebug(s"HostBinaryListBufferer grew offsets buffer to $newSize bytes")
      }
    }
  }

  private def growDataIfNeeded(requiredEnd: Long): Unit = {
    if (requiredEnd > dataBuffer.getLength) {
      val newSize = math.max(dataBuffer.getLength * 2, requiredEnd)
      // Use HostAlloc for pinned memory preference
      closeOnExcept(HostAlloc.alloc(newSize, preferPinned = true)) { newBuff =>
        newBuff.copyFromHostBuffer(0, dataBuffer, 0, dataLocation)
        dataBuffer.close()
        dataBuffer = newBuff
        logDebug(s"HostBinaryListBufferer grew data buffer to $newSize bytes")
      }
    }
  }

  def addBytes(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    val newEnd = dataLocation + len
    if (newEnd > Int.MaxValue) {
      throw new IllegalStateException(
        s"Binary column child size $newEnd would exceed INT32 offset limit")
    }
    growOffsetsIfNeeded()
    growDataIfNeeded(newEnd)
    val offsetPosition = numRows.toLong * DType.INT32.getSizeInBytes
    val startDataLocation = dataLocation
    dataBuffer.setBytes(dataLocation, bytes, offset, len)
    dataLocation = newEnd
    // Write offset only after successful data write
    offsetsBuffer.setInt(offsetPosition, startDataLocation.toInt)
    numRows += 1
  }

  /**
   * Returns the host memory buffers (data and offsets) and releases ownership.
   * The caller is responsible for closing the returned buffers.
   * This is used by the multi-file reader which needs host buffers for later GPU transfer.
   *
   * @return a tuple of (Some(dataBuffer), Some(offsetsBuffer)) if there is data,
   *         or (None, None) if empty
   */
  def getHostBuffersAndRelease(): (Option[HostMemoryBuffer], Option[HostMemoryBuffer]) = {
    if (numRows == 0) {
      return (None, None)
    }

    if (dataLocation > Int.MaxValue) {
      throw new IllegalStateException(
        s"Binary column child size $dataLocation exceeds INT32 offset limit")
    }
    // Write the final offset
    offsetsBuffer.setInt(numRows.toLong * DType.INT32.getSizeInBytes, dataLocation.toInt)

    // Transfer ownership of the existing host buffers to the caller. The downstream
    // H2D path uses numRows and the final offset value to determine the valid data range,
    // so these buffers do not need to be resized to the exact payload length here.
    val outDataBuffer = dataBuffer
    val outOffsetsBuffer = offsetsBuffer
    dataBuffer = null
    offsetsBuffer = null

    (Some(outDataBuffer), Some(outOffsetsBuffer))
  }

  override def close(): Unit = {
    if (dataBuffer != null) {
      dataBuffer.close()
      dataBuffer = null
    }
    if (offsetsBuffer != null) {
      offsetsBuffer.close()
      offsetsBuffer = null
    }
  }
}

/**
 * Represents a single chunk of SequenceFile binary data with its offsets.
 * Used for GPU concat optimization - each file becomes one chunk.
 *
 * @param dataBuffer host memory buffer containing binary data
 * @param offsetsBuffer host memory buffer containing INT32 offsets
 * @param numRows number of rows in this chunk
 */
private[sequencefile] final class SequenceFileChunk(
    private var dataBuffer: HostMemoryBuffer,
    private var offsetsBuffer: HostMemoryBuffer,
    val numRows: Int) extends AutoCloseable {
  def data: HostMemoryBuffer = dataBuffer
  def offsets: HostMemoryBuffer = offsetsBuffer

  override def close(): Unit = {
    if (dataBuffer != null) {
      dataBuffer.close()
      dataBuffer = null
    }
    if (offsetsBuffer != null) {
      offsetsBuffer.close()
      offsetsBuffer = null
    }
  }
}

/**
 * Host memory buffer metadata for SequenceFile multi-thread reader.
 *
 * Supports two modes:
 * 1. Single file mode: keyChunks/valueChunks have one element
 * 2. Combined mode (GPU concat): keyChunks/valueChunks have multiple elements,
 *    which will be concatenated on GPU for better performance (zero CPU copy)
 *
 * @param partitionedFile the partitioned file info
 * @param memBuffersAndSizes array of buffer metadata
 * @param bytesRead total bytes read from the file
 * @param keyChunks array of key data chunks (one per file when combined)
 * @param valueChunks array of value data chunks (one per file when combined)
 * @param totalRows total number of rows across all chunks
 * @param wantsKey whether the key column is requested
 * @param wantsValue whether the value column is requested
 * @param allPartValues optional array of (rowCount, partitionValues) when combining
 */
private[sequencefile] case class SequenceFileHostBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[SingleHMBAndMeta],
    override val bytesRead: Long,
    keyChunks: Array[SequenceFileChunk],
    valueChunks: Array[SequenceFileChunk],
    totalRows: Int,
    wantsKey: Boolean,
    wantsValue: Boolean,
    override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
  extends HostMemoryBuffersWithMetaDataBase {

  // When chunks are transferred to a combined result, this flag prevents
  // double-close: the combined result owns the chunks, not the original wrapper.
  private var _chunksTransferred: Boolean = false

  /** Mark that this wrapper's chunks have been transferred to a combined result. */
  def markChunksTransferred(): Unit = {
    _chunksTransferred = true
  }

  override def close(): Unit = {
    if (!_chunksTransferred) {
      keyChunks.foreach(_.close())
      valueChunks.foreach(_.close())
    }
    super.close()
  }
}

/**
 * Empty metadata returned when a file has no records.
 *
 * @param partitionedFile the partitioned file info
 * @param bytesRead total bytes read from the file
 * @param numRows number of rows (usually 0 for empty files, but may be > 0 when combining)
 * @param allPartValues optional array of (rowCount, partitionValues) when combining multiple files
 */
private[sequencefile] case class SequenceFileEmptyMetaData(
    override val partitionedFile: PartitionedFile,
    override val bytesRead: Long,
    numRows: Long = 0,
    override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
  extends HostMemoryBuffersWithMetaDataBase {
  override def memBuffersAndSizes: Array[SingleHMBAndMeta] = Array(SingleHMBAndMeta.empty())
}

/**
 * Multi-threaded cloud reader for SequenceFile format.
 * Reads multiple files in parallel using a thread pool.
 * Supports combining small files into larger batches for better GPU efficiency.
 */
class MultiFileCloudSequenceFilePartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    requiredSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Int,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    poolConf: ThreadPoolConf,
    maxNumFileProcessed: Int,
    execMetrics: Map[String, GpuMetric],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean,
    queryUsesInputFile: Boolean,
    combineConf: CombineConf = CombineConf(-1, -1))
  extends MultiFileCloudPartitionReaderBase(conf, files, poolConf, maxNumFileProcessed,
    Array.empty[Filter], execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    ignoreCorruptFiles, combineConf = combineConf) with MultiFileReaderFunctions with Logging {

  private val wantsKey = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(GpuSequenceFileReaders.KEY_FIELD))
  private val wantsValue = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(GpuSequenceFileReaders.VALUE_FIELD))

  private def toBatchRowCount(numRows: Long): Int = {
    try {
      Math.toIntExact(numRows)
    } catch {
      case _: ArithmeticException =>
        throw new IllegalArgumentException(
          s"SequenceFile batch row count $numRows exceeds " +
            "ColumnarBatch Int row limit")
    }
  }

  override def getFileFormatShortName: String = "SequenceFileBinary"

  /**
   * Whether to use combine mode to merge multiple small files into larger batches.
   * This improves GPU efficiency by reducing the number of small batches.
   */
  override def canUseCombine: Boolean = {
    if (queryUsesInputFile) {
      logDebug("Can't use combine mode because query uses 'input_file_xxx' function(s)")
      false
    } else {
      val canUse = combineConf.combineThresholdSize > 0
      if (!canUse) {
        logDebug("Cannot use combine mode because the threshold size <= 0")
      }
      canUse
    }
  }

  private def collectCombinedPartitionValues(
      input: Array[HostMemoryBuffersWithMetaDataBase]): Array[(Long, InternalRow)] = {
    val allPartValues = new ArrayBuffer[(Long, InternalRow)]()
    input.foreach { buf =>
      val partValues = buf.partitionedFile.partitionValues
      buf match {
        case empty: SequenceFileEmptyMetaData if empty.numRows > 0 =>
          allPartValues.append((empty.numRows, partValues))
        case meta: SequenceFileHostBuffersWithMetaData =>
          allPartValues.append((meta.totalRows.toLong, partValues))
        case _ =>
      }
    }
    allPartValues.toArray
  }

  private def addPartitionValuesToBatch(
      batch: ColumnarBatch,
      singlePartValues: InternalRow,
      combinedPartValues: Option[Array[(Long, InternalRow)]]): Iterator[ColumnarBatch] = {
    combinedPartValues match {
      case Some(partRowsAndValues) =>
        val (rowsPerPart, partValues) = partRowsAndValues.unzip
        BatchWithPartitionDataUtils.addPartitionValuesToBatch(
          batch,
          rowsPerPart,
          partValues,
          partitionSchema,
          maxGpuColumnSizeBytes)
      case None =>
        BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(
          batch,
          singlePartValues,
          partitionSchema,
          maxGpuColumnSizeBytes)
    }
  }

  /**
   * Combines multiple SequenceFile host memory buffers into a single buffer.
   * This reduces the number of batches sent to the GPU, improving performance.
   */
  override def combineHMBs(
      buffers: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    if (buffers.length == 1) {
      logDebug("No need to combine because there is only one buffer.")
      buffers.head
    } else {
      assert(buffers.length > 1)
      logDebug(s"Got ${buffers.length} buffers, combining them")
      doCombineHmbs(buffers)
    }
  }

  /**
   * Performs the actual combining of multiple SequenceFile buffers.
   *
   * OPTIMIZATION: Uses zero-copy approach similar to Parquet!
   * Instead of copying data on CPU, we just collect buffer references
   * and let GPU concatenate handle the merging (much faster due to high bandwidth).
   */
  private def doCombineHmbs(
      input: Array[HostMemoryBuffersWithMetaDataBase]): HostMemoryBuffersWithMetaDataBase = {
    val startCombineTime = System.currentTimeMillis()

    // Separate empty and non-empty buffers
    val (emptyBuffers, nonEmptyBuffers) = input.partition {
      case _: SequenceFileEmptyMetaData => true
      case meta: SequenceFileHostBuffersWithMetaData => meta.totalRows == 0
      case _ => false
    }

    val allPartValues = collectCombinedPartitionValues(input)

    // If all buffers are empty, return an empty combined result
    if (nonEmptyBuffers.isEmpty) {
      val totalBytesRead = input.map(_.bytesRead).sum
      val firstPart = input.head.partitionedFile
      emptyBuffers.foreach(_.close())
      return SequenceFileEmptyMetaData(
        firstPart,
        totalBytesRead,
        numRows = allPartValues.map(_._1).sum,
        allPartValues = if (allPartValues.nonEmpty) Some(allPartValues) else None)
    }

    // Close empty buffers since we don't need them
    emptyBuffers.foreach(_.close())

    // Cast non-empty buffers to the correct type
    val toCombine = nonEmptyBuffers.map(_.asInstanceOf[SequenceFileHostBuffersWithMetaData])

    logDebug(s"Using zero-copy Combine mode, collecting ${toCombine.length} non-empty files, " +
      s"files: ${toCombine.map(_.partitionedFile.filePath).mkString(",")}")

    // ZERO-COPY: Just collect all chunks without copying data!
    // The actual concatenation will happen on GPU (much faster)
    val allKeyChunks = toCombine.flatMap(_.keyChunks)
    val allValueChunks = toCombine.flatMap(_.valueChunks)
    val totalRowsLong = toCombine.map(_.totalRows.toLong).sum
    if (totalRowsLong > Int.MaxValue) {
      throw new IllegalStateException(
        s"Combined SequenceFile batch row count $totalRowsLong exceeds Int.MaxValue")
    }
    val totalRows = totalRowsLong.toInt
    val totalBytesRead = input.map(_.bytesRead).sum
    val firstMeta = toCombine.head

    val result = closeOnExcept(allKeyChunks ++ allValueChunks) { _ =>
      SequenceFileHostBuffersWithMetaData(
        partitionedFile = firstMeta.partitionedFile,
        memBuffersAndSizes = Array(SingleHMBAndMeta.empty(totalRows)),
        bytesRead = totalBytesRead,
        keyChunks = allKeyChunks,
        valueChunks = allValueChunks,
        totalRows = totalRows,
        wantsKey = wantsKey,
        wantsValue = wantsValue,
        allPartValues =
          if (allPartValues.nonEmpty) Some(allPartValues) else None)
    }

    // Mark original wrappers as transferred so their close() won't double-close
    // chunks that are now owned by the combined result.
    toCombine.foreach { wrapper =>
      wrapper.markChunksTransferred()
      wrapper.combineReleaseCallbacks(result)
    }

    logDebug(s"Zero-copy combine took ${System.currentTimeMillis() - startCombineTime} ms, " +
      s"collected ${toCombine.length} files with ${allKeyChunks.length} key chunks, " +
      s"${allValueChunks.length} value chunks, total ${totalRows} rows, " +
      s"task id: ${TaskContext.get().taskAttemptId()}")

    result
  }

  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      config: Configuration,
      filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(tc, file, config)
  }

  override def readBatches(
      fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
    fileBufsAndMeta match {
      case empty: SequenceFileEmptyMetaData =>
        try {
          // No data, but we might need to emit partition values
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val emptyBatch = new ColumnarBatch(Array.empty, toBatchRowCount(empty.numRows))
          addPartitionValuesToBatch(
            emptyBatch,
            empty.partitionedFile.partitionValues,
            empty.allPartValues)
        } finally {
          empty.close()
        }

      case meta: SequenceFileHostBuffersWithMetaData =>
        val batchIter = try {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          val batch = buildColumnarBatchFromHostBuffers(meta)
          closeOnExcept(batch) { _ =>
            addPartitionValuesToBatch(
              batch,
              meta.partitionedFile.partitionValues,
              meta.allPartValues)
          }
        } finally {
          // SequenceFile copies the full host buffers into device columns up front,
          // so the host-side chunks can be released immediately after the batch iterator
          // has been created.
          meta.close()
        }
        currentFileHostBuffers = None
        batchIter

      case other =>
        throw new RuntimeException(s"Unknown buffer type: ${other.getClass.getSimpleName}")
    }
  }

  private def buildColumnarBatchFromHostBuffers(
      meta: SequenceFileHostBuffersWithMetaData): ColumnarBatch = {
    val numRows = meta.totalRows

    if (numRows == 0 || requiredSchema.isEmpty) {
      return new ColumnarBatch(Array.empty, numRows)
    }

    // Build device columns from host buffers
    // If multiple chunks exist (combined mode), concatenate on GPU for better performance
    val keyCol: Option[ColumnVector] = if (meta.wantsKey && meta.keyChunks.nonEmpty) {
      Some(buildDeviceColumnFromChunks(meta.keyChunks))
    } else None

    val valueCol: Option[ColumnVector] = closeOnExcept(keyCol) { _ =>
      if (meta.wantsValue && meta.valueChunks.nonEmpty) {
        Some(buildDeviceColumnFromChunks(meta.valueChunks))
      } else None
    }

    withResource(keyCol) { kc =>
      withResource(valueCol) { vc =>
        val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
          if (f.name.equalsIgnoreCase(GpuSequenceFileReaders.KEY_FIELD)) {
            GpuColumnVector.from(kc.get.incRefCount(), BinaryType)
          } else if (f.name.equalsIgnoreCase(GpuSequenceFileReaders.VALUE_FIELD)) {
            GpuColumnVector.from(vc.get.incRefCount(), BinaryType)
          } else {
            GpuColumnVector.fromNull(numRows, f.dataType)
          }
        }
        closeOnExcept(cols) { _ =>
          new ColumnarBatch(cols, numRows)
        }
      }
    }
  }

  /**
   * Build a device column from multiple chunks using GPU concatenation.
   * This is the key optimization: instead of copying on CPU, we transfer each chunk
   * to GPU separately and use cudf::concatenate which is much faster.
   */
  private def buildDeviceColumnFromChunks(chunks: Array[SequenceFileChunk]): ColumnVector = {
    if (chunks.length == 1) {
      // Single chunk: use the original fast path
      val chunk = chunks.head
      buildDeviceColumnFromHostBuffers(chunk.data, chunk.offsets, chunk.numRows)
    } else {
      // Multiple chunks: transfer each to GPU and concatenate
      // GPU concat is much faster than CPU copy + offset adjustment
      val gpuCols = closeOnExcept(new ArrayBuffer[ColumnVector]()) { cols =>
        chunks.foreach { chunk =>
          cols += buildDeviceColumnFromHostBuffers(chunk.data, chunk.offsets, chunk.numRows)
        }
        cols
      }
      withResource(gpuCols) { _ =>
        // Use cudf concatenate - this is highly optimized and uses GPU memory bandwidth
        ColumnVector.concatenate(gpuCols.toArray: _*)
      }
    }
  }

  /**
   * Build a device column (LIST<UINT8>) from host memory buffers.
   * Uses a temporary nested HostColumnVector structure for a single copyToDevice() call.
   *
   * The input buffers remain owned by the caller/chunk metadata. This method creates
   * temporary copied slices first. Once the LIST HostColumnVector is created, it becomes
   * the sole owner of the child core plus both copied slices, so the success path closes
   * only that top-level owner.
   */
  private def buildDeviceColumnFromHostBuffers(
      dataBuffer: HostMemoryBuffer,
      offsetsBuffer: HostMemoryBuffer,
      numRows: Int): ColumnVector = {
    // Get the actual data length from the final offset
    val dataLen = offsetsBuffer.getInt(numRows.toLong * DType.INT32.getSizeInBytes)
    // Only copy the valid payload bytes. The backing host buffer may be much larger
    // because HostBinaryListBufferer preallocates for future growth.
    // When all payloads are empty (dataLen == 0), allocate a minimal 1-byte buffer
    // because HostColumnVectorCore requires a non-null data buffer.
    val dataSlice = if (dataLen > 0) {
      dataBuffer.sliceWithCopy(0, dataLen.toLong)
    } else {
      HostAlloc.alloc(1, preferPinned = false)
    }
    closeOnExcept(dataSlice) { _ =>
      val offsetsLen = (numRows.toLong + 1L) * DType.INT32.getSizeInBytes
      // LIST offsets only need numRows + 1 entries,
      // even if the reusable backing buffer grew larger.
      val offsetsSlice = offsetsBuffer.sliceWithCopy(0, offsetsLen)
      closeOnExcept(offsetsSlice) { _ =>
        val emptyChildren = new util.ArrayList[HostColumnVectorCore]()
        val childCore = new HostColumnVectorCore(DType.UINT8, dataLen,
          Optional.of[java.lang.Long](0L), dataSlice, null, null, emptyChildren)
        closeOnExcept(childCore) { _ =>
          val listChildren = new util.ArrayList[HostColumnVectorCore]()
          listChildren.add(childCore)
          withResource(new HostColumnVector(DType.LIST, numRows,
            Optional.of[java.lang.Long](0L), // nullCount = 0
            null, // no data buffer for LIST type
            null, // no validity buffer (no nulls)
            offsetsSlice, // offsets buffer
            listChildren))(_.copyToDevice())
        }
      }
    }
  }

  /**
   * Async runner that reads a single SequenceFile to host memory buffers.
   */
  private class ReadBatchRunner(
      taskContext: TaskContext,
      partFile: PartitionedFile,
      config: Configuration)
    extends UnboundedAsyncRunner[HostMemoryBuffersWithMetaDataBase] with Logging {

    override def callImpl(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      RmmSpark.poolThreadWorkingOnTask(taskContext.taskAttemptId())
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${partFile.filePath}", e)
          SequenceFileEmptyMetaData(partFile, 0L)
        // Explicit rethrow: FileNotFoundException extends IOException, so without this
        // case a missing file with !ignoreMissingFiles && ignoreCorruptFiles would be
        // silently swallowed by the IOException handler below.
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e: UnsupportedSequenceFileCompressionException => throw e
        case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(s"Skipped corrupted file: ${partFile.filePath}", e)
          SequenceFileEmptyMetaData(partFile, 0L)
      } finally {
        RmmSpark.poolThreadFinishedForTask(taskContext.taskAttemptId())
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()
      val path = new org.apache.hadoop.fs.Path(new URI(partFile.filePath.toString))

      withResource(new SequenceFile.Reader(config, SequenceFile.Reader.file(path))) { reader =>
        // Check for compression before starting split processing.
        // This remains an execution-time guard because planning-time sampling is conservative.
        if (reader.isCompressed || reader.isBlockCompressed) {
          val compressionType = reader.getCompressionType
          val msg = s"SequenceFile reader does not support " +
            s"compressed SequenceFiles (compressionType=$compressionType), file=$path"
          throw new UnsupportedSequenceFileCompressionException(msg)
        }

        val start = partFile.start
        if (start > 0) {
          reader.sync(start)
        }
        val end = partFile.start + partFile.length

        // Buffers for reading - reuse these across all records
        val keyDataOut = new DataOutputBuffer()
        val valueBytes = reader.createValueBytes()

        // Pre-allocate buffers based on the split size for fewer growth copies.
        // For uncompressed SequenceFiles, the value data is roughly proportional to the
        // split size. Using a generous initial estimate avoids repeated doubling + copy
        // operations (each doubling copies all existing data to a new buffer).
        val splitSize = partFile.length
        val initialSize = math.max(math.min(splitSize, 256L * 1024L * 1024L), 1024L * 1024L)
        val estimatedRows = math.max(
          math.min(splitSize / 512L, Int.MaxValue.toLong).toInt, 1024)
        val initialRows = math.min(estimatedRows, 4 * 1024 * 1024) // cap at 4M rows

        val keyBufferer = if (wantsKey) {
          Some(new HostBinaryListBufferer(initialSize, initialRows))
        } else None

        val valueBufferer = closeOnExcept(keyBufferer) { _ =>
          if (wantsValue) {
            Some(new HostBinaryListBufferer(initialSize, initialRows))
          } else None
        }

        // Reusable buffer for extracting value bytes from Hadoop ValueBytes.
        // This avoids creating a new ByteArrayOutputStream per record (which was
        // the #1 CPU-side performance bottleneck). DataOutputBuffer.getData() returns
        // the internal array without copying, unlike ByteArrayOutputStream.toByteArray().
        val valueDataOut = new DataOutputBuffer()

        withResource(keyBufferer) { keyBuf =>
          withResource(valueBufferer) { valBuf =>
            var numRows = 0
            var reachedEof = false

            // Hadoop SequenceFileRecordReader saves the position before each read, then decides
            // after the read whether to stop by checking that saved pre-read position together
            // with syncSeen() from the read that just happened. We mirror that policy here:
            // if the read started at/after the split end and crossed a sync marker, the record
            // belongs to the next split and is discarded from the current one.
            while (!reachedEof) {
              val posBeforeRead = reader.getPosition
              keyDataOut.reset()
              val recLen = reader.nextRaw(keyDataOut, valueBytes)
              if (recLen < 0) {
                // End of file reached
                reachedEof = true
              } else if (posBeforeRead >= end && reader.syncSeen()) {
                // We were already past the split end, and this read crossed a sync marker.
                // This record belongs to the next split - discard it.
                reachedEof = true
              } else {
                if (wantsKey) {
                  val keyLen = keyDataOut.getLength
                  keyBuf.foreach { buf =>
                    GpuSequenceFileReaders.addBytesWritablePayload(
                      buf, keyDataOut.getData, 0, keyLen)
                  }
                }
                if (wantsValue) {
                  // Use reusable DataOutputBuffer instead of per-record ByteArrayOutputStream.
                  // getData() returns the internal array (zero-copy), then
                  // addBytesWritablePayload does a single copy to the host buffer.
                  valueDataOut.reset()
                  valueBytes.writeUncompressedBytes(valueDataOut)
                  valBuf.foreach { buf =>
                    GpuSequenceFileReaders.addBytesWritablePayload(
                      buf, valueDataOut.getData, 0, valueDataOut.getLength)
                  }
                }
                numRows += 1
              }
            }

            val bytesRead = fileSystemBytesRead() - startingBytesRead

            if (numRows == 0) {
              SequenceFileEmptyMetaData(partFile, bytesRead)
            } else {
              // Extract host memory buffers from the streaming bufferers
              // Create SequenceFileChunk for each column (key/value)
              val keyChunks: Array[SequenceFileChunk] = keyBuf.map { kb =>
                val (dataOpt, offsetsOpt) = kb.getHostBuffersAndRelease()
                (dataOpt, offsetsOpt) match {
                  case (Some(data), Some(offsets)) =>
                    Array(new SequenceFileChunk(data, offsets, numRows))
                  case _ => Array.empty[SequenceFileChunk]
                }
              }.getOrElse(Array.empty)

              val valueChunks: Array[SequenceFileChunk] = closeOnExcept(keyChunks) { _ =>
                valBuf.map { vb =>
                  val (dataOpt, offsetsOpt) = vb.getHostBuffersAndRelease()
                  (dataOpt, offsetsOpt) match {
                    case (Some(data), Some(offsets)) =>
                      Array(new SequenceFileChunk(data, offsets, numRows))
                    case _ => Array.empty[SequenceFileChunk]
                  }
                }.getOrElse(Array.empty)
              }

              SequenceFileHostBuffersWithMetaData(
                partitionedFile = partFile,
                memBuffersAndSizes = Array(SingleHMBAndMeta.empty(numRows)),
                bytesRead = bytesRead,
                keyChunks = keyChunks,
                valueChunks = valueChunks,
                totalRows = numRows,
                wantsKey = wantsKey,
                wantsValue = wantsValue)
            }
          }
        }
      }
    }
  }
}

case class GpuSequenceFileMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  // COALESCING mode is not beneficial for SequenceFile since decoding happens on CPU
  // (using Hadoop's SequenceFile.Reader). There's no GPU-side decoding to amortize.
  // However, COMBINE mode is supported to merge multiple small files into larger batches.
  override val canUseCoalesceFilesReader: Boolean = false

  override val canUseMultiThreadReader: Boolean =
    rapidsConf.isSequenceFileMultiThreadReadEnabled

  private val maxNumFileProcessed = rapidsConf.maxNumSequenceFilesParallel
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  private val poolConf = ThreadPoolConfBuilder(rapidsConf).build

  // Combine configuration for merging small files into larger batches
  private val combineThresholdSize = rapidsConf.getMultithreadedCombineThreshold
  private val combineWaitTime = rapidsConf.getMultithreadedCombineWaitTime

  override protected def getFileFormatShortName: String = "SequenceFileBinary"

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val combineConf = CombineConf(combineThresholdSize, combineWaitTime)
    // Multi-threaded reader for cloud/parallel file reading with optional combining
    new PartitionReaderWithBytesRead(
      new MultiFileCloudSequenceFilePartitionReader(
        conf,
        files,
        readDataSchema,
        partitionSchema,
        maxReadBatchSizeRows,
        maxReadBatchSizeBytes,
        maxGpuColumnSizeBytes,
        poolConf,
        maxNumFileProcessed,
        metrics,
        ignoreMissingFiles,
        ignoreCorruptFiles,
        queryUsesInputFile,
        combineConf))
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // This should never be called since canUseCoalesceFilesReader = false
    throw new IllegalStateException(
      "COALESCING mode is not supported for SequenceFile. " +
      "Use MULTITHREADED or AUTO instead.")
  }
}
