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

import java.io.{DataOutputStream, FileNotFoundException, IOException}
import java.net.URI
import java.util
import java.util.Optional

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.io.async.{AsyncRunner, UnboundedAsyncRunner}
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shims.ShimFilePartitionReaderFactory
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

private[sequencefile] final case class PendingRecord(
    key: Option[Array[Byte]],
    value: Option[Array[Byte]],
    bytes: Long)

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
   * Add bytes from a BytesWritable serialized format, extracting only the payload.
   * BytesWritable serialization: 4-byte big-endian length prefix + payload bytes
   * This method skips the length prefix and only stores the actual payload.
   *
   * @param bytes the raw BytesWritable serialized bytes
   * @param offset the starting offset in the array
   * @param totalLen the total length of the serialized data (including length prefix)
   */
  def addBytesWritablePayload(bytes: Array[Byte], offset: Int, totalLen: Int): Unit = {
    if (totalLen < 4) {
      // Invalid or empty BytesWritable - add empty bytes
      addBytes(bytes, offset, 0)
    } else {
      // Read the 4-byte big-endian length prefix
      val payloadLen = ((bytes(offset) & 0xFF) << 24) |
                      ((bytes(offset + 1) & 0xFF) << 16) |
                      ((bytes(offset + 2) & 0xFF) << 8) |
                      (bytes(offset + 3) & 0xFF)
      // Extract the payload (skip the 4-byte length prefix)
      if (payloadLen > 0 && payloadLen <= totalLen - 4) {
        addBytes(bytes, offset + 4, payloadLen)
      } else {
        addBytes(bytes, offset, 0) // Empty payload
      }
    }
  }

  /**
   * Add value bytes directly from Hadoop's ValueBytes to the buffer.
   * This extracts the payload from BytesWritable serialization format, skipping the
   * 4-byte length prefix.
   *
   * @param valueBytes the Hadoop ValueBytes containing the raw value data
   * @param len the expected length of the value (from valueBytes.getSize())
   */
  def addValueBytes(valueBytes: SequenceFile.ValueBytes, len: Int): Unit = {
    if (len < 4) {
      // Invalid or empty BytesWritable - add empty bytes
      growOffsetsIfNeeded()
      val offsetPosition = numRows.toLong * DType.INT32.getSizeInBytes
      offsetsBuffer.setInt(offsetPosition, dataLocation.toInt)
      numRows += 1
      return
    }

    // Write to a temporary buffer first to read the length prefix
    val tempOut = new java.io.ByteArrayOutputStream(len)
    val tempDos = new java.io.DataOutputStream(tempOut)
    valueBytes.writeUncompressedBytes(tempDos)
    val rawBytes = tempOut.toByteArray

    // Extract payload from BytesWritable format: 4-byte length prefix + payload
    val payloadLen = ((rawBytes(0) & 0xFF) << 24) |
                    ((rawBytes(1) & 0xFF) << 16) |
                    ((rawBytes(2) & 0xFF) << 8) |
                    (rawBytes(3) & 0xFF)
    
    val actualPayloadLen = if (payloadLen > 0 && payloadLen <= rawBytes.length - 4) {
      payloadLen
    } else {
      0
    }

    val newEnd = dataLocation + actualPayloadLen
    if (newEnd > Int.MaxValue) {
      throw new IllegalStateException(
        s"Binary column child size $newEnd would exceed INT32 offset limit")
    }
    growOffsetsIfNeeded()
    growDataIfNeeded(newEnd)

    // Record the offset before writing
    val offsetPosition = numRows.toLong * DType.INT32.getSizeInBytes
    offsetsBuffer.setInt(offsetPosition, dataLocation.toInt)

    // Write only the payload (skip the 4-byte length prefix)
    if (actualPayloadLen > 0) {
      dataBuffer.setBytes(dataLocation, rawBytes, 4, actualPayloadLen)
      dataLocation = newEnd
    }
    numRows += 1
  }

  /**
   * Builds a cuDF LIST<UINT8> device column (Spark BinaryType equivalent) and releases host
   * buffers.
   * The returned ColumnVector owns its device memory and must be closed by the caller.
   *
   * This method builds a proper nested HostColumnVector (LIST containing UINT8 child) and
   * uses a single copyToDevice() call, which is more efficient than the alternative approach
   * of copying child and offsets separately then calling makeListFromOffsets().
   *
   * The makeListFromOffsets() approach has a performance issue: it internally creates new
   * cudf::column objects from column_view, which copies GPU memory. This results in:
   *   - 2 H2D transfers (child + offsets)
   *   - 2 extra GPU memory copies inside makeListFromOffsets()
   *
   * By using a proper nested HostColumnVector structure and single copyToDevice(), we get:
   *   - 1 logical H2D transfer (the nested structure handles all buffers)
   *   - 0 extra GPU memory copies
   */
  def getDeviceListColumnAndRelease(): ColumnVector = {
    if (dataLocation > Int.MaxValue) {
      throw new IllegalStateException(
        s"Binary column child size $dataLocation exceeds INT32 offset limit")
    }
    // Write the final offset
    offsetsBuffer.setInt(numRows.toLong * DType.INT32.getSizeInBytes, dataLocation.toInt)

    val childRowCount = dataLocation.toInt

    // Create the child HostColumnVectorCore (UINT8 data) - this will be nested inside the LIST
    val emptyChildren = new util.ArrayList[HostColumnVectorCore]()
    val childCore = closeOnExcept(dataBuffer) { _ =>
      closeOnExcept(offsetsBuffer) { _ =>
        new HostColumnVectorCore(DType.UINT8, childRowCount,
          Optional.of[java.lang.Long](0L), dataBuffer, null, null, emptyChildren)
      }
    }
    dataBuffer = null

    // Create the children list for the LIST column
    val listChildren = new util.ArrayList[HostColumnVectorCore]()
    listChildren.add(childCore)

    // Create the LIST HostColumnVector with proper nested structure
    // For LIST type: data buffer is null, offsets buffer contains the list offsets,
    // and the child column (UINT8) is in the nestedChildren list
    val listHost = closeOnExcept(childCore) { _ =>
      closeOnExcept(offsetsBuffer) { _ =>
        new HostColumnVector(DType.LIST, numRows,
          Optional.of[java.lang.Long](0L), // nullCount = 0
          null, // no data buffer for LIST type
          null, // no validity buffer (no nulls)
          offsetsBuffer, // offsets buffer
          listChildren) // nested children containing the UINT8 child
      }
    }
    offsetsBuffer = null

    // Single copyToDevice() call handles the entire nested structure efficiently
    // This avoids the extra GPU memory copies that makeListFromOffsets() would cause
    withResource(listHost)(_.copyToDevice())
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

    // Transfer ownership - the caller is now responsible for closing these buffers
    val retData = dataBuffer
    val retOffsets = offsetsBuffer
    dataBuffer = null
    offsetsBuffer = null
    // Note: directOut doesn't own any resources, no need to close

    (Some(retData), Some(retOffsets))
  }

  override def close(): Unit = {
    // directOut doesn't own any resources, no need to close
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
 * Reads a single SequenceFile split (PartitionedFile) and outputs ColumnarBatch on the GPU.
 *
 * Parsing is CPU-side using Hadoop SequenceFile.Reader, then bytes are copied to GPU and
 * represented as Spark BinaryType columns (cuDF LIST<UINT8>).
 */
class SequenceFilePartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    requiredSchema: StructType,
    maxRowsPerBatch: Int,
    maxBytesPerBatch: Long,
    execMetrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] with Logging {

  private[this] val path = new org.apache.hadoop.fs.Path(new URI(partFile.filePath.toString))
  private[this] val reader = {
    val r = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))
    closeOnExcept(r) { _ =>
      val start = partFile.start
      if (start > 0) {
        r.sync(start)
      }
      // For the initial version, we explicitly fail fast on compressed SequenceFiles.
      // (Record- and block-compressed files can be added later.)
      if (r.isCompressed || r.isBlockCompressed) {
        val compressionType = r.getCompressionType
        val msg = s"SequenceFileBinaryFileFormat does not support " +
          s"compressed SequenceFiles (compressionType=$compressionType), " +
          s"file=$path, keyClass=${r.getKeyClassName}, " +
          s"valueClass=${r.getValueClassName}"
        logError(msg)
        throw new UnsupportedOperationException(msg)
      }
      r
    }
  }
  private[this] val start = partFile.start
  private[this] val end = start + partFile.length

  private[this] val wantsKey = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD))
  private[this] val wantsValue = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD))

  private[this] val keyBuf = new DataOutputBuffer()
  private[this] val valueBytes = reader.createValueBytes()

  private[this] val pendingValueOut = new DataOutputBuffer()
  private[this] val pendingValueDos = new DataOutputStream(pendingValueOut)

  private[this] var pending: Option[PendingRecord] = None
  private[this] var exhausted = false
  private[this] var batch: Option[ColumnarBatch] = None

  private def bufferMetric: GpuMetric = execMetrics.getOrElse(BUFFER_TIME, NoopMetric)
  private def decodeMetric: GpuMetric = execMetrics.getOrElse(GPU_DECODE_TIME, NoopMetric)

  override def next(): Boolean = {
    // Close any batch that was prepared but never consumed via get()
    val previousBatch = batch
    batch = None
    previousBatch.foreach(_.close())

    if (exhausted) {
      false
    } else {
      batch = readBatch()
      batch.isDefined
    }
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException("No batch available"))
    batch = None
    ret
  }

  private def recordBytes(keyLen: Int, valueLen: Int): Long = {
    (if (wantsKey) keyLen.toLong else 0L) + (if (wantsValue) valueLen.toLong else 0L)
  }

  private def makePending(keyLen: Int, valueLen: Int): PendingRecord = {
    val keyArr =
      if (wantsKey) Some(util.Arrays.copyOf(keyBuf.getData, keyLen)) else None
    val valueArr =
      if (wantsValue) {
        pendingValueOut.reset()
        valueBytes.writeUncompressedBytes(pendingValueDos)
        Some(util.Arrays.copyOf(pendingValueOut.getData, pendingValueOut.getLength))
      } else None
    PendingRecord(keyArr, valueArr, recordBytes(keyLen, valueLen))
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val initialSize = math.min(maxBytesPerBatch, 1024L * 1024L) // 1MiB
    val initialRows = math.min(maxRowsPerBatch, 1024)

    val keyBufferer = if (wantsKey) {
      Some(new HostBinaryListBufferer(initialSize, initialRows))
    } else None

    val valueBufferer = closeOnExcept(keyBufferer) { _ =>
      if (wantsValue) {
        Some(new HostBinaryListBufferer(initialSize, initialRows))
      } else None
    }

    // Both bufferers need to be open throughout the read loop, so nesting is necessary.
    withResource(keyBufferer) { keyBuf =>
      withResource(valueBufferer) { valBuf =>
        var rows = 0
        var bytes = 0L

        bufferMetric.ns {
          // Handle a pending record (spill-over from previous batch).
          // Note: If rows == 0, we always add the pending record even if it exceeds
          // maxBytesPerBatch. This is intentional to ensure forward progress and avoid
          // infinite loops when a single record is larger than the batch size limit.
          pending.foreach { p =>
            if (rows == 0 || bytes + p.bytes <= maxBytesPerBatch) {
              p.key.foreach { k => keyBuf.foreach(_.addBytesWritablePayload(k, 0, k.length)) }
              p.value.foreach { v => valBuf.foreach(_.addBytesWritablePayload(v, 0, v.length)) }
              rows += 1
              bytes += p.bytes
              pending = None
            }
          }

          // Read new records.
          // Hadoop SequenceFile split boundary logic (matches SequenceFileRecordReader):
          // 1. Get position BEFORE reading
          // 2. Read the record
          // 3. If posBeforeRead >= end AND syncSeen (from this read), DISCARD the record
          // This ensures each record is processed by exactly one split.
          var keepReading = true
          while (keepReading && rows < maxRowsPerBatch) {
            val posBeforeRead = reader.getPosition
            this.keyBuf.reset()
            val recLen = reader.nextRaw(this.keyBuf, valueBytes)
            if (recLen < 0) {
              exhausted = true
              keepReading = false
            } else if (posBeforeRead >= end && reader.syncSeen()) {
              // We were already past the split end, and this read crossed a sync marker.
              // This record belongs to the next split - discard it.
              exhausted = true
              keepReading = false
            } else {
              val keyLen = this.keyBuf.getLength
              val valueLen = valueBytes.getSize
              val recBytes = recordBytes(keyLen, valueLen)

              // If this record doesn't fit, keep it for next batch (unless it's the first row)
              if (rows > 0 && bytes + recBytes > maxBytesPerBatch) {
                pending = Some(makePending(keyLen, valueLen))
                keepReading = false
              } else {
                keyBuf.foreach(_.addBytesWritablePayload(this.keyBuf.getData, 0, keyLen))
                valBuf.foreach(_.addValueBytes(valueBytes, valueLen))
                rows += 1
                bytes += recBytes
              }
            }
          }
        }

        if (rows == 0) {
          None
        } else {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())

          val outBatch = if (requiredSchema.isEmpty) {
            new ColumnarBatch(Array.empty, rows)
          } else {
            decodeMetric.ns {
              buildColumnarBatch(rows, keyBuf, valBuf)
            }
          }
          Some(outBatch)
        }
      }
    }
  }

  private def buildColumnarBatch(
      rows: Int,
      keyBufferer: Option[HostBinaryListBufferer],
      valueBufferer: Option[HostBinaryListBufferer]): ColumnarBatch = {
    // Build device columns once, then reference them for each schema field.
    // Use closeOnExcept to ensure keyCol is cleaned up if valueCol creation fails.
    val keyCol = keyBufferer.map(_.getDeviceListColumnAndRelease())
    val valueCol = closeOnExcept(keyCol) { _ =>
      valueBufferer.map(_.getDeviceListColumnAndRelease())
    }

    // Both columns need to be open for the mapping, so nesting is necessary here.
    withResource(keyCol) { kc =>
      withResource(valueCol) { vc =>
        val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
          if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)) {
            GpuColumnVector.from(kc.get.incRefCount(), BinaryType)
          } else if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)) {
            GpuColumnVector.from(vc.get.incRefCount(), BinaryType)
          } else {
            GpuColumnVector.fromNull(rows, f.dataType)
          }
        }
        closeOnExcept(cols) { _ =>
          new ColumnarBatch(cols, rows)
        }
      }
    }
  }

  override def close(): Unit = {
    reader.close()
    batch.foreach(_.close())
    batch = None
    exhausted = true
  }
}

/**
 * Host memory buffer metadata for SequenceFile multi-thread reader.
 */
private[sequencefile] case class SequenceFileHostBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[SingleHMBAndMeta],
    override val bytesRead: Long,
    keyBuffer: Option[HostMemoryBuffer],
    valueBuffer: Option[HostMemoryBuffer],
    keyOffsets: Option[HostMemoryBuffer],
    valueOffsets: Option[HostMemoryBuffer],
    numRows: Int,
    wantsKey: Boolean,
    wantsValue: Boolean) extends HostMemoryBuffersWithMetaDataBase {

  override def close(): Unit = {
    keyBuffer.foreach(_.close())
    valueBuffer.foreach(_.close())
    keyOffsets.foreach(_.close())
    valueOffsets.foreach(_.close())
    super.close()
  }
}

/**
 * Empty metadata returned when a file has no records.
 */
private[sequencefile] case class SequenceFileEmptyMetaData(
    override val partitionedFile: PartitionedFile,
    override val bytesRead: Long) extends HostMemoryBuffersWithMetaDataBase {
  override def memBuffersAndSizes: Array[SingleHMBAndMeta] = Array(SingleHMBAndMeta.empty())
}

/**
 * Multi-threaded cloud reader for SequenceFile format.
 * Reads multiple files in parallel using a thread pool.
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
    queryUsesInputFile: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, poolConf, maxNumFileProcessed,
    Array.empty[Filter], execMetrics, maxReadBatchSizeRows, maxReadBatchSizeBytes,
    ignoreCorruptFiles) with MultiFileReaderFunctions with Logging {

  private val wantsKey = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD))
  private val wantsValue = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD))

  override def getFileFormatShortName: String = "SequenceFileBinary"

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
        // No data, but we might need to emit partition values
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val emptyBatch = new ColumnarBatch(Array.empty, 0)
        BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(
          emptyBatch,
          empty.partitionedFile.partitionValues,
          partitionSchema,
          maxGpuColumnSizeBytes)

      case meta: SequenceFileHostBuffersWithMetaData =>
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        val batch = buildColumnarBatchFromHostBuffers(meta)
        val partValues = meta.partitionedFile.partitionValues
        closeOnExcept(batch) { _ =>
          BatchWithPartitionDataUtils.addSinglePartitionValueToBatch(
            batch,
            partValues,
            partitionSchema,
            maxGpuColumnSizeBytes)
        }

      case other =>
        throw new RuntimeException(s"Unknown buffer type: ${other.getClass.getSimpleName}")
    }
  }

  private def buildColumnarBatchFromHostBuffers(
      meta: SequenceFileHostBuffersWithMetaData): ColumnarBatch = {
    val numRows = meta.numRows

    if (numRows == 0 || requiredSchema.isEmpty) {
      return new ColumnarBatch(Array.empty, numRows)
    }

    // Build device columns from host buffers
    val keyCol: Option[ColumnVector] = if (meta.wantsKey && meta.keyBuffer.isDefined) {
      Some(buildDeviceColumnFromHostBuffers(
        meta.keyBuffer.get, meta.keyOffsets.get, numRows))
    } else None

    val valueCol: Option[ColumnVector] = closeOnExcept(keyCol) { _ =>
      if (meta.wantsValue && meta.valueBuffer.isDefined) {
        Some(buildDeviceColumnFromHostBuffers(
          meta.valueBuffer.get, meta.valueOffsets.get, numRows))
      } else None
    }

    withResource(keyCol) { kc =>
      withResource(valueCol) { vc =>
        val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
          if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)) {
            GpuColumnVector.from(kc.get.incRefCount(), BinaryType)
          } else if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)) {
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
   * Build a device column (LIST<UINT8>) from host memory buffers.
   * Uses proper nested HostColumnVector structure for efficient single copyToDevice().
   */
  private def buildDeviceColumnFromHostBuffers(
      dataBuffer: HostMemoryBuffer,
      offsetsBuffer: HostMemoryBuffer,
      numRows: Int): ColumnVector = {
    val dataLen = dataBuffer.getLength.toInt

    // Create the child HostColumnVectorCore (UINT8 data)
    val emptyChildren = new util.ArrayList[HostColumnVectorCore]()
    val childCore = new HostColumnVectorCore(DType.UINT8, dataLen,
      Optional.of[java.lang.Long](0L), dataBuffer, null, null, emptyChildren)

    // Create the children list for the LIST column
    val listChildren = new util.ArrayList[HostColumnVectorCore]()
    listChildren.add(childCore)

    // Create the LIST HostColumnVector with proper nested structure
    val listHost = closeOnExcept(childCore) { _ =>
      new HostColumnVector(DType.LIST, numRows,
        Optional.of[java.lang.Long](0L), // nullCount = 0
        null, // no data buffer for LIST type
        null, // no validity buffer (no nulls)
        offsetsBuffer, // offsets buffer
        listChildren) // nested children containing the UINT8 child
    }

    // Single copyToDevice() handles the entire nested structure efficiently
    withResource(listHost)(_.copyToDevice())
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
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
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

      val reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(path))
      try {
        // Check for compression - use closeOnExcept to ensure reader is closed on failure
        closeOnExcept(reader) { _ =>
          if (reader.isCompressed || reader.isBlockCompressed) {
            val compressionType = reader.getCompressionType
            val msg = s"SequenceFileBinaryFileFormat does not support " +
              s"compressed SequenceFiles (compressionType=$compressionType), file=$path"
            throw new UnsupportedOperationException(msg)
          }

          val start = partFile.start
          if (start > 0) {
            reader.sync(start)
          }
        }
        val end = partFile.start + partFile.length

        // Buffers for reading - reuse these across all records
        val keyDataOut = new DataOutputBuffer()
        val valueBytes = reader.createValueBytes()

        // Use streaming buffers to avoid holding all data in Java heap.
        // Start with reasonable initial sizes that will grow as needed.
        val initialSize = math.min(partFile.length, 1024L * 1024L) // 1MB or file size
        val initialRows = 1024

        val keyBufferer = if (wantsKey) {
          Some(new HostBinaryListBufferer(initialSize, initialRows))
        } else None

        val valueBufferer = closeOnExcept(keyBufferer) { _ =>
          if (wantsValue) {
            Some(new HostBinaryListBufferer(initialSize, initialRows))
          } else None
        }

        withResource(keyBufferer) { keyBuf =>
          withResource(valueBufferer) { valBuf =>
            var numRows = 0
            var reachedEof = false

            // Hadoop SequenceFile split boundary logic (matches SequenceFileRecordReader):
            // 1. Get position BEFORE reading
            // 2. Read the record
            // 3. If posBeforeRead >= end AND syncSeen (from this read), DISCARD the record
            // This ensures each record is processed by exactly one split.
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
                  keyBuf.foreach(_.addBytesWritablePayload(keyDataOut.getData, 0, keyLen))
                }
                if (wantsValue) {
                  val valueLen = valueBytes.getSize
                  valBuf.foreach(_.addValueBytes(valueBytes, valueLen))
                }
                numRows += 1
              }
            }

            val bytesRead = fileSystemBytesRead() - startingBytesRead

            if (numRows == 0) {
              SequenceFileEmptyMetaData(partFile, bytesRead)
            } else {
              // Extract host memory buffers from the streaming bufferers
              val (keyBuffer, keyOffsets) = keyBuf.map { kb =>
                kb.getHostBuffersAndRelease()
              }.getOrElse((None, None))

              val (valueBuffer, valueOffsets) = closeOnExcept(keyBuffer) { _ =>
                closeOnExcept(keyOffsets) { _ =>
                  valBuf.map { vb =>
                    vb.getHostBuffersAndRelease()
                  }.getOrElse((None, None))
                }
              }

              SequenceFileHostBuffersWithMetaData(
                partitionedFile = partFile,
                memBuffersAndSizes = Array(SingleHMBAndMeta.empty(numRows)),
                bytesRead = bytesRead,
                keyBuffer = keyBuffer,
                valueBuffer = valueBuffer,
                keyOffsets = keyOffsets,
                valueOffsets = valueOffsets,
                numRows = numRows,
                wantsKey = wantsKey,
                wantsValue = wantsValue)
            }
          }
        }
      } finally {
        reader.close()
      }
    }
  }
}

case class GpuSequenceFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) {

  private val maxReadBatchSizeRows = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes = rapidsConf.maxReadBatchSizeBytes
  private val maxGpuColumnSizeBytes = rapidsConf.maxGpuColumnSizeBytes

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val reader = new PartitionReaderWithBytesRead(
      new SequenceFilePartitionReader(
        conf,
        partFile,
        readDataSchema,
        maxReadBatchSizeRows,
        maxReadBatchSizeBytes,
        metrics))
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema,
      maxGpuColumnSizeBytes)
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
  override val canUseCoalesceFilesReader: Boolean = false

  override val canUseMultiThreadReader: Boolean =
    rapidsConf.isSequenceFileMultiThreadReadEnabled

  private val maxNumFileProcessed = rapidsConf.maxNumSequenceFilesParallel
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles
  private val poolConf = ThreadPoolConfBuilder(rapidsConf).build

  override protected def getFileFormatShortName: String = "SequenceFileBinary"

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // Multi-threaded reader for cloud/parallel file reading
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
        queryUsesInputFile))
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // This should never be called since canUseCoalesceFilesReader = false
    throw new IllegalStateException(
      "COALESCING mode is not supported for SequenceFile. " +
      "Use PERFILE or MULTITHREADED instead.")
  }
}
