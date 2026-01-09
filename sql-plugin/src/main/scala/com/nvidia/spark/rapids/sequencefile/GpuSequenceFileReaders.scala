/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.io.DataOutputStream
import java.net.URI
import java.util
import java.util.Optional

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, HostColumnVectorCore,
  HostMemoryBuffer}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.GpuMetric._
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
import org.apache.spark.sql.rapids.InputFileUtils
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
 */
private[sequencefile] final class HostBinaryListBufferer(
    initialSizeBytes: Long,
    initialRows: Int) extends AutoCloseable {
  private var dataBuffer: HostMemoryBuffer =
    HostMemoryBuffer.allocate(math.max(initialSizeBytes, 1L))
  private var dataLocation: Long = 0L

  private var rowsAllocated: Int = math.max(initialRows, 1)
  private var offsetsBuffer: HostMemoryBuffer =
    HostMemoryBuffer.allocate((rowsAllocated.toLong + 1L) * DType.INT32.getSizeInBytes)
  private var numRows: Int = 0

  private var out: HostMemoryOutputStream = new HostMemoryOutputStream(dataBuffer)
  private var dos: DataOutputStream = new DataOutputStream(out)

  def rows: Int = numRows

  def usedBytes: Long = dataLocation

  private def growOffsetsIfNeeded(): Unit = {
    if (numRows + 1 > rowsAllocated) {
      val newRowsAllocated = math.min(rowsAllocated * 2, Int.MaxValue - 1)
      val tmpBuffer =
        HostMemoryBuffer.allocate((newRowsAllocated.toLong + 1L) * DType.INT32.getSizeInBytes)
      tmpBuffer.copyFromHostBuffer(0, offsetsBuffer, 0, offsetsBuffer.getLength)
      offsetsBuffer.close()
      offsetsBuffer = tmpBuffer
      rowsAllocated = newRowsAllocated
    }
  }

  private def growDataIfNeeded(requiredEnd: Long): Unit = {
    if (requiredEnd > dataBuffer.getLength) {
      val newSize = math.max(dataBuffer.getLength * 2, requiredEnd)
      closeOnExcept(HostMemoryBuffer.allocate(newSize)) { newBuff =>
        newBuff.copyFromHostBuffer(0, dataBuffer, 0, dataLocation)
        dataBuffer.close()
        dataBuffer = newBuff
        out = new HostMemoryOutputStream(dataBuffer)
        dos = new DataOutputStream(out)
      }
    }
  }

  def addBytes(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    growOffsetsIfNeeded()
    val end = dataLocation + len
    growDataIfNeeded(end)
    offsetsBuffer.setInt(numRows.toLong * DType.INT32.getSizeInBytes, dataLocation.toInt)
    dataBuffer.setBytes(dataLocation, bytes, offset, len)
    dataLocation = end
    numRows += 1
  }

  def addValueBytes(valueBytes: SequenceFile.ValueBytes, len: Int): Unit = {
    growOffsetsIfNeeded()
    val end = dataLocation + len
    growDataIfNeeded(end)
    offsetsBuffer.setInt(numRows.toLong * DType.INT32.getSizeInBytes, dataLocation.toInt)
    out.seek(dataLocation)
    valueBytes.writeUncompressedBytes(dos)
    dataLocation = out.getPos
    numRows += 1
  }

  /**
   * Builds a cuDF LIST<UINT8> device column (Spark BinaryType equivalent) and releases host
   * buffers.
   * The returned ColumnVector owns its device memory and must be closed by the caller.
   */
  def getDeviceListColumnAndRelease(): ColumnVector = {
    if (dataLocation > Int.MaxValue) {
      throw new IllegalStateException(
        s"Binary column child size $dataLocation exceeds INT32 offset limit")
    }
    offsetsBuffer.setInt(numRows.toLong * DType.INT32.getSizeInBytes, dataLocation.toInt)

    val emptyChildren = new util.ArrayList[HostColumnVectorCore]()
    val childRowCount = dataLocation.toInt
    val offsetsRowCount = numRows + 1

    val childHost = new HostColumnVector(DType.UINT8, childRowCount,
      Optional.of[java.lang.Long](0L), dataBuffer, null, null, emptyChildren)
    val offsetsHost = new HostColumnVector(DType.INT32, offsetsRowCount,
      Optional.of[java.lang.Long](0L), offsetsBuffer, null, null, emptyChildren)

    // Transfer ownership of the host buffers to the HostColumnVectors.
    dataBuffer = null
    offsetsBuffer = null
    out = null
    dos = null

    var list: ColumnVector = null
    try {
      val childDev = childHost.copyToDevice()
      try {
        val offsetsDev = offsetsHost.copyToDevice()
        try {
          list = childDev.makeListFromOffsets(numRows, offsetsDev)
        } finally {
          offsetsDev.close()
        }
      } finally {
        childDev.close()
      }
      list
    } finally {
      // Close host columns (releasing the host buffers).
      childHost.close()
      offsetsHost.close()
      // Close result on failure.
      if (list == null) {
        // nothing
      }
    }
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
  private[this] val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))
  private[this] val start = partFile.start
  private[this] val end = start + partFile.length
  if (start > 0) {
    reader.sync(start)
  }

  // For the initial version, we explicitly fail fast on compressed SequenceFiles.
  // (Record- and block-compressed files can be added later.)
  if (reader.isCompressed || reader.isBlockCompressed) {
    val msg = s"${SequenceFileBinaryFileFormat.SHORT_NAME} does not support " +
      s"compressed SequenceFiles " +
      s"(isCompressed=${reader.isCompressed}, " +
      s"isBlockCompressed=${reader.isBlockCompressed}), " +
      s"file=$path, keyClass=${reader.getKeyClassName}, " +
      s"valueClass=${reader.getValueClassName}"
    logError(msg)
    reader.close()
    throw new UnsupportedOperationException(msg)
  }

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
    batch.foreach(_.close())
    batch = if (exhausted) {
      None
    } else {
      readBatch()
    }
    batch.isDefined
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

    var keyBufferer: HostBinaryListBufferer = null
    var valueBufferer: HostBinaryListBufferer = null
    if (wantsKey) keyBufferer = new HostBinaryListBufferer(initialSize, initialRows)
    if (wantsValue) valueBufferer = new HostBinaryListBufferer(initialSize, initialRows)

    try {
      var rows = 0
      var bytes = 0L

      bufferMetric.ns {
        // Handle a pending record (spill-over from previous batch)
        pending.foreach { p =>
          if (rows == 0 || bytes + p.bytes <= maxBytesPerBatch) {
            p.key.foreach { k => keyBufferer.addBytes(k, 0, k.length) }
            p.value.foreach { v => valueBufferer.addBytes(v, 0, v.length) }
            rows += 1
            bytes += p.bytes
            pending = None
          }
        }

        // Read new records
        var keepReading = true
        while (keepReading && rows < maxRowsPerBatch && reader.getPosition < end) {
          val recLen = reader.nextRaw(keyBuf, valueBytes)
          if (recLen < 0) {
            exhausted = true
            keepReading = false
          } else {
            val keyLen = keyBuf.getLength
            val valueLen = valueBytes.getSize
            val recBytes = recordBytes(keyLen, valueLen)

            // If this record doesn't fit, keep it for the next batch (unless it's the first row)
            if (rows > 0 && recBytes > 0 && bytes + recBytes > maxBytesPerBatch) {
              pending = Some(makePending(keyLen, valueLen))
              keepReading = false
            } else {
              if (wantsKey) {
                keyBufferer.addBytes(keyBuf.getData, 0, keyLen)
              }
              if (wantsValue) {
                valueBufferer.addValueBytes(valueBytes, valueLen)
              }
              rows += 1
              bytes += recBytes
            }
          }
        }
      }

      if (rows == 0) {
        None
      } else {
        // Acquire the semaphore before doing any GPU work (including partition columns downstream).
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val outBatch = if (requiredSchema.isEmpty) {
          new ColumnarBatch(Array.empty, rows)
        } else {
          decodeMetric.ns {
            val cols = new Array[SparkVector](requiredSchema.length)
            var success = false
            try {
              requiredSchema.fields.zipWithIndex.foreach { case (f, i) =>
                if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)) {
                  val cudf = keyBufferer.getDeviceListColumnAndRelease()
                  cols(i) = GpuColumnVector.from(cudf, BinaryType)
                } else if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)) {
                  val cudf = valueBufferer.getDeviceListColumnAndRelease()
                  cols(i) = GpuColumnVector.from(cudf, BinaryType)
                } else {
                  cols(i) = GpuColumnVector.fromNull(rows, f.dataType)
                }
              }
              val cb = new ColumnarBatch(cols, rows)
              success = true
              cb
            } finally {
              if (!success) {
                cols.foreach { cv =>
                  if (cv != null) {
                    cv.close()
                  }
                }
              }
            }
          }
        }
        Some(outBatch)
      }
    } finally {
      if (keyBufferer != null) keyBufferer.close()
      if (valueBufferer != null) valueBufferer.close()
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
 * A multi-file reader that iterates through the PartitionedFiles in a Spark FilePartition and
 * emits batches for each file sequentially (no cross-file coalescing).
 */
class SequenceFileMultiFilePartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    requiredSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Int,
    maxReadBatchSizeBytes: Long,
    maxGpuColumnSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean) extends PartitionReader[ColumnarBatch] with Logging {

  private[this] var fileIndex = 0
  private[this] var currentReader: PartitionReader[ColumnarBatch] = null
  private[this] var batch: Option[ColumnarBatch] = None

  override def next(): Boolean = {
    // Close any batch that was prepared but never consumed via get()
    batch.foreach(_.close())
    batch = None

    while (fileIndex < files.length) {
      val pf = files(fileIndex)
      if (currentReader == null) {
        if (queryUsesInputFile) {
          InputFileUtils.setInputFileBlock(pf.filePath.toString(), pf.start, pf.length)
        } else {
          // Still set it to avoid stale values if any downstream uses it unexpectedly.
          InputFileUtils.setInputFileBlock(pf.filePath.toString(), pf.start, pf.length)
        }

        val base = new SequenceFilePartitionReader(
          conf,
          pf,
          requiredSchema,
          maxReadBatchSizeRows,
          maxReadBatchSizeBytes,
          execMetrics)
        val withBytesRead = new PartitionReaderWithBytesRead(base)
        currentReader = ColumnarPartitionReaderWithPartitionValues.newReader(
          pf, withBytesRead, partitionSchema, maxGpuColumnSizeBytes)
      }

      if (currentReader.next()) {
        batch = Some(currentReader.get())
        return true
      } else {
        currentReader.close()
        currentReader = null
        fileIndex += 1
      }
    }
    false
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException("No batch available"))
    batch = None
    ret
  }

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
      currentReader = null
    }
    batch.foreach(_.close())
    batch = None
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
    requiredSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  override val canUseCoalesceFilesReader: Boolean = true
  override val canUseMultiThreadReader: Boolean = false

  override protected def getFileFormatShortName: String = "SequenceFileBinary"

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // No special cloud implementation yet; read sequentially on the task thread.
    new PartitionReaderWithBytesRead(
      new SequenceFileMultiFilePartitionReader(conf, files, requiredSchema, partitionSchema,
        maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
        metrics, queryUsesInputFile))
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // Sequential multi-file reader (no cross-file coalescing).
    new PartitionReaderWithBytesRead(
      new SequenceFileMultiFilePartitionReader(conf, files, requiredSchema, partitionSchema,
        maxReadBatchSizeRows, maxReadBatchSizeBytes, maxGpuColumnSizeBytes,
        metrics, queryUsesInputFile))
  }
}


