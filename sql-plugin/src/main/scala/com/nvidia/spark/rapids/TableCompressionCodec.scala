/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, DeviceMemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType, TableMeta}

import org.apache.spark.internal.Logging

/**
 * Compressed table descriptor
 * @note the buffer may be significantly oversized for the amount of compressed data
 * @param compressedSize size of the compressed data in bytes
 * @param meta metadata describing the table layout when uncompressed
 * @param buffer buffer containing the compressed data
 */
case class CompressedTable(
    compressedSize: Long,
    meta: TableMeta,
    buffer: DeviceMemoryBuffer) extends AutoCloseable {
  override def close(): Unit = buffer.close()
}

/** An interface to a compression codec that can compress a contiguous Table on the GPU */
trait TableCompressionCodec {
  /** The name of the codec, used for logging. */
  val name: String

  /** The ID used for this codec.  See the definitions in `CodecType`. */
  val codecId: Byte

  /**
   * Compress a contiguous table.
   * @note The contiguous table is NOT closed by this operation and must be closed separately.
   * @note The compressed buffer MAY NOT be ideally sized to the compressed data. It may be
   *       significantly larger than the size of the compressed data. Releasing this unused
   *       memory will require making a copy of the data to a buffer of the appropriate size.
   * @param tableId ID to use for this table
   * @param contigTable contiguous table to compress
   * @return compressed table
   */
  def compress(tableId: Int, contigTable: ContiguousTable): CompressedTable

  /**
   * Decompress the compressed data buffer from a table compression operation.
   * @note The compressed buffer is NOT closed by this method.
   * @param outputBuffer buffer where uncompressed data will be written
   * @param outputOffset offset in the uncompressed buffer to start writing data
   * @param outputLength expected length of the uncompressed data in bytes
   * @param inputBuffer buffer containing the compressed data
   * @param inputOffset offset in the compressed buffer where compressed data starts
   * @param inputLength length of the compressed data in bytes
   */
  def decompressBuffer(
      outputBuffer: DeviceMemoryBuffer,
      outputOffset: Long,
      outputLength: Long,
      inputBuffer: DeviceMemoryBuffer,
      inputOffset: Long,
      inputLength: Long): Unit

  /**
   * Create a batched compressor instance
   * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
   *                           which a batch should be compressed. A single table that requires
   *                           temporary and output memory above this limit is allowed but will
   *                           be compressed individually.
   * @return batched compressor instance
   */
  def createBatchCompressor(maxBatchMemorySize: Long): BatchedTableCompressor

  /**
   * Create a batched decompressor instance
   * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
   *                           which a batch should be decompressed. A single buffer that requires
   *                           temporary and output memory above this limit is allowed but will
   *                           be decompressed individually.
   * @return batched decompressor instance
   */
  def createBatchDecompressor(maxBatchMemorySize: Long): BatchedBufferDecompressor
}

object TableCompressionCodec {
  private val codecNameToId = Map(
    "copy" -> CodecType.COPY)

  /** Get a compression codec by short name or fully qualified class name */
  def getCodec(name: String): TableCompressionCodec = {
    val codecId = codecNameToId.getOrElse(name,
      throw new IllegalArgumentException(s"Unknown table codec: $name"))
    getCodec(codecId)
  }

  /** Get a compression codec by ID, using a cache. */
  def getCodec(codecId: Byte): TableCompressionCodec = {
    codecId match {
      case CodecType.COPY => new CopyCompressionCodec
      case _ => throw new IllegalArgumentException(s"Unknown codec ID: $codecId")
    }
  }
}

/**
 * Base class for batched compressors
 * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
 *                           which a batch should be compressed. A single table that requires
 *                           temporary and output memory above this limit is allowed but will
 *                           be compressed individually.
 */
abstract class BatchedTableCompressor(maxBatchMemorySize: Long) extends AutoCloseable with Arm
    with Logging {
  // The tables that need to be compressed in the next batch
  private[this] val tables = new ArrayBuffer[ContiguousTable]

  // The temporary compression buffers needed to compress each table in the next batch
  private[this] val tempBuffers = new ArrayBuffer[DeviceMemoryBuffer]

  // The estimate-sized output buffers to hold the compressed output in the next batch
  private[this] val oversizedOutBuffers = new ArrayBuffer[DeviceMemoryBuffer]

  // The compressed outputs of all tables across all batches
  private[this] val results = new ArrayBuffer[CompressedTable]

  // temporary and output memory being used as part of the current batch
  private[this] var batchMemUsed: Long = 0

  /**
   * Add a contiguous table to be batch-compressed. Ownership of the table is transferred to the
   * batch compressor which is responsible for closing the table.
   * @param contigTable the contiguous table to be compressed
   */
  def addTableToCompress(contigTable: ContiguousTable): Unit = {
    closeOnExcept(contigTable) { contigTable =>
      val tempSize = getTempSpaceNeeded(contigTable.getBuffer)
      var memNeededToCompressThisBuffer = tempSize
      if (batchMemUsed + memNeededToCompressThisBuffer > maxBatchMemorySize) {
        compressBatch()
      }
      val tempBuffer = if (tempSize > 0) {
        DeviceMemoryBuffer.allocate(memNeededToCompressThisBuffer)
      } else {
        null
      }
      try {
        val outputSize = getOutputSpaceNeeded(contigTable.getBuffer, tempBuffer)
        memNeededToCompressThisBuffer += outputSize
        if (batchMemUsed + memNeededToCompressThisBuffer > maxBatchMemorySize) {
          compressBatch()
        }
        oversizedOutBuffers += DeviceMemoryBuffer.allocate(outputSize)
        tempBuffers += tempBuffer
        tables += contigTable
        batchMemUsed += memNeededToCompressThisBuffer
      } catch {
        case t: Throwable =>
          if (tempBuffer != null) {
            tempBuffer.safeClose()
          }
          throw t
      }
    }
  }

  /**
   * Add an array of contiguous tables to be compressed. The tables will be closed by the
   * batch compressor.
   * @param contigTable contiguous tables to compress
   */
  def addTables(contigTable: Array[ContiguousTable]): Unit = {
    var i = 0
    try {
      contigTable.foreach { ct =>
        addTableToCompress(ct)
        i += 1
      }
    } catch {
      case t: Throwable =>
        contigTable.drop(i).foreach(_.safeClose())
        throw t
    }
  }

  /**
   * This must be called after all tables to be compressed have been added to retrieve the
   * compression results.
   * @note the table IDs in the TableMeta of all tables will be set to zero
   * @return compressed tables
   */
  def finish(): Array[CompressedTable] = {
    // compress the last batch
    compressBatch()

    val compressedTables = results.toArray
    results.clear()
    compressedTables
  }

  /** Must be closed to release the resources owned by the batch compressor */
  override def close(): Unit = {
    tables.safeClose()
    tempBuffers.safeClose()
    oversizedOutBuffers.safeClose()
    results.safeClose()
  }

  private def compressBatch(): Unit = if (tables.nonEmpty) {
    require(oversizedOutBuffers.length == tables.length)
    require(tempBuffers.length == tables.length)
    val startTime = System.nanoTime()
    val metas = withResource(new NvtxRange("batch ompress", NvtxColor.ORANGE)) { _ =>
      compress(oversizedOutBuffers.toArray, tables.toArray, tempBuffers.toArray)
    }
    require(metas.length == tables.length)

    val inputSize = tables.map(_.getBuffer.getLength).sum
    var outputSize: Long = 0

    // copy the output data into correctly-sized buffers
    withResource(new NvtxRange("copy compressed buffers", NvtxColor.PURPLE)) { _ =>
      metas.zipWithIndex.foreach { case (meta, i) =>
        val oversizedBuffer = oversizedOutBuffers(i)
        val compressedSize = meta.bufferMeta.size
        outputSize += compressedSize
        val buffer = if (oversizedBuffer.getLength > compressedSize) {
          oversizedBuffer.sliceWithCopy(0, compressedSize)
        } else {
          // use this buffer as-is, don't close it at the end of this method
          oversizedOutBuffers(i) = null
          oversizedBuffer
        }
        results += CompressedTable(compressedSize, meta, buffer)
      }
    }

    val duration = (System.nanoTime() - startTime).toFloat
    logDebug(s"Compressed ${tables.length} tables from $inputSize to $outputSize " +
        s"in ${duration / 1000000} msec rate=${inputSize / duration} GB/s " +
        s"ratio=${outputSize.toFloat/inputSize}")

    // free all the inputs to this batch
    tables.safeClose()
    tables.clear()
    tempBuffers.safeClose()
    tempBuffers.clear()
    oversizedOutBuffers.safeClose()
    oversizedOutBuffers.clear()
    batchMemUsed = 0
  }

  /** Return the amount of temporary space needed to compress this buffer */
  protected def getTempSpaceNeeded(buffer: DeviceMemoryBuffer): Long

  /** Return the amount of estimated output space needed to compress this buffer */
  protected def getOutputSpaceNeeded(
      dataBuffer: DeviceMemoryBuffer,
      tempBuffer: DeviceMemoryBuffer): Long

  /**
   * Batch-compress contiguous tables
   * @param outputBuffers output buffers allocated based on `getOutputSpaceNeeded` results
   * @param tables contiguous tables to compress
   * @param tempBuffers temporary buffers allocated based on `getTempSpaceNeeded` results.
   *                    If the temporary space needed was zero then the corresponding buffer
   *                    entry may be null.
   * @return table metadata for the compressed tables. Table IDs should be set to 0.
   */
  protected def compress(
      outputBuffers: Array[DeviceMemoryBuffer],
      tables: Array[ContiguousTable],
      tempBuffers: Array[DeviceMemoryBuffer]): Array[TableMeta]
}

/**
 * Base class for batched decompressors
 * @param maxBatchMemorySize The upper limit in bytes of temporary and output memory usage at
 *                           which a batch should be compressed. A single table that requires
 *                           temporary and output memory above this limit is allowed but will
 *                           be compressed individually.
 */
abstract class BatchedBufferDecompressor(maxBatchMemorySize: Long) extends AutoCloseable with Arm
    with Logging {
  // The buffers of compressed data that will be decompressed in the next batch
  private[this] val inputBuffers = new ArrayBuffer[DeviceMemoryBuffer]

  // The temporary buffers needed to be decompressed the next batch
  private[this] val tempBuffers = new ArrayBuffer[DeviceMemoryBuffer]

  // The output buffers that will contain the decompressed data in the next batch
  private[this] val outputBuffers = new ArrayBuffer[DeviceMemoryBuffer]

  // The decompressed data results for all input buffers across all batches
  private[this] val results = new ArrayBuffer[DeviceMemoryBuffer]

  // temporary and output memory being used as part of the current batch
  private[this] var batchMemUsed: Long = 0

  /** The codec ID corresponding to this decompressor */
  val codecId: Byte

  def addBufferToDecompress(buffer: DeviceMemoryBuffer, meta: BufferMeta): Unit = {
    closeOnExcept(buffer) { buffer =>
      // Only supports a single codec per buffer for now.
      require(meta.codecBufferDescrsLength == 1)
      val descr = meta.codecBufferDescrs(0)
      require(descr.codec == codecId)

      // Only support codec that consumes entire input buffer for now.
      require(descr.compressedOffset == 0)
      require(descr.compressedSize == buffer.getLength)

      val tempNeeded = decompressTempSpaceNeeded(buffer)
      val outputNeeded = descr.uncompressedSize
      if (batchMemUsed + tempNeeded + outputNeeded > maxBatchMemorySize) {
        decompressBatch()
      }

      val tempBuffer = if (tempNeeded > 0) {
        DeviceMemoryBuffer.allocate(tempNeeded)
      } else {
        null
      }
      val outputBuffer = DeviceMemoryBuffer.allocate(outputNeeded)
      batchMemUsed += tempNeeded + outputNeeded
      tempBuffers += tempBuffer
      outputBuffers += outputBuffer
      inputBuffers += buffer
    }
  }

  /**
   * This must be called after all buffers to be decompressed have been added to retrieve the
   * decompression results.
   * @return decompressed tables
   */
  def finish(): Array[DeviceMemoryBuffer] = {
    // decompress the last batch
    decompressBatch()
    val resultsArray = results.toArray
    results.clear()
    resultsArray
  }

  override def close(): Unit = {
    inputBuffers.safeClose()
    tempBuffers.safeClose()
    outputBuffers.safeClose()
    results.safeClose()
  }

  protected def decompressBatch(): Unit = {
    if (inputBuffers.nonEmpty) {
      require(outputBuffers.length == inputBuffers.length)
      require(tempBuffers.length == inputBuffers.length)
      val startTime = System.nanoTime()
      withResource(new NvtxRange("batch decompress", NvtxColor.ORANGE)) { _ =>
        decompress(outputBuffers.toArray, inputBuffers.toArray, tempBuffers.toArray)
      }
      val duration = (System.nanoTime - startTime).toFloat
      val inputSize = inputBuffers.map(_.getLength).sum
      val outputSize = outputBuffers.map(_.getLength).sum

      results ++= outputBuffers
      outputBuffers.clear()

      logDebug(s"Decompressed ${inputBuffers.length} buffers from $inputSize " +
          s"to $outputSize in ${duration / 1000000} msec rate=${outputSize / duration} GB/s")

      // free all the inputs to this batch
      inputBuffers.safeClose()
      inputBuffers.clear()
      tempBuffers.safeClose()
      tempBuffers.clear()
      batchMemUsed = 0
    }
  }

  /**
   * Compute the amount of temporary buffer space required to decompress a buffer
   * @param inputBuffer buffer to decompress
   * @return required temporary buffer space in bytes
   */
  protected def decompressTempSpaceNeeded(inputBuffer: DeviceMemoryBuffer): Long

  /**
   * Decompress a batch of compressed buffers
   * @param outputBuffers buffers that will contain the uncompressed output
   * @param inputBuffers buffers that contain the compressed input
   * @param tempBuffers buffers to used for temporary space
   */
  protected def decompress(
      outputBuffers: Array[DeviceMemoryBuffer],
      inputBuffers: Array[DeviceMemoryBuffer],
      tempBuffers: Array[DeviceMemoryBuffer]): Unit

}
