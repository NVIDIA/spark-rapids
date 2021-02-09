/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ContiguousTable, Cuda, DeviceMemoryBuffer}
import ai.rapids.cudf.nvcomp.{BatchedLZ4Compressor, BatchedLZ4Decompressor, CompressionType, Decompressor, LZ4Compressor}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec that uses nvcomp's LZ4-GPU codec */
class NvcompLZ4CompressionCodec extends TableCompressionCodec with Arm {
  override val name: String = "nvcomp-LZ4"
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def compress(
      tableId: Int,
      contigTable: ContiguousTable,
      stream: Cuda.Stream): CompressedTable = {
    val tableBuffer = contigTable.getBuffer
    val (compressedSize, oversizedBuffer) = NvcompLZ4CompressionCodec.compress(tableBuffer, stream)
    closeOnExcept(oversizedBuffer) { oversizedBuffer =>
      require(compressedSize <= oversizedBuffer.getLength, "compressed buffer overrun")
      val tableMeta = MetaUtils.buildTableMeta(
        Some(tableId),
        contigTable,
        CodecType.NVCOMP_LZ4,
        compressedSize)
      CompressedTable(compressedSize, tableMeta, oversizedBuffer)
    }
  }

  override def decompressBufferAsync(
      outputBuffer: DeviceMemoryBuffer,
      outputOffset: Long,
      outputLength: Long,
      inputBuffer: DeviceMemoryBuffer,
      inputOffset: Long,
      inputLength: Long,
      stream: Cuda.Stream): Unit = {
    withResource(outputBuffer.slice(outputOffset, outputLength)) { outSlice =>
      withResource(inputBuffer.slice(inputOffset, inputLength)) { inSlice =>
        NvcompLZ4CompressionCodec.decompressAsync(outSlice, inSlice, stream)
      }
    }
  }

  override def createBatchCompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedTableCompressor = {
    new BatchedNvcompLZ4Compressor(maxBatchMemoryBytes, stream)
  }

  override def createBatchDecompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor = {
    new BatchedNvcompLZ4Decompressor(maxBatchMemoryBytes, stream)
  }
}

object NvcompLZ4CompressionCodec extends Arm {
  // TODO: Make this a config?
  val LZ4_CHUNK_SIZE: Int = 64 * 1024

  /**
   * Compress a data buffer.
   * @param input buffer containing data to compress
   * @param stream CUDA stream to use
   * @return the size of the compressed data in bytes and the (probably oversized) output buffer
   */
  def compress(input: DeviceMemoryBuffer, stream: Cuda.Stream): (Long, DeviceMemoryBuffer) = {
    val tempSize = LZ4Compressor.getTempSize(input, CompressionType.CHAR, LZ4_CHUNK_SIZE)
    withResource(DeviceMemoryBuffer.allocate(tempSize)) { tempBuffer =>
      var compressedSize: Long = 0L
      val outputSize = LZ4Compressor.getOutputSize(input, CompressionType.CHAR, LZ4_CHUNK_SIZE,
        tempBuffer)
      closeOnExcept(DeviceMemoryBuffer.allocate(outputSize)) { outputBuffer =>
        compressedSize = LZ4Compressor.compress(input, CompressionType.CHAR, LZ4_CHUNK_SIZE,
          tempBuffer, outputBuffer, stream)
        require(compressedSize <= outputBuffer.getLength, "compressed buffer overrun")
        (compressedSize, outputBuffer)
      }
    }
  }

  /**
   * Decompress data asynchronously that was compressed with nvcomp's LZ4-GPU codec
   * @param outputBuffer where the uncompressed data will be written
   * @param inputBuffer buffer of compressed data to decompress
   * @param stream CUDA stream to use
   */
  def decompressAsync(
      outputBuffer: DeviceMemoryBuffer,
      inputBuffer: DeviceMemoryBuffer,
      stream: Cuda.Stream): Unit = {
    withResource(Decompressor.getMetadata(inputBuffer, stream)) { metadata =>
      val outputSize = Decompressor.getOutputSize(metadata)
      if (outputSize != outputBuffer.getLength) {
        throw new IllegalStateException(
          s"metadata uncompressed size is $outputSize, buffer size is ${outputBuffer.getLength}")
      }
      val tempSize = Decompressor.getTempSize(metadata)
      withResource(DeviceMemoryBuffer.allocate(tempSize)) { tempBuffer =>
        Decompressor.decompressAsync(inputBuffer, tempBuffer, metadata, outputBuffer, stream)
      }
    }
  }
}

class BatchedNvcompLZ4Compressor(maxBatchMemorySize: Long, stream: Cuda.Stream)
    extends BatchedTableCompressor(maxBatchMemorySize, stream) {
  override protected def compress(
      tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable] = {
    val inputBuffers: Array[BaseDeviceMemoryBuffer] = tables.map(_.getBuffer)
    val compressionResult = BatchedLZ4Compressor.compress(inputBuffers,
      NvcompLZ4CompressionCodec.LZ4_CHUNK_SIZE, stream)
    val compressedTables = try {
      val buffers = compressionResult.getCompressedBuffers
      val compressedSizes = compressionResult.getCompressedSizes
      buffers.zipWithIndex.map { case (buffer, i) =>
        val contigTable = tables(i)
        val compressedSize = compressedSizes(i)
        require(compressedSize <= buffer.getLength, "compressed buffer overrun")
        val meta = MetaUtils.buildTableMeta(
          None,
          contigTable,
          CodecType.NVCOMP_LZ4,
          compressedSize)
        CompressedTable(compressedSize, meta, buffer)
      }
    } catch {
      case t: Throwable =>
        compressionResult.getCompressedBuffers.safeClose()
        throw t
    }

    // output buffer sizes were estimated and probably significantly oversized, so copy any
    // oversized buffers to properly sized buffers in order to release the excess memory.
    resizeOversizedOutputs(compressedTables)
  }
}

class BatchedNvcompLZ4Decompressor(maxBatchMemory: Long, stream: Cuda.Stream)
    extends BatchedBufferDecompressor(maxBatchMemory, stream) {
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def decompressAsync(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta],
      stream: Cuda.Stream): Array[DeviceMemoryBuffer] = {
    BatchedLZ4Decompressor.decompressAsync(inputBuffers, stream)
  }
}