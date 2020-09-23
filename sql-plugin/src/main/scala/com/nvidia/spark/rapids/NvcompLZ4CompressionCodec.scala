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
      contigTable: ContiguousTable): CompressedTable = {
    val tableBuffer = contigTable.getBuffer
    val (compressedSize, oversizedBuffer) = NvcompLZ4CompressionCodec.compress(tableBuffer)
    closeOnExcept(oversizedBuffer) { oversizedBuffer =>
      val tableMeta = MetaUtils.buildTableMeta(
        tableId,
        contigTable.getTable,
        tableBuffer,
        CodecType.NVCOMP_LZ4,
        compressedSize)
      CompressedTable(compressedSize, tableMeta, oversizedBuffer)
    }
  }

  override def decompressBuffer(
      outputBuffer: DeviceMemoryBuffer,
      outputOffset: Long,
      outputLength: Long,
      inputBuffer: DeviceMemoryBuffer,
      inputOffset: Long,
      inputLength: Long): Unit = {
    withResource(outputBuffer.slice(outputOffset, outputLength)) { outSlice =>
      withResource(inputBuffer.slice(inputOffset, inputLength)) { inSlice =>
        NvcompLZ4CompressionCodec.decompress(outSlice, inSlice)
      }
    }
  }

  override def createBatchCompressor(maxBatchMemoryBytes: Long): BatchedTableCompressor = {
    new BatchedNvcompLZ4Compressor(maxBatchMemoryBytes)
  }

  override def createBatchDecompressor(maxBatchMemoryBytes: Long): BatchedBufferDecompressor = {
    new BatchedNvcompLZ4Decompressor(maxBatchMemoryBytes)
  }
}

object NvcompLZ4CompressionCodec extends Arm {
  // TODO: Make this a config?
  val LZ4_CHUNK_SIZE: Int = 64 * 1024

  /**
   * Compress a data buffer.
   * @param input buffer containing data to compress
   * @return the size of the compressed data in bytes and the (probably oversized) output buffer
   */
  def compress(input: DeviceMemoryBuffer): (Long, DeviceMemoryBuffer) = {
    val tempSize = LZ4Compressor.getTempSize(input, CompressionType.CHAR, LZ4_CHUNK_SIZE)
    withResource(DeviceMemoryBuffer.allocate(tempSize)) { tempBuffer =>
      var compressedSize: Long = 0L
      val outputSize = LZ4Compressor.getOutputSize(input, CompressionType.CHAR, LZ4_CHUNK_SIZE,
        tempBuffer)
      closeOnExcept(DeviceMemoryBuffer.allocate(outputSize)) { outputBuffer =>
        compressedSize = LZ4Compressor.compress(input, CompressionType.CHAR, LZ4_CHUNK_SIZE,
          tempBuffer, outputBuffer, Cuda.DEFAULT_STREAM)
        (compressedSize, outputBuffer)
      }
    }
  }

  /**
   * Decompress data that was compressed with nvcomp's LZ4-GPU codec
   * @param outputBuffer where the uncompressed data will be written
   * @param inputBuffer buffer of compressed data to decompress
   */
  def decompress(outputBuffer: DeviceMemoryBuffer, inputBuffer: DeviceMemoryBuffer): Unit = {
    withResource(Decompressor.getMetadata(inputBuffer, Cuda.DEFAULT_STREAM)) { metadata =>
      val outputSize = Decompressor.getOutputSize(metadata)
      if (outputSize != outputBuffer.getLength) {
        throw new IllegalStateException(
          s"metadata uncompressed size is $outputSize, buffer size is ${outputBuffer.getLength}")
      }
      val tempSize = Decompressor.getTempSize(metadata)
      withResource(DeviceMemoryBuffer.allocate(tempSize)) { tempBuffer =>
        Decompressor.decompressAsync(inputBuffer, tempBuffer, metadata, outputBuffer,
          Cuda.DEFAULT_STREAM)
      }
    }
  }
}

class BatchedNvcompLZ4Compressor(maxBatchMemorySize: Long)
    extends BatchedTableCompressor(maxBatchMemorySize) {
  override protected def compress(tables: Array[ContiguousTable]): Array[CompressedTable] = {
    val inputBuffers: Array[BaseDeviceMemoryBuffer] = tables.map(_.getBuffer)
    val compressionResult = BatchedLZ4Compressor.compress(inputBuffers,
      NvcompLZ4CompressionCodec.LZ4_CHUNK_SIZE, Cuda.DEFAULT_STREAM)
    val compressedTables = try {
      val buffers = compressionResult.getCompressedBuffers
      val compressedSizes = compressionResult.getCompressedSizes
      buffers.zipWithIndex.map { case (buffer, i) =>
        val contigTable = tables(i)
        val compressedSize = compressedSizes(i)
        val meta = MetaUtils.buildTableMeta(
          tableId = 0,
          contigTable.getTable,
          contigTable.getBuffer,
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

class BatchedNvcompLZ4Decompressor(maxBatchMemory: Long)
    extends BatchedBufferDecompressor(maxBatchMemory) {
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  // TODO: Need to pass stream as arg, make name async

  override def decompress(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta]): Array[DeviceMemoryBuffer] = {
    BatchedLZ4Decompressor.decompressAsync(inputBuffers, Cuda.DEFAULT_STREAM)
  }
}