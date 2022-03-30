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

import ai.rapids.cudf.{BaseDeviceMemoryBuffer, ContiguousTable, Cuda, DeviceMemoryBuffer, NvtxColor, NvtxRange}
import ai.rapids.cudf.nvcomp.{BatchedLZ4Compressor, BatchedLZ4Decompressor, CompressionType, LZ4Compressor, LZ4Decompressor}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec that uses nvcomp's LZ4-GPU codec */
class NvcompLZ4CompressionCodec(codecConfigs: TableCompressionCodecConfig)
    extends TableCompressionCodec with Arm {
  override val name: String = "nvcomp-LZ4"
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def createBatchCompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedTableCompressor = {
    new BatchedNvcompLZ4Compressor(maxBatchMemoryBytes, codecConfigs, stream)
  }

  override def createBatchDecompressor(
      maxBatchMemoryBytes: Long,
      stream: Cuda.Stream): BatchedBufferDecompressor = {
    new BatchedNvcompLZ4Decompressor(maxBatchMemoryBytes, codecConfigs, stream)
  }
}

object NvcompLZ4CompressionCodec extends Arm {
  /**
   * Compress a data buffer.
   * @param input buffer containing data to compress
   * @param codecConfigs codec specific configuration options
   * @param stream CUDA stream to use
   * @return the size of the compressed data in bytes and the (probably oversized) output buffer
   */
  def compress(
      input: DeviceMemoryBuffer,
      codecConfigs: TableCompressionCodecConfig,
      stream: Cuda.Stream): (Long, DeviceMemoryBuffer) = {
    val lz4Config = LZ4Compressor.configure(codecConfigs.lz4ChunkSize, input.getLength)
    withResource(DeviceMemoryBuffer.allocate(lz4Config.getTempBytes)) { tempBuffer =>
      var compressedSize: Long = 0L
      val outputSize = lz4Config.getMaxCompressedBytes
      closeOnExcept(DeviceMemoryBuffer.allocate(outputSize)) { outputBuffer =>
        compressedSize = LZ4Compressor.compress(input, CompressionType.CHAR,
          codecConfigs.lz4ChunkSize, tempBuffer, outputBuffer, stream)
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
    withResource(LZ4Decompressor.configure(inputBuffer, stream)) { decompressConf =>
      val outputSize = decompressConf.getUncompressedBytes
      if (outputSize != outputBuffer.getLength) {
        throw new IllegalStateException(
          s"metadata uncompressed size is $outputSize, buffer size is ${outputBuffer.getLength}")
      }
      val tempSize = decompressConf.getTempBytes
      withResource(DeviceMemoryBuffer.allocate(tempSize)) { tempBuffer =>
        LZ4Decompressor.decompressAsync(inputBuffer, decompressConf, tempBuffer, outputBuffer,
          stream)
      }
    }
  }
}

class BatchedNvcompLZ4Compressor(maxBatchMemorySize: Long,
    codecConfigs: TableCompressionCodecConfig, stream: Cuda.Stream)
    extends BatchedTableCompressor(maxBatchMemorySize, stream) {
  override protected def compress(
      tables: Array[ContiguousTable],
      stream: Cuda.Stream): Array[CompressedTable] = {
    val batchCompressor = new BatchedLZ4Compressor(codecConfigs.lz4ChunkSize,
      maxBatchMemorySize)
    val inputBuffers: Array[BaseDeviceMemoryBuffer] = tables.map { table =>
      val buffer = table.getBuffer
      // cudf compressor guarantees that close will be called for 'inputBuffers' and will not throw
      // before doing so, but this interface does not close inputs so we need to increment the ref
      // count.
      buffer.incRefCount()
      buffer
    }
    closeOnExcept(batchCompressor.compress(inputBuffers, stream)) { compressedBuffers =>
      withResource(new NvtxRange("lz4 post process", NvtxColor.YELLOW)) { _ =>
        require(compressedBuffers.length == tables.length,
          s"expected ${tables.length} buffers, but compress() returned ${compressedBuffers.length}")
        compressedBuffers.zip(tables).map { case (buffer, table) =>
          val compressedSize = buffer.getLength
          val meta = MetaUtils.buildTableMeta(
            None,
            table,
            CodecType.NVCOMP_LZ4,
            compressedSize)
          CompressedTable(compressedSize, meta, buffer)
        }.toArray
      }
    }
  }
}

class BatchedNvcompLZ4Decompressor(maxBatchMemory: Long,
    codecConfigs: TableCompressionCodecConfig, stream: Cuda.Stream)
    extends BatchedBufferDecompressor(maxBatchMemory, stream) {
  override val codecId: Byte = CodecType.NVCOMP_LZ4

  override def decompressAsync(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta],
      stream: Cuda.Stream): Array[DeviceMemoryBuffer] = {
    require(inputBuffers.length == bufferMetas.length,
      s"number of input buffers (${inputBuffers.length}) does not equal number of metadata " +
          s"buffers (${bufferMetas.length}")
    val outputBuffers = allocateOutputBuffers(inputBuffers, bufferMetas)
    BatchedLZ4Decompressor.decompressAsync(
      codecConfigs.lz4ChunkSize,
      inputBuffers,
      outputBuffers.asInstanceOf[Array[BaseDeviceMemoryBuffer]],
      stream)
    outputBuffers
  }

  private def allocateOutputBuffers(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta]): Array[DeviceMemoryBuffer] = {
    withResource(new NvtxRange("alloc output bufs", NvtxColor.YELLOW)) { _ =>
      bufferMetas.zip(inputBuffers).safeMap { case (meta, input) =>
        // cudf decompressor guarantees that close will be called for 'inputBuffers' and will not
        // throw before doing so, but this interface does not close inputs so we need to increment
        // the ref count.
        input.incRefCount()
        DeviceMemoryBuffer.allocate(meta.uncompressedSize())
      }
    }
  }
}
