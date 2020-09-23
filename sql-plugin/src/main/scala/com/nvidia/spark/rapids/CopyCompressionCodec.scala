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
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.format.{BufferMeta, CodecType}

/** A table compression codec used only for testing that copies the data. */
class CopyCompressionCodec extends TableCompressionCodec with Arm {
  override val name: String = "COPY"
  override val codecId: Byte = CodecType.COPY

  override def compress(
      tableId: Int,
      contigTable: ContiguousTable): CompressedTable = {
    val buffer = contigTable.getBuffer
    closeOnExcept(buffer.sliceWithCopy(0, buffer.getLength)) { outputBuffer =>
      val meta = MetaUtils.buildTableMeta(
        tableId,
        contigTable.getTable,
        buffer,
        codecId,
        outputBuffer.getLength)
      CompressedTable(buffer.getLength, meta, outputBuffer)
    }
  }

  override def decompressBuffer(
      outputBuffer: DeviceMemoryBuffer,
      outputOffset: Long,
      outputLength: Long,
      inputBuffer: DeviceMemoryBuffer,
      inputOffset: Long,
      inputLength: Long): Unit = {
    require(outputLength == inputLength)
    outputBuffer.copyFromDeviceBufferAsync(
      outputOffset,
      inputBuffer,
      inputOffset,
      inputLength,
      Cuda.DEFAULT_STREAM)
  }

  override def createBatchCompressor(maxBatchMemorySize: Long): BatchedTableCompressor =
    new BatchedCopyCompressor(maxBatchMemorySize)

  override def createBatchDecompressor(maxBatchMemorySize: Long): BatchedBufferDecompressor =
    new BatchedCopyDecompressor(maxBatchMemorySize)
}

class BatchedCopyCompressor(maxBatchMemory: Long) extends BatchedTableCompressor(maxBatchMemory) {
  override protected def compress(tables: Array[ContiguousTable]): Array[CompressedTable] = {
    tables.safeMap { ct =>
      val inBuffer = ct.getBuffer
      closeOnExcept(DeviceMemoryBuffer.allocate(inBuffer.getLength)) { outBuffer =>
        outBuffer.copyFromDeviceBufferAsync(0, inBuffer, 0, inBuffer.getLength, Cuda.DEFAULT_STREAM)
        val meta = MetaUtils.buildTableMeta(
          0,
          ct.getTable,
          inBuffer,
          CodecType.COPY,
          outBuffer.getLength)
        CompressedTable(outBuffer.getLength, meta, outBuffer)
      }
    }
  }
}

class BatchedCopyDecompressor(maxBatchMemory: Long)
    extends BatchedBufferDecompressor(maxBatchMemory) {
  override val codecId: Byte = CodecType.COPY

  override def decompress(
      inputBuffers: Array[BaseDeviceMemoryBuffer],
      bufferMetas: Array[BufferMeta]): Array[DeviceMemoryBuffer] = {
    inputBuffers.safeMap { inBuffer =>
      closeOnExcept(DeviceMemoryBuffer.allocate(inBuffer.getLength)) { buffer =>
        buffer.copyFromDeviceBufferAsync(0, inBuffer, 0, inBuffer.getLength, Cuda.DEFAULT_STREAM)
        buffer
      }
    }
  }
}
