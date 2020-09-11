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

import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableArray

import org.apache.spark.sql.rapids.TempSpillBufferId
import org.apache.spark.sql.vectorized.ColumnarBatch

class SpillableColumnarBatchBuffer extends Arm {

  private[this] val ids: ArrayBuffer[TempSpillBufferId] = ArrayBuffer.empty

  def append(batch: ColumnarBatch, priority: Long): Unit = {
    val id = TempSpillBufferId()
    RapidsBufferCatalog.addBatch(id, batch, priority)
    ids.append(id)
  }

  /**
   * Close and clear any stored batches.
   */
  def clear(): Unit = {
    ids.foreach { id =>
      withResource(RapidsBufferCatalog.acquireBuffer(id)) { rapidsBuffer =>
        rapidsBuffer.free()
      }
    }
    ids.clear()
  }

  def popAll(): Array[ColumnarBatch] = {
    val ret = new Array[ColumnarBatch](ids.length)
    try {
      ids.indices.foreach { index =>
        val id = ids(index)
        withResource(RapidsBufferCatalog.acquireBuffer(id)) { rapidsBuffer =>
          ret(index) = rapidsBuffer.getColumnarBatch
          rapidsBuffer.free()
        }
      }
      ids.clear()
    } catch {
      case t: Throwable =>
        ret.safeClose()
        throw t
    }
    ret
  }
}

class SpillableDecompressionColumnarBatchBuffer(maxDecompressBatchMemory: Long)
    extends SpillableColumnarBatchBuffer {

  private[this] var codec: TableCompressionCodec = _

  def popAllDecompressed(): Array[ColumnarBatch] = {
    val batches = popAll()
    try {
      val compressedBatchIndices = batches.zipWithIndex.filter { pair =>
        GpuCompressedColumnVector.isBatchCompressed(pair._1)
      }.map(_._2)
      if (compressedBatchIndices.nonEmpty) {
        val compressedVecs = compressedBatchIndices.map { batchIndex =>
          batches(batchIndex).column(0).asInstanceOf[GpuCompressedColumnVector]
        }
        if (codec == null) {
          val descr = compressedVecs.head.getTableMeta.bufferMeta.codecBufferDescrs(0)
          codec = TableCompressionCodec.getCodec(descr.codec)
        }
        withResource(codec.createBatchDecompressor(maxDecompressBatchMemory)) { decompressor =>
          compressedVecs.foreach { cv =>
            val bufferMeta = cv.getTableMeta.bufferMeta
            // don't currently support switching codecs when partitioning
            val buffer = cv.getBuffer.slice(0, cv.getBuffer.getLength)
            decompressor.addBufferToDecompress(buffer, bufferMeta)
          }
          withResource(decompressor.finish()) { outputBuffers =>
            outputBuffers.zipWithIndex.foreach { case (outputBuffer, outputIndex) =>
              val cv = compressedVecs(outputIndex)
              val batchIndex = compressedBatchIndices(outputIndex)
              val compressedBatch = batches(batchIndex)
              batches(batchIndex) = MetaUtils.getBatchFromMeta(outputBuffer, cv.getTableMeta)
              compressedBatch.close()
            }
          }
        }
      }
    } catch {
      case t: Throwable =>
        batches.safeClose()
        throw t
    }
    batches
  }
}

