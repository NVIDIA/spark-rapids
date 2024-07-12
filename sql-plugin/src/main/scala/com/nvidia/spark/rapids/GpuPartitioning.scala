/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.jni.GpuSplitAndRetryOOM

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuPartitioning {
  // The maximum size of an Array minus a bit for overhead for metadata
  val MaxCpuBatchSize = 2147483639L - 2048L
}

trait GpuPartitioning extends Partitioning {
  private[this] val (maxCompressionBatchSize, _useGPUShuffle, _useMultiThreadedShuffle,
      _isSerdeOnGPU) = {
    val rapidsConf = new RapidsConf(SQLConf.get)
    (rapidsConf.shuffleCompressionMaxBatchMemory,
      GpuShuffleEnv.useGPUShuffle(rapidsConf),
      GpuShuffleEnv.useMultiThreadedShuffle(rapidsConf),
      GpuShuffleEnv.isSerdeOnGpu(rapidsConf))
  }

  final def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    throw new IllegalStateException(
      "Partitioners do not support columnarEval, only columnarEvalAny")
  }

  def usesGPUShuffle: Boolean = _useGPUShuffle

  def usesMultiThreadedShuffle: Boolean = _useMultiThreadedShuffle

  def serdeOnGPU: Boolean = _isSerdeOnGPU

  def sliceBatch(vectors: Array[RapidsHostColumnVector], start: Int, end: Int): ColumnarBatch = {
    var ret: ColumnarBatch = null
    val count = end - start
    if (count > 0) {
      ret = new ColumnarBatch(vectors.map(vec => new SlicedGpuColumnVector(vec, start, end)))
      ret.setNumRows(count)
    }
    ret
  }

  def sliceInternalOnGpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[ColumnarBatch] = {
    val batches = if (numRows > 0) {
      withResource(new NvtxRange("Split Compress", NvtxColor.GREEN)) { _ =>
        // The first index will always be 0, so we need to skip it.
        val parts = partitionIndexes.slice(1, partitionIndexes.length)
        closeOnExcept(new ArrayBuffer[ColumnarBatch](numPartitions)) { splits =>
          val contiguousTables = withResource(partitionColumns) { _ =>
            withResource(new Table(partitionColumns.map(_.getBase).toArray: _*)) { table =>
              withRetryNoSplit(table.contiguousSplit(parts: _*))
            }
          }
          GpuShuffleEnv.rapidsShuffleCodec match {
            case Some(codec) =>
              compressSplits(splits, codec, contiguousTables)
            case None =>
              // GpuPackedTableColumn takes ownership of the contiguous tables
              closeOnExcept(contiguousTables) { cts =>
                cts.foreach { ct => splits.append(GpuPackedTableColumn.from(ct)) }
              }
          }
          // synchronize our stream to ensure we have caught up with contiguous split
          // as downstream consumers (RapidsShuffleManager) will add hundreds of buffers
          // to the spill framework, this makes it so here we synchronize once.
          Cuda.DEFAULT_STREAM.sync()
          splits.toArray
        }
      }
    } else {
      Array[ColumnarBatch]()
    }

    if (_isSerdeOnGPU) {
      withResource(new NvtxRange("Buffering toHost", NvtxColor.BLUE)) { _ =>
        closeOnExcept(moveToHostAndClose(batches)) { hostBatches =>
          // All the data should be on host now for shuffle, leaving GPU for a while.
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          hostBatches
        }
      }
    } else {
      batches
    }
  }

  private case class CloseableBufferSeq(bufs: Seq[DeviceMemoryBuffer]) extends AutoCloseable {
    override def close(): Unit = bufs.safeClose()
  }

  private def splitBufsToMergeFn: CloseableBufferSeq => Seq[CloseableBufferSeq] = {
    devBufs => {
      closeOnExcept(devBufs) { _ =>
        val numBufs = devBufs.bufs.length
        if (numBufs <= 1) {
          throw new GpuSplitAndRetryOOM(s"Cannot split a sequence of $numBufs device buffers")
        }
        val (head, tail) = devBufs.bufs.splitAt(numBufs / 2)
        Seq(CloseableBufferSeq(head), CloseableBufferSeq(tail))
      }
    }
  }

  /**
   * Move the small split batches from device to host in the following steps:
   *   1) Concatenate all the split device buffers into a single contiguous buffer,
   *   2) Move the single device buffer to host,
   *   3) Rebuild the split batches on host.
   * to avoid too many small data copying between device and host.
   */
  private def moveToHostAndClose(batches: Array[ColumnarBatch]): Array[ColumnarBatch] = {
    // 1) Merge all the split device buffers into one more more big ones,
    // and collect offsets and table metas
    val (devBufs, metas) = withResource(batches) { _ =>
      closeOnExcept(new Array[DeviceMemoryBuffer](batches.length)) { devBufs =>
        val metas = batches.zipWithIndex.map { case (cb, idx) =>
          cb.column(0) match {
            case packCol: GpuPackedTableColumn =>
              packCol.getTableBuffer.incRefCount()
              devBufs(idx) = packCol.getTableBuffer
              MetaUtils.buildTableMeta(0, packCol.getContiguousTable)
            case compCol: GpuCompressedColumnVector =>
              compCol.getTableBuffer.incRefCount()
              devBufs(idx) = compCol.getTableBuffer
              compCol.getTableMeta
          }
        }
        (devBufs, metas)
      }
    }
    val it =
      RmmRapidsRetryIterator.withRetry(CloseableBufferSeq(devBufs), splitBufsToMergeFn) {
        attemptBufs =>
          val bufs = attemptBufs.bufs
          val offsets = bufs.scanLeft(0L)(_ + _.getLength)
          closeOnExcept(DeviceMemoryBuffer.allocate(offsets.last)) { buf =>
            bufs.zipWithIndex.foreach { case (b, idx) =>
              buf.copyFromDeviceBufferAsync(offsets(idx), b, 0, b.getLength,
                Cuda.DEFAULT_STREAM)
            }
            Cuda.DEFAULT_STREAM.sync()
            (buf, offsets)
          }
      }
    // 2) Move the merged device buffers to host
    closeOnExcept(new Array[HostMemoryBuffer](metas.length)) { hostBufs =>
      var idx = 0
      it.foreach { case (singleDevBuf, offsets) =>
        val singleHostBuf = withResource(singleDevBuf) { _ =>
          closeOnExcept(HostMemoryBuffer.allocate(singleDevBuf.getLength)) { buf =>
            buf.copyFromDeviceBuffer(singleDevBuf)
            buf
          }
        }
        withResource(singleHostBuf) { _ =>
          if (offsets.length > 1) {
            offsets.sliding(2).foreach { case Seq(start, end) =>
              hostBufs(idx) = singleHostBuf.slice(start, end - start)
              idx += 1
            }
          }
        }
      }
      assert(idx == metas.length, s"Expect ${metas.length} buffers, but got $idx ones")
      // 3) Rebuild the split batches on host
      metas.zip(hostBufs).map { case (meta, buf) => PackedTableHostColumnVector.from(meta, buf) }
    }
  }

  private def reslice(batch: ColumnarBatch, numSlices: Int): Seq[ColumnarBatch] = {
    if (batch.numCols() > 0) {
      withResource(batch) { _ =>
        val totalRows = batch.numRows()
        val rowsPerBatch = math.ceil(totalRows.toDouble / numSlices).toInt
        val first = batch.column(0).asInstanceOf[SlicedGpuColumnVector]
        val startOffset = first.getStart
        val endOffset = first.getEnd
        val hostColumns = (0 until batch.numCols()).map { index =>
          batch.column(index).asInstanceOf[SlicedGpuColumnVector].getWrap
        }.toArray

        startOffset.until(endOffset, rowsPerBatch).map { startIndex =>
          val end = math.min(startIndex + rowsPerBatch, endOffset)
          sliceBatch(hostColumns, startIndex, end)
        }.toList
      }
    } else {
      // This should never happen, but...
      Seq(batch)
    }
  }

  def sliceInternalOnCpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[(ColumnarBatch, Int)] = {
    // We need to make sure that we have a null count calculated ahead of time.
    // This should be a temp work around.
    partitionColumns.foreach(_.getBase.getNullCount)
    val totalInputSize = GpuColumnVector.getTotalDeviceMemoryUsed(partitionColumns)
    val mightNeedToSplit = totalInputSize > GpuPartitioning.MaxCpuBatchSize

    val hostPartColumns = withResource(partitionColumns) { _ =>
      withRetryNoSplit {
        partitionColumns.safeMap(_.copyToHost())
      }
    }
    try {
      // Leaving the GPU for a while
      GpuSemaphore.releaseIfNecessary(TaskContext.get())

      val origParts = new Array[ColumnarBatch](numPartitions)
      var start = 0
      for (i <- 1 until Math.min(numPartitions, partitionIndexes.length)) {
        val idx = partitionIndexes(i)
        origParts(i - 1) = sliceBatch(hostPartColumns, start, idx)
        start = idx
      }
      origParts(numPartitions - 1) = sliceBatch(hostPartColumns, start, numRows)
      val tmp = origParts.zipWithIndex.filter(_._1 != null)
      // Spark CPU shuffle in some cases has limits on the size of the data a single
      //  row can have. It is a little complicated because the limit is on the compressed
      //  and encrypted buffer, but for now we are just going to assume it is about the same
      // size.
      if (mightNeedToSplit) {
        tmp.flatMap {
          case (batch, part) =>
            val totalSize = SlicedGpuColumnVector.getTotalHostMemoryUsed(batch)
            val numOutputBatches =
              math.ceil(totalSize.toDouble / GpuPartitioning.MaxCpuBatchSize).toInt
            if (numOutputBatches > 1) {
              // For now we are going to slice it on number of rows instead of looking
              // at each row to try and decide. If we get in trouble we can probably
              // make this recursive and keep splitting more until it is small enough.
              reslice(batch, numOutputBatches).map { subBatch =>
                (subBatch, part)
              }
            } else {
              Seq((batch, part))
            }
        }
      } else {
        tmp
      }
    } finally {
      hostPartColumns.safeClose()
    }
  }

  def sliceInternalGpuOrCpuAndClose(numRows: Int, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[(ColumnarBatch, Int)] = {
    val sliceOnGpu = usesGPUShuffle || _isSerdeOnGPU
    val nvtxRangeKey = if (sliceOnGpu) {
      "sliceInternalOnGpu"
    } else {
      "sliceInternalOnCpu"
    }
    // If we are not using the Rapids shuffle we fall back to CPU splits way to avoid the hit
    // for large number of small splits.
    withResource(new NvtxRange(nvtxRangeKey, NvtxColor.CYAN)) { _ =>
      if (sliceOnGpu) {
        val tmp = sliceInternalOnGpuAndClose(numRows, partitionIndexes, partitionColumns)
        tmp.zipWithIndex.filter(_._1 != null)
      } else {
        sliceInternalOnCpuAndClose(numRows, partitionIndexes, partitionColumns)
      }
    }
  }

  /**
   * Compress contiguous tables representing the splits into compressed columnar batches.
   * Contiguous tables corresponding to splits with no data will not be compressed.
   * @param outputBatches where to collect the corresponding columnar batches for the splits
   * @param codec compression codec to use
   * @param contiguousTables contiguous tables to compress
   */
  def compressSplits(
      outputBatches: ArrayBuffer[ColumnarBatch],
      codec: TableCompressionCodec,
      contiguousTables: Array[ContiguousTable]): Unit = {
    withResource(codec.createBatchCompressor(maxCompressionBatchSize,
        Cuda.DEFAULT_STREAM)) { compressor =>
      // tracks batches with no data and the corresponding output index for the batch
      val emptyBatches = new ArrayBuffer[(ColumnarBatch, Int)]

      // add each table either to the batch to be compressed or to the empty batch tracker
      contiguousTables.zipWithIndex.foreach { case (ct, i) =>
        // Buffer is empty when no rows or all rows are null, so check the buffer directly.
        if (ct.getBuffer == null || ct.getBuffer.getLength == 0) {
          emptyBatches.append((GpuPackedTableColumn.from(ct), i))
        } else {
          compressor.addTableToCompress(ct)
        }
      }

      withResource(compressor.finish()) { compressedTables =>
        var compressedTableIndex = 0
        var outputIndex = 0
        emptyBatches.foreach { case (emptyBatch, emptyOutputIndex) =>
          require(emptyOutputIndex >= outputIndex)
          // add any compressed batches that need to appear before the next empty batch
          val numCompressedToAdd = emptyOutputIndex - outputIndex
          (0 until numCompressedToAdd).foreach { _ =>
            val compressedTable = compressedTables(compressedTableIndex)
            outputBatches.append(GpuCompressedColumnVector.from(compressedTable))
            compressedTableIndex += 1
          }
          outputBatches.append(emptyBatch)
          outputIndex = emptyOutputIndex + 1
        }

        // add any compressed batches that remain after the last empty batch
        (compressedTableIndex until compressedTables.length).foreach { i =>
          val ct = compressedTables(i)
          outputBatches.append(GpuCompressedColumnVector.from(ct))
        }
      }
    }
  }
}
