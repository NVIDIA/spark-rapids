/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, Cuda, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.vectorized.ColumnarBatch

case class DevicePartedBatch(
    numRows: Int,
    batchSizeInBytes: Long,
    partitionIndexes: Array[Int],
    columns: Array[GpuColumnVector])

object DevicePartedBatch {
  def create(index: Array[Int], data: Array[GpuColumnVector]): DevicePartedBatch = {
    val numRows = data.head.getRowCount.toInt
    require(data.forall(_.getRowCount == numRows),
      "All columns must have the same number of rows")
    val batchSize = GpuColumnVector.getTotalDeviceMemoryUsed(data)
    DevicePartedBatch(numRows, batchSize, index, data)
  }
}

case class HostPartedBatch(
    numRows: Int,
    batchSizeInBytes: Long,
    partitionIndexes: Array[Int],
    columns: Array[RapidsHostColumnVector])

trait GpuPartitioning extends Partitioning {

  private[this] val (
    maxCpuBatchSize, maxCompressionBatchSize, _useGPUShuffle, _useMultiThreadedShuffle) = {
    val rapidsConf = new RapidsConf(SQLConf.get)
    (rapidsConf.shuffleParitioningMaxCpuBatchSize,
      rapidsConf.shuffleCompressionMaxBatchMemory,
      GpuShuffleEnv.useGPUShuffle(rapidsConf),
      GpuShuffleEnv.useMultiThreadedShuffle(rapidsConf))
  }

  /**
   * Do partitioning logically without slicing entire column vectors into smaller ones. This
   * method should be totally executed on the GPU.
   */
  def doPartition(batch: ColumnarBatch): DevicePartedBatch = {
    throw new IllegalStateException("this method should be implemented by subclasses")
  }

  final def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    throw new IllegalStateException(
      "Partitioners do not support columnarEval, only columnarEvalAny")
  }

  def usesGPUShuffle: Boolean = _useGPUShuffle

  def usesMultiThreadedShuffle: Boolean = _useMultiThreadedShuffle

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
    // The first index will always be 0, so we need to skip it.
    val batches = if (numRows > 0) {
      val parts = partitionIndexes.slice(1, partitionIndexes.length)
      closeOnExcept(new ArrayBuffer[ColumnarBatch](numPartitions)) { splits =>
        val contiguousTables = withResource(partitionColumns) { _ =>
          withResource(new Table(partitionColumns.map(_.getBase).toArray: _*)) { table =>
            table.contiguousSplit(parts: _*)
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
    } else {
      Array[ColumnarBatch]()
    }

    GpuSemaphore.releaseIfNecessary(TaskContext.get())
    batches
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

  def sliceInternalGpuOrCpuAndClose(batch: DevicePartedBatch): GpuPartitioning.Result = {
    val sliceOnGpu = usesGPUShuffle
    val nvtxRangeKey = if (sliceOnGpu) {
      "sliceInternalOnGpu"
    } else {
      "sliceInternalOnCpu"
    }
    // If we are not using the Rapids shuffle we fall back to CPU splits way to avoid the hit
    // for large number of small splits.
    withResource(new NvtxRange(nvtxRangeKey, NvtxColor.CYAN)) { _ =>
      if (sliceOnGpu) {
        val tmp = sliceInternalOnGpuAndClose(
          batch.numRows, batch.partitionIndexes, batch.columns)
        tmp.zipWithIndex.filter(_._1 != null)
      } else {
        val hostPartedBatch = copyToHost(batch)
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        sliceOnHost(hostPartedBatch)
      }
    }
  }

  def copyToHost(batch: DevicePartedBatch): HostPartedBatch = {
    // We have to wrap the NvtxWithMetrics over both copyToHostAsync and corresponding CudaSync,
    // because the copyToHostAsync calls above are not guaranteed to be asynchronous (e.g.: when
    // the copy is from pageable memory, and we're not guaranteed to be using pinned memory).
    withResource(
      new NvtxWithMetrics("PartitionD2H", NvtxColor.CYAN, memCopyTime)) { _ =>
      val hostColumns = withResource(batch.columns) { _ =>
        withRetryNoSplit {
          batch.columns.safeMap(_.copyToHostAsync(Cuda.DEFAULT_STREAM))
        }
      }
      closeOnExcept(hostColumns) { _ =>
        Cuda.DEFAULT_STREAM.sync()
      }
      HostPartedBatch(batch.numRows,
        batch.batchSizeInBytes, batch.partitionIndexes, hostColumns)
    }
  }

  def sliceOnHost(partedBatch: HostPartedBatch): GpuPartitioning.Result = {
    val columns = partedBatch.columns
    val partIndices = partedBatch.partitionIndexes
    val mightNeedToSplit = partedBatch.batchSizeInBytes > maxCpuBatchSize
    withResource(columns) { _ =>
      val origParts = new Array[ColumnarBatch](numPartitions)
      var start = 0
      for (i <- 1 until Math.min(numPartitions, partIndices.length)) {
        val idx = partIndices(i)
        origParts(i - 1) = sliceBatch(columns, start, idx)
        start = idx
      }
      origParts(numPartitions - 1) = sliceBatch(columns, start, partedBatch.numRows)
      val tmp = origParts.zipWithIndex.filter(_._1 != null)
      // Spark CPU shuffle in some cases has limits on the size of the colWithIndex a single
      // row can have. It is a little complicated because the limit is on the compressed
      // and encrypted buffer, but for now we are just going to assume it is about the same
      // size.
      if (mightNeedToSplit) {
        tmp.flatMap {
          case (batch, part) =>
            val totalSize = SlicedGpuColumnVector.getTotalHostMemoryUsed(batch)
            val numOutputBatches =
              math.ceil(totalSize.toDouble / maxCpuBatchSize).toInt
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
        if (ct.getRowCount == 0) {
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

  private var memCopyTime: GpuMetric = NoopMetric

  /**
   * Setup sub-metrics for the performance debugging of GpuPartition. This method is expected to
   * be called at the query planning stage. Therefore, this method is NOT thread safe.
   */
  def setupDebugMetrics(metrics: Map[String, GpuMetric]): Unit = {
    metrics.get(GpuMetric.COPY_TO_HOST_TIME).foreach(memCopyTime = _)
  }
}

object GpuPartitioning {
  // The protocol for communicating with ShuffleWriter.
  type Result = Array[(ColumnarBatch, Int)]
}
