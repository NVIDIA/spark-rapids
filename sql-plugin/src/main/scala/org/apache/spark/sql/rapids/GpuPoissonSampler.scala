/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids

import ai.rapids.cudf.{DeviceMemoryBuffer, DType, GatherMap, HostMemoryBuffer, NvtxColor}
import com.nvidia.spark.rapids.{Arm, GpuColumnVector, GpuMetric, NvtxWithMetrics}
import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.random.PoissonSampler

class GpuPoissonSampler(fraction: Double, useGapSamplingIfPossible: Boolean,
                        numOutputRows: GpuMetric, numOutputBatches: GpuMetric, opTime: GpuMetric)
  extends PoissonSampler[ColumnarBatch](fraction, useGapSamplingIfPossible) with Arm {

  private val rng = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
  }

  override def clone: PoissonSampler[ColumnarBatch] =
    new GpuPoissonSampler(fraction, useGapSamplingIfPossible,
      numOutputRows, numOutputBatches, opTime)

  override def sample(batchIterator: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    if (fraction <= 0.0) {
      Iterator.empty
    } else {
      batchIterator.map { columnarBatch =>
        withResource(new NvtxWithMetrics("Sample Exec", NvtxColor.YELLOW, opTime)) { _ =>
          numOutputBatches += 1
          withResource(columnarBatch) { cb =>
            val rows = cb.numRows()
            val intBytes = DType.INT32.getSizeInBytes()

            // 1. select rows, same with CPU version
            withResource(generateHostBuffer(cb.numRows())) { hostBufferWithRowNum =>
              val hostBuffer = hostBufferWithRowNum.buffer
              val selectedRows = hostBufferWithRowNum.rowNum
              // 2. generate gather map and send to GPU to gather
              withResource(DeviceMemoryBuffer.allocate(selectedRows * intBytes)) { deviceBuffer =>
                deviceBuffer.copyFromHostBuffer(0, hostBuffer, 0, selectedRows * intBytes)
                withResource(new GatherMap(deviceBuffer).toColumnView(0, selectedRows)) {
                  gatherCv =>
                    val colTypes = GpuColumnVector.extractTypes(cb)
                    withResource(GpuColumnVector.from(cb)) { table =>
                      withResource(table.gather(gatherCv)) { gatheredTable =>
                        GpuColumnVector.from(gatheredTable, colTypes)
                      }
                    }
                }
              }
            }
          }
        }
      }
    }
  }

  private case class HostBufferWithRowNum(buffer: HostMemoryBuffer, rowNum: Int)
    extends AutoCloseable {
    @throws[Exception]
    def close(): Unit = {
      buffer.close()
    }
  }

  private def generateHostBuffer(rows: Int): HostBufferWithRowNum = {
    val intBytes = DType.INT32.getSizeInBytes()
    val estimateBytes = (rows * intBytes * fraction).toLong + 128L
    var buffer = HostMemoryBuffer.allocate(estimateBytes)
    var selectedRows = 0
    for (row <- 0 until rows) {
      val rowCount = rng.sample()
      if (rowCount > 0) {
        numOutputRows += rowCount
        for (_ <- 0 until rowCount) {
          // select row with rowCount times
          buffer = safeSetInt(buffer, selectedRows * intBytes, row)
          selectedRows += 1
        }
      }
    }
    HostBufferWithRowNum(buffer, selectedRows)
  }

  // set int, expand if necessary
  private def safeSetInt(buffer: HostMemoryBuffer, offset: Int, value: Int): HostMemoryBuffer = {
    val buf = ensureCapacity(buffer, offset)
    buf.setInt(offset, value)
    buf
  }

  // expand if buffer is full
  private def ensureCapacity(buffer: HostMemoryBuffer, offset: Int): HostMemoryBuffer = {
    if (offset + DType.INT32.getSizeInBytes <= buffer.getLength) {
      buffer
    } else {
      withResource(buffer) { buf =>
        val newBuffer = HostMemoryBuffer.allocate(buf.getLength * 2)
        newBuffer.copyFromHostBuffer(0, buf, 0, buf.getLength)
        newBuffer
      }
    }
  }
}
