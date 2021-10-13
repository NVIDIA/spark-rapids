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

import scala.collection.mutable.ArrayBuffer

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
            // collect sampled row idx
            // samples idx in batch one by one, so it's same with CPU version
            val sampledRows = sample(cb.numRows())

            val intBytes = DType.INT32.getSizeInBytes()
            val totalBytes = sampledRows.length * intBytes
            withResource(HostMemoryBuffer.allocate(totalBytes)) { hostBuffer =>
              // copy row idx to host buffer
              for (idx <- 0 until sampledRows.length) {
                hostBuffer.setInt(idx * intBytes, sampledRows(idx))
              }

              // generate gather map and send to GPU to gather
              withResource(DeviceMemoryBuffer.allocate(totalBytes)) { deviceBuffer =>
                deviceBuffer.copyFromHostBuffer(0, hostBuffer, 0, totalBytes)
                withResource(new GatherMap(deviceBuffer).toColumnView(0, sampledRows.length)) {
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

  // collect the sampled row idx
  private def sample(numRows: Int): ArrayBuffer[Int] = {
    val buf = new ArrayBuffer[Int]
    for (rowIdx <- 0 until numRows) {
      val rowCount = rng.sample()
      if (rowCount > 0) {
        numOutputRows += rowCount
        for (_ <- 0 until rowCount) {
          buf += rowIdx
        }
      }
    }
    buf
  }
}
