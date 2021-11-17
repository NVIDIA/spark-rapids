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
package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DeviceMemoryBuffer, DType, GatherMap, HostMemoryBuffer}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GatherUtils extends Arm {
  def gather(cb: ColumnarBatch, rows: ArrayBuffer[Int]): ColumnarBatch = {
    val colTypes = GpuColumnVector.extractTypes(cb)
    if (rows.length == 0) {
      GpuColumnVector.emptyBatchFromTypes(colTypes)

    } else if(cb.numCols() == 0) {
      // for count agg, num of cols is 0
      val c = GpuColumnVector.emptyBatchFromTypes(Array.empty[DataType])
      c.setNumRows(rows.length)
      c
    } else {
      val intBytes = DType.INT32.getSizeInBytes()
      val totalBytes = rows.length * intBytes
      withResource(HostMemoryBuffer.allocate(totalBytes)) { hostBuffer =>
        // copy row indexes to host buffer
        var idx = 0
        while (idx < rows.length) {
          hostBuffer.setInt(idx * intBytes, rows(idx))
          idx += 1
        }
        // generate gather map and send to GPU to gather
        withResource(DeviceMemoryBuffer.allocate(totalBytes)) { deviceBuf =>
          deviceBuf.copyFromHostBuffer(0, hostBuffer, 0, totalBytes)
          // generate gather map
          withResource(new GatherMap(deviceBuf).toColumnView(0, rows.length)) {
            gatherCv =>
              withResource(GpuColumnVector.from(cb)) { table =>
                // GPU gather
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
