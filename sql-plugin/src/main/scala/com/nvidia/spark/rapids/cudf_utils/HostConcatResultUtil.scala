/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.cudf_utils

import ai.rapids.cudf
import com.nvidia.spark.rapids.{Arm,  GpuColumnVectorFromBuffer}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object HostConcatResultUtil extends Arm {
  /**
   * Create a rows-only `HostConcatResult`.
   */
  def rowsOnlyHostConcatResult(numRows: Int): cudf.JCudfSerialization.HostConcatResult = {
    new cudf.JCudfSerialization.HostConcatResult(
      new cudf.JCudfSerialization.SerializedTableHeader(numRows),
      cudf.HostMemoryBuffer.allocate(0, false))
  }

  /**
   * Given a `HostConcatResult` and a SparkSchema produce a `ColumnarBatch`,
   * handling the rows-only case.
   *
   * @note This function does not consume the `HostConcatResult`, and
   *       callers are responsible for closing the resulting `ColumnarBatch`
   */
  def getColumnarBatch(
      hostConcatResult: cudf.JCudfSerialization.HostConcatResult,
      sparkSchema: Array[DataType]): ColumnarBatch = {
    if (hostConcatResult.getTableHeader.getNumColumns == 0) {
      // We expect the caller to have acquired the GPU unconditionally before calling
      // `getColumnarBatch`, as a downstream exec may need the GPU, and the assumption is
      // that it is acquired in the coalesce code.
      new ColumnarBatch(Array.empty, hostConcatResult.getTableHeader.getNumRows)
    } else {
      withResource(hostConcatResult.toContiguousTable) { ct =>
        GpuColumnVectorFromBuffer.from(ct, sparkSchema)
      }
    }
  }
}
