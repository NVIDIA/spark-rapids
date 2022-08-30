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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.GpuOrcScan.{testLongMultiplicationOverflow, withResource}

object OrcCastingShims {
  /**
   * Cast ColumnView of integer types to timestamp (in milliseconds).
   * @param col The column view of integer types.
   * @param fromType BOOL8, INT8/16/32/64
   * @return A new timestamp columnar vector.
   */
  def castIntegerToTimestamp(col: ColumnView, fromType: DType): ColumnView = {
    fromType match {
      case DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 =>
        // From spark311 until spark314 (not include it), spark consider the integers as
        // milli-seconds.
        // cuDF requires casting to Long first, then we can cast Long to Timestamp(in microseconds)
        // In CPU code of ORC casting, its conversion is 'integer -> milliseconds -> microseconds'
        withResource(col.castTo(DType.INT64)) { longs =>
          withResource(Scalar.fromLong(1000L)) { thousand =>
            withResource(longs.mul(thousand)) { milliSeconds =>
              milliSeconds.castTo(DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }
      case DType.INT64 =>
        // We need overflow checking here, since max value of INT64 is about 9 * 1e18, and convert
        // INT64 to milliseconds(also a INT64 actually), we need multiply 1000, it may cause long
        // integer-overflow.
        // If these two 'testLongMultiplicationOverflow' throw no exception, it means no
        // Long-overflow when casting 'col' to TIMESTAMP_MICROSECONDS.
        if (col.max() != null) {
          testLongMultiplicationOverflow(col.max().getLong, 1000L)
        }
        if (col.min() != null) {
          testLongMultiplicationOverflow(col.min().getLong, 1000L)
        }
        withResource(Scalar.fromLong(1000L)) { thousand =>
          withResource(col.mul(thousand)) { milliSeconds =>
            milliSeconds.castTo(DType.TIMESTAMP_MICROSECONDS)
          }
        }
    }
  }
}
