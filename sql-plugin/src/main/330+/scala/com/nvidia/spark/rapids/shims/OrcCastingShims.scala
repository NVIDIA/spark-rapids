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
        // From spark330, spark consider the integers as seconds.
        withResource(col.castTo(DType.INT64)) { longs =>
          // In CPU, ORC assumes the integer value is in seconds, and returns timestamp in
          // micro seconds, so we need to multiply 1e6 here.
          withResource(Scalar.fromLong(1000000L)) { value =>
            withResource(longs.mul(value)) { microSeconds =>
              microSeconds.castTo(DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }

      case DType.INT64 =>
        // In CPU code of ORC casting, its conversion is 'integer -> milliseconds -> microseconds'
        withResource(Scalar.fromLong(1000L)) { thousand =>
          withResource(col.mul(thousand)) { milliSeconds =>
            // We need to check long-overflow here. If milliseconds can not convert to
            // micorseconds, then testLongMultiplicationOverflow will throw exception.
            if (milliSeconds.max() != null) {
              testLongMultiplicationOverflow(milliSeconds.max().getLong, 1000L)
            }
            if (milliSeconds.min() != null) {
              testLongMultiplicationOverflow(milliSeconds.min().getLong, 1000L)
            }
            withResource(milliSeconds.mul(thousand)) { microSeconds =>
              microSeconds.castTo(DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }
    }
  }
}
