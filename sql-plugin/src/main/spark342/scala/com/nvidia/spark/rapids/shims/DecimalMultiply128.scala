/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "342"}
{"spark": "343"}
{"spark": "350db"}
{"spark": "351"}
{"spark": "352"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnView, Table}
import com.nvidia.spark.rapids.jni.DecimalUtils._

object DecimalMultiply128{
  def apply(castLhs: ColumnView, castRhs: ColumnView, scale: Int): Table = {
    /**
     * Calling the version of multiplying 128-bit decimal numbers that casts the result only once
     * to the final precision and scale.
     * This version of multiplying 128-bit decimal numbers should only be used with Spark versions
     * greater than or equal to 3.4.2, 4.0.0, 3.5.1
     */
    multiply128(castLhs, castRhs, scale, false)
  }
}
