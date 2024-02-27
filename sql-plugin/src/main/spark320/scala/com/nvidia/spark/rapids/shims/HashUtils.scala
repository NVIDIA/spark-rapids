/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ColumnCastUtil

object HashUtils {
  /**
   * In Spark 3.2.0+ -0.0 is normalized to 0.0
   * @param in the input to normalize
   * @return the result
   */
  def normalizeInput(in: cudf.ColumnVector): cudf.ColumnVector = {
    // This looks really stupid, but -0.0 in cudf is equal to  0.0 so we can check if they are
    // equal and replace it with the same thing to normalize it.
    ColumnCastUtil.deepTransform(in) {
      case (cv, _) if cv.getType == cudf.DType.FLOAT32 =>
        withResource(cudf.Scalar.fromFloat(0.0f)) { zero =>
          withResource(cv.equalTo(zero)) { areEqual =>
            areEqual.ifElse(zero, cv)
          }
        }
      case (cv, _) if cv.getType == cudf.DType.FLOAT64 =>
        withResource(cudf.Scalar.fromDouble(0.0)) { zero =>
          withResource(cv.equalTo(zero)) { areEqual =>
            areEqual.ifElse(zero, cv)
          }
        }
    }
  }
}
