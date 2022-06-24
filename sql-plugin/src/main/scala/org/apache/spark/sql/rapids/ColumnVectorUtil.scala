/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{Arm, GpuScalar}

object ColumnVectorUtil extends Arm {
  private[rapids] def getFirstFalseKey(indices: ColumnVector, keyExists: ColumnVector): String = {
    withResource(new ai.rapids.cudf.Table(Array(indices, keyExists):_*)) { table =>
      withResource(keyExists.not()) { keyNotExist =>
        withResource(table.filter(keyNotExist)) { tableWithBadKeys =>
          val badKeys = tableWithBadKeys.getColumn(0)
          withResource(badKeys.getScalarElement(0)) { firstBadKey =>
            val key = GpuScalar.extract(firstBadKey)
            if (key != null) {
              key.toString
            } else {
              "null"
            }
          }
        }
      }
    }
  }
}
