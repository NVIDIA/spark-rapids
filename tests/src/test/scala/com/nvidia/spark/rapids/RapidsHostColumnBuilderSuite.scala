/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.DType
import ai.rapids.cudf.HostColumnVector.BasicType
import org.scalatest.funsuite.AnyFunSuite

class RapidsHostColumnBuilderSuite extends AnyFunSuite {
  test("growing buffer preserves correctness") {
    val b1 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 0) // grows
    val b2 = new RapidsHostColumnBuilder(new BasicType(false, DType.INT32), 8) // does not grow
    for (i <- 0 to 7) {
      b1.append(i)
      b2.append(i)
    }
    val v1 = b1.build()
    val v2 = b2.build()
    for (i <- 0 to 7) {
      assertResult(v1.getInt(i))(v2.getInt(i))
    }
    v1.close()
    v2.close()
    b1.close()
    b2.close()
  }
}
