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

package com.nvidia.spark.rapids.unit

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids._

import org.apache.spark.sql.types.IntegerType

class GpuScalarUnitTest extends GpuUnitTests {

  test("Test internal scalar is null after closed") {
    val gsv = GpuScalar(1, IntegerType)
    gsv.close()
    assert(gsv.getBase == null)

    val gsc = GpuScalar(Scalar.fromInt(1), IntegerType)
    gsc.close()
    assert(gsc.getBase == null)
  }

  test("Test closed too many times") {
    val gsv = GpuScalar(1, IntegerType)
    gsv.close()
    assertThrows[IllegalStateException](gsv.close())

    val gsc = GpuScalar(Scalar.fromInt(1), IntegerType)
    gsc.close()
    assertThrows[IllegalStateException](gsc.close())
  }

  test("Test null is invalid") {
    withResource(GpuScalar(null, IntegerType)) { gs =>
      assert(!gs.isValid)
      assert(!gs.getBase.isValid)
    }
  }
}
