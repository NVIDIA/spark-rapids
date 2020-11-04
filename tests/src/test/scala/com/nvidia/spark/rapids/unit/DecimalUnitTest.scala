/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.math.{BigDecimal => BigDec}

import scala.util.Random

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{GpuScalar, GpuUnitTests}
import org.scalatest.Matchers

import org.apache.spark.sql.types.DecimalType

class DecimalUnitTest extends GpuUnitTests with Matchers {
  Random.setSeed(1234L)

  private val dec32Data = Array.fill[BigDecimal](10)(
    BigDecimal(Random.nextInt() / 10, Random.nextInt(5)))
  private val dec64Data = Array.fill[BigDecimal](10)(
    BigDecimal(Random.nextLong() / 1000, Random.nextInt(10)))

  test("test decimal as scalar") {
    Array(dec32Data, dec64Data).flatten.foreach { dec =>
      // test GpuScalar.from(v: Any)
      withResource(GpuScalar.from(dec)) { s =>
        s.getType.getScale shouldEqual -dec.scale
        GpuScalar.extract(s).asInstanceOf[BigDec] shouldEqual dec.bigDecimal
      }
      // test GpuScalar.from(v: Any, t: DataType)
      val dt = DecimalType(DType.DECIMAL64_MAX_PRECISION, dec.scale)
      val dbl = dec.doubleValue()
      withResource(GpuScalar.from(dbl, dt)) { s =>
        s.getType.getScale shouldEqual -dt.scale
        GpuScalar.extract(s).asInstanceOf[BigDec].doubleValue() shouldEqual dbl
      }
      val str = dec.toString()
      withResource(GpuScalar.from(str, dt)) { s =>
        s.getType.getScale shouldEqual -dt.scale
        GpuScalar.extract(s).asInstanceOf[BigDec].toString shouldEqual str
      }
      val long = dec.longValue()
      withResource(GpuScalar.from(long, DecimalType(DType.DECIMAL64_MAX_PRECISION, 0))) { s =>
        s.getType.getScale shouldEqual 0
        GpuScalar.extract(s).asInstanceOf[BigDec].longValue() shouldEqual long
      }
    }
    // test exception throwing
    assertThrows[IllegalStateException] {
      withResource(GpuScalar.from(true, DecimalType(10, 1))) { _ => }
    }
    assertThrows[IllegalArgumentException] {
      val bigDec = BigDecimal(Long.MaxValue / 100, 0)
      withResource(GpuScalar.from(bigDec, DecimalType(15, 1))) { _ => }
    }
  }
}
