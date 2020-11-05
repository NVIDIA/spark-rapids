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

import scala.util.Random
import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{GpuAlias, GpuLiteral, GpuOverrides, GpuScalar, GpuUnitTests, RapidsConf, TestUtils}
import org.scalatest.Matchers
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.types.{Decimal, DecimalType}

class DecimalUnitTest extends GpuUnitTests with Matchers {
  Random.setSeed(1234L)

  private val dec32Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextInt() / 10, Random.nextInt(5))))
  private val dec64Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextLong() / 1000, Random.nextInt(10))))

  test("test decimal as scalar") {
    Array(dec32Data, dec64Data).flatten.foreach { dec =>
      // test GpuScalar.from(v: Any)
      withResource(GpuScalar.from(dec)) { s =>
        s.getType.getScale shouldEqual -dec.scale
        GpuScalar.extract(s).asInstanceOf[Decimal] shouldEqual dec
      }
      // test GpuScalar.from(v: Any, t: DataType)
      val dt = DecimalType(DType.DECIMAL64_MAX_PRECISION, dec.scale)
      withResource(GpuScalar.from(dec.toDouble, dt)) { s =>
        s.getType.getScale shouldEqual -dt.scale
        GpuScalar.extract(s).asInstanceOf[Decimal].toDouble shouldEqual dec.toDouble
      }
      withResource(GpuScalar.from(dec.toString(), dt)) { s =>
        s.getType.getScale shouldEqual -dt.scale
        GpuScalar.extract(s).asInstanceOf[Decimal].toString shouldEqual dec.toString()
      }
      val long = dec.toLong
      withResource(GpuScalar.from(long, DecimalType(dec.precision, 0))) { s =>
        s.getType.getScale shouldEqual 0
        GpuScalar.extract(s).asInstanceOf[Decimal].toLong shouldEqual long
      }
    }
    // test exception throwing
    assertThrows[IllegalStateException] {
      withResource(GpuScalar.from(true, DecimalType(10, 1))) { _ => }
    }
    assertThrows[IllegalArgumentException] {
      val bigDec = Decimal(BigDecimal(Long.MaxValue / 100, 0))
      withResource(GpuScalar.from(bigDec, DecimalType(15, 1))) { _ => }
    }
  }

  test("test basic expressions with decimal data") {
    val rapidsConf = new RapidsConf(Map[String, String]())

    val cpuLit = Literal(dec32Data(0), DecimalType(dec32Data(0).precision, dec32Data(0).scale))
    val wrapperLit = GpuOverrides.wrapExpr(cpuLit, rapidsConf, None)
    wrapperLit.tagForGpu()
    wrapperLit.canExprTreeBeReplaced shouldBe true
    val gpuLit = wrapperLit.convertToGpu().asInstanceOf[GpuLiteral]
    gpuLit.columnarEval(null) shouldEqual cpuLit.eval(null)
    gpuLit.sql shouldEqual cpuLit.sql

    val cpuAlias = Alias(cpuLit, "A")()
    val wrapperAlias = GpuOverrides.wrapExpr(cpuAlias, rapidsConf, None)
    wrapperAlias.tagForGpu()
    wrapperAlias.canExprTreeBeReplaced shouldBe true
    val gpuAlias = wrapperAlias.convertToGpu().asInstanceOf[GpuAlias]
    gpuAlias.dataType shouldEqual cpuAlias.dataType
    gpuAlias.sql shouldEqual cpuAlias.sql
    gpuAlias.columnarEval(null) shouldEqual cpuAlias.eval(null)

    val cpuAttrRef = AttributeReference("test123", cpuLit.dataType)()
    val wrapperAttrRef = GpuOverrides.wrapExpr(cpuAttrRef, rapidsConf, None)
    wrapperAttrRef.tagForGpu()
    wrapperAttrRef.canExprTreeBeReplaced shouldBe true
    val gpuAttrRef = wrapperAttrRef.convertToGpu().asInstanceOf[AttributeReference]
    gpuAttrRef.sql shouldEqual cpuAttrRef.sql
    gpuAttrRef.sameRef(cpuAttrRef) shouldBe true

    // inconvertible because of precision overflow
    val wrp = GpuOverrides.wrapExpr(Literal(Decimal(12345L), DecimalType(38, 10)), rapidsConf, None)
    wrp.tagForGpu()
    wrp.canExprTreeBeReplaced shouldBe false
  }
}
