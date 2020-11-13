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

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.{GpuAlias, GpuColumnVector, GpuIsNotNull, GpuIsNull, GpuLiteral, GpuOverrides, GpuScalar, GpuUnaryExpression, GpuUnitTests, HostColumnarToGpu, RapidsConf, RapidsHostColumnVector, TestUtils}
import org.scalatest.Matchers

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal}
import org.apache.spark.sql.types.{Decimal, DecimalType}

class DecimalUnitTest extends GpuUnitTests with Matchers {
  Random.setSeed(1234L)

  private val dec32Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextInt() / 1000, 3 + Random.nextInt(3))))
  private val dec64Data = Array.fill[Decimal](10)(
    Decimal.fromDecimal(BigDecimal(Random.nextLong() / 1000, 7 + Random.nextInt(3))))

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

  test("test decimal as column vector") {
    val dt32 = DecimalType(DType.DECIMAL32_MAX_PRECISION, 5)
    val dt64 = DecimalType(DType.DECIMAL64_MAX_PRECISION, 9)
    withResource(
      GpuColumnVector.from(ColumnVector.fromDecimals(dec32Data.map(_.toJavaBigDecimal): _*),
        dt32)) { cv: GpuColumnVector =>
      cv.getRowCount shouldEqual dec32Data.length
      val (precision, scale) = cv.dataType() match {
        case dt: DecimalType => (dt.precision, dt.scale)
      }
      withResource(cv.copyToHost()) { hostCv: RapidsHostColumnVector =>
        dec32Data.zipWithIndex.foreach { case (dec, i) =>
          val rescaled = dec.toJavaBigDecimal.setScale(scale)
          hostCv.getInt(i) shouldEqual rescaled.unscaledValue().intValueExact()
          hostCv.getDecimal(i, precision, scale) shouldEqual Decimal(rescaled)
        }
      }
    }
    val dec64WithNull = Array(null) ++ dec64Data.map(_.toJavaBigDecimal) ++ Array(null, null)
    withResource(GpuColumnVector.from(ColumnVector.fromDecimals(dec64WithNull: _*), dt64)) { cv =>
      cv.getRowCount shouldEqual dec64WithNull.length
      cv.hasNull shouldBe true
      cv.numNulls() shouldEqual 3
      val (precision, scale) = cv.dataType() match {
        case dt: DecimalType => (dt.precision, dt.scale)
      }
      withResource(cv.copyToHost()) { hostCv: RapidsHostColumnVector =>
        dec64WithNull.zipWithIndex.foreach {
          case (dec, i) if dec == null =>
            hostCv.getBase.isNull(i) shouldBe true
          case (dec, i) =>
            val rescaled = dec.setScale(scale)
            hostCv.getLong(i) shouldEqual rescaled.unscaledValue().longValueExact()
            hostCv.getDecimal(i, precision, scale) shouldEqual Decimal(rescaled)
        }
      }
    }
    // assertion error throws while running `typeConversionAllowed` check
    assertThrows[AssertionError] {
      withResource(GpuColumnVector.from(ColumnVector.decimalFromLongs(0, 1L),
        DecimalType(DType.DECIMAL64_MAX_PRECISION + 1, 0))) { _ => }
    }
    assertThrows[AssertionError] {
      withResource(GpuColumnVector.from(ColumnVector.decimalFromInts(0, 1),
        DecimalType(DType.DECIMAL32_MAX_PRECISION + 1, 0))) { _ => }
    }
    // FIXME: Enable below test after creating decimal vectors from scalar supported by cuDF.
    /*
    withResource(GpuScalar.from(dec64Data(0), dt64)) { scalar =>
      withResource(GpuColumnVector.from(scalar, 10, dt64)) { cv =>
        cv.getRowCount shouldEqual 10
        withResource(cv.copyToHost()) { hcv =>
          (0 until 10).foreach { i =>
            hcv.getLong(i) shouldEqual scalar.getLong
            hcv.getDecimal(i, dt64.precision, dt64.scale).toJavaBigDecimal shouldEqual
              scalar.getBigDecimal
          }
        }
      }
    }
    */
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

  test("test gpu null check operators with decimal data") {
    val decArray = Array(BigDecimal(0).bigDecimal, null, BigDecimal(1).bigDecimal)
    withResource(GpuColumnVector.from(ColumnVector.fromDecimals(decArray: _*), DecimalType(1, 0))
    ) { cv =>
      withResource(GpuIsNull(null).doColumnar(cv).copyToHost()) { ret =>
        ret.getBoolean(0) shouldBe false
        ret.getBoolean(1) shouldBe true
        ret.getBoolean(2) shouldBe false
      }
      withResource(GpuIsNotNull(null).doColumnar(cv).copyToHost()) { ret =>
        ret.getBoolean(0) shouldBe true
        ret.getBoolean(1) shouldBe false
        ret.getBoolean(2) shouldBe true
      }
    }
  }

  test("test HostColumnarToGpu.columnarCopy") {
    withResource(
      GpuColumnVector.from(ColumnVector.fromDecimals(dec64Data.map(_.toJavaBigDecimal): _*),
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 9))) { cv =>
      val dt = new HostColumnVector.BasicType(false, GpuColumnVector.getRapidsType(cv.dataType()))
      val builder = new HostColumnVector.ColumnBuilder(dt, cv.getRowCount)
      withResource(cv.copyToHost()) { hostCV =>
        HostColumnarToGpu.columnarCopy(hostCV, builder, false, cv.getRowCount.toInt)
        val actual = builder.build()
        val expected = hostCV.getBase
        actual.getDataType shouldEqual expected.getDataType
        actual.getRowCount shouldEqual expected.getRowCount
        (0 until actual.getRowCount.toInt).foreach { i =>
          actual.getBigDecimal(i) shouldEqual expected.getBigDecimal(i)
        }
      }
    }
    val dec32WithNull = dec32Data.splitAt(5) match {
      case (left, right) =>
        Array(null) ++ left.map(_.toJavaBigDecimal) ++ Array(null) ++
          right.map(_.toJavaBigDecimal) ++ Array(null)
    }
    withResource(
      GpuColumnVector.from(ColumnVector.fromDecimals(dec32WithNull: _*),
        DecimalType(DType.DECIMAL32_MAX_PRECISION, 5))) { cv =>
      val dt = new HostColumnVector.BasicType(true, GpuColumnVector.getRapidsType(cv.dataType()))
      val builder = new HostColumnVector.ColumnBuilder(dt, cv.getRowCount)
      withResource(cv.copyToHost()) { hostCV =>
        HostColumnarToGpu.columnarCopy(hostCV, builder, true, cv.getRowCount.toInt)
        val actual = builder.build()
        val expected = hostCV.getBase
        actual.getDataType shouldEqual expected.getDataType
        actual.getRowCount shouldEqual expected.getRowCount
        (0 until actual.getRowCount.toInt).foreach { i =>
          actual.isNull(i) shouldEqual expected.isNull(i)
          if (!actual.isNull(i)) {
            actual.getBigDecimal(i) shouldEqual expected.getBigDecimal(i)
          }
        }
      }
    }
  }
}
