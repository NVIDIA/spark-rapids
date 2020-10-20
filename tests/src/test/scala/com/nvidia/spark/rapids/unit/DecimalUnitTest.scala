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

import com.nvidia.spark.rapids._
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, CheckOverflow, Expression, Literal, Multiply, Pmod, PromotePrecision, Subtract}
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType}


class DecimalUnitTest extends FunSuite with Arm {
  test("GpuDecimalExpressionMeta") {
    val rapidsConf = new RapidsConf(Map[String, String]())
    val testWrapper = (input: Expression, expected: Expression) => {
      val output = GpuOverrides.wrapExpr(input, rapidsConf, None).convertToGpu()
      expected.semanticEquals(output) shouldBe true
    }
    val sparkConf = new SparkConf()
      .set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    TestUtils.withGpuSparkSession(sparkConf) { _ =>
      testWrapper(
        PromotePrecision(Cast(Literal(123.45), DecimalType(10, 3))),
        GpuPromotePrecision(GpuCast(GpuLiteral(123.45, DoubleType), DecimalType(10, 3))))
      testWrapper(
        PromotePrecision(Cast(Literal(1000.123456789), DecimalType(22, 10))),
        GpuPromotePrecision(GpuCast(GpuLiteral(1000.123456789, DoubleType), DecimalType(19, 7))))
      testWrapper(
        PromotePrecision(Cast(Literal(123456789.123), DecimalType(24, 3))),
        GpuPromotePrecision(GpuCast(GpuLiteral(123456789.123, DoubleType), DecimalType(19, -2))))

      var cpuPP = PromotePrecision(Cast(Literal(1), DecimalType(20, 3)))
      var gpuPP = GpuPromotePrecision(GpuCast(GpuLiteral(1, IntegerType), DecimalType(19, 2)))
      testWrapper(
        CheckOverflow(Add(cpuPP, cpuPP), DecimalType(20, 3), false),
        GpuCheckOverflow(GpuAdd(gpuPP, gpuPP)))
      testWrapper(
        CheckOverflow(Subtract(cpuPP, cpuPP), DecimalType(20, 3), false),
        GpuCheckOverflow(GpuSubtract(gpuPP, gpuPP)))
      testWrapper(
        CheckOverflow(Pmod(cpuPP, cpuPP), DecimalType(20, 3), false),
        GpuCheckOverflow(GpuPmod(gpuPP, gpuPP)))
      cpuPP = PromotePrecision(Cast(Literal(1), DecimalType(10, 3)))
      gpuPP = GpuPromotePrecision(GpuCast(GpuLiteral(1, IntegerType), DecimalType(10, 3)))
      assertThrows[IllegalStateException] {
        testWrapper(
          CheckOverflow(Multiply(cpuPP, cpuPP), DecimalType(20, 6), false),
          GpuCheckOverflow(GpuMultiply(gpuPP, gpuPP)))
      }
    }
  }
}
