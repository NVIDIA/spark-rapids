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
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Expression, Literal, PromotePrecision}
import org.apache.spark.sql.rapids.GpuAdd
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType}


class DecimalUnitTest extends FunSuite with Arm {
  test("PromotePrecision elimination test") {
    val rapidsConf = new RapidsConf(Map[String, String]())
    val testWrapper = (input: Expression, expected: Expression) => {
      val output = GpuOverrides.wrapExpr(input, rapidsConf, None).convertToGpu()
      expected.semanticEquals(output) shouldBe true
    }
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val exp1 = PromotePrecision(Cast(Literal(12.345f), DoubleType))
      testWrapper(exp1, GpuLiteral(12.345f, FloatType))
      val exp2 = Add(exp1, Cast(Literal(123), DoubleType))
      testWrapper(exp2,
        GpuAdd(GpuLiteral(12.345f, FloatType), GpuCast(GpuLiteral(123, IntegerType), DoubleType)))
    }
  }
}
