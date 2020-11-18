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

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType

import org.apache.spark.sql.rapids.{GpuLessThan}
import org.apache.spark.sql.types.{DataTypes, Decimal, DecimalType}

class DecimalBinaryOpSuite extends GpuExpressionTestSuite {
  private val schema = FuzzerUtils.createSchema(Seq(
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 4),
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 2)))
  private val litValue = Decimal(12345.6789)
  private val lit = GpuLiteral(litValue, DecimalType(DType.DECIMAL64_MAX_PRECISION, 5))
  private val leftExpr = GpuBoundReference(0, schema.head.dataType, nullable = true)
  private val rightExpr = GpuBoundReference(1, schema(1).dataType, nullable = true)

  test("GpuLessThan") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l < r)
    checkEvaluateGpuBinaryExpression(GpuLessThan(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)

    val expectedFunVS = (x: Decimal) => Option(x < litValue)
    checkEvaluateGpuUnaryExpression(GpuLessThan(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue < x)
    checkEvaluateGpuUnaryExpression(GpuLessThan(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }
}