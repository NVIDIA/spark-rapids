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

import org.apache.spark.sql.rapids.{GpuAdd, GpuEqualTo, GpuGreaterThan, GpuGreaterThanOrEqual, GpuLessThan, GpuLessThanOrEqual, GpuMultiply, GpuSubtract}
import org.apache.spark.sql.types.{DataType, DataTypes, Decimal, DecimalType, StructType}

/*
  unsupported operators are as below:
  - GpuEqualNullSafe
  - GpuDivModLike (GpuDivide/GpuIntegralDivide/GpuPmod/GpuRemainder)
*/
class DecimalBinaryOpSuite extends GpuExpressionTestSuite {
  private val schema = FuzzerUtils.createSchema(Seq(
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 4),
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 2)))
  private val litValue = Decimal(12345.6789)
  private val lit = GpuLiteral(litValue, DecimalType(DType.DECIMAL64_MAX_PRECISION, 5))
  private val leftExpr = GpuBoundReference(0, schema.head.dataType, nullable = true)
  private val rightExpr = GpuBoundReference(1, schema(1).dataType, nullable = true)

  private val schemaSame = FuzzerUtils.createSchema(Seq(
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 3),
    DecimalType(DType.DECIMAL32_MAX_PRECISION, 3)))
  private val leftExprSame = GpuBoundReference(0, schemaSame.head.dataType, nullable = true)
  private val rightExprSame = GpuBoundReference(1, schemaSame(1).dataType, nullable = true)

  test("GpuEqualTo") {
    val expectedFun = (l: Decimal, r: Decimal) => Option(l == r)
    checkEvaluateGpuBinaryExpression(GpuEqualTo(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFun, schema)
    checkEvaluateGpuBinaryExpression(GpuEqualTo(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFun, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x == litValue)
    checkEvaluateGpuUnaryExpression(GpuEqualTo(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue == x)
    checkEvaluateGpuUnaryExpression(GpuEqualTo(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuGreaterThan") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l > r)
    checkEvaluateGpuBinaryExpression(GpuGreaterThan(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)

    val expectedFunVS = (x: Decimal) => Option(x > litValue)
    checkEvaluateGpuUnaryExpression(GpuGreaterThan(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue > x)
    checkEvaluateGpuUnaryExpression(GpuGreaterThan(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuGreaterThanOrEqual") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l >= r)
    checkEvaluateGpuBinaryExpression(GpuGreaterThanOrEqual(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)
    checkEvaluateGpuBinaryExpression(GpuGreaterThanOrEqual(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x >= litValue)
    checkEvaluateGpuUnaryExpression(GpuGreaterThanOrEqual(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue >= x)
    checkEvaluateGpuUnaryExpression(GpuGreaterThanOrEqual(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

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

  test("GpuLessThanOrEqual") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l <= r)
    checkEvaluateGpuBinaryExpression(GpuLessThanOrEqual(leftExpr, rightExpr),
      schema.head.dataType, schema(1).dataType, DataTypes.BooleanType,
      expectedFunVV, schema)
    checkEvaluateGpuBinaryExpression(GpuLessThanOrEqual(leftExprSame, rightExprSame),
      schemaSame.head.dataType, schemaSame.head.dataType, DataTypes.BooleanType,
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x <= litValue)
    checkEvaluateGpuUnaryExpression(GpuLessThanOrEqual(leftExpr, lit),
      schema.head.dataType, DataTypes.BooleanType, expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue <= x)
    checkEvaluateGpuUnaryExpression(GpuLessThanOrEqual(lit, leftExpr),
      schema.head.dataType, DataTypes.BooleanType, expectedFunSV, schema)
  }

  test("GpuAdd") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l + r)
    var outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale max
      schema(1).dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuMathBinaryExpression(GpuAdd(leftExpr, rightExpr),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schema)
    outputScale = schemaSame.head.dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuMathBinaryExpression(GpuAdd(leftExprSame, rightExprSame),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x + litValue)
    outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale max
      lit.dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuUnaryExpression(GpuAdd(leftExpr, lit),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue + x)
    checkEvaluateGpuUnaryExpression(GpuAdd(lit, leftExpr),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunSV, schema)
  }

  test("GpuMinus") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l - r)
    var outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale max
      schema(1).dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuMathBinaryExpression(GpuSubtract(leftExpr, rightExpr),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schema)
    outputScale = schemaSame.head.dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuMathBinaryExpression(GpuSubtract(leftExprSame, rightExprSame),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x - litValue)
    outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale max
      lit.dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuUnaryExpression(GpuSubtract(leftExpr, lit),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue - x)
    checkEvaluateGpuUnaryExpression(GpuSubtract(lit, leftExpr),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunSV, schema)
  }

  test("GpuMultiply") {
    val expectedFunVV = (l: Decimal, r: Decimal) => Option(l * r)
    var outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale +
      schema(1).dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuMathBinaryExpression(GpuMultiply(leftExpr, rightExpr),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schema)
    outputScale = schemaSame.head.dataType.asInstanceOf[DecimalType].scale * 2
    checkEvaluateGpuMathBinaryExpression(GpuMultiply(leftExprSame, rightExprSame),
      DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVV, schemaSame)

    val expectedFunVS = (x: Decimal) => Option(x * litValue)
    outputScale = schema.head.dataType.asInstanceOf[DecimalType].scale +
      lit.dataType.asInstanceOf[DecimalType].scale
    checkEvaluateGpuUnaryExpression(GpuMultiply(leftExpr, lit),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunVS, schema)
    val expectedFunSV = (x: Decimal) => Option(litValue * x)
    checkEvaluateGpuUnaryExpression(GpuMultiply(lit, leftExpr),
      schema.head.dataType, DecimalType(DType.DECIMAL64_MAX_PRECISION, outputScale),
      expectedFunSV, schema)
  }

  private def checkEvaluateGpuMathBinaryExpression[U](
    inputExpr: GpuExpression,
    outputType: DataType,
    expectedFun: (Decimal, Decimal) => Option[U],
    schema: StructType): Unit = {

    val fun = (left: Any, right: Any) => {
      if (left == null || right == null) {
        null
      } else {
        expectedFun(left.asInstanceOf[Decimal], right.asInstanceOf[Decimal])
      }
    }

    super.checkEvaluateGpuBinaryExpression(inputExpr,
      schema.head.dataType, schema(1).dataType, outputType, fun, schema)
  }
}
