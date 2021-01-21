/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import scala.math.min

import ai.rapids.cudf.{ColumnVector, DType, Scalar}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution of CheckOverflow, serves as a placeholder.
 */
case class GpuCheckOverflow(child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val base = input.getBase
    val foundCudfScale = base.getType.getScale
    val expectedCudfScale = -dataType.scale
    if (foundCudfScale == expectedCudfScale) {
      base.incRefCount()
    } else if (foundCudfScale < expectedCudfScale) {
      base.castTo(DType.create(DType.DTypeEnum.DECIMAL64, expectedCudfScale))
    } else {
      // need to round off
      base.round(-expectedCudfScale, ai.rapids.cudf.RoundMode.HALF_UP)
    }
  }
}

/**
 * A GPU substitution of PromotePrecision, serves as a placeholder.
 */
case class GpuPromotePrecision(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.incRefCount()
  override def dataType: DataType = child.dataType
}

object DecimalExpressions {
  // Underlying storage type of decimal data in cuDF is int64_t, whose max capacity is 19.
  val GPU_MAX_PRECISION: Int = 19
  val GPU_MAX_SCALE: Int = 19
  // Keep up with MINIMUM_ADJUSTED_SCALE, is this better to be configurable?
  val GPU_MINIMUM_ADJUSTED_SCALE = 6

  /**
   * A forked version of [[org.apache.spark.sql.types.DecimalType]] with GPU constants replacement
   */
  private[rapids] def adjustPrecisionScale(dt: DecimalType): DecimalType = {
    if (dt.precision <= GPU_MAX_PRECISION) {
      // Adjustment only needed when we exceed max precision
      dt
    } else if (dt.scale < 0) {
      // Decimal can have negative scale (SPARK-24468). In this case, we cannot allow a precision
      // loss since we would cause a loss of digits in the integer part.
      // In this case, we are likely to meet an overflow.
      DecimalType(GPU_MAX_PRECISION, dt.scale)
    } else {
      // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
      val intDigits = dt.precision - dt.scale
      // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
      // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
      val minScaleValue = Math.min(dt.scale, GPU_MINIMUM_ADJUSTED_SCALE)
      // The resulting scale is the maximum between what is available without causing a loss of
      // digits for the integer part of the decimal and the minimum guaranteed scale, which is
      // computed above
      val adjustedScale = Math.max(GPU_MAX_PRECISION - intDigits, minScaleValue)

      DecimalType(GPU_MAX_PRECISION, adjustedScale)
    }
  }

  /**
   * A forked version of [[org.apache.spark.sql.types.DecimalType]] with GPU constants replacement
   */
  private[rapids] def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(min(precision, GPU_MAX_PRECISION), min(scale, GPU_MAX_SCALE))
  }
}

case class GpuUnscaledValue(child: Expression) extends GpuUnaryExpression {
  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.logicalCastTo(DType.INT64)) { view =>
      view.copyToColumnVector()
    }
  }
}

case class GpuMakeDecimal(
    child: Expression,
    precision: Int,
    sparkScale: Int,
    nullOnOverflow: Boolean) extends GpuUnaryExpression {

  override def dataType: DataType = DecimalType(precision, sparkScale)
  override def nullable: Boolean = child.nullable || nullOnOverflow
  override def toString: String = s"MakeDecimal($child,$precision,$sparkScale)"

  private lazy val cudfScale = -sparkScale
  private lazy val maxValue = BigDecimal(("9"*precision) + "e" + cudfScale.toString)
      .bigDecimal.unscaledValue().longValue()

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val base = input.getBase
    val outputType = DType.create(DType.DTypeEnum.DECIMAL64, cudfScale)
    if (nullOnOverflow) {
      val overflowed = withResource(Scalar.fromLong(maxValue)) { limit =>
        base.greaterThan(limit)
      }
      withResource(overflowed) { overflowed =>
        withResource(Scalar.fromNull(outputType)) { nullVal =>
          withResource(base.logicalCastTo(outputType)) { view =>
            overflowed.ifElse(nullVal, view)
          }
        }
      }
    } else {
      withResource(base.logicalCastTo(outputType)) { view =>
        view.copyToColumnVector()
      }
    }
  }
}