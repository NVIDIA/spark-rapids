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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution of CheckOverflow, does not actually check for overflow because
 * the precision checks for 64-bit support prevent the need for that.
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
    } else if (-foundCudfScale < -expectedCudfScale) {
      base.castTo(DType.create(DType.DTypeEnum.DECIMAL64, expectedCudfScale))
    } else {
      // need to round off
      base.round(-expectedCudfScale, ai.rapids.cudf.RoundMode.HALF_UP)
    }
  }
}

/**
 * A GPU substitution of PromotePrecision, which is a NOOP in Spark too.
 */
case class GpuPromotePrecision(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.incRefCount()
  override def dataType: DataType = child.dataType
}

case class GpuUnscaledValue(child: Expression) extends GpuUnaryExpression {
  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.bitCastTo(DType.INT64)) { view =>
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
          withResource(base.bitCastTo(outputType)) { view =>
            overflowed.ifElse(nullVal, view)
          }
        }
      }
    } else {
      withResource(base.bitCastTo(outputType)) { view =>
        view.copyToColumnVector()
      }
    }
  }
}
