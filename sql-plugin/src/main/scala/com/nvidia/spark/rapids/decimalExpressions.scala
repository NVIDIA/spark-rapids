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

import ai.rapids.cudf
import ai.rapids.cudf.{ColumnVector, DType, Scalar}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution of CheckOverflow. This cannot match the Spark CheckOverflow 100% because
 * Spark will calculate values in BigDecimal and then see if there was an overflow. This assumes
 * that we will fall back to the CPU in any case where a real overflow could happen, although
 * there are some corner cases where it would be good for us to still check.
 */
case class GpuCheckOverflow(child: Expression,
    dataType: DecimalType,
    nullOnOverflow: Boolean) extends GpuUnaryExpression {
  private[this] val expectedCudfScale = -dataType.scale
  private[this] lazy val resultDType = if (dataType.precision > DType.DECIMAL64_MAX_PRECISION) {
    DType.create(DType.DTypeEnum.DECIMAL128, expectedCudfScale)
  } else if (dataType.precision > DType.DECIMAL32_MAX_PRECISION) {
    DType.create(DType.DTypeEnum.DECIMAL64, expectedCudfScale)
  } else {
    DType.create(DType.DTypeEnum.DECIMAL32, expectedCudfScale)
  }

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val base = input.getBase
    if (resultDType.equals(base.getType)) {
      base.incRefCount()
    } else {
      val foundScale = -base.getType.getScale
      val rounded = if (foundScale > dataType.scale) {
        base.round(dataType.scale, cudf.RoundMode.HALF_UP)
      } else {
        base.incRefCount()
      }
      withResource(rounded) { rounded =>
        if (resultDType.getTypeId != base.getType.getTypeId) {
          rounded.castTo(resultDType)
        } else {
          rounded.incRefCount()
        }
      }
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
    if (input.getBase.getType.isBackedByInt) {
      withResource(input.getBase.bitCastTo(DType.INT32)) { int32View =>
        int32View.castTo(DType.INT64)
      }
    } else {
      withResource(input.getBase.bitCastTo(DType.INT64)) { view =>
        view.copyToColumnVector()
      }
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
    val outputType = DecimalUtil.createCudfDecimal(precision, sparkScale)
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
