/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
import ai.rapids.cudf.{ColumnVector, DecimalUtils, DType, Scalar}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution for CheckOverflow. This cannot match the Spark CheckOverflow 100% because
 * Spark will calculate values in BigDecimal with unbounded precision and then see if there was an
 * overflow. This will check bounds, but can only detect that an overflow happened if the result is
 * outside the bounds of what the Spark type supports, but did not yet overflow the bounds for what
 * the CUDF type supports. For most operations when this is a possibility for the given precision
 * then the operator should fall back to the CPU, or have alternative ways of checking for overflow
 * prior to this being called.
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
    val rounded = if (resultDType.equals(base.getType)) {
      base.incRefCount()
    } else {
      withResource(base.round(dataType.scale, cudf.RoundMode.HALF_UP)) { rounded =>
        if (resultDType.getTypeId != base.getType.getTypeId) {
          rounded.castTo(resultDType)
        } else {
          rounded.incRefCount()
        }
      }
    }
    withResource(rounded) { rounded =>
      GpuCast.checkNFixDecimalBounds(rounded, dataType, !nullOnOverflow)
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

  override def dataType: DecimalType = DecimalType(precision, sparkScale)
  override def nullable: Boolean = child.nullable || nullOnOverflow
  override def toString: String = s"MakeDecimal($child,$precision,$sparkScale)"

  private lazy val (minValue, maxValue) = {
    val bounds = DecimalUtils.bounds(dataType.precision, dataType.scale)
    (bounds.getKey.unscaledValue().longValue(), bounds.getValue.unscaledValue().longValue())
  }

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val outputType = DecimalUtils.createDecimalType(precision, sparkScale)
    val base = input.getBase
    val outOfBounds = withResource(Scalar.fromLong(maxValue)) { maxScalar =>
      withResource(base.greaterThan(maxScalar)) { over =>
        withResource(Scalar.fromLong(minValue)) { minScalar =>
          withResource(base.lessThan(minScalar)) { under =>
            over.or(under)
          }
        }
      }
    }
    withResource(outOfBounds) { outOfBounds =>
      withResource(base.bitCastTo(outputType)) { outputView =>
        if (!nullOnOverflow) {
          withResource(outOfBounds.any()) { isAny =>
            if (isAny.isValid && isAny.getBoolean) {
              throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
            }
          }
          outputView.copyToColumnVector()
        } else {
          withResource(Scalar.fromNull(outputType)) { nullVal =>
            outOfBounds.ifElse(nullVal, outputView)
          }
        }
      }
    }
  }
}
