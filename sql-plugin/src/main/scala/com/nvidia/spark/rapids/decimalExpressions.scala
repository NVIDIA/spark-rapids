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
import scala.math.{max, min}

import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Expression, PromotePrecision}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids._
import org.apache.spark.sql.types.{DataType, DecimalType, LongType}

/**
 * A GPU substitution of CheckOverflow, serves as a placeholder.
 */
case class GpuCheckOverflow(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.incRefCount()
  override def dataType: DataType = child.dataType
}

/**
 * A GPU substitution of PromotePrecision, serves as a placeholder.
 */
case class GpuPromotePrecision(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.incRefCount()
  override def dataType: DataType = child.dataType
}

/** Meta-data for checkOverflow */
class CheckOverflowExprMeta(
    expr: CheckOverflow,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMeta[CheckOverflow](expr, conf, parent, rule) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuCheckOverflow(child)
}

/** Meta-data for promotePrecision */
class PromotePrecisionExprMeta(
    expr: PromotePrecision,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMeta[PromotePrecision](expr, conf, parent, rule) {
  override def convertToGpu(child: Expression): GpuExpression =
    GpuPromotePrecision(child)
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