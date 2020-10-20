/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import scala.math.max

import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Expression, PromotePrecision}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuAdd, GpuDivide, GpuIntegralDivide, GpuMultiply, GpuPmod, GpuRemainder, GpuSubtract}
import org.apache.spark.sql.types.{DataType, DecimalType}

/**
 * A GPU substitution of CheckOverflow, serves as a placeholder.
 */
case class GpuCheckOverflow(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = input
  override def dataType: DataType = child.dataType
}

/**
 * A GPU substitution of PromotePrecision, serves as a placeholder.
 */
case class GpuPromotePrecision(child: Expression) extends GpuUnaryExpression {
  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = input
  override def dataType: DataType = child.dataType
}

/** Meta-data for checkOverflow */
class CheckOverflowExprMeta(
    expr: CheckOverflow,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends UnaryExprMeta[CheckOverflow](expr, conf, parent, rule) {
  override def convertToGpu(child: Expression): GpuExpression = {
    child match {
      // For Add | Subtract | Remainder | Pmod | IntegralDivide,
      // resultTypes have less or same precision compared with inputTypes.
      // Since inputTypes are checked in PromotePrecisionExprMeta, resultTypes are also safe.
      case _: GpuAdd =>
      case _: GpuSubtract =>
      case _: GpuRemainder =>
      case _: GpuPmod =>
      case _: GpuIntegralDivide =>
      // For Multiply, we need to infer result's precision from inputs' precision.
      case GpuMultiply(GpuPromotePrecision(lhs: GpuCast), _) =>
        val dt = lhs.dataType.asInstanceOf[DecimalType]
        if (dt.precision * 2 + 1 > DecimalExpressions.gpuMaxPrecision) {
          throw new IllegalStateException("DecimalPrecision overflow may occur because " +
            s"inferred result precision(${dt.precision * 2 + 1}) exceeds GpuMaxPrecision.")
        }
      // For Divide, we need to infer result's precision from inputs' precision and scale.
      case GpuDivide(GpuPromotePrecision(lhs: GpuCast), _) =>
        val dt = lhs.dataType.asInstanceOf[DecimalType]
        val scale = max(DecimalType.MINIMUM_ADJUSTED_SCALE, dt.precision + dt.scale + 1)
        if (dt.precision + scale > DecimalExpressions.gpuMaxPrecision) {
          throw new IllegalStateException("DecimalPrecision overflow may occur because " +
            s"inferred result precision(${dt.precision + scale}) exceeds GpuMaxPrecision.")
        }
      case c =>
        throw new IllegalAccessException(
          s"Unknown child expression of CheckOverflow ${c.prettyName}.")
    }
    GpuCheckOverflow(child)
  }
}

/** Meta-data for promotePrecision */
class PromotePrecisionExprMeta(
    expr: PromotePrecision,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends UnaryExprMeta[PromotePrecision](expr, conf, parent, rule) {
  override def convertToGpu(child: Expression): GpuExpression = {
    child match {
      case GpuCast(cc: Expression, dt: DecimalType, a: Boolean, t: Option[String]) =>
        GpuPromotePrecision(GpuCast(cc, DecimalExpressions.gpuCheckPrecision(dt), a, t))
      case c => throw new IllegalStateException(
        s"Child expression of PromotePrecision should always be GpuCast with DecimalType, " +
          s"but found ${c.prettyName}")
    }
  }
}

object DecimalExpressions {
  // Underlying storage type of decimal data in cuDF is int64_t, whose max capacity is 19.
  val gpuMaxPrecision: Int = 19

  /**
   * Check whether there exists Gpu precision overflow for given DecimalType.
   * And try to shrink scale to avoid it if possible. Otherwise, an exception will be thrown.
   *
   * @return checked DecimalType
   */
  def gpuCheckPrecision(dt: DecimalType): DecimalType = {
    if (dt.precision > DecimalExpressions.gpuMaxPrecision) {
      if (!SQLConf.get.decimalOperationsAllowPrecisionLoss) {
        throw new IllegalStateException(
          "Failed to reduce scale of decimalType which exceeds GpuMaxPrecision, " +
            "because SQLConf.get.decimalOperationsAllowPrecisionLoss is disabled.")
      }
      val scale = dt.scale - (dt.precision - DecimalExpressions.gpuMaxPrecision)
      return DecimalType(DecimalExpressions.gpuMaxPrecision, scale)
    }
    dt
  }
}
