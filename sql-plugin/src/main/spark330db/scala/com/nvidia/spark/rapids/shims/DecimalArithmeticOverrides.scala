/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{BinaryAstExprMeta, BinaryExprMeta, DecimalUtil, ExprChecks, ExprRule, GpuExpression, TypeSig}
import com.nvidia.spark.rapids.GpuOverrides.expr

import org.apache.spark.sql.catalyst.expressions.{Divide, Expression, IntegralDivide, Multiply, Remainder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{DecimalMultiplyChecks, GpuAnsi, GpuDecimalDivide, GpuDecimalMultiply, GpuDecimalRemainder, GpuDivide, GpuIntegralDecimalDivide, GpuIntegralDivide, GpuMultiply, GpuRemainder}
import org.apache.spark.sql.types.DecimalType

object DecimalArithmeticOverrides {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    // We don't override PromotePrecision or CheckOverflow for Spark 3.4
    Seq(
      expr[Multiply](
        "Multiplication",
        ExprChecks.binaryProjectAndAst(
          TypeSig.implicitCastsAstTypes,
          TypeSig.gpuNumeric,
          TypeSig.cpuNumeric,
          ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
          ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
        (a, conf, p, r) => new BinaryAstExprMeta[Multiply](a, conf, p, r) {
          override def tagExprForGpu(): Unit = {
            if (SQLConf.get.ansiEnabled && GpuAnsi.needBasicOpOverflowCheck(a.dataType)) {
              willNotWorkOnGpu("GPU Multiplication does not support ANSI mode")
            }
          }

          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
            lazy val lhsDecimalType =
              DecimalUtil.asDecimalType(lhs.dataType)
            lazy val rhsDecimalType =
              DecimalUtil.asDecimalType(rhs.dataType)

            a.dataType match {
              case d: DecimalType =>
                val intermediatePrecision =
                  DecimalMultiplyChecks.nonRoundedIntermediatePrecision(lhsDecimalType,
                    rhsDecimalType, d)
                GpuDecimalMultiply(lhs, rhs, d,
                  useLongMultiply = intermediatePrecision > DType.DECIMAL128_MAX_PRECISION)
              case _ =>
                GpuMultiply(lhs, rhs)
            }
          }
        }),
      expr[Divide](
        "Division",
        ExprChecks.binaryProject(
          TypeSig.DOUBLE + TypeSig.DECIMAL_128,
          TypeSig.DOUBLE + TypeSig.DECIMAL_128,
          ("lhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
              TypeSig.DOUBLE + TypeSig.DECIMAL_128),
          ("rhs", TypeSig.DOUBLE + TypeSig.DECIMAL_128,
              TypeSig.DOUBLE + TypeSig.DECIMAL_128)),
        (a, conf, p, r) => new BinaryExprMeta[Divide](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            a.dataType match {
              case d: DecimalType =>
                GpuDecimalDivide(lhs, rhs, d)
              case _ =>
                GpuDivide(lhs, rhs)
            }
        }),
      expr[IntegralDivide](
        "Division with a integer result",
        ExprChecks.binaryProject(
          TypeSig.LONG, TypeSig.LONG,
          ("lhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128),
          ("rhs", TypeSig.LONG + TypeSig.DECIMAL_128, TypeSig.LONG + TypeSig.DECIMAL_128)),
        (a, conf, p, r) => new BinaryExprMeta[IntegralDivide](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            if (lhs.dataType.isInstanceOf[DecimalType] && rhs.dataType.isInstanceOf[DecimalType]) {
              GpuIntegralDecimalDivide(lhs, rhs)
            } else {
              GpuIntegralDivide(lhs, rhs)
            }
        }),

      expr[Remainder](
        "Remainder or modulo",
        ExprChecks.binaryProject(
          TypeSig.gpuNumeric, TypeSig.cpuNumeric,
          ("lhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric),
          ("rhs", TypeSig.gpuNumeric, TypeSig.cpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[Remainder](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            if (lhs.dataType.isInstanceOf[DecimalType] && rhs.dataType.isInstanceOf[DecimalType]) {
              GpuDecimalRemainder(lhs, rhs)
            } else {
              GpuRemainder(lhs, rhs)
            }
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
