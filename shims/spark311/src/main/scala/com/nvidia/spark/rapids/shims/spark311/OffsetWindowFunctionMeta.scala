/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark311

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, ExprMeta, GpuOverrides, RapidsConf, RapidsMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, Lag, Literal, OffsetWindowFunction}
import org.apache.spark.sql.types.IntegerType

/**
 * Spark 3.1.1-specific replacement for com.nvidia.spark.rapids.OffsetWindowFunctionMeta.
 * This is required primarily for two reasons:
 *   1. com.nvidia.spark.rapids.OffsetWindowFunctionMeta (compiled against Spark 3.0.x)
 *      fails class load in Spark 3.1.x. (`expr.input` is not recognized as an Expression.)
 *   2. The semantics of offsets in LAG() are reversed/negated in Spark 3.1.1.
 *      E.g. The expression `LAG(col, 5)` causes Lag.offset to be set to `-5`,
 *      as opposed to `5`, in prior versions of Spark.
 * This class adjusts the LAG offset to use similar semantics to Spark 3.0.x.
 */
abstract class OffsetWindowFunctionMeta[INPUT <: OffsetWindowFunction] (
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {
  val input: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.input, conf, Some(this))
  // For LAG() expressions, negate the offset value.
  val adjustedOffset: Expression = {
    expr match {
      case lag: Lag =>
        lag.offset match {
          case Literal(v: Int, IntegerType) =>
            Literal(-v)
          case _ => expr.offset
        }
      case _ => expr.offset
    }
  }
  val offset: BaseExprMeta[_] =
    GpuOverrides.wrapExpr(adjustedOffset, conf, Some(this))
  val default: BaseExprMeta[_] = GpuOverrides.wrapExpr(expr.default, conf, Some(this))
  override val childExprs: Seq[BaseExprMeta[_]] = Seq(input, offset, default)
}
