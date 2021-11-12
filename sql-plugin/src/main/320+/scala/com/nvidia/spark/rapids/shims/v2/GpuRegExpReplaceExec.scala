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
package com.nvidia.spark.rapids.shims.v2

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{CudfRegexTranspiler, DataFromReplacementRule, GpuColumnVector, GpuExpression, GpuLiteral, GpuOverrides, GpuScalar, GpuTernaryExpression, QuaternaryExprMeta, RapidsConf, RapidsMeta, RegexUnsupportedException}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, RegExpReplace}
import org.apache.spark.sql.rapids.GpuRegExpReplace
import org.apache.spark.sql.types.{DataType, DataTypes, StringType}
import org.apache.spark.unsafe.types.UTF8String

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends QuaternaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    expr.regexp match {
      case Literal(null, _) =>
        willNotWorkOnGpu(s"RegExpReplace with null pattern is not supported on GPU")
      case Literal(s: UTF8String, DataTypes.StringType) =>
        val pattern = s.toString
        if (pattern.isEmpty) {
          willNotWorkOnGpu(s"RegExpReplace with empty pattern is not supported on GPU")
        }

        try {
          new CudfRegexTranspiler().transpile(pattern, replace = true)
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }

      case _ =>
        willNotWorkOnGpu(s"RegExpReplace with non-literal pattern is not supported on GPU")
    }

    GpuOverrides.extractLit(expr.pos).foreach { lit =>
      if (lit.value.asInstanceOf[Int] != 1) {
        willNotWorkOnGpu("Only a search starting position of 1 is supported")
      }
    }
  }

  override def convertToGpu(
      lhs: Expression,
      regexp: Expression,
      rep: Expression,
      pos: Expression): GpuExpression = {
    // ignore the pos expression which must be a literal 1 after tagging check
    require(childExprs.length == 4,
      s"Unexpected child count for RegExpReplace: ${childExprs.length}")
    val Seq(subject, regexp, rep) = childExprs.take(3).map(_.convertToGpu())
    GpuRegExpReplace(subject, regexp, rep)
  }
}