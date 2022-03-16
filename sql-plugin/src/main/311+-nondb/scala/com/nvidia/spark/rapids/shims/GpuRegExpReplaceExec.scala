/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{CudfRegexTranspiler, DataFromReplacementRule, GpuExpression, GpuOverrides, QuaternaryExprMeta, RapidsConf, RapidsMeta, RegexReplaceMode, RegexUnsupportedException}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.rapids.{GpuRegExpReplace, GpuRegExpUtils, GpuStringReplace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends QuaternaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  private var pattern: Option[String] = None
  private var replacement: Option[String] = None

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)
    expr.regexp match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null =>
        if (GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
          // use GpuStringReplace
        } else {
          try {
            pattern = Some(new CudfRegexTranspiler(RegexReplaceMode).transpile(s.toString))
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        }

      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    expr.rep match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null =>
        if (GpuRegExpUtils.containsBackrefs(s.toString)) {
          willNotWorkOnGpu("regexp_replace with back-references is not supported")
        }
        replacement = Some(GpuRegExpUtils.unescapeReplaceString(s.toString))
      case _ =>
    }

    GpuOverrides.extractLit(expr.pos).foreach { lit =>
      if (lit.value.asInstanceOf[Int] != 1) {
        willNotWorkOnGpu("only a search starting position of 1 is supported")
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
    if (GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
      GpuStringReplace(subject, regexp, rep)
    } else {
      (pattern, replacement) match {
        case (Some(cudfPattern), Some(cudfReplacement)) =>
          GpuRegExpReplace(lhs, regexp, rep, cudfPattern, cudfReplacement)
        case _ =>
          throw new IllegalStateException("Expression has not been tagged correctly")
      }
    }
  }
}
