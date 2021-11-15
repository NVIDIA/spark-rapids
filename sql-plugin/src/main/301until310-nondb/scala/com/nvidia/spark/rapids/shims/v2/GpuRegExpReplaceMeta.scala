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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.rapids.{GpuRegExpReplace, GpuStringReplace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    expr.regexp match {
      case Literal(null, _) =>
        willNotWorkOnGpu(s"RegExpReplace with null pattern is not supported on GPU")
      case Literal(s: UTF8String, DataTypes.StringType) =>
        val pattern = s.toString
        if (pattern.isEmpty) {
          willNotWorkOnGpu(s"RegExpReplace with empty pattern is not supported on GPU")
        }

        if (GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
          // use GpuStringReplace
        } else {
          try {
            new CudfRegexTranspiler(replace = true).transpile(pattern)
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        }

      case _ =>
        willNotWorkOnGpu(s"RegExpReplace with non-literal pattern is not supported on GPU")
    }
  }

  override def convertToGpu(
      lhs: Expression,
      regexp: Expression,
      rep: Expression): GpuExpression = {
    if (GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
      GpuStringReplace(lhs, regexp, rep)
    } else {
      GpuRegExpReplace(lhs, regexp, rep)
    }
  }
}
