/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  private var pattern: Option[String] = None
  private var replacement: Option[String] = None

  override def tagExprForGpu(): Unit = {
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
  }
}

object GpuRegExpUtils {

  /**
   * Determine if a string contains back-references such as `$1` but ignoring
   * if preceded by escape character.
   */
  def containsBackrefs(s: String): Boolean = {
    var i = 0
    while (i < s.length) {
      if (s.charAt(i) == '\\') {
        i += 2
      } else {
        if (s.charAt(i) == '$'  && i+1 < s.length) {
          if (s.charAt(i+1).isDigit) {
            return true
          }
        }
        i += 1
      }
    }
    false
  }

  /**
   * We need to remove escape characters in the regexp_replace
   * replacement string before passing to cuDF.
   */
  def unescapeReplaceString(s: String): String = {
    val b = new StringBuilder
    var i = 0
    while (i < s.length) {
      if (s.charAt(i) == '\\' && i+1 < s.length) {
        i += 1
      }
      b.append(s.charAt(i))
      i += 1
    }
    b.toString
  }

  def tagForRegExpEnabled(meta: ExprMeta[_]): Unit = {
    if (!meta.conf.isRegExpEnabled) {
      meta.willNotWorkOnGpu(s"regular expression support is disabled. " +
        s"Set ${RapidsConf.ENABLE_REGEXP}=true to enable it")
    }
  }
}
