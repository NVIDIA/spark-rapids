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
package com.nvidia.spark.rapids

import java.nio.charset.Charset

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  private var javaPattern: Option[String] = None
  private var cudfPattern: Option[String] = None
  private var replacement: Option[String] = None
  private var canUseGpuStringReplace = false
  private var containsBackref: Boolean = false

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)
    replacement = expr.rep match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null => Some(s.toString)
      case _ => None
    }

    expr.regexp match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null =>
        if (GpuOverrides.isSupportedStringReplacePattern(expr.regexp)) {
          canUseGpuStringReplace = true
        } else {
          try {
            javaPattern = Some(s.toString())
            val (pat, repl) =
                new CudfRegexTranspiler(RegexReplaceMode).getTranspiledAST(s.toString, replacement)
            GpuRegExpUtils.validateRegExpComplexity(this, pat)
            cudfPattern = Some(pat.toRegexString)
            repl.map { r => GpuRegExpUtils.backrefConversion(r.toRegexString) }.foreach {
                case (hasBackref, convertedRep) =>
                  containsBackref = hasBackref
                  replacement = Some(GpuRegExpUtils.unescapeReplaceString(convertedRep))
            }
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        }

      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }
    // Spark 2.x is ternary and doesn't have pos parameter
    /*
    GpuOverrides.extractLit(expr.pos).foreach { lit =>
      if (lit.value.asInstanceOf[Int] != 1) {
        willNotWorkOnGpu("only a search starting position of 1 is supported")
      }
    }
    */
  }
}

object GpuRegExpUtils {
  private def parseAST(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

  /**
   * Convert symbols of back-references if input string contains any.
   * In spark's regex rule, there are two patterns of back-references:
   * \group_index and $group_index
   * This method transforms above two patterns into cuDF pattern ${group_index}, except they are
   * preceded by escape character.
   *
   * @param rep replacement string
   * @return A pair consists of a boolean indicating whether containing any backref and the
   *         converted replacement.
   */
  def backrefConversion(rep: String): (Boolean, String) = {
    val b = new StringBuilder
    var i = 0
    while (i < rep.length) {
      // match $group_index or \group_index
      if (Seq('$', '\\').contains(rep.charAt(i))
        && i + 1 < rep.length && rep.charAt(i + 1).isDigit) {

        b.append("${")
        var j = i + 1
        do {
          b.append(rep.charAt(j))
          j += 1
        } while (j < rep.length && rep.charAt(j).isDigit)
        b.append("}")
        i = j
      } else if (rep.charAt(i) == '\\' && i + 1 < rep.length) {
        // skip potential \$group_index or \\group_index
        b.append('\\').append(rep.charAt(i + 1))
        i += 2
      } else {
        b.append(rep.charAt(i))
        i += 1
      }
    }

    val converted = b.toString
    !rep.equals(converted) -> converted
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

    Charset.defaultCharset().name() match {
      case "UTF-8" =>
        // supported
      case _ =>
        meta.willNotWorkOnGpu(s"regular expression support is disabled because the GPU only " +
        "supports the UTF-8 charset when using regular expressions")
    }
  }

  def validateRegExpComplexity(meta: ExprMeta[_], regex: RegexAST): Unit = {
    if(!RegexComplexityEstimator.isValid(meta.conf, regex)) {
      meta.willNotWorkOnGpu(s"estimated memory needed for regular expression exceeds the maximum." +
        s" Set ${RapidsConf.REGEXP_MAX_STATE_MEMORY_BYTES} to change it.")
    }
  }

  /**
   * Recursively check if pattern contains only zero-match repetitions
   * ?, *, {0,}, or {0,n} or any combination of them.
   */
  def isEmptyRepetition(pattern: String): Boolean = {
    def isASTEmptyRepetition(regex: RegexAST): Boolean = {
      regex match {
        case RegexRepetition(_, term) => term match {
          case SimpleQuantifier('*') | SimpleQuantifier('?') => true
          case QuantifierFixedLength(0) => true
          case QuantifierVariableLength(0, _) => true
          case _ => false
        }
        case RegexGroup(_, term) =>
          isASTEmptyRepetition(term)
        case RegexSequence(parts) =>
          parts.forall(isASTEmptyRepetition)
        // cuDF does not support repetitions adjacent to a choice (eg. "a*|a"), but if
        // we did, we would need to add a `case RegexChoice()` here
        case _ => false
      }
    }
    isASTEmptyRepetition(parseAST(pattern))
  }

  /**
   * Returns the number of groups in regexp
   * (includes both capturing and non-capturing groups)
   */
  def countGroups(pattern: String): Int = {
    def countGroups(regexp: RegexAST): Int = {
      regexp match {
        case RegexGroup(_, term) => 1 + countGroups(term)
        case other => other.children().map(countGroups).sum
      }
   }
   countGroups(parseAST(pattern))
  }
}
