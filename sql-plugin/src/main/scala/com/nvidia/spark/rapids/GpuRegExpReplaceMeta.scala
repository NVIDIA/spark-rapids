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
package com.nvidia.spark.rapids

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RegExpReplace}
import org.apache.spark.sql.rapids.{GpuRegExpReplace, GpuRegExpReplaceWithBackref, GpuRegExpUtils, GpuRLike, GpuStringReplace}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.unsafe.types.UTF8String

sealed trait GpuRegExpReplaceAction
object ActionStringReplace extends GpuRegExpReplaceAction
case class ActionStringReplaceMulti(patterns: Seq[String],
    javaPattern: String,
    cudfPattern: String,
    replacement: String)
  extends GpuRegExpReplaceAction
case class ActionRegExpReplace(
    javaPattern: String,
    cudfPattern: String,
    replacement: String,
    containsBackref: Boolean
) extends GpuRegExpReplaceAction

class GpuRegExpReplaceMeta(
    expr: RegExpReplace,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends QuaternaryExprMeta[RegExpReplace](expr, conf, parent, rule) {

  private var action: Option[GpuRegExpReplaceAction] = None

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)
    val replacement: String = expr.rep match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null => s.toString
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
        return
    }

    expr.regexp match {
      case Literal(s: UTF8String, DataTypes.StringType) if s != null =>
        try {
          val (pattern, repl) = new CudfRegexTranspiler(RegexReplaceMode)
              .getTranspiledAST(s.toString, Some(replacement))
          GpuRegExpUtils.validateRegExpComplexity(this, pattern)
          val maybeBackref = repl.map { r => GpuRegExpUtils.backrefConversion(r.toRegexString) }
          val hasBackRef = maybeBackref.exists(_._1)

          pattern match {
            case RegexChoice(a, b) if !hasBackRef =>
              // special handling for "AB|CD" where we can use string replace instead of
              // regexp replace for improved performance. We can only perform this optimization
              // if the following conditions are met:
              //
              // - Both strings must be supported by string_replace, so cannot contain regexp chars
              // - There must be no back-references
              val str1 = a.toRegexString
              val str2 = b.toRegexString
              if (GpuOverrides.isSupportedStringReplacePattern(str1) &&
                  GpuOverrides.isSupportedStringReplacePattern(str2)) {
                action = Some(ActionStringReplaceMulti(Seq(str1, str2),
                  s.toString,
                  pattern.toRegexString,
                  replacement
                ))
              } else {
                maybeBackref.foreach {
                  case (hasBackref, convertedRep) =>
                    action = Some(ActionRegExpReplace(s.toString,
                      pattern.toRegexString,
                      GpuRegExpUtils.unescapeReplaceString(convertedRep),
                      hasBackref))
                }
              }

            case _ if GpuOverrides.isSupportedStringReplacePattern(expr.regexp) =>
              action = Some(ActionStringReplace)

            case _ =>
              maybeBackref.foreach {
                case (hasBackref, convertedRep) =>
                  action = Some(ActionRegExpReplace(s.toString,
                    pattern.toRegexString,
                    GpuRegExpUtils.unescapeReplaceString(convertedRep),
                    hasBackref))
              }
          }
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }

      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
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

    action match {
      case Some(ActionStringReplace) =>
        GpuStringReplace(lhs, regexp, rep)
      case Some(ActionStringReplaceMulti(patterns, javaPattern, cudfPattern, replacement))
          if patterns.length == 2 =>
        // We can't just replace the two strings in turn because it can lead to incorrect
        // results, so we first replace string1 with a placeholder that is guaranteed to not
        // appear in the string, then we replace string2 and finally replace the placeholder.
        //
        // Example:
        // - input string "cabd"
        // - pattern "ab|cd"
        // - replacement string ""
        //
        // step 1: replace "ab" with "^", resulting in "c^d"
        // step 2: replace "cd" with "^", resulting in "c^d"
        // step 3: replace "^" with "", resulting in "cd"
        //
        // We also have to check if the input data contains the placeholder and fall back to
        // regexp_replace in that case. We artificially declare that the expression has
        // side-effects to trigger the gather/scatter logic so that we only invoke
        // regexp_replace when needed
        val placeholder = GpuLiteral("^")

        // first replace string 1 with a special placeholder
        val repl1 = GpuStringReplace(lhs, GpuLiteral(patterns.head), placeholder)
        // now replace string 2
        val repl2 = GpuStringReplace(repl1, GpuLiteral(patterns(1)), rep)
        // finally replace the placeholder
        // declare that this has side-effects to force the scatter-gather algorithm
        val stringReplace = GpuStringReplace(repl2, placeholder, rep, hasSideEffects = true)

        val regexpReplace = GpuRegExpReplace(
          lhs, regexp, rep, javaPattern, cudfPattern, replacement)

        val containsPlaceholder = GpuRLike(lhs, GpuLiteral("\\^"), "\\^")

        GpuIf(containsPlaceholder, regexpReplace, stringReplace)
      case Some(ActionRegExpReplace(javaPattern, cudfPattern, cudfReplacement, containsBackref)) =>
        if (containsBackref) {
          GpuRegExpReplaceWithBackref(lhs, cudfPattern, cudfReplacement)
        } else {
          GpuRegExpReplace(lhs, regexp, rep, javaPattern, cudfPattern, cudfReplacement)
        }
      case _ =>
        throw new IllegalStateException("Expression has not been tagged correctly")
    }
  }
}
