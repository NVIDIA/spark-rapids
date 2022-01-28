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

package org.apache.spark.sql.rapids

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Literal, RegExpExtract, RLike, StringSplit, SubstringIndex}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class GpuRLikeMeta(
    expr: RLike,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends BinaryExprMeta[RLike](expr, conf, parent, rule) {

    private var pattern: Option[String] = None

    override def tagExprForGpu(): Unit = {
      expr.right match {
        case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
          try {
            // verify that we support this regex and can transpile it to cuDF format
            pattern = Some(new CudfRegexTranspiler(replace = false).transpile(str.toString))
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        case _ =>
          willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
      }
    }
}

class GpuRegExpExtractMeta(
    expr: RegExpExtract,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtract](expr, conf, parent, rule) {

  private var pattern: Option[String] = None
  private var numGroups = 0

  override def tagExprForGpu(): Unit = {

    def countGroups(regexp: RegexAST): Int = {
      regexp match {
        case RegexGroup(_, term) => 1 + countGroups(term)
        case other => other.children().map(countGroups).sum
      }
    }

    expr.regexp match {
      case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
        try {
          val javaRegexpPattern = str.toString
          // verify that we support this regex and can transpile it to cuDF format
          val cudfRegexPattern = new CudfRegexTranspiler(replace = false)
            .transpile(javaRegexpPattern)
          pattern = Some(cudfRegexPattern)
          numGroups = countGroups(new RegexParser(javaRegexpPattern).parse())
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    expr.idx match {
      case Literal(value, DataTypes.IntegerType) =>
        val idx = value.asInstanceOf[Int]
        if (idx < 0) {
          willNotWorkOnGpu("the specified group index cannot be less than zero")
        }
        if (idx > numGroups) {
          willNotWorkOnGpu(
            s"regex group count is $numGroups, but the specified group index is $idx")
        }
      case _ =>
        willNotWorkOnGpu("GPU only supports literal index")
    }
  }
}

class SubstringIndexMeta(
    expr: SubstringIndex,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends TernaryExprMeta[SubstringIndex](expr, conf, parent, rule) {
  private var regexp: String = _

  override def tagExprForGpu(): Unit = {
    val delim = GpuOverrides.extractStringLit(expr.delimExpr).getOrElse("")
    if (delim == null || delim.length != 1) {
      willNotWorkOnGpu("only a single character deliminator is supported")
    }

    val count = GpuOverrides.extractLit(expr.countExpr)
    if (canThisBeReplaced) {
      val c = count.get.value.asInstanceOf[Integer]
      this.regexp = GpuSubstringIndex.makeExtractRe(delim, c)
    }
  }
}

object CudfRegexp {
  val escapeForCudfCharSet = Seq('^', '-', ']')

  def notCharSet(c: Char): String = c match {
    case '\n' => "(?:.|\r)"
    case '\r' => "(?:.|\n)"
    case chr if escapeForCudfCharSet.contains(chr) => "(?:[^\\" + chr + "]|\r|\n)"
    case chr => "(?:[^" + chr + "]|\r|\n)"
  }

  val escapeForCudf = Seq('[', '^', '$', '.', '|', '?', '*','+', '(', ')', '\\', '{', '}')

  def cudfQuote(c: Character): String = c match {
    case chr if escapeForCudf.contains(chr) => "\\" + chr
    case chr => Character.toString(chr)
  }
}

object GpuSubstringIndex {
  def makeExtractRe(delim: String, count: Integer): String = {
    if (delim.length != 1) {
      throw new IllegalStateException("NOT SUPPORTED")
    }
    val quotedDelim = CudfRegexp.cudfQuote(delim.charAt(0))
    val notDelim = CudfRegexp.notCharSet(delim.charAt(0))
    // substring_index has a deliminator and a count.  If the count is positive then
    // you get back a substring from 0 until the Nth deliminator is found
    // If the count is negative it goes in reverse
    if (count == 0) {
      // Count is zero so return a null regexp as a special case
      null
    } else if (count == 1) {
      // If the count is 1 we want to match everything from the beginning of the string until we
      // find the first occurrence of the deliminator or the end of the string
      "\\A(" + notDelim + "*)"
    } else if (count > 0) {
      // If the count is > 1 we first match 0 up to count - 1 occurrences of the patten
      // `not the deliminator 0 or more times followed by the deliminator`
      // After that we go back to matching everything until we find the deliminator or the end of
      // the string
      "\\A((?:" + notDelim + "*" + quotedDelim + "){0," + (count - 1) + "}" + notDelim + "*)"
    } else if (count == -1) {
      // A -1 looks like 1 but we start looking at the end of the string
      "(" + notDelim + "*)\\Z"
    } else { //count < 0
      // All others look like a positive count, but again we are matching starting at the end of
      // the string instead of the beginning
      "((?:" + notDelim + "*" + quotedDelim + "){0," + ((-count) - 1) + "}" + notDelim + "*)\\Z"
    }
  }
}

class GpuStringSplitMeta(
    expr: StringSplit,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends BinaryExprMeta[StringSplit](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    // 2.x uses expr.pattern not expr.regex
    val regexp = extractLit(expr.pattern)
    if (regexp.isEmpty) {
      willNotWorkOnGpu("only literal regexp values are supported")
    } else {
      val str = regexp.get.value.asInstanceOf[UTF8String]
      if (str != null) {
        if (!canRegexpBeTreatedLikeARegularString(str)) {
          willNotWorkOnGpu("regular expressions are not supported yet")
        }
        if (str.numChars() == 0) {
          willNotWorkOnGpu("An empty regex is not supported yet")
        }
      } else {
        willNotWorkOnGpu("null regex is not supported yet")
      }
    }
    // 2.x has no limit parameter
    /*
    if (!isLit(expr.limit)) {
      willNotWorkOnGpu("only literal limit is supported")
    }
    */
  }
}
