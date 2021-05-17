/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, PadSide, Scalar, Table}
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, Predicate, StringSplit, SubstringIndex}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class GpuUnaryString2StringExpression extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StringType
}

case class GpuUpper(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"upper($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.upper()
}

case class GpuLower(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"lower($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.lower()
}

case class GpuLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.getCharLengths()
}

case class GpuStringLocate(substr: Expression, col: Expression, start: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = Seq(substr, col, start)

  def this(substr: Expression, col: Expression) = {
    this(substr, col, GpuLiteral(1, IntegerType))
  }

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuScalar,
      val1: GpuColumnVector,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuScalar,
      val1: GpuScalar,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: GpuScalar, val1: GpuColumnVector,
      val2: GpuScalar): ColumnVector = {
    if (!val2.isValid) {
      withResource(Scalar.fromInt(0)) { zeroScalar =>
        ColumnVector.fromScalar(zeroScalar, val1.getRowCount().toInt)
      }
    } else if (!val0.isValid) {
      //if null substring // or null column? <-- needs to be looked for/tested
      GpuColumnVector.columnVectorFromNull(val1.getRowCount().toInt, IntegerType)
    } else {
      val val2Int = val2.getValue.asInstanceOf[Int]
      val val0Str = val0.getValue.asInstanceOf[UTF8String]
      if (val2Int < 1 || val0Str.numChars() == 0) {
        withResource(val1.getBase.isNotNull()) { isNotNullColumn =>
          withResource(Scalar.fromNull(DType.INT32)) { nullScalar =>
            if (val2Int >= 1) {
              withResource(Scalar.fromInt(1)) { sv1 =>
                isNotNullColumn.ifElse(sv1, nullScalar)
              }
            } else {
              withResource(Scalar.fromInt(0)) { sv0 =>
                isNotNullColumn.ifElse(sv0, nullScalar)
              }
            }
          }
        }
      } else {
        withResource(val1.getBase.stringLocate(val0.getBase, val2Int - 1, -1)) {
          skewedResult =>
            withResource(Scalar.fromInt(1)) { sv1 =>
              skewedResult.add(sv1)
            }
        }
      }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val1, numRows, col.dataType)) { val1Col =>
      doColumnar(val0, val1Col, val2)
    }
  }

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuScalar,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuScalar,
      val2: GpuScalar): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: GpuScalar): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuStartsWith(left: Expression, right: Expression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL STARTSWITH ($listSQL))"
  }

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.startsWith(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have two column vectors as input in StartsWith")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have a scalar as left side operand in StartsWith")
}

case class GpuEndsWith(left: Expression, right: Expression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL ENDSWITH ($listSQL))"
  }

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.endsWith(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have two column vectors as input in EndsWith")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have a scalar as left side operand in EndsWith")
}

case class GpuStringTrim(column: Expression, trimParameters: Option[Expression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "BOTH"

  override def strippedColumnVector(column: GpuColumnVector, t: Scalar): GpuColumnVector =
    GpuColumnVector.from(column.getBase.strip(t), dataType)
}

case class GpuStringTrimLeft(column: Expression, trimParameters: Option[Expression] = None)
  extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "LEADING"

  override def strippedColumnVector(column: GpuColumnVector, t: Scalar): GpuColumnVector =
    GpuColumnVector.from(column.getBase.lstrip(t), dataType)
}

case class GpuStringTrimRight(column: Expression, trimParameters: Option[Expression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "TRAILING"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector =
    GpuColumnVector.from(column.getBase.rstrip(t), dataType)
}

case class GpuContains(left: Expression, right: Expression) extends GpuBinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql
    s"($inputSQL CONTAINS ($listSQL))"
  }

  override def toString: String = s"gpucontains($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    lhs.getBase.stringContains(rhs.getBase)

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in Contains")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here," +
      "Cannot have a scalar as left side operand in Contains")
}

case class GpuSubstring(str: Expression, pos: Expression, len: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def children: Seq[Expression] = Seq(str, pos, len)

  def this(str: Expression, pos: Expression) = {
    this(str, pos, GpuLiteral(Integer.MAX_VALUE, IntegerType))
  }

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuScalar,
      val1: GpuColumnVector,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: GpuScalar, val1: GpuScalar, val2: GpuColumnVector): ColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: GpuScalar, val1: GpuColumnVector, val2: GpuScalar): ColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuScalar,
      val2: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(column: GpuColumnVector,
                          position: GpuScalar,
                          length: GpuScalar): ColumnVector = {
    val substringPos = position.getValue.asInstanceOf[Int]
    val substringLen = length.getValue.asInstanceOf[Int]
    if (substringLen < 0) { // Spark returns empty string if length is negative
      column.getBase.substring(0, 0)
    } else if (substringPos >= 0) { // If position is non negative
      if (substringPos == 0) {  // calculate substring from first character to length
        column.getBase.substring(substringPos, substringLen)
      } else { // calculate substring from position to length
        column.getBase.substring(substringPos - 1, substringPos + substringLen - 1)
      }
    } else { // If position is negative, evaluate from end.
      column.getBase.substring(substringPos, Integer.MAX_VALUE)
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, str.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: GpuScalar): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuInitCap(child: Expression) extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType
  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.toTitle
}

case class GpuStringReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def children: Seq[Expression] = Seq(srcExpr, searchExpr, replaceExpr)

  def this(srcExpr: Expression, searchExpr: Expression) = {
    this(srcExpr, searchExpr, GpuLiteral("", StringType))
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuScalar,
      replaceExpr: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuScalar,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuScalar): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuColumnVector): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    // When search or replace string is null, return all nulls like the CPU does.
    if (!searchExpr.isValid || !replaceExpr.isValid) {
      GpuColumnVector.columnVectorFromNull(strExpr.getRowCount.toInt, StringType)
    } else if (searchExpr.getValue.asInstanceOf[UTF8String].numChars() == 0) {
      // Return original string if search string is empty
      strExpr.getBase.asStrings()
    } else {
      strExpr.getBase.stringReplace(searchExpr.getBase, replaceExpr.getBase)
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, srcExpr.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuScalar): ColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
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

case class GpuLike(left: Expression, right: Expression, escapeChar: Char)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant  {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def toString: String = escapeChar match {
    case '\\' => s"$left gpulike $right"
    case c => s"$left gpulike $right ESCAPE '$c'"
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in Like")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have a scalar as left side operand in Like")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val likeStr = if (rhs.isValid) {
      rhs.getValue.asInstanceOf[UTF8String].toString
    } else {
      null
    }
    val regexStr = escapeLikeRegex(likeStr, escapeChar)
    lhs.getBase.matchesRe(regexStr)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
  /**
   * Validate and convert SQL 'like' pattern to a cuDF regular expression.
   *
   * Underscores (_) are converted to '.' including newlines and percent signs (%)
   * are converted to '.*' including newlines, other characters are quoted literally or escaped.
   * An invalid pattern will throw an `IllegalArgumentException`.
   *
   * @param pattern the SQL pattern to convert
   * @param escapeChar the escape string contains one character.
   * @return the equivalent cuDF regular expression of the pattern
   */
  def escapeLikeRegex(pattern: String, escapeChar: Char): String = {
    val in = pattern.toIterator
    val out = new StringBuilder()

    def fail(message: String) = throw new IllegalArgumentException(
      s"the pattern '$pattern' is invalid, $message")

    import CudfRegexp.cudfQuote

    while (in.hasNext) {
      in.next match {
        case c1 if c1 == escapeChar && in.hasNext =>
          val c = in.next
          c match {
            case '_' | '%' => out ++= cudfQuote(c)
            // special case for cudf
            case c if c == escapeChar => out ++= cudfQuote(c)
            case _ => fail(s"the escape character is not allowed to precede '$c'")
          }
        case c if c == escapeChar => fail("it is not allowed to end with the escape character")
        case '_' => out ++= "(.|\n)"
        case '%' => out ++= "(.|\n)*"
        case c => out ++= cudfQuote(c)
      }
    }
    out.result() + "\\Z" // makes this match for cuDF expected format for `matchesRe`
  }
}

class SubstringIndexMeta(
    expr: SubstringIndex,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
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

  override def convertToGpu(
      column: Expression,
      delim: Expression,
      count: Expression): GpuExpression = GpuSubstringIndex(column, this.regexp, delim, count)
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

case class GpuSubstringIndex(strExpr: Expression,
    regexp: String,
    ignoredDelimExpr: Expression,
    ignoredCountExpr: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = Seq(strExpr, ignoredDelimExpr, ignoredCountExpr)
  override def prettyName: String = "substring_index"

  // This is a bit hacked up at the moment. We are going to use a regular expression to extract
  // a single value. It only works if the delim is a single character. A full version of
  // substring_index for the GPU has been requested at https://github.com/rapidsai/cudf/issues/5158
  override def doColumnar(str: GpuColumnVector, delim: GpuScalar,
      count: GpuScalar): ColumnVector = {
    if (regexp == null) {
      withResource(str.getBase.isNull) { isNull =>
        withResource(Scalar.fromString("")) { emptyString =>
          isNull.ifElse(str.getBase, emptyString)
        }
      }
    } else {
      withResource(str.getBase.extractRe(regexp)) { table: Table =>
        table.getColumn(0).incRefCount()
      }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, strExpr.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      delim: GpuColumnVector,
      count: GpuColumnVector): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuScalar,
      delim: GpuColumnVector,
      count: GpuColumnVector): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuScalar,
      delim: GpuScalar,
      count: GpuColumnVector): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuScalar,
      delim: GpuColumnVector,
      count: GpuScalar): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuColumnVector,
      delim: GpuScalar,
      count: GpuColumnVector): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuColumnVector,
      delim: GpuColumnVector,
      count: GpuScalar): ColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")
}

trait BasePad extends GpuTernaryExpression with ImplicitCastInputTypes with NullIntolerant {
  val str: Expression
  val len: Expression
  val pad: Expression
  val direction: PadSide

  override def children: Seq[Expression] = str :: len :: pad :: Nil
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, IntegerType, StringType)

  override def doColumnar(str: GpuColumnVector, len: GpuScalar, pad: GpuScalar): ColumnVector = {
    if (len.isValid && pad.isValid) {
      val l = math.max(0, len.getValue.asInstanceOf[Int])
      val padStr = if (pad.isValid) {
        pad.getValue.asInstanceOf[UTF8String].toString
      } else {
        null
      }
      withResource(str.getBase.pad(l, direction, padStr)) { padded =>
        padded.substring(0, l)
      }
    } else {
      GpuColumnVector.columnVectorFromNull(str.getRowCount.toInt, StringType)
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, str.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      len: GpuColumnVector,
      pad: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      len: GpuColumnVector,
      pad: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(str: GpuScalar, len: GpuScalar, pad: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(str: GpuScalar, len: GpuColumnVector, pad: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      len: GpuScalar,
      pad: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      len: GpuColumnVector,
      pad: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")
}

case class GpuStringLPad(str: Expression, len: Expression, pad: Expression)
  extends BasePad {
  val direction = PadSide.LEFT
  override def prettyName: String = "lpad"

  def this(str: Expression, len: Expression) = {
    this(str, len, GpuLiteral(" ", StringType))
  }
}

case class GpuStringRPad(str: Expression, len: Expression, pad: Expression)
  extends BasePad {
  val direction = PadSide.RIGHT
  override def prettyName: String = "rpad"

  def this(str: Expression, len: Expression) = {
    this(str, len, GpuLiteral(" ", StringType))
  }
}

class GpuStringSplitMeta(
    expr: StringSplit,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends TernaryExprMeta[StringSplit](expr, conf, parent, rule) {
  import GpuOverrides._

  override def tagExprForGpu(): Unit = {
    val regexp = extractLit(expr.regex)
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
    if (!isLit(expr.limit)) {
      willNotWorkOnGpu("only literal limit is supported")
    }
  }
  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      limit: Expression): GpuExpression =
    GpuStringSplit(str, regexp, limit)
}

case class GpuStringSplit(str: Expression, regex: Expression, limit: Expression)
    extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = str :: regex :: limit :: Nil

  def this(exp: Expression, regex: Expression) = this(exp, regex, GpuLiteral(-1, IntegerType))

  override def prettyName: String = "split"

  override def doColumnar(str: GpuColumnVector, regex: GpuScalar,
      limit: GpuScalar): ColumnVector = {
    val intLimit = limit.getValue.asInstanceOf[Int]
    str.getBase.stringSplitRecord(regex.getBase, intLimit)
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, str.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuColumnVector,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuScalar,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuScalar,
      regex: GpuColumnVector,
      limit: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuScalar,
      limit: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: GpuScalar): ColumnVector =
    throw new IllegalStateException("This is not supported yet")
}
