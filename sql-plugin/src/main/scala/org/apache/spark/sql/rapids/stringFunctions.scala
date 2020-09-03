/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, DType, PadSide, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, Predicate, StringSplit, SubstringIndex}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

abstract class GpuUnaryString2StringExpression extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StringType
}

case class GpuUpper(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"upper($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.upper())
}

case class GpuLower(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"lower($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.lower())
}

case class GpuLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"length($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.getCharLengths())
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
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(
      val0: Scalar,
      val1: GpuColumnVector,
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(
      val0: Scalar,
      val1: Scalar,
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector = {
    if (!val2.isValid()) {
      val zeroScalar = GpuScalar.from(0, IntegerType)
      try {
        GpuColumnVector.from(zeroScalar, val1.getRowCount().toInt)
      } finally {
        zeroScalar.close()
      }
    } else if (!val0.isValid()) {
      //if null substring // or null column? <-- needs to be looked for/tested
      val nullScalar = GpuScalar.from(null, IntegerType)
      try {
        GpuColumnVector.from(nullScalar, val1.getRowCount().toInt)
      } finally {
        nullScalar.close()
      }
    } else if (val2.getInt() < 1 || val0.getJavaString().isEmpty()) {
      val isNotNullColumn = val1.getBase.isNotNull()
      try {
        val nullScalar = GpuScalar.from(null, IntegerType)
        try {
          if (val2.getInt() >= 1) {
            val sv1 = GpuScalar.from(1, IntegerType)
            try {
              GpuColumnVector.from(isNotNullColumn.ifElse(sv1, nullScalar))
            } finally {
              sv1.close()
            }
          } else {
            val sv0 = GpuScalar.from(0, IntegerType)
            try {
              GpuColumnVector.from(isNotNullColumn.ifElse(sv0, nullScalar))
            } finally {
              sv0.close()
            }
          }
        } finally {
          nullScalar.close()
        }
      } finally {
        isNotNullColumn.close()
      }
    } else {
      val skewedResult = val1.getBase.stringLocate(val0, val2.getInt() - 1, -1)
      try {
        val sv1 = GpuScalar.from(1, IntegerType)
        try {
          GpuColumnVector.from(skewedResult.add(sv1))
        } finally {
          sv1.close()
        }
      } finally {
        skewedResult.close()
      }
    }
  }
  override def doColumnar(
      val0: GpuColumnVector,
      val1: Scalar,
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(
      val0: GpuColumnVector,
      val1: Scalar,
      val2: Scalar): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: Scalar): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuStartsWith(left: Expression, right: Expression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL STARTSWITH ($listSQL))"
  }

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector =
    GpuColumnVector.from(lhs.getBase.startsWith(rhs))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have two column vectors as input in StartsWith")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have a scalar as left side operand in StartsWith")
}

case class GpuEndsWith(left: Expression, right: Expression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL ENDSWITH ($listSQL))"
  }

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector =
    GpuColumnVector.from(lhs.getBase.endsWith(rhs))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException(
      "Really should not be here, cannot have two column vectors as input in EndsWith")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector =
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

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.strip(t))
  }
}

case class GpuStringTrimLeft(column: Expression, trimParameters: Option[Expression] = None)
  extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "LEADING"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.lstrip(t))
  }
}

case class GpuStringTrimRight(column: Expression, trimParameters: Option[Expression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: Expression = column

  override def trimStr:Option[Expression] = trimParameters

  def this(trimParameters: Expression, column: Expression) =
    this(column, Option(trimParameters))

  def this(column: Expression) = this(column, None)

  override protected def direction: String = "TRAILING"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.rstrip(t))
  }
}

case class GpuConcat(children: Seq[Expression]) extends GpuComplexTypeMergingExpression {
  override def dataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def columnarEval(batch: ColumnarBatch): Any = {
    var nullStrScalar: Scalar = null
    var emptyStrScalar: Scalar = null
    val rows = batch.numRows()
    val childEvals: ArrayBuffer[Any] = new ArrayBuffer[Any](children.length)
    val columns: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]()
    try {
      nullStrScalar = GpuScalar.from(null, StringType)
      children.foreach(childEvals += _.columnarEval(batch))
      childEvals.foreach {
        case vector: GpuColumnVector =>
          columns += vector.getBase
        case col => if (col == null) {
          columns += GpuColumnVector.from(nullStrScalar, rows).getBase
        } else {
          val stringScalar = GpuScalar.from(col.asInstanceOf[UTF8String].toString, StringType)
          try {
            columns += GpuColumnVector.from(stringScalar, rows).getBase
          } finally {
            stringScalar.close()
          }
        }
      }
      emptyStrScalar = GpuScalar.from("", StringType)
      GpuColumnVector.from(ColumnVector.stringConcatenate(emptyStrScalar, nullStrScalar,
        columns.toArray[ColumnVector]))
    } finally {
      columns.safeClose()
      if (emptyStrScalar != null) {
        emptyStrScalar.close()
      }
      if (nullStrScalar != null) {
        nullStrScalar.close()
      }
    }
  }
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

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector =
    GpuColumnVector.from(lhs.getBase.stringContains(rhs))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in Contains")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector =
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
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: Scalar,
      val1: GpuColumnVector,
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      val0: GpuColumnVector,
      val1: Scalar,
      val2: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(column: GpuColumnVector,
                          position: Scalar,
                          length: Scalar): GpuColumnVector = {
    val substringPos = position.getInt
    val substringLen = length.getInt
    if (substringLen < 0) { // Spark returns empty string if length is negative
      GpuColumnVector.from(column.getBase.substring(0, 0))
    } else if (substringPos >= 0) { // If position is non negative
      if (substringPos == 0) {  // calculate substring from first character to length
        GpuColumnVector.from(column.getBase.substring(substringPos, substringLen))
      } else { // calculate substring from position to length
        GpuColumnVector.from(
          column.getBase.substring(substringPos - 1, substringPos + substringLen - 1))
      }
    } else { // If position is negative, evaluate from end.
      GpuColumnVector.from(column.getBase.substring(substringPos, Integer.MAX_VALUE))
    }
  }

  override def doColumnar(
      val0: GpuColumnVector,
      val1: GpuColumnVector,
      val2: Scalar): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuInitCap(child: Expression) extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType
  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.toTitle)
  }
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
      replaceExpr: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: Scalar,
      searchExpr: GpuColumnVector,
      replaceExpr: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: Scalar,
      searchExpr: Scalar,
      replaceExpr: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: Scalar,
      searchExpr: GpuColumnVector,
      replaceExpr: Scalar): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: Scalar,
      replaceExpr: GpuColumnVector): GpuColumnVector =
        throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: Scalar,
      replaceExpr: Scalar) : GpuColumnVector = {
    // When search or replace string is null, return all nulls like the CPU does.
    if (!searchExpr.isValid || !replaceExpr.isValid) {
      withResource(GpuScalar.from(null, StringType)) { nullStrScalar =>
        return GpuColumnVector.from(nullStrScalar, strExpr.getRowCount.toInt)
      }
    }
    if (searchExpr.getJavaString.isEmpty) { // Return original string if search string is empty
      GpuColumnVector.from(strExpr.getBase.asStrings())
    } else {
      GpuColumnVector.from(strExpr.getBase.stringReplace(searchExpr, replaceExpr))
    }
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuColumnVector,
      replaceExpr: Scalar): GpuColumnVector =
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in Like")

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have a scalar as left side operand in Like")

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    val regexStr = escapeLikeRegex(rhs.getJavaString, escapeChar)
    GpuColumnVector.from(lhs.getBase.matchesRe(regexStr))
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
    rule: ConfKeysAndIncompat) extends TernaryExprMeta[SubstringIndex](expr, conf, parent, rule) {
  private var regexp: String = _

  override def tagExprForGpu(): Unit = {
    val delim = GpuOverrides.extractStringLit(expr.delimExpr).getOrElse{
      willNotWorkOnGpu("only literal parameters supported for deliminator")
      ""
    }

    if (delim == null || delim.length != 1) {
      willNotWorkOnGpu("only a single character deliminator is supported")
    }

    val count = GpuOverrides.extractLit(expr.countExpr)
    if (count.isEmpty) {
      willNotWorkOnGpu("only literal parameters supported for count")
    }
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
  override def doColumnar(str: GpuColumnVector, delim: Scalar, count: Scalar): GpuColumnVector = {
    if (regexp == null) {
      withResource(str.getBase.isNull) { isNull =>
        withResource(Scalar.fromString("")) { emptyString =>
          GpuColumnVector.from(isNull.ifElse(str.getBase, emptyString))
        }
      }
    } else {
      withResource(str.getBase.extractRe(regexp)) { table: Table =>
        GpuColumnVector.from(table.getColumn(0).incRefCount())
      }
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      delim: GpuColumnVector,
      count: GpuColumnVector): GpuColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: Scalar,
      delim: GpuColumnVector,
      count: GpuColumnVector): GpuColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: Scalar,
      delim: Scalar,
      count: GpuColumnVector): GpuColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: Scalar,
      delim: GpuColumnVector,
      count: Scalar): GpuColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuColumnVector,
      delim: Scalar,
      count: GpuColumnVector): GpuColumnVector =
        throw new IllegalStateException(
          "Internal Error: this version of substring index is not supported")

  override def doColumnar(
      str: GpuColumnVector,
      delim: GpuColumnVector,
      count: Scalar): GpuColumnVector =
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

  override def doColumnar(str: GpuColumnVector, len: Scalar, pad: Scalar): GpuColumnVector = {
    if (len.isValid && pad.isValid) {
      val l = math.max(0, len.getInt)
      withResource(str.getBase.pad(l, direction, pad.getJavaString)) { padded =>
        GpuColumnVector.from(padded.substring(0, l))
      }
    } else {
      withResource(Scalar.fromNull(DType.STRING)) { ns =>
        GpuColumnVector.from(ColumnVector.fromScalar(ns, str.getRowCount.toInt))
      }
    }
  }

  override def doColumnar(
      str: GpuColumnVector,
      len: GpuColumnVector,
      pad: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: Scalar,
      len: GpuColumnVector,
      pad: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(str: Scalar, len: Scalar, pad: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(str: Scalar, len: GpuColumnVector, pad: Scalar): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      len: Scalar,
      pad: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      len: GpuColumnVector,
      pad: Scalar): GpuColumnVector =
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
    rule: ConfKeysAndIncompat)
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

  // For now we support all of the possible input and output types for this operator
  override def areAllSupportedTypes(types: DataType*): Boolean = true
}

case class GpuStringSplit(str: Expression, regex: Expression, limit: Expression)
    extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = str :: regex :: limit :: Nil

  def this(exp: Expression, regex: Expression) = this(exp, regex, GpuLiteral(-1, IntegerType))

  override def prettyName: String = "split"

  override def doColumnar(str: GpuColumnVector, regex: Scalar, limit: Scalar): GpuColumnVector = {
    val intLimit = limit.getInt
    GpuColumnVector.from(str.getBase.stringSplitRecord(regex, intLimit))
  }

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: Scalar,
      regex: GpuColumnVector,
      limit: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: Scalar,
      regex: Scalar,
      limit: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: Scalar,
      regex: GpuColumnVector,
      limit: Scalar): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: Scalar,
      limit: GpuColumnVector): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")

  override def doColumnar(
      str: GpuColumnVector,
      regex: GpuColumnVector,
      limit: Scalar): GpuColumnVector =
    throw new IllegalStateException("This is not supported yet")
}