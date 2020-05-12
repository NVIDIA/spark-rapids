/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import ai.rapids.spark.RapidsPluginImplicits._
import ai.rapids.cudf.{ColumnVector, Scalar}
import ai.rapids.spark.{GpuBinaryExpression, GpuColumnVector, GpuComplexTypeMergingExpression, GpuExpression, GpuLiteral,
  GpuScalar, GpuString2TrimExpression, GpuTernaryExpression, GpuUnaryExpression}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, Predicate}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ArrayBuffer

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

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector = {
    if (!val2.isValid()) {
      val zeroScalar = GpuScalar.from(0, IntegerType)
      try {
        GpuColumnVector.from(zeroScalar, val1.getRowCount().toInt)
      } finally {
        zeroScalar.close()
      }
    } else if (!val0.isValid()) { //if null substring // or null column? <-- needs to be looked for/tested
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
  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuStartsWith(left: GpuExpression, right: GpuExpression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL STARTSWITH ($listSQL))"
  }

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.startsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in StartsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here," +
    "Cannot have a scalar as left side operand in StartsWith")
}

case class GpuEndsWith(left: GpuExpression, right: GpuExpression)
  extends GpuBinaryExpression with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL ENDSWITH ($listSQL))"
  }

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.endsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in EndsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have a scalar as left side operand in EndsWith")
}

case class GpuStringTrim(column: GpuExpression, trimParameters: Option[GpuExpression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: GpuExpression = column

  override def trimStr:Option[GpuExpression] = trimParameters

  def this(trimParameters: GpuExpression, column: GpuExpression) = this(column, Option(trimParameters))

  def this(column: GpuExpression) = this(column, None)

  override protected def direction: String = "BOTH"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.strip(t))
  }
}

case class GpuStringTrimLeft(column: GpuExpression, trimParameters: Option[GpuExpression] = None)
  extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: GpuExpression = column

  override def trimStr:Option[GpuExpression] = trimParameters

  def this(trimParameters: GpuExpression, column: GpuExpression) = this(column, Option(trimParameters))

  def this(column: GpuExpression) = this(column, None)

  override protected def direction: String = "LEADING"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.lstrip(t))
  }
}

case class GpuStringTrimRight(column: GpuExpression, trimParameters: Option[GpuExpression] = None)
    extends GpuString2TrimExpression with ImplicitCastInputTypes {

  override def srcStr: GpuExpression = column

  override def trimStr:Option[GpuExpression] = trimParameters

  def this(trimParameters: GpuExpression, column: GpuExpression) = this(column, Option(trimParameters))

  def this(column: GpuExpression) = this(column, None)

  override protected def direction: String = "TRAILING"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector = {
    GpuColumnVector.from(column.getBase.rstrip(t))
  }
}

case class GpuConcat(children: Seq[GpuExpression]) extends GpuComplexTypeMergingExpression {
  override def dataType = StringType
  override def nullable: Boolean = children.head.nullable

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
      GpuColumnVector.from(ColumnVector.stringConcatenate(emptyStrScalar, nullStrScalar, columns.toArray[ColumnVector]))
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

case class GpuContains(left: GpuExpression, right: GpuExpression) extends GpuBinaryExpression
  with Predicate with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def sql: String = {
    val inputSQL = left.sql
    val listSQL = right.sql.toString
    s"($inputSQL CONTAINS ($listSQL))"
  }

  override def toString: String = s"gpucontains($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      val boolScalar = Scalar.fromBool(true)
      try {
        GpuColumnVector.from(ColumnVector.fromScalar(boolScalar, lhs.getRowCount.toInt))
      } finally {
        boolScalar.close()
      }
    } else {
      GpuColumnVector.from(lhs.getBase.stringContains(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in Contains")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here," +
    "Cannot have a scalar as left side operand in Contains")
}

case class GpuSubString(str: Expression, pos: Expression, len: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def dataType: DataType = str.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, BinaryType), IntegerType, IntegerType)

  override def children: Seq[Expression] = Seq(str, pos, len)

  def this(str: Expression, pos: Expression) = {
    this(str, pos, GpuLiteral(Integer.MAX_VALUE, IntegerType))
  }

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(column: GpuColumnVector, position: Scalar, length: Scalar): GpuColumnVector = {
    val substringPos = position.getInt
    val substringLen = length.getInt
    if (substringLen < 0) { // Spark returns empty string if length is negative
      GpuColumnVector.from(column.getBase.substring(0, 0))
    } else if (substringPos >= 0) { // If position is non negative
      if (substringPos == 0) {  // calculate substring from first character to length
        GpuColumnVector.from(column.getBase.substring(substringPos, substringLen))
      } else { // calculate substring from position to length
        GpuColumnVector.from(column.getBase.substring(substringPos - 1, substringPos + substringLen - 1))
      }
    } else { // If position is negative, evaluate from end.
      GpuColumnVector.from(column.getBase.substring(substringPos, Integer.MAX_VALUE))
    }
  }

  override def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: Scalar)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuInitCap(child: GpuExpression) extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(StringType)
  override def dataType: DataType = StringType
  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.toTitle)
  }
}

case class GpuStringReplace(srcExpr: GpuExpression, searchExpr: GpuExpression, replaceExpr: GpuExpression)
    extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def children: Seq[Expression] = Seq(srcExpr, searchExpr, replaceExpr)

  def this(srcExpr: GpuExpression, searchExpr: GpuExpression) = {
    this(srcExpr, searchExpr, GpuLiteral("", StringType))
  }

  override def doColumnar(strExpr: GpuColumnVector, searchExpr: GpuColumnVector, replaceExpr: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(strExpr: Scalar, searchExpr: GpuColumnVector, replaceExpr: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(strExpr: Scalar, searchExpr: Scalar, replaceExpr: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(strExpr: Scalar, searchExpr: GpuColumnVector, replaceExpr: Scalar): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(strExpr: GpuColumnVector, searchExpr: Scalar, replaceExpr: GpuColumnVector)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(strExpr: GpuColumnVector, searchExpr: Scalar, replaceExpr: Scalar) : GpuColumnVector = {
    if (searchExpr.getJavaString.isEmpty) { // Return original string if search string is empty
      GpuColumnVector.from(strExpr.getBase.asStrings())
    } else {
      GpuColumnVector.from(strExpr.getBase.stringReplace(searchExpr, replaceExpr))
    }
  }

  override def doColumnar(strExpr: GpuColumnVector, searchExpr: GpuColumnVector, replaceExpr: Scalar)
  : GpuColumnVector = throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

case class GpuLike(left: GpuExpression, right: GpuExpression, escapeChar: Char)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant  {

  def this(left: GpuExpression, right: GpuExpression) = this(left, right, '\\')

  override def toString: String = escapeChar match {
    case '\\' => s"$left gpulike $right"
    case c => s"$left gpulike $right ESCAPE '$c'"
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in Like")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
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
    * An invalid pattern will throw an [[IllegalArgumentException]].
    *
    * @param pattern the SQL pattern to convert
    * @param escapeChar the escape string contains one character.
    * @return the equivalent cuDF regular expression of the pattern
    */
  def escapeLikeRegex(pattern: String, escapeChar: Char): String = {
    val in = pattern.toIterator
    val out = new StringBuilder()

    val escapeForCudf = Seq('[', '^', '$', '.', '|', '?', '*','+', '(', ')', '\\', '{', '}')
    def fail(message: String) = throw new IllegalArgumentException(
      s"the pattern '$pattern' is invalid, $message")

    def cudfQuote(c: Character): String = c match {
      case chr if escapeForCudf.contains(chr) => "\\" + chr
      case chr => Character.toString(chr)
    }

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

