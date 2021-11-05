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

import java.sql.SQLException

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, PadSide, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, Literal, NullIntolerant, Predicate, RLike, StringSplit, SubstringIndex}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
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
  override def first: Expression = substr
  override def second: Expression = col
  override def third: Expression = start

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

  val trimMethod = "gpuTrim"

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

  val trimMethod = "gpuTrimLeft"

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

  val trimMethod = "gpuTrimRight"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector =
    GpuColumnVector.from(column.getBase.rstrip(t), dataType)
}

case class GpuConcatWs(children: Seq[Expression])
    extends GpuExpression with ShimExpression with ImplicitCastInputTypes {
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  /** The 1st child (separator) is str, and rest are either str or array of str. */
  override def inputTypes: Seq[AbstractDataType] = {
    val arrayOrStr = TypeCollection(ArrayType(StringType), StringType)
    StringType +: Seq.fill(children.size - 1)(arrayOrStr)
  }

  private def concatArrayCol(colOrScalarSep: Any,
      cv: ColumnView): GpuColumnVector = {
    colOrScalarSep match {
      case sepScalar: GpuScalar =>
        withResource(GpuScalar.from("", StringType)) { emptyStrScalar =>
          GpuColumnVector.from(cv.stringConcatenateListElements(sepScalar.getBase, emptyStrScalar,
            false, false), dataType)
        }
      case sepVec: GpuColumnVector =>
        GpuColumnVector.from(cv.stringConcatenateListElements(sepVec.getBase), dataType)
    }
  }

  private def processSingleColScalarSep(cv: ColumnVector): GpuColumnVector = {
    // single column with scalar separator just replace any nulls with empty string
    withResource(Scalar.fromString("")) { emptyStrScalar =>
      GpuColumnVector.from(cv.replaceNulls(emptyStrScalar), dataType)
    }
  }

  private def stringConcatSeparatorScalar(columns: ArrayBuffer[ColumnVector],
      sep: GpuScalar): GpuColumnVector = {
    withResource(Scalar.fromString("")) { emptyStrScalar =>
      GpuColumnVector.from(ColumnVector.stringConcatenate(sep.getBase, emptyStrScalar,
        columns.toArray[ColumnView], false), dataType)
    }
  }

  private def stringConcatSeparatorVector(columns: ArrayBuffer[ColumnVector],
      sep: GpuColumnVector): GpuColumnVector = {
    // GpuOverrides doesn't allow only specifying a separator, you have to specify at
    // least one value column
    GpuColumnVector.from(ColumnVector.stringConcatenate(columns.toArray[ColumnView],
      sep.getBase), dataType)
  }

  private def resolveColumnVectorAndConcatArrayCols(
      expr: Expression,
      numRows: Int,
      colOrScalarSep: Any,
      batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(expr.columnarEval(batch)) {
      case vector: GpuColumnVector =>
        vector.dataType() match {
          case ArrayType(st: StringType, _) => concatArrayCol(colOrScalarSep, vector.getBase)
          case _ => vector.incRefCount()
        }
      case s: GpuScalar =>
        s.dataType match {
          case ArrayType(st: StringType, _) =>
            // we have to first concatenate any array types
            withResource(GpuColumnVector.from(s, numRows, s.dataType).getBase) { cv =>
              concatArrayCol(colOrScalarSep, cv)
            }
          case _ => GpuColumnVector.from(s, numRows, s.dataType)
        }
      case other =>
        throw new IllegalArgumentException(s"Cannot resolve a ColumnVector from the value:" +
          s" $other. Please convert it to a GpuScalar or a GpuColumnVector before returning.")
      }
  }

  private def checkScalarSeparatorNull(colOrScalarSep: Any,
      numRows: Int): Option[GpuColumnVector] = {
    colOrScalarSep match {
      case sepScalar: GpuScalar if (!sepScalar.getBase.isValid()) =>
        // if null scalar separator just return a column of all nulls
        Some(GpuColumnVector.from(sepScalar, numRows, dataType))
      case _ =>
        None
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val numRows = batch.numRows()
    withResourceIfAllowed(children.head.columnarEval(batch)) { colOrScalarSep =>
      // check for null scalar separator
      checkScalarSeparatorNull(colOrScalarSep, numRows).getOrElse {
        withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
          columns ++= children.tail.map {
            resolveColumnVectorAndConcatArrayCols(_, numRows, colOrScalarSep, batch).getBase
          }
          colOrScalarSep match {
            case sepScalar: GpuScalar =>
              if (columns.size == 1) {
                processSingleColScalarSep(columns.head)
              } else {
                stringConcatSeparatorScalar(columns, sepScalar)
              }
            case sepVec: GpuColumnVector  => stringConcatSeparatorVector(columns, sepVec)
          }
        }
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

  override def first: Expression = str
  override def second: Expression = pos
  override def third: Expression = len

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
    withResource(Scalar.fromString(" ")) { space =>
      // Spark only sees the space character as a word deliminator.
      input.getBase.capitalize(space)
    }
}

case class GpuStringRepeat(input: Expression, repeatTimes: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def left: Expression = input
  override def right: Expression = repeatTimes
  override def dataType: DataType = input.dataType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)

  def doColumnar(input: GpuScalar, repeatTimes: GpuColumnVector): ColumnVector = {
    assert(input.dataType == StringType)

    withResource(GpuColumnVector.from(input, repeatTimes.getRowCount.asInstanceOf[Int],
                 input.dataType)) {
      replicatedInput => doColumnar(replicatedInput, repeatTimes)
    }
  }

  def doColumnar(input: GpuColumnVector, repeatTimes: GpuColumnVector): ColumnVector = {
    val repeatTimesCV = repeatTimes.getBase

    // Compute the output size to check for overflow.
    withResource(input.getBase.repeatStringsSizes(repeatTimesCV)) { outputSizes =>
      if (outputSizes.getTotalSize > Int.MaxValue.asInstanceOf[Long]) {
        throw new RuntimeException("Output strings have total size exceed maximum allowed size")
      }

      // Finally repeat the strings using the pre-computed strings' sizes.
      input.getBase.repeatStrings(repeatTimesCV, outputSizes.getStringSizes)
    }
  }

  def doColumnar(input: GpuColumnVector, repeatTimes: GpuScalar): ColumnVector = {
    if (!repeatTimes.isValid) {
      // If the input scala repeatTimes is invalid, the results should be all nulls.
      withResource(Scalar.fromNull(DType.STRING)) {
        nullString => ColumnVector.fromScalar(nullString, input.getRowCount.asInstanceOf[Int])
      }
    } else {
      assert(repeatTimes.dataType == IntegerType)
      val repeatTimesVal = repeatTimes.getBase.getInt

      // Get the input size to check for overflow for the output.
      // Note that this is not an accurate check since the total buffer size of the input
      // strings column may be larger than the total length of strings that will be repeated in
      // this function.
      val inputBufferSize: Long = Option(input.getBase.getData).map(_.getLength).getOrElse(0)
      if (repeatTimesVal > 0 && inputBufferSize > Int.MaxValue / repeatTimesVal) {
        throw new RuntimeException("Output strings have total size exceed maximum allowed size")
      }

      // Finally repeat the strings.
      input.getBase.repeatStrings(repeatTimesVal)
    }
  }

  def doColumnar(numRows: Int, input: GpuScalar, repeatTimes: GpuScalar): ColumnVector = {
    assert(input.dataType == StringType)

    if (!repeatTimes.isValid) {
      // If the input scala repeatTimes is invalid, the results should be all nulls.
      withResource(Scalar.fromNull(DType.STRING)) {
        nullString => ColumnVector.fromScalar(nullString, numRows)
      }
    } else {
      assert(repeatTimes.dataType == IntegerType)
      val repeatTimesVal = repeatTimes.getBase.getInt

      withResource(input.getBase.repeatString(repeatTimesVal)) {
        repeatedString => ColumnVector.fromScalar(repeatedString, numRows)
      }
    }
  }

}

case class GpuStringReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

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

class GpuRLikeMeta(
    expr: RLike,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends BinaryExprMeta[RLike](expr, conf, parent, rule) {

    override def tagExprForGpu(): Unit = {
      expr.right match {
        case Literal(str: UTF8String, _) =>
          try {
            // verify that we support this regex and can transpile it to cuDF format
            new CudfRegexTranspiler().transpile(str.toString)
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        case _ =>
          willNotWorkOnGpu(s"RLike with non-literal pattern is not supported on GPU")
      }
    }

    override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
      GpuRLike(lhs, rhs)
}

/**
 * Regular expression parser based on a Pratt Parser design.
 *
 * The goal of this parser is to build a minimal AST that allows us
 * to validate that we can support the expression on the GPU. The goal
 * is not to parse with the level of detail that would be required if
 * we were building an evaluation engine. For example, operator precedence is
 * largely ignored but could be added if we need it later.
 *
 * The Java and cuDF regular expression documentation has been used as a reference:
 *
 * Java regex: https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 * cuDF regex: https://docs.rapids.ai/api/libcudf/stable/md_regex.html
 *
 * The following blog posts provide some background on Pratt Parsers and parsing regex.
 *
 * - https://journal.stuffwithstuff.com/2011/03/19/pratt-parsers-expression-parsing-made-easy/
 * - https://matt.might.net/articles/parsing-regex-with-recursive-descent/
 */
class RegexParser(pattern: String) {

  /** index of current position within the string being parsed */
  private var i = 0

  def parse(): RegexAST = {
    val ast = parseInternal()
    if (!eof()) {
      throw new RegexUnsupportedException("failed to parse full regex")
    }
    ast
  }

  private def parseInternal(): RegexAST = {
    val term = parseTerm(() => peek().contains('|'))
    if (!eof() && peek().contains('|')) {
      consumeExpected('|')
      RegexChoice(term, parseInternal())
    } else {
      term
    }
  }

  private def parseTerm(until: () => Boolean): RegexAST = {
    val sequence = RegexSequence(new ListBuffer())
    while (!eof() && !until()) {
      parseFactor() match {
        case RegexSequence(parts) =>
          sequence.parts ++= parts
        case other =>
          sequence.parts += other
      }
    }
    sequence
  }

  private def isValidQuantifierAhead(): Boolean = {
    if (peek().contains('{')) {
      val bookmark = i
      consumeExpected('{')
      val q = parseQuantifierOrLiteralBrace()
      i = bookmark
      q match {
        case _: QuantifierFixedLength | _: QuantifierVariableLength => true
        case _ => false
      }
    } else {
      false
    }
  }

  private def parseFactor(): RegexAST = {
    var base = parseBase()
    while (!eof() && (peek().exists(ch => ch == '*' || ch == '+' || ch == '?')
        || isValidQuantifierAhead())) {

      if (peek().contains('{')) {
        consumeExpected('{')
        base = RegexRepetition(base, parseQuantifierOrLiteralBrace().asInstanceOf[RegexQuantifier])
      } else {
        base = RegexRepetition(base, SimpleQuantifier(consume()))
      }
    }
    base
  }

  private def parseBase(): RegexAST = {
    consume() match {
      case '(' =>
        parseGroup()
      case '[' =>
        parseCharacterClass()
      case '\\' =>
        parseEscapedCharacter()
      case '\u0000' =>
        throw new RegexUnsupportedException(
          "cuDF does not support null characters in regular expressions", Some(i))
      case other =>
        RegexChar(other)
    }
  }

  private def parseGroup(): RegexAST = {
    val term = parseTerm(() => peek().contains(')'))
    consumeExpected(')')
    RegexGroup(term)
  }

  private def parseCharacterClass(): RegexCharacterClass = {
    val start = i
    val characterClass = RegexCharacterClass(negated = false, characters = ListBuffer())
    // loop until the end of the character class or EOF
    var characterClassComplete = false
    while (!eof() && !characterClassComplete) {
      val ch = consume()
      ch match {
        case '[' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case ']' =>
          characterClassComplete = true
        case '^' if i == start + 1 =>
          // Negates the character class, causing it to match a single character not listed in
          // the character class. Only valid immediately after the opening '['
          characterClass.negated = true
        case '\n' | '\r' | '\t' | '\b' | '\f' | '\007' =>
          // treat as a literal character and add to the character class
          characterClass.append(ch)
        case '\\' =>
          peek() match {
            case None =>
              throw new RegexUnsupportedException(
                s"unexpected EOF while parsing escaped character", Some(i))
            case Some(ch) =>
              ch match {
                case '\\' | '^' | '-' | ']' | '+' =>
                  // escaped metacharacter within character class
                  characterClass.appendEscaped(consumeExpected(ch))
              }
          }
        case '\u0000' =>
          throw new RegexUnsupportedException(
            "cuDF does not support null characters in regular expressions", Some(i))
        case _ =>
          // check for range
          val start = ch
          peek() match {
            case Some('-') =>
              consumeExpected('-')
              peek() match {
                case Some(']') =>
                  // '-' at end of class e.g. "[abc-]"
                  characterClass.append(ch)
                  characterClass.append('-')
                case Some(end) =>
                  skip()
                  characterClass.appendRange(start, end)
              }
            case _ =>
              // treat as supported literal character
              characterClass.append(ch)
          }
      }
    }
    if (!characterClassComplete) {
      throw new RegexUnsupportedException(
        s"unexpected EOF while parsing character class", Some(i))
    }
    characterClass
  }


  /**
   * Parse a quantifier in one of the following formats:
   *
   * {n}
   * {n,}
   * {n,m} (only valid if m >= n)
   */
  private def parseQuantifierOrLiteralBrace(): RegexAST = {

    // assumes that '{' has already been consumed
    val start = i

    def treatAsLiteralBrace() = {
      // this was not a quantifier, just a literal '{'
      i = start + 1
      RegexChar('{')
    }

    consumeInt match {
      case Some(minLength) =>
        peek() match {
          case Some(',') =>
            consumeExpected(',')
            val max = consumeInt()
            if (peek().contains('}')) {
              consumeExpected('}')
              max match {
                case None =>
                  QuantifierVariableLength(minLength, None)
                case Some(m) =>
                  if (m >= minLength) {
                    QuantifierVariableLength(minLength, max)
                  } else {
                    treatAsLiteralBrace()
                  }
              }
            } else {
              treatAsLiteralBrace()
            }
          case Some('}') =>
            consumeExpected('}')
            QuantifierFixedLength(minLength)
          case _ =>
            treatAsLiteralBrace()
        }
      case None =>
        treatAsLiteralBrace()
    }
  }

  private def parseEscapedCharacter(): RegexAST = {
    peek() match {
      case None =>
        throw new RegexUnsupportedException("escape at end of string", Some(i))
      case Some(ch) =>
        consumeExpected(ch)
        ch match {
          case 'A' | 'Z' =>
            // BOL / EOL anchors
            RegexEscaped(ch)
          case 's' | 'S' | 'd' | 'D' | 'w' | 'W' =>
            // meta sequences
            RegexEscaped(ch)
          case 'B' | 'b' =>
            // word boundaries
            RegexEscaped(ch)
          case '[' | '\\' | '^' | '$' | '.' | '⎮' | '?' | '*' | '+' | '(' | ')' | '{' | '}' =>
            // escaped metacharacter
            RegexEscaped(ch)
          case 'x' =>
            parseHexDigit
          case _ if ch.isDigit =>
            parseOctalDigit
          case other =>
            throw new RegexUnsupportedException(
              s"invalid or unsupported escape character '$other'", Some(i - 1))
        }
    }
  }

  private def parseHexDigit: RegexAST = {

    def isHexDigit(ch: Char): Boolean = ch.isDigit ||
      (ch >= 'a' && ch <= 'f') ||
      (ch >= 'A' && ch <= 'F')

    if (i + 1 < pattern.length
        && isHexDigit(pattern.charAt(i))
        && isHexDigit(pattern.charAt(i+1))) {
      val hex = pattern.substring(i, i + 2)
      i += 2
      RegexHexChar(hex)
    } else {
      throw new RegexUnsupportedException(
        "Invalid hex digit", Some(i))
    }
  }

  private def parseOctalDigit = {

    if (i + 2 < pattern.length
        && pattern.charAt(i).isDigit
        && pattern.charAt(i+1).isDigit
        && pattern.charAt(i+2).isDigit) {
      val hex = pattern.substring(i, i + 2)
      i += 3
      RegexOctalChar(hex)
    } else {
      throw new RegexUnsupportedException(
        "Invalid octal digit", Some(i))
    }
  }

  /** Determine if we are at the end of the input */
  private def eof(): Boolean = i == pattern.length

  /** Advance the index by one */
  private def skip(): Unit = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(i))
    }
    i += 1
  }

  /** Get the next character and advance the index by one */
  private def consume(): Char = {
    if (eof()) {
      throw new RegexUnsupportedException("Unexpected EOF", Some(i))
    } else {
      i += 1
      pattern.charAt(i - 1)
    }
  }

  /** Consume the next character if it is the one we expect */
  private def consumeExpected(expected: Char): Char = {
    val consumed = consume()
    if (consumed != expected) {
      throw new RegexUnsupportedException(
        s"Expected '$expected' but found '$consumed'", Some(i-1))
    }
    consumed
  }

  /** Peek at the next character without consuming it */
  private def peek(): Option[Char] = {
    if (eof()) {
      None
    } else {
      Some(pattern.charAt(i))
    }
  }

  private def consumeInt(): Option[Int] = {
    val start = i
    while (!eof() && peek().exists(_.isDigit)) {
      skip()
    }
    if (start == i) {
      None
    } else {
      Some(pattern.substring(start, i).toInt)
    }
  }

}

/**
 * Transpile Java/Spark regular expression to a format that cuDF supports, or throw an exception
 * if this is not possible.
 */
class CudfRegexTranspiler {
  
  val nothingToRepeat = "nothing to repeat"

  def transpile(pattern: String): String = {
    // parse the source regular expression
    val regex = new RegexParser(pattern).parse()
    // validate that the regex is supported by cuDF
    validate(regex)
    // write out to regex string, performing minor transformations
    // such as adding additional escaping
    regex.toRegexString
  }

  private def validate(regex: RegexAST): Unit = {
    regex match {
      case RegexGroup(RegexSequence(parts)) if parts.isEmpty =>
        // example: "()"
        throw new RegexUnsupportedException("cuDF does not support empty groups")
      case RegexRepetition(RegexEscaped(_), _) =>
        // example: "\B?"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexRepetition(RegexChar(a), _) if "$^".contains(a) =>
        // example: "$*"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexRepetition(RegexRepetition(_, _), _) =>
        // example: "a*+"
        throw new RegexUnsupportedException(nothingToRepeat)
      case RegexChoice(l, r) =>
        (l, r) match {
          // check for empty left-hand side caused by ^ or $ or a repetition
          case (RegexSequence(a), _) =>
            a.lastOption match {
              case None =>
                // example: "|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexChar(ch)) if ch == '$' || ch == '^' =>
                // example: "^|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexRepetition(_, _)) =>
                // example: "a*|a"
                throw new RegexUnsupportedException(nothingToRepeat)
              case _ =>
            }
          // check for empty right-hand side caused by ^ or $
          case (_, RegexSequence(b)) =>
            b.headOption match {
              case None =>
                // example: "|b"
                throw new RegexUnsupportedException(nothingToRepeat)
              case Some(RegexChar(ch)) if ch == '$' || ch == '^' =>
                // example: "a|$"
                throw new RegexUnsupportedException(nothingToRepeat)
              case _ =>
            }
          case (RegexRepetition(_, _), _) =>
            // example: "a*|a"
            throw new RegexUnsupportedException(nothingToRepeat)
          case _ =>
        }

      case RegexSequence(parts) =>
        if (isRegexChar(parts.head, '|') || isRegexChar(parts.last, '|')) {
          // examples: "a|", "|b"
          throw new RegexUnsupportedException(nothingToRepeat)
        }
        if (isRegexChar(parts.head, '{')) {
          // example: "{"
          // cuDF would treat this as a quantifier even though in this
          // context (being at the start of a sequence) it is not quantifying anything
          // note that we could choose to escape this in the transpiler rather than
          // falling back to CPU
          throw new RegexUnsupportedException(nothingToRepeat)
        }
        parts.foreach {
          case RegexEscaped(ch) if ch == 'b' || ch == 'B' =>
            // example: "a\Bb"
            // this needs further analysis to determine why words boundaries behave
            // differently between Java and cuDF
            throw new RegexUnsupportedException("word boundaries are not supported")
          case _ =>
        }
      case RegexCharacterClass(_, characters) =>
        characters.foreach {
          case RegexChar(ch) if ch == '[' || ch == ']' =>
            // examples:
            // - "[a[]" should match the literal characters "a" and "["
            // - "[a-b[c-d]]" is supported by Java but not cuDF
            throw new RegexUnsupportedException("nested character classes are not supported")
          case _ =>
        }

      case _ =>
    }

    // walk down the tree and validate children
    regex.children().foreach(validate)
  }

  private def isRegexChar(expr: RegexAST, value: Char): Boolean = expr match {
    case RegexChar(ch) => ch == value
    case _ => false
  }
}

sealed trait RegexAST {
  def children(): Seq[RegexAST]
  def toRegexString: String
}

sealed case class RegexSequence(parts: ListBuffer[RegexAST]) extends RegexAST {
  override def children(): Seq[RegexAST] = parts
  override def toRegexString: String = parts.map(_.toRegexString).mkString
}

sealed case class RegexGroup(term: RegexAST) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(term)
  override def toRegexString: String = s"(${term.toRegexString})"
}

sealed case class RegexChoice(a: RegexAST, b: RegexAST) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(a, b)
  override def toRegexString: String = s"${a.toRegexString}|${b.toRegexString}"
}

sealed case class RegexRepetition(a: RegexAST, quantifier: RegexQuantifier) extends RegexAST {
  override def children(): Seq[RegexAST] = Seq(a)
  override def toRegexString: String = s"${a.toRegexString}${quantifier.toRegexString}"
}

sealed trait RegexQuantifier extends RegexAST

sealed case class SimpleQuantifier(ch: Char) extends RegexQuantifier {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = ch.toString
}

sealed case class QuantifierFixedLength(length: Int)
    extends RegexQuantifier {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    s"{$length}"
  }
}

sealed case class QuantifierVariableLength(minLength: Int, maxLength: Option[Int])
    extends RegexQuantifier{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = {
    maxLength match {
      case Some(max) =>
        s"{$minLength,$max}"
      case _ =>
        s"{$minLength,}"
    }
  }
}

sealed trait RegexCharacterClassComponent extends RegexAST

sealed case class RegexHexChar(a: String) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\x$a"
}

sealed case class RegexOctalChar(a: String) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexChar(a: Char) extends RegexCharacterClassComponent {
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"$a"
}

sealed case class RegexEscaped(a: Char) extends RegexCharacterClassComponent{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"\\$a"
}

sealed case class RegexCharacterRange(start: Char, end: Char)
    extends RegexCharacterClassComponent{
  override def children(): Seq[RegexAST] = Seq.empty
  override def toRegexString: String = s"$start-$end"
}

sealed case class RegexCharacterClass(
      var negated: Boolean,
      var characters: ListBuffer[RegexCharacterClassComponent])
    extends RegexAST {

  override def children(): Seq[RegexAST] = characters
  def append(ch: Char): Unit = {
    characters += RegexChar(ch)
  }

  def appendEscaped(ch: Char): Unit = {
    characters += RegexEscaped(ch)
  }

  def appendRange(start: Char, end: Char): Unit = {
    characters += RegexCharacterRange(start, end)
  }

  override def toRegexString: String = {
    val builder = new StringBuilder("[")
    if (negated) {
      builder.append("^")
    }
    for (a <- characters) {
      a match {
        case RegexChar(ch) if requiresEscaping(ch) =>
          // cuDF has stricter escaping requirements for certain characters
          // within a character class compared to Java or Python regex
          builder.append(s"\\$ch")
        case other =>
          builder.append(other.toRegexString)
      }
    }
    builder.append("]")
    builder.toString()
  }

  private def requiresEscaping(ch: Char): Boolean = {
    // there are likely other cases that we will need to add here but this
    // covers everything we have seen so far during fuzzing
    ch match {
      case '-' =>
        // cuDF requires '-' to be escaped when used as a character within a character
        // to disambiguate from the character range syntax 'a-b'
        true
      case _ =>
        false
    }
  }
}

class RegexUnsupportedException(message: String, index: Option[Int] = None)
    extends SQLException {
  override def getMessage: String = {
    index match {
      case Some(i) => s"$message at index $index"
      case _ => message
    }
  }
}

case class GpuRLike(left: Expression, right: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant  {

  override def toString: String = s"$left gpurlike $right"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in RLike")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have a scalar as left side operand in RLike")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val pattern = if (rhs.isValid) {
      rhs.getValue.asInstanceOf[UTF8String].toString
    } else {
      throw new IllegalStateException("Really should not be here, " +
        "Cannot have an invalid scalar value as right side operand in RLike")
    }
    try {
      val cudfRegex = new CudfRegexTranspiler().transpile(pattern)
      lhs.getBase.containsRe(cudfRegex)
    } catch {
      case _: RegexUnsupportedException =>
        throw new IllegalStateException("Really should not be here, " +
          "regular expression should have been verified during tagging")
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
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

  override def first: Expression = strExpr
  override def second: Expression = ignoredDelimExpr
  override def third: Expression = ignoredCountExpr

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

  override def first: Expression = str
  override def second: Expression = len
  override def third: Expression = pad

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
  override def first: Expression = str
  override def second: Expression = regex
  override def third: Expression = limit

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
