/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, PadSide, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, InputFileName, Literal, NullIntolerant, Predicate, RegExpExtract, RegExpExtractAll, RLike, StringSplit, StringToMap, SubstringIndex, TernaryExpression}
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

case class GpuBitLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"bit_length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.getByteCount) { byteCnt =>
      // bit count = byte count * 8
      withResource(GpuScalar.from(3, IntegerType)) { factor =>
        byteCnt.binaryOp(BinaryOp.SHIFT_LEFT, factor, DType.INT32)
      }
    }
  }
}

case class GpuOctetLength(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"octet_length($child)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.getByteCount
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

object GpuRegExpUtils {

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
  }

}

class GpuRLikeMeta(
    expr: RLike,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends BinaryExprMeta[RLike](expr, conf, parent, rule) {

    private var pattern: Option[String] = None

    override def tagExprForGpu(): Unit = {
      GpuRegExpUtils.tagForRegExpEnabled(this)
      expr.right match {
        case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
          try {
            // verify that we support this regex and can transpile it to cuDF format
            pattern = Some(new CudfRegexTranspiler(RegexFindMode).transpile(str.toString, None)._1)
          } catch {
            case e: RegexUnsupportedException =>
              willNotWorkOnGpu(e.getMessage)
          }
        case _ =>
          willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
      }
    }

    override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
      GpuRLike(lhs, rhs, pattern.getOrElse(
        throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern")))
    }
}

case class GpuRLike(left: Expression, right: Expression, pattern: String)
  extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant  {

  override def toString: String = s"$left gpurlike $right"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have two column vectors as input in RLike")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector =
    throw new IllegalStateException("Really should not be here, " +
      "Cannot have a scalar as left side operand in RLike")

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    lhs.getBase.containsRe(pattern)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
}

abstract class GpuRegExpTernaryBase extends GpuTernaryExpression {

  override def dataType: DataType = StringType

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
      searchExpr: GpuColumnVector,
      replaceExpr: GpuScalar): ColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, first.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
    }
  }

}

case class GpuRegExpReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression,
    cudfRegexPattern: String,
    cudfReplacementString: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

  def this(srcExpr: Expression, searchExpr: Expression, cudfRegexPattern: String,
      cudfReplacementString: String) = {
    this(srcExpr, searchExpr, GpuLiteral("", StringType), cudfRegexPattern, cudfReplacementString)
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    withResource(Scalar.fromString(cudfReplacementString)) { rep =>
      strExpr.getBase.replaceRegex(cudfRegexPattern, rep)
    }
  }

}

case class GpuRegExpReplaceWithBackref(
    override val child: Expression,
    cudfRegexPattern: String,
    cudfReplacementString: String)
  extends GpuUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def dataType: DataType = StringType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.getBase.stringReplaceWithBackrefs(cudfRegexPattern, cudfReplacementString)
  }

}

class GpuRegExpExtractMeta(
    expr: RegExpExtract,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtract](expr, conf, parent, rule) {

  private var pattern: Option[String] = None
  private var numGroups = 0

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    ShimLoader.getShimVersion match {
      case _: DatabricksShimVersion if expr.subject.isInstanceOf[InputFileName] =>
        willNotWorkOnGpu("avoiding Databricks Delta problem with regexp extract")
      case _ =>
    }

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
          pattern = Some(new CudfRegexTranspiler(RegexFindMode)
            .transpile(javaRegexpPattern, None)._1)
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

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtract(str, regexp, idx, cudfPattern)
  }
}

case class GpuRegExpExtract(
    subject: Expression,
    regexp: Expression,
    idx: Expression,
    cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = subject
  override def second: Expression = regexp
  override def third: Expression = idx

  override def prettyName: String = "regexp_extract"

  override def doColumnar(
      str: GpuColumnVector,
      regexp: GpuScalar,
      idx: GpuScalar): ColumnVector = {

    // When group index is 0, it means extract the entire pattern including those parts which
    // don't belong to any group. For instance,
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 0) => 123abcE
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 1) => 123
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 2) => E
    //
    // To support the full match (group index 0), we wrap a pair of parentheses on the original
    // cudfRegexPattern.
    val (extractPattern, groupIndex) = idx.getValue match {
      case i: Int if i == 0 =>
        ("(" + cudfRegexPattern + ")", 0)
      case i =>
        (cudfRegexPattern, i.asInstanceOf[Int] - 1)
    }

    // There are some differences in behavior between cuDF and Java so we have
    // to handle those cases here.
    //
    // Given the pattern `^([a-z]*)([0-9]*)([a-z]*)$` the following table
    // shows the value that would be extracted for group index 2 given a range
    // of inputs. The behavior is mostly consistent except for the case where
    // the input is non-null and does not match the pattern.
    //
    // | Input  | Java  | cuDF  |
    // |--------|-------|-------|
    // | ''     | ''    | ''    |
    // | NULL   | NULL  | NULL  |
    // | 'a1a'  | '1'   | '1'   |
    // | '1a1'  | ''    | NULL  |

    withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
      withResource(GpuScalar.from(null, DataTypes.StringType)) { nullString =>
        withResource(str.getBase.containsRe(cudfRegexPattern)) { matches =>
          withResource(str.getBase.isNull) { isNull =>
            withResource(str.getBase.extractRe(extractPattern)) { extract =>
              withResource(matches.ifElse(extract.getColumn(groupIndex), emptyString)) {
                isNull.ifElse(nullString, _)
              }
            }
          }
        }
      }
    }
  }
}

class GpuRegExpExtractAllMeta(
    expr: RegExpExtractAll,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtractAll](expr, conf, parent, rule) {

  private var pattern: Option[String] = None
  private var numGroups = 0

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    ShimLoader.getShimVersion match {
      case _: DatabricksShimVersion if expr.subject.isInstanceOf[InputFileName] =>
        willNotWorkOnGpu("avoiding Databricks Delta problem with regexp extract")
      case _ =>
    }

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
          pattern = Some(new CudfRegexTranspiler(RegexFindMode)
            .transpile(javaRegexpPattern, None)._1)
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

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtractAll(str, regexp, idx, cudfPattern)
  }
}

case class GpuRegExpExtractAll(
    subject: Expression,
    regexp: Expression,
    idx: Expression,
    cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = subject
  override def second: Expression = regexp
  override def third: Expression = idx

  override def prettyName: String = "regexp_extract_all"

  override def doColumnar(
      str: GpuColumnVector,
      regexp: GpuScalar,
      idx: GpuScalar): ColumnVector = {

    // When group index is 0, it means extract the entire pattern including those parts which
    // don't belong to any group. For instance,
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 0) => 123abcE
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 1) => 123
    // regexp_extract('123abcEfg', '([0-9]+)[a-z]+([A-Z])', 2) => E
    //
    // To support the full match (group index 0), we wrap a pair of parentheses on the original
    // cudfRegexPattern.
    val (extractPattern, groupIndex) = idx.getValue match {
      case i: Int if i == 0 =>
        ("(" + cudfRegexPattern + ")", 0)
      case i =>
        (cudfRegexPattern, i.asInstanceOf[Int] - 1)
    }

    // There are some differences in behavior between cuDF and Java so we have
    // to handle those cases here.
    //
    // Given the pattern `^([a-z]*)([0-9]*)([a-z]*)$` the following table
    // shows the value that would be extracted for group index 2 given a range
    // of inputs. The behavior is mostly consistent except for the case where
    // the input is non-null and does not match the pattern.
    //
    // | Input  | Java  | cuDF  |
    // |--------|-------|-------|
    // | ''     | ''    | ''    |
    // | NULL   | NULL  | NULL  |
    // | 'a1a'  | '1'   | '1'   |
    // | '1a1'  | ''    | NULL  |

    withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
      withResource(GpuScalar.from(null, DataTypes.StringType)) { nullString =>
        withResource(str.getBase.containsRe(cudfRegexPattern)) { matches =>
          withResource(str.getBase.isNull) { isNull =>
            withResource(str.getBase.extractRe(extractPattern)) { extract =>
              withResource(matches.ifElse(extract.getColumn(groupIndex), emptyString)) {
                isNull.ifElse(nullString, _)
              }
            }
          }
        }
      }
    }
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

abstract class StringSplitRegExpMeta[INPUT <: TernaryExpression](expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[INPUT](expr, conf, parent, rule) {
  import GpuOverrides._

  /**
   * Return the cudf transpiled regex pattern, and a boolean flag indicating whether the input
   * delimiter is really a regex patter or just a literal string.
   */
  def checkRegExp(delimExpr: Expression): Option[(String, Boolean)] = {
    var pattern: String = ""
    var isRegExp: Boolean = false

    val delim = extractLit(delimExpr)
    if (delim.isEmpty) {
      willNotWorkOnGpu("Only literal delimiter patterns are supported")
    } else {
      val utf8Str = delim.get.value.asInstanceOf[UTF8String]
      if (utf8Str == null) {
        willNotWorkOnGpu("Delimiter pattern is null")
      } else {
        if (utf8Str.numChars() == 0) {
          willNotWorkOnGpu("An empty delimiter pattern is not supported")
        }
        val transpiler = new CudfRegexTranspiler(RegexSplitMode)
        transpiler.transpileToSplittableString(utf8Str.toString) match {
          case Some(simplified) =>
            pattern = simplified
          case None =>
            try {
              pattern = transpiler.transpile(utf8Str.toString, None)._1
              isRegExp = true
            } catch {
              case e: RegexUnsupportedException =>
                willNotWorkOnGpu(e.getMessage)
            }
        }
      }
    }

    Some((pattern, isRegExp))
  }

  def throwUncheckedDelimiterException() =
    throw new IllegalStateException("Delimiter expression has not been checked for regex pattern")
}

class GpuStringSplitMeta(
    expr: StringSplit,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends StringSplitRegExpMeta[StringSplit](expr, conf, parent, rule) {
  import GpuOverrides._

  private var pattern = ""
  private var isRegExp = false

  override def tagExprForGpu(): Unit = {
    checkRegExp(expr.regex) match {
      case Some((p, isRe)) =>
        pattern = p
        isRegExp = isRe
      case _ => throwUncheckedDelimiterException()
    }
    
    // if this is a valid regular expression, then we should check the configuration
    // whether to run this on the GPU
    if (isRegExp) {
      GpuRegExpUtils.tagForRegExpEnabled(this)
    }

    extractLit(expr.limit) match {
      case Some(Literal(n: Int, _)) =>
        if (n == 0 || n == 1) {
          // https://github.com/NVIDIA/spark-rapids/issues/4720
          willNotWorkOnGpu("limit of 0 or 1 is not supported")
        }
      case _ =>
        willNotWorkOnGpu("only literal limit is supported")
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      limit: Expression): GpuExpression = {
    GpuStringSplit(str, regexp, limit, pattern, isRegExp)
  }
}

case class GpuStringSplit(str: Expression, regex: Expression, limit: Expression,
    pattern: String, isRegExp: Boolean)
  extends GpuTernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = ArrayType(StringType, containsNull = false)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = str
  override def second: Expression = regex
  override def third: Expression = limit

  override def prettyName: String = "split"

  override def doColumnar(str: GpuColumnVector, regex: GpuScalar,
      limit: GpuScalar): ColumnVector = {
    val intLimit = limit.getValue.asInstanceOf[Int]
    str.getBase.stringSplitRecord(pattern, intLimit, isRegExp)
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

class GpuStringToMapMeta(expr: StringToMap,
                         conf: RapidsConf,
                         parent: Option[RapidsMeta[_, _, _]],
                         rule: DataFromReplacementRule)
  extends StringSplitRegExpMeta[StringToMap](expr, conf, parent, rule) {

  private def checkFoldable(children: Seq[Expression]): Unit = {
    if (children.forall(_.foldable)) {
      willNotWorkOnGpu("result can be compile-time evaluated")
    }
  }

  private var pairDelimInfo: Option[(String, Boolean)] = None
  private var keyValueDelimInfo: Option[(String, Boolean)] = None

  override def tagExprForGpu(): Unit = {
    checkFoldable(expr.children)
    pairDelimInfo = checkRegExp(expr.pairDelim)
    keyValueDelimInfo = checkRegExp(expr.keyValueDelim)
  }

  override def convertToGpu(strExpr: Expression,
                            pairDelimExpr: Expression,
                            keyValueDelimExpr: Expression): GpuExpression = {
    val pairDelim: (String, Boolean) = pairDelimInfo.getOrElse(throwUncheckedDelimiterException())
    val keyValueDelim: (String, Boolean) = keyValueDelimInfo.getOrElse(
      throwUncheckedDelimiterException())

    GpuStringToMap(strExpr, pairDelimExpr, keyValueDelimExpr, pairDelim._1, pairDelim._2,
      keyValueDelim._1, keyValueDelim._2)
  }
}

case class GpuStringToMap(strExpr: Expression,
                          pairDelimExpr: Expression,
                          keyValueDelimExpr: Expression,
                          pairDelim: String, isPairDelimRegExp: Boolean,
                          keyValueDelim: String, isKeyValueDelimRegExp: Boolean)
  extends GpuExpression with ShimExpression with ExpectsInputTypes {
  override def dataType: MapType = MapType(StringType, StringType)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def prettyName: String = "str_to_map"
  override def children: Seq[Expression] = Seq(strExpr, pairDelimExpr, keyValueDelimExpr)
  override def nullable: Boolean = children.head.nullable
  override def foldable: Boolean = children.forall(_.foldable)

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(strExpr.columnarEval(batch)) {
      case strsCol: GpuColumnVector => toMap(strsCol)
      case str: GpuScalar =>
        withResource(GpuColumnVector.from(str, batch.numRows, str.dataType)) {
          strsCol => toMap(strsCol)
        }
      case v =>
        throw new UnsupportedOperationException(s"Unsupported data '($v: ${v.getClass} " +
          "for GpuStringToMap.")
    }
  }

  private def toMap(str: GpuColumnVector): GpuColumnVector = {
    // Firstly, split the input strings into lists of strings.
    withResource(str.getBase.stringSplitRecord(pairDelim, isPairDelimRegExp)) { listsOfStrings =>
      // Extract strings column from the output lists column.
      withResource(listsOfStrings.getChildColumnView(0)) { stringsCol =>
        // Split the key-value strings into pairs of strings of key-value (using limit = 2).
        withResource(stringsCol.stringSplit(keyValueDelim, 2, isKeyValueDelimRegExp)) {
          keysValuesTable =>

          def toMapFromValues(values: ColumnVector): GpuColumnVector = {
            // This code is safe, because the `keysValuesTable` always has at least one column
            // (guarantee by `cudf::strings::split` implementation).
            val keys = keysValuesTable.getColumn(0)

            // Zip the key-value pairs into structs.
            withResource(ColumnView.makeStructView(keys, values)) { structsCol =>
              // Make a lists column from the new structs column, which will have the same shape
              // as the previous lists of strings column.
              withResource(GpuListUtils.replaceListDataColumnAsView(listsOfStrings, structsCol)) {
                listsOfStructs =>
                  GpuCreateMap.createMapFromKeysValuesAsStructs(dataType, listsOfStructs)
              }
            }
          }

          // If the output from stringSplit has only one column (the map keys), we set all the
          // output values to nulls.
          if (keysValuesTable.getNumberOfColumns < 2) {
            withResource(GpuColumnVector.columnVectorFromNull(
              keysValuesTable.getRowCount.asInstanceOf[Int], StringType)) {
              allNulls => toMapFromValues(allNulls)
            }
          } else {
            toMapFromValues(keysValuesTable.getColumn(1))
          }
        }
      }
    }
  }
}
