/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import java.nio.charset.Charset
import java.text.DecimalFormatSymbols
import java.util.{Locale, Optional}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, BinaryOperable, CaptureGroups, ColumnVector, ColumnView, DType, PadSide, RegexProgram, RoundMode, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.shims.{ShimExpression, SparkShimImpl}

import org.apache.spark.sql.catalyst.expressions._
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
  extends GpuTernaryExpressionArgsScalarAnyScalar
      with ImplicitCastInputTypes {

  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = substr
  override def second: Expression = col
  override def third: Expression = start

  def this(substr: Expression, col: Expression) = {
    this(substr, col, GpuLiteral(1, IntegerType))
  }

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

}

case class GpuStartsWith(left: Expression, right: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with Predicate
      with ImplicitCastInputTypes
      with NullIntolerant {

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
}

case class GpuEndsWith(left: Expression, right: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with Predicate
      with ImplicitCastInputTypes
      with NullIntolerant {

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
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.strip(t), dataType)
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

  val trimMethod = "gpuTrimLeft"

  override def strippedColumnVector(column: GpuColumnVector, t: Scalar): GpuColumnVector =
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.lstrip(t), dataType)
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

  val trimMethod = "gpuTrimRight"

  override def strippedColumnVector(column:GpuColumnVector, t:Scalar): GpuColumnVector =
    if (column.getBase.getData == null) {
      GpuColumnVector.from(column.getBase.incRefCount, dataType)
    } else {
      GpuColumnVector.from(column.getBase.rstrip(t), dataType)
    }
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
    withResourceIfAllowed(expr.columnarEvalAny(batch)) {
      case vector: GpuColumnVector =>
        vector.dataType() match {
          case ArrayType(_: StringType, _) => concatArrayCol(colOrScalarSep, vector.getBase)
          case _ => vector.incRefCount()
        }
      case s: GpuScalar =>
        s.dataType match {
          case ArrayType(_: StringType, _) =>
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

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    withResourceIfAllowed(children.head.columnarEvalAny(batch)) { colOrScalarSep =>
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

case class GpuContains(left: Expression, right: Expression)
    extends GpuBinaryExpressionArgsAnyScalar
        with Predicate
        with ImplicitCastInputTypes
        with NullIntolerant {

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

  private[this] def computeStarts(strs: ColumnView, poses: ColumnView): ColumnVector = {
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    // cudf `substring(column, column, column)` treats negative start always as 0, so
    // need to do the similar calculation as CPU here.

    // 1) pos + str_size
    val negConvertedPoses = withResource(strs.getCharLengths) { strSizes =>
      poses.add(strSizes, DType.INT32)
    }
    withResource(negConvertedPoses) { _ =>
      withResource(Scalar.fromInt(0)) { zero =>
        // 2) (pos > 0) ? pos - 1 : 0
        val subOnePoses = withResource(Scalar.fromInt(1)) { one =>
          poses.sub(one, DType.INT32)
        }
        val zeroBasedPoses = withResource(subOnePoses) { _ =>
          withResource(poses.greaterThan(zero)) { posPosFlags =>
            // Use "poses" here instead of "zero" as the false path to keep the nulls,
            // since "ifElse" will erase the null mask of "poses".
            posPosFlags.ifElse(subOnePoses, poses)
          }
        }

        withResource(zeroBasedPoses) { _ =>
          withResource(poses.lessThan(zero)) { negPosFlags =>
            negPosFlags.ifElse(negConvertedPoses, zeroBasedPoses)
          }
        }
      }
    }
  }

  private[this] def computeEnds(starts: BinaryOperable, lens: BinaryOperable): ColumnVector = {
    // CPU:
    //     end = start + length
    //   , along with integer overflow check
    val endLongCol = withResource(starts.add(lens, DType.INT64)) { endColAsLong =>
      // If (end < 0), end = 0, let cudf return empty string.
      // If (end > Int.MaxValue), end = Int.MaxValue, let cudf return string
      //   from start until the string end.
      // To align with the CPU's behavior.
      withResource(Scalar.fromLong(0L)) { zero =>
        withResource(Scalar.fromLong(Int.MaxValue.toLong)) { maxInt =>
          endColAsLong.clamp(zero, maxInt)
        }
      }
    }
    withResource(endLongCol) { _ =>
      endLongCol.castTo(DType.INT32)
    }
  }

  private[this] def substringColumn(strs: ColumnView, starts: ColumnView,
      ends: ColumnView): ColumnVector = {
    // cudf does not allow nulls in starts and ends.
    val noNullStarts = new ColumnView(starts.getType, starts.getRowCount, Optional.of(0L),
      starts.getData, null)
    withResource(noNullStarts) { _ =>
      val noNullEnds = new ColumnView(ends.getType, ends.getRowCount, Optional.of(0L),
        ends.getData, null)
      withResource(noNullEnds) { _ =>
        // Spark returns null if any of (str, pos, len) is null, and `ends`'s null mask
        // should already cover pos and len.
        withResource(strs.mergeAndSetValidity(BinaryOp.BITWISE_AND, strs, ends)) { rets =>
          rets.substring(noNullStarts, noNullEnds)
        }
      }
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posCol: GpuColumnVector,
      lenCol: GpuColumnVector): ColumnVector = {
    val strs = strCol.getBase
    val poses = posCol.getBase
    val lens = lenCol.getBase
    withResource(computeStarts(strs, poses)) { starts =>
      withResource(computeEnds(starts, lens)) { ends =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar(strS: GpuScalar, posCol: GpuColumnVector,
      lenCol: GpuColumnVector): ColumnVector = {
    val numRows = posCol.getRowCount.toInt
    withResource(GpuColumnVector.from(strS, numRows, strS.dataType)) { strCol =>
      doColumnar(strCol, posCol, lenCol)
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posS: GpuScalar,
      lenCol: GpuColumnVector): ColumnVector = {
    val strs = strCol.getBase
    val lens = lenCol.getBase
    val pos = posS.getValue.asInstanceOf[Int]
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    val starts = if (pos < 0) {
      withResource(strs.getCharLengths) { strSizes =>
        withResource(Scalar.fromInt(pos)) { posS =>
          posS.add(strSizes, DType.INT32)
        }
      }
    } else { // pos >= 0
      val start = if (pos > 0) pos - 1 else 0
      withResource(Scalar.fromInt(start)) { startS =>
        ColumnVector.fromScalar(startS, strs.getRowCount.toInt)
      }
    }

    withResource(starts) { _ =>
      withResource(computeEnds(starts, lens)) { ends =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar (strS: GpuScalar, posS: GpuScalar,
      lenCol: GpuColumnVector): ColumnVector = {
    val strValue = strS.getValue.asInstanceOf[UTF8String]
    val pos = posS.getValue.asInstanceOf[Int]
    val lens = lenCol.getBase
    val numRows = lenCol.getRowCount.toInt
    // CPU:
    //     start = (pos < 0) ? pos + str_size : ((pos > 0) ? pos - 1 : 0)
    val start = if (pos < 0) {
      pos + strValue.numChars()
    } else if (pos > 0) {
      pos - 1
    } else 0

    val starts = withResource(Scalar.fromInt(start)) { startS =>
      ColumnVector.fromScalar(startS, numRows)
    }

    withResource(starts) { _ =>
      withResource(computeEnds(starts, lens)) { ends =>
        withResource(ColumnVector.fromScalar(strS.getBase, numRows)) { strs =>
          substringColumn(strs, starts, ends)
        }
      }
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posCol: GpuColumnVector,
      lenS: GpuScalar): ColumnVector = {
    val strs = strCol.getBase
    val poses = posCol.getBase
    val numRows =  strCol.getRowCount.toInt
    withResource(computeStarts(strs, poses)) { starts =>
      val ends = withResource(ColumnVector.fromScalar(lenS.getBase, numRows)) { lens =>
        computeEnds(starts, lens)
      }
      withResource(ends) { _ =>
        substringColumn(strs, starts, ends)
      }
    }
  }

  override def doColumnar(strS: GpuScalar, posCol: GpuColumnVector,
      lenS: GpuScalar): ColumnVector = {
    val numRows = posCol.getRowCount.toInt
    withResource(GpuColumnVector.from(strS, numRows, strS.dataType)) { strCol =>
      doColumnar(strCol, posCol, lenS)
    }
  }

  override def doColumnar(strCol: GpuColumnVector, posS: GpuScalar,
      lenS: GpuScalar): ColumnVector = {
    val strs = strCol.getBase
    val pos = posS.getValue.asInstanceOf[Int]
    val len = lenS.getValue.asInstanceOf[Int]
    val (start, endOpt) = if (len <= 0) {
      // Spark returns empty string if length is negative or zero
      (0, Some(0))
    } else if (pos > 0) {
      // 1-based index, convert to 0-based index
      val head = pos - 1
      val tail = if (head.toLong + len > Int.MaxValue) Int.MaxValue else head + len
      (head, Some(tail))
    } else if (pos == 0) {
      // 0-based index, calculate substring from 0 to length
      (0, Some(len))
    } else if (pos + len < 0) {
      // Drop the last "abs(substringPos + substringLen)" chars.
      // e.g.
      //    >> substring("abc", -3, 1)
      //    >> "a"  // dropping the last 2 [= abs(-3+1)] chars.
      // `pos + len` does not overflow as `pos < 0 && len > 0` here.
      (pos, Some(pos + len))
    } else { // pos + len >= 0
      // Read from start until the end.
      // e.g. `substring("abc", -3, 4)` outputs "abc".
      (pos, None)
    }
    endOpt.map(strs.substring(start, _)).getOrElse(strs.substring(start))
  }

  override def doColumnar(numRows: Int, strS: GpuScalar, posS: GpuScalar,
      lenS: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(strS, numRows, strS.dataType)) { strCol =>
      doColumnar(strCol, posS, lenS)
    }
  }
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
    input.getBase.repeatStrings(repeatTimes.getBase)
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

trait HasGpuStringReplace {
  def doStringReplace(
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

  def doStringReplaceMulti(
    strExpr: GpuColumnVector,
    search: Seq[String],
    replacement: String): ColumnVector = {
      withResource(ColumnVector.fromStrings(search: _*)) { targets =>
        withResource(ColumnVector.fromStrings(replacement)) {  repls =>
          strExpr.getBase.stringReplace(targets, repls)
        }
      }
  }
}

case class GpuStringReplace(
    srcExpr: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
  extends GpuTernaryExpressionArgsAnyScalarScalar
      with ImplicitCastInputTypes
      with HasGpuStringReplace {

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
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    doStringReplace(strExpr, searchExpr, replaceExpr)
  }

  override def doColumnar(
      numRows: Int,
      strExpr: GpuScalar,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(strExpr, numRows, srcExpr.dataType)) { strExprCol =>
      doColumnar(strExprCol, searchExpr, replaceExpr)
    }
  }
}

case class GpuStringTranslate(
    srcExpr: Expression,
    fromExpr: Expression,
    toExpr: Expression)
  extends GpuTernaryExpressionArgsAnyScalarScalar
      with ImplicitCastInputTypes {

  override def dataType: DataType = srcExpr.dataType

  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = fromExpr
  override def third: Expression = toExpr

  private def buildLists(fromExpr: GpuScalar, toExpr: GpuScalar): (List[String], List[String]) = {
    val fromString = fromExpr.getValue.asInstanceOf[UTF8String].toString
    val toString = toExpr.getValue.asInstanceOf[UTF8String].toString
    var fromCharsArray = Array[String]()
    var toCharsArray = Array[String]()
    var i = 0
    var j = 0
    while (i < fromString.length) {
      val replaceStr = if (j < toString.length) {
        val repCharCount = Character.charCount(toString.codePointAt(j))
        val repStr = toString.substring(j, j + repCharCount)
        j += repCharCount
        repStr
      } else {
        ""
      }
      val matchCharCount = Character.charCount(fromString.codePointAt(i))
      val matchStr = fromString.substring(i, i + matchCharCount)
      i += matchCharCount
      fromCharsArray :+= matchStr
      toCharsArray :+= replaceStr
    }
    (fromCharsArray.toList, toCharsArray.toList)
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      fromExpr: GpuScalar,
      toExpr: GpuScalar): ColumnVector = {
    // When from or to string is null, return all nulls like the CPU does.
    if (!fromExpr.isValid || !toExpr.isValid) {
      GpuColumnVector.columnVectorFromNull(strExpr.getRowCount.toInt, StringType)
    } else if (fromExpr.getValue.asInstanceOf[UTF8String].numChars() == 0) {
      // Return original string if search string is empty
      strExpr.getBase.incRefCount()
    } else {
      val (fromStringList, toStringList) = buildLists(fromExpr, toExpr)
      withResource(ColumnVector.fromStrings(fromStringList: _*)) { fromStringCol =>
        withResource(ColumnVector.fromStrings(toStringList: _*)) { toStringCol =>
          strExpr.getBase.stringReplace(fromStringCol, toStringCol)
        }
      }
    }
  }

  override def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar,
      val2: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(val0, numRows, srcExpr.dataType)) { val0Col =>
      doColumnar(val0Col, val1, val2)
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

case class GpuLike(left: Expression, right: Expression, escapeChar: Char)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerant  {

  def this(left: Expression, right: Expression) = this(left, right, '\\')

  override def toString: String = escapeChar match {
    case '\\' => s"$left gpulike $right"
    case c => s"$left gpulike $right ESCAPE '$c'"
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(Scalar.fromString(Character.toString(escapeChar))) { escapeScalar =>
      lhs.getBase.like(rhs.getBase, escapeScalar)
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

object GpuRegExpUtils {
  private def parseAST(pattern: String): RegexAST = {
    new RegexParser(pattern).parse()
  }

  /**
   * Convert symbols of back-references if input string contains any.
   * In spark's regex rule, there are two patterns of back-references:
   * \group_index and \$group_index
   * This method transforms above two patterns into cuDF pattern \${group_index}, except they are
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
        case RegexGroup(_, term, _) =>
          isASTEmptyRepetition(term)
        case RegexSequence(parts) =>
          parts.forall(isASTEmptyRepetition)
        // cuDF does not support repetitions adjacent to a choice (eg. "a*|a"), but if
        // we did, we would need to add a `case RegexChoice()` here
        case _ => false
      }
    }
    parseAST(pattern) match {
      case RegexSequence(parts) if parts.lastOption.contains(RegexChar('$')) =>
        // handle pattern ".*$"
        isASTEmptyRepetition(RegexSequence(parts.dropRight(1)))
      case other => isASTEmptyRepetition(other)
    }
  }

  /**
   * Returns the number of groups in regexp
   * (includes both capturing and non-capturing groups)
   */
  def countGroups(pattern: String): Int = {
    def countGroups(regexp: RegexAST): Int = {
      regexp match {
        case RegexGroup(_, term, _) => 1 + countGroups(term)
        case other => other.children().map(countGroups).sum
      }
   }
   countGroups(parseAST(pattern))
  }

  def getChoicesFromRegex(regex: RegexAST): Option[Seq[String]] = {
    regex match {
      case RegexGroup(_, t, None) =>
        getChoicesFromRegex(t)
      case RegexChoice(a, b) =>
        getChoicesFromRegex(a) match {
          case Some(la) =>
            getChoicesFromRegex(b) match {
              case Some(lb) => Some(la ++ lb)
              case _ => None
            }
          case _ => None
        }
      case RegexSequence(parts) =>
        if (GpuOverrides.isSupportedStringReplacePattern(regex.toRegexString)) {
          Some(Seq(regex.toRegexString))
        } else {
          parts.foldLeft(Some(Seq[String]()): Option[Seq[String]]) { (m: Option[Seq[String]], r) =>
            getChoicesFromRegex(r) match {
              case Some(l) => m.map(_ ++ l)
              case _ => None
            }
          }
        }
      case _ =>
        if (GpuOverrides.isSupportedStringReplacePattern(regex.toRegexString)) {
          Some(Seq(regex.toRegexString))
        } else {
          None
        }
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
            val (transpiledAST, _) =
                new CudfRegexTranspiler(RegexFindMode).getTranspiledAST(str.toString, None, None)
            GpuRegExpUtils.validateRegExpComplexity(this, transpiledAST)
            pattern = Some(transpiledAST.toRegexString)
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
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerant  {

  override def toString: String = s"$left gpurlike $right"

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    lhs.getBase.containsRe(new RegexProgram(pattern, CaptureGroups.NON_CAPTURE))
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def dataType: DataType = BooleanType
}

abstract class GpuRegExpTernaryBase
    extends GpuTernaryExpressionArgsAnyScalarScalar {

  override def dataType: DataType = StringType

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
    replaceExpr: Expression)
    (javaRegexpPattern: String,
    cudfRegexPattern: String,
    cudfReplacementString: String,
    searchList: Option[Seq[String]],
    replaceOpt: Option[GpuRegExpReplaceOpt])
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with HasGpuStringReplace {

  override def otherCopyArgs: Seq[AnyRef] = Seq(javaRegexpPattern,
    cudfRegexPattern, cudfReplacementString, searchList, replaceOpt)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, StringType)

  override def first: Expression = srcExpr
  override def second: Expression = searchExpr
  override def third: Expression = replaceExpr

  def this(srcExpr: Expression, searchExpr: Expression)(javaRegexpPattern: String,
    cudfRegexPattern: String, cudfReplacementString: String) = {

    this(srcExpr, searchExpr, GpuLiteral("", StringType))(javaRegexpPattern,
      cudfRegexPattern, cudfReplacementString, None, None)
  }

  override def doColumnar(
      strExpr: GpuColumnVector,
      searchExpr: GpuScalar,
      replaceExpr: GpuScalar): ColumnVector = {
    // For empty strings and a regex containing only a zero-match repetition,
    // the behavior in some versions of Spark is different.
    // see https://github.com/NVIDIA/spark-rapids/issues/5456
    replaceOpt match {
      case Some(GpuRegExpStringReplace) =>
        doStringReplace(strExpr, searchExpr, replaceExpr)
      case Some(GpuRegExpStringReplaceMulti) =>
        searchList match {
          case Some(searches) =>
            doStringReplaceMulti(strExpr, searches, cudfReplacementString)
          case _ =>
            throw new IllegalStateException("Need a replace")
        }
      case _ =>
        val prog = new RegexProgram(cudfRegexPattern, CaptureGroups.NON_CAPTURE)
        if (SparkShimImpl.reproduceEmptyStringBug &&
            GpuRegExpUtils.isEmptyRepetition(javaRegexpPattern)) {
          val isEmpty = withResource(strExpr.getBase.getCharLengths) { len =>
            withResource(Scalar.fromInt(0)) { zero =>
              len.equalTo(zero)
            }
          }
          withResource(isEmpty) { _ =>
            withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
              withResource(GpuScalar.from(cudfReplacementString, DataTypes.StringType)) { rep =>
                withResource(strExpr.getBase.replaceRegex(prog, rep)) { replacement =>
                  isEmpty.ifElse(emptyString, replacement)
                }
              }
            }
          }
        } else {
          withResource(Scalar.fromString(cudfReplacementString)) { rep =>
            strExpr.getBase.replaceRegex(prog, rep)
          }
        }
    }

  }

}

case class GpuRegExpReplaceWithBackref(
    override val child: Expression,
    searchExpr: Expression,
    replaceExpr: Expression)
    (javaRegexpPattern: String,
     cudfRegexPattern: String,
    cudfReplacementString: String)
  extends GpuUnaryExpression with ImplicitCastInputTypes {

  override def otherCopyArgs: Seq[AnyRef] = Seq(javaRegexpPattern, cudfRegexPattern,
    cudfReplacementString)
  override def inputTypes: Seq[DataType] = Seq(StringType)

  override def dataType: DataType = StringType

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    val prog = new RegexProgram(cudfRegexPattern)
    if (SparkShimImpl.reproduceEmptyStringBug &&
        GpuRegExpUtils.isEmptyRepetition(javaRegexpPattern)) {
      val isEmpty = withResource(input.getBase.getCharLengths) { len =>
        withResource(Scalar.fromInt(0)) { zero =>
          len.equalTo(zero)
        }
      }
      withResource(isEmpty) { _ =>
        withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
          withResource(input.getBase.stringReplaceWithBackrefs(prog,
              cudfReplacementString)) { replacement =>
            isEmpty.ifElse(emptyString, replacement)
          }
        }
      }
    } else {
      input.getBase.stringReplaceWithBackrefs(prog, cudfReplacementString)
    }
  }

}

class GpuRegExpExtractMeta(
    expr: RegExpExtract,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends TernaryExprMeta[RegExpExtract](expr, conf, parent, rule) {

  private var pattern: Option[String] = None

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    ShimLoader.getShimVersion match {
      case _: DatabricksShimVersion if expr.subject.isInstanceOf[InputFileName] =>
        willNotWorkOnGpu("avoiding Databricks Delta problem with regexp extract")
      case _ =>
    }

    var numGroups = 0
    val groupIdx = expr.idx match {
      case Literal(value, DataTypes.IntegerType) =>
        Some(value.asInstanceOf[Int])
      case _ =>
        willNotWorkOnGpu("GPU only supports literal index")
        None
    }

    expr.regexp match {
      case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
        try {
          val javaRegexpPattern = str.toString
          // verify that we support this regex and can transpile it to cuDF format
          val (transpiledAST, _) =
            new CudfRegexTranspiler(RegexFindMode).getTranspiledAST(
              javaRegexpPattern, groupIdx, None)
          GpuRegExpUtils.validateRegExpComplexity(this, transpiledAST)
          pattern = Some(transpiledAST.toRegexString)
          numGroups = GpuRegExpUtils.countGroups(javaRegexpPattern)
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    groupIdx.foreach { idx =>
      if (idx < 0) {
        willNotWorkOnGpu("the specified group index cannot be less than zero")
      }
      if (idx > numGroups) {
        willNotWorkOnGpu(
          s"regex group count is $numGroups, but the specified group index is $idx")
      }
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtract(str, regexp, idx)(cudfPattern)
  }
}

case class GpuRegExpExtract(
    subject: Expression,
    regexp: Expression,
    idx: Expression)(cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerant {

  override def otherCopyArgs: Seq[AnyRef] = cudfRegexPattern :: Nil
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
      case _ =>
        // Since we have transpiled all but one of the capture groups to non-capturing, the index
        // here moves to 0 to single out the one capture group left
        (cudfRegexPattern, 0)
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

    withResource(str.getBase.extractRe(new RegexProgram(extractPattern))) { extract =>
      withResource(GpuScalar.from("", DataTypes.StringType)) { emptyString =>
        val outputNullAndInputNotNull =
          withResource(extract.getColumn(groupIndex).isNull) { outputNull =>
            withResource(str.getBase.isNotNull) { inputNotNull =>
              outputNull.and(inputNotNull)
            }
          }
        withResource(outputNullAndInputNotNull) {
          _.ifElse(emptyString, extract.getColumn(groupIndex))
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

  override def tagExprForGpu(): Unit = {
    GpuRegExpUtils.tagForRegExpEnabled(this)

    var numGroups = 0
    val groupIdx = expr.idx match {
      case Literal(value, DataTypes.IntegerType) =>
        Some(value.asInstanceOf[Int])
      case _ =>
        willNotWorkOnGpu("GPU only supports literal index")
        None
    }

    expr.regexp match {
      case Literal(str: UTF8String, DataTypes.StringType) if str != null =>
        try {
          val javaRegexpPattern = str.toString
          // verify that we support this regex and can transpile it to cuDF format
          val (transpiledAST, _) =
            new CudfRegexTranspiler(RegexFindMode).getTranspiledAST(
              javaRegexpPattern, groupIdx, None)
          GpuRegExpUtils.validateRegExpComplexity(this, transpiledAST)
          pattern = Some(transpiledAST.toRegexString)
          numGroups = GpuRegExpUtils.countGroups(javaRegexpPattern)
        } catch {
          case e: RegexUnsupportedException =>
            willNotWorkOnGpu(e.getMessage)
        }
      case _ =>
        willNotWorkOnGpu(s"only non-null literal strings are supported on GPU")
    }

    groupIdx.foreach { idx =>
      if (idx < 0) {
        willNotWorkOnGpu("the specified group index cannot be less than zero")
      }
      if (idx > numGroups) {
        willNotWorkOnGpu(
          s"regex group count is $numGroups, but the specified group index is $idx")
      }
    }
  }

  override def convertToGpu(
      str: Expression,
      regexp: Expression,
      idx: Expression): GpuExpression = {
    val cudfPattern = pattern.getOrElse(
      throw new IllegalStateException("Expression has not been tagged with cuDF regex pattern"))
    GpuRegExpExtractAll(str, regexp, idx)(cudfPattern)
  }
}

case class GpuRegExpExtractAll(
    str: Expression,
    regexp: Expression,
    idx: Expression)(cudfRegexPattern: String)
  extends GpuRegExpTernaryBase with ImplicitCastInputTypes with NullIntolerant {

  override def otherCopyArgs: Seq[AnyRef] = cudfRegexPattern :: Nil
  override def dataType: DataType = ArrayType(StringType, containsNull = true)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def first: Expression = str
  override def second: Expression = regexp
  override def third: Expression = idx

  override def prettyName: String = "regexp_extract_all"

  override def doColumnar(
      str: GpuColumnVector,
      regexp: GpuScalar,
      idx: GpuScalar): ColumnVector = {
    idx.getValue.asInstanceOf[Int] match {
      case 0 =>
        val prog = new RegexProgram(cudfRegexPattern, CaptureGroups.NON_CAPTURE)
        str.getBase.extractAllRecord(prog, 0)
      case _ =>
        // Extract matches corresponding to idx. cuDF's extract_all_record does not support
        // group idx, so we must manually extract the relevant matches. Example:
        // Given the pattern (\d+)-(\d+) and idx=1
        //
        // |      Input      |      Java       |               cuDF             |
        // |-----------------|-----------------|--------------------------------|
        // | '1-2, 3-4, 5-6' | ['1', '3', '5'] | ['1', '2', '3', '4', '5', '6'] |
        //
        // Since idx=1 and the pattern has 2 capture groups, we take the 1st element and every
        // 2nd element afterwards from the cuDF list

        val rowCount = str.getRowCount
        val prog = new RegexProgram(cudfRegexPattern)

        val extractedWithNulls = withResource(
          // Now the index is always 1 because we have transpiled all the capture groups to the
          // single group that we care about, so we just have to handle the idx = 1 case here
          str.getBase.extractAllRecord(prog, 1)) { allExtracted =>
            withResource(allExtracted.countElements) { listSizes =>
              withResource(listSizes.max) { maxSize =>
                val maxSizeInt = maxSize.getInt
                val stringCols = Range(0, maxSizeInt, 1).safeMap {
                  i =>
                    withResource(Scalar.fromInt(i)) { scalarIndex =>
                      withResource(ColumnVector.fromScalar(scalarIndex, rowCount.toInt)) {
                        index => allExtracted.extractListElement(index)
                      }
                    }
                }
                withResource(stringCols) { _ =>
                  ColumnVector.makeList(stringCols: _*)
                }
              }
            }
          }
        // Filter out null values in the lists
        val extractedStrings = withResource(extractedWithNulls) { _ =>
          val booleanMask = withResource(extractedWithNulls.getListOffsetsView) { offsetsCol =>
            withResource(extractedWithNulls.getChildColumnView(0)) { stringCol =>
              withResource(stringCol.isNotNull) { isNotNull =>
                isNotNull.makeListFromOffsets(rowCount, offsetsCol)
              }
            }
          }
          withResource(booleanMask) {
            extractedWithNulls.applyBooleanMask
          }
        }

        // If input is null, output should also be null
        withResource(extractedStrings) { s =>
          withResource(GpuScalar.from(null, DataTypes.createArrayType(DataTypes.StringType))) {
            nullStringList =>
              withResource(str.getBase.isNull) { isInputNull =>
                isInputNull.ifElse(nullStringList, s)
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
  extends GpuTernaryExpressionArgsAnyScalarScalar with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)

  override def first: Expression = strExpr
  override def second: Expression = ignoredDelimExpr
  override def third: Expression = ignoredCountExpr

  override def prettyName: String = "substring_index"

  // This is a bit hacked up at the moment. We are going to use a regular expression to extract
  // a single value. It only works if the delim is a single character. A full version of
  // substring_index for the GPU has been requested at https://github.com/rapidsai/cudf/issues/5158
  // spark-rapids plugin issue https://github.com/NVIDIA/spark-rapids/issues/8750
  override def doColumnar(str: GpuColumnVector, delim: GpuScalar,
      count: GpuScalar): ColumnVector = {
    if (regexp == null) {
      withResource(str.getBase.isNull) { isNull =>
        withResource(Scalar.fromString("")) { emptyString =>
          isNull.ifElse(str.getBase, emptyString)
        }
      }
    } else {
      withResource(str.getBase.extractRe(new RegexProgram(regexp))) { table: Table =>
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
}

trait BasePad
    extends GpuTernaryExpressionArgsAnyScalarScalar
        with ImplicitCastInputTypes
        with NullIntolerant {
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
              val (transpiledAST, _) = transpiler.getTranspiledAST(utf8Str.toString, None, None)
              GpuRegExpUtils.validateRegExpComplexity(this, transpiledAST)
              pattern = transpiledAST.toRegexString
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
      case Some(Literal(_: Int, _)) =>
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
    limit.getValue.asInstanceOf[Int] match {
      case 0 =>
        // Same as splitting as many times as possible
        if (isRegExp) {
          str.getBase.stringSplitRecord(new RegexProgram(pattern, CaptureGroups.NON_CAPTURE), -1)
        } else {
          str.getBase.stringSplitRecord(pattern, -1)
        }
      case 1 =>
        // Short circuit GPU and just return a list containing the original input string
        withResource(str.getBase.isNull) { isNull =>
          withResource(GpuScalar.from(null, DataTypes.createArrayType(DataTypes.StringType))) {
            nullStringList =>
              withResource(ColumnVector.makeList(str.getBase)) { list =>
                  isNull.ifElse(nullStringList, list)
              }
          }
        }
      case n =>
        if (isRegExp) {
          str.getBase.stringSplitRecord(new RegexProgram(pattern, CaptureGroups.NON_CAPTURE), n)
        } else {
          str.getBase.stringSplitRecord(pattern, n)
        }
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

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(strExpr.columnarEvalAny(batch)) {
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
    val listsOfStrings = if (isPairDelimRegExp) {
      str.getBase.stringSplitRecord(new RegexProgram(pairDelim, CaptureGroups.NON_CAPTURE))
    } else {
      str.getBase.stringSplitRecord(pairDelim)
    }
    withResource(listsOfStrings) { listsOfStrings =>
      // Extract strings column from the output lists column.
      withResource(listsOfStrings.getChildColumnView(0)) { stringsCol =>
        // Split the key-value strings into pairs of strings of key-value (using limit = 2).
        val keysValuesTable = if (isKeyValueDelimRegExp) {
          stringsCol.stringSplit(new RegexProgram(keyValueDelim, CaptureGroups.NON_CAPTURE), 2)
        } else {
          stringsCol.stringSplit(keyValueDelim, 2)
        }
        withResource(keysValuesTable) { keysValuesTable =>

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

case class GpuStringInstr(str: Expression, substr: Expression)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerant  {
  // Locate the position of the first occurrence of substr column in the given string.
  // returns null if one of the arguments is null
  // returns zero if not found
  // return values are 1 based.
  override def dataType: DataType = IntegerType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def left: Expression = str
  override def right: Expression = substr

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(lhs.getBase.stringLocate(rhs.getBase)) { strLocateRes =>
      withResource(Scalar.fromInt(1)) { sv1 =>
        strLocateRes.add(sv1)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, str.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

class GpuConvMeta(
  expr: Conv,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_,_,_]],
  rule: DataFromReplacementRule) extends TernaryExprMeta(expr, conf, parent, rule) {

  override def tagExprForGpu(): Unit = {
    val fromBaseLit = GpuOverrides.extractLit(expr.fromBaseExpr)
    val toBaseLit = GpuOverrides.extractLit(expr.toBaseExpr)
    (fromBaseLit, toBaseLit) match {
      case (Some(Literal(fromBaseVal, IntegerType)), Some(Literal(toBaseVal, IntegerType)))
        if Set(fromBaseVal, toBaseVal).subsetOf(Set(10, 16)) => ()
      case _ =>
        willNotWorkOnGpu(because = "only literal 10 or 16 for from_base and to_base are supported")
    }
  }

  override def convertToGpu(
    numStr: Expression,
    fromBase: Expression,
    toBase: Expression): GpuExpression = GpuConv(numStr, fromBase, toBase)
}


case class GpuConv(num: Expression, fromBase: Expression, toBase: Expression)
  extends GpuTernaryExpression {

  override def doColumnar(
    v1: GpuColumnVector,
    v2: GpuColumnVector,
    v3: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(v1: GpuScalar, v2: GpuColumnVector, v3: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(v1: GpuScalar, v2: GpuScalar, v3: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(v1: GpuScalar, v2: GpuColumnVector, v3: GpuScalar): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(v1: GpuColumnVector, v2: GpuScalar, v3: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(v1: GpuColumnVector, v2: GpuColumnVector, v3: GpuScalar): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(
    numRows: Int,
    strScalar: GpuScalar,
    fromBase: GpuScalar,
    toBase: GpuScalar
  ): ColumnVector = {
    withResource(GpuColumnVector.from(strScalar, numRows, strScalar.dataType)) { strCV =>
      doColumnar(strCV, fromBase, toBase)
    }
  }

  override def doColumnar(
    str: GpuColumnVector,
    fromBase: GpuScalar,
    toBase: GpuScalar
  ): ColumnVector = {
    (fromBase.getValue, toBase.getValue) match {
      case (fromRadix: Int, toRadix: Int) =>
        withResource(
          CastStrings.toIntegersWithBase(str.getBase, fromRadix, false, DType.UINT64)
        ) { intCV =>
          CastStrings.fromIntegersWithBase(intCV, toRadix)
        }
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def first: Expression = num

  override def second: Expression = fromBase

  override def third: Expression = toBase

  override def dataType: DataType = StringType
}

case class GpuFormatNumber(x: Expression, d: Expression)
    extends GpuBinaryExpression with ExpectsInputTypes with NullIntolerant {
  
  override def left: Expression = x
  override def right: Expression = d
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  private def removeNegSign(cv: ColumnVector): ColumnVector = {
    withResource(Scalar.fromString("-")) { negativeSign =>
      cv.lstrip(negativeSign)
    }
  }

  private def getZeroCv(size: Int): ColumnVector = {
    withResource(Scalar.fromString("0")) { zero =>
      ColumnVector.fromScalar(zero, size)
    }
  }

  private def handleDoublePosExp(cv: ColumnVector, intPart: ColumnVector, decPart: ColumnVector, 
      exp: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // handle cases when exp is positive
    // append "0" * zerosNum after end of strings, zerosNum = exp - decLen + d
    val expSubDecLen = withResource(decPart.getCharLengths) { decLen =>
      exp.sub(decLen)
    }
    val zerosNum = withResource(expSubDecLen) { _ =>
      withResource(Scalar.fromInt(d)) { dScalar =>
        expSubDecLen.add(dScalar)
      }
    }
    val zeroCv = withResource(Scalar.fromString("0")) { zero =>
      ColumnVector.fromScalar(zero, cv.getRowCount.toInt)
    }
    val zeros = withResource(zerosNum) { _ =>
      withResource(zeroCv) { _ =>
        zeroCv.repeatStrings(zerosNum)
      }
    }

    val intAndDecParts = withResource(zeros) { _ =>
      ColumnVector.stringConcatenate(Array(intPart, decPart, zeros))
    }
    // split intAndDecParts to intPart and decPart with substrings, start = len(intAndDecParts) - d
    val (intPartPosExp, decPartPosExpTemp) = withResource(intAndDecParts) { _ =>
      val (start, end) = withResource(intAndDecParts.getCharLengths) { partsLength =>
        (withResource(Scalar.fromInt(d)) { d =>
          partsLength.sub(d)
        }, partsLength.incRefCount())
      }
      val zeroCv = withResource(Scalar.fromInt(0)) { zero =>
        ColumnVector.fromScalar(zero, cv.getRowCount.toInt)
      }
      withResource(start) { _ =>
        val intPart = withResource(zeroCv) { _ =>
          intAndDecParts.substring(zeroCv, start)
        }
        val decPart = withResource(end) { _ =>
          intAndDecParts.substring(start, end)
        }
        (intPart, decPart)
      }
    }
    // if decLen - exp > d, convert to float/double, round, convert back to string
    // decLen's max value is 9, abs(expPart)'s min value is 7, so it is possible only when d < 2
    // because d is small, we can use double to do the rounding
    val decPartPosExp = if (0 < d && d < 2) {
      val pointCv = withResource(Scalar.fromString(".")) { point =>
        ColumnVector.fromScalar(point, cv.getRowCount.toInt)
      }
      val withPoint = withResource(decPartPosExpTemp) { _ =>
        withResource(pointCv) { pointCv =>
          ColumnVector.stringConcatenate(Array(pointCv, decPartPosExpTemp))
        }
      }
      val decimalTypeRounding = DType.create(DType.DTypeEnum.DECIMAL128, -9)
      val withPointDecimal = withResource(withPoint) { _ =>
        withResource(withPoint.castTo(decimalTypeRounding)) { decimal =>
          decimal.round(d, RoundMode.HALF_EVEN)
        }
      }
      val roundedString = withResource(withPointDecimal) { _ =>
        withPointDecimal.castTo(DType.STRING)
      }
      withResource(roundedString) { _ =>
        withResource(roundedString.stringSplit(".", 2)) { splited =>
          splited.getColumn(1).incRefCount()
        }
      }
    } else {
      decPartPosExpTemp
    }
    (intPartPosExp, decPartPosExp)
  }

  private def handleDoubleNegExp(cv: ColumnVector, intPart: ColumnVector, decPart: ColumnVector, 
      exp: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // handle cases when exp is negative
    // "0." + (- exp - 1) * "0" + intPart + decPart
    // if -1 - d <= exp and decLen - exp > d, need to rounding
    val cond1 = withResource(Scalar.fromInt(-1 - d)) { negOneSubD =>
      exp.greaterOrEqualTo(negOneSubD)
    }
    val decLenSubExp = withResource(decPart.getCharLengths) { decLen =>
      decLen.sub(exp)
    }
    val cond2 = withResource(decLenSubExp) { _ =>
      withResource(Scalar.fromInt(d)) { d =>
        decLenSubExp.greaterThan(d)
      }
    }
    val needRounding = withResource(cond1) { _ =>
      withResource(cond2) { _ =>
        cond1.and(cond2)
      }
    }
    val anyNeedRounding = withResource(needRounding) { _ =>
      withResource(needRounding.any()) { any =>
        any.isValid && any.getBoolean
      }
    }
    anyNeedRounding match {
      case false =>
        // a shortcut when no need to rounding
        // "0." + (- exp - 1) * "0" + intPart + decPart
        withResource(getZeroCv(cv.getRowCount.toInt)) { zeroCv =>
          val expSubOne = withResource(Scalar.fromInt(-1)) { negOne =>
            negOne.sub(exp)
          }
          val addingZeros = withResource(expSubOne) { _ =>
            zeroCv.repeatStrings(expSubOne)
          }
          val decPartNegExp = withResource(addingZeros) { _ =>
            ColumnVector.stringConcatenate(Array(addingZeros, intPart, decPart))
          }
          val decPartNegSubstr = withResource(decPartNegExp) { _ =>
            decPartNegExp.substring(0, d)
          }
          (zeroCv.incRefCount(), decPartNegSubstr)
        }
      case true =>
        // if -exp <= d + 1 && -exp + decLen + 1 > d, need to rounding
        // dec will be round to (d + exp + 1) digits
        val dExpOne = withResource(Scalar.fromInt(d + 1)) { dExpOne =>
          exp.add(dExpOne)
        }
        // To do a dataframe operation, add some zeros before 
        // (intPat + decPart) and round them to 10
        // zerosNumRounding = (10 - (d + exp + 1)) . max(0)
        val tenSubDExpOne = withResource(Scalar.fromInt(10)) { ten =>
          withResource(dExpOne) { _ =>
            ten.sub(dExpOne)
          }
        }
        val zerosNumRounding = withResource(tenSubDExpOne) { _ =>
          withResource(Scalar.fromInt(0)) { zero =>
            withResource(tenSubDExpOne.lessThan(zero)) { lessThanZero =>
              lessThanZero.ifElse(zero, tenSubDExpOne)
            }
          }
        }
        // add zeros before (intPat + decPart)
        val leadingZeros = withResource(zerosNumRounding) { _ =>
          withResource(getZeroCv(cv.getRowCount.toInt)) { zeroCv =>
            zeroCv.repeatStrings(zerosNumRounding)
          }
        }
        val zeroPointCv = withResource(Scalar.fromString("0.")) { point =>
          ColumnVector.fromScalar(point, cv.getRowCount.toInt)
        }
        val numberToRoundStr = withResource(zeroPointCv) { _ =>
          withResource(leadingZeros) { _ =>
            ColumnVector.stringConcatenate(Array(zeroPointCv, leadingZeros, intPart, decPart))
          }
        }
        // use a decimal type to round, set scale to -20 to keep all digits
        val decimalTypeRounding = DType.create(DType.DTypeEnum.DECIMAL128, -20)
        val numberToRound = withResource(numberToRoundStr) { _ =>
          numberToRoundStr.castTo(decimalTypeRounding)
        }
        val rounded = withResource(numberToRound) { _ =>
          numberToRound.round(10, RoundMode.HALF_EVEN)
        }
        val roundedStr = withResource(rounded) { _ =>
          rounded.castTo(DType.STRING)
        }
        // substr 2 to remove "0."
        val roundedDecPart = withResource(roundedStr) { _ =>
          roundedStr.substring(2)
        }
        val stripZerosDecPart = withResource(roundedDecPart) { _ =>
          withResource(Scalar.fromString("0")) { zero =>
            roundedDecPart.lstrip(zero)
          }
        }
        // rounding 10 digits
        val negExp = withResource(Scalar.fromInt(-1)) { negOne =>
          negOne.sub(exp)
        }
        val zerosRemaining = withResource(negExp) { _ =>
          withResource(getZeroCv(cv.getRowCount.toInt)) { zeroCv =>
            zeroCv.repeatStrings(negExp)
          }
        }
        val decPartNegExp = withResource(zerosRemaining) { _ =>
          withResource(stripZerosDecPart) { _ =>
            ColumnVector.stringConcatenate(Array(zerosRemaining, stripZerosDecPart))
          }
        }
        val decPartPad = withResource(decPartNegExp) { _ =>
          decPartNegExp.pad(d, PadSide.RIGHT, "0")
        }
        val decPartNegSubstr = withResource(decPartPad) { _ =>
          decPartPad.substring(0, d)
        }
        (getZeroCv(cv.getRowCount.toInt), decPartNegSubstr)
    }
  }

  private def normalDoubleSplit(cv: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    val roundedStr = withResource(cv.round(d, RoundMode.HALF_EVEN)) { rounded =>
      rounded.castTo(DType.STRING)
    }
    val (intPart, decPart) = withResource(roundedStr) { _ =>
      withResource(roundedStr.stringSplit(".", 2)) { intAndDec =>
        (intAndDec.getColumn(0).incRefCount(), intAndDec.getColumn(1).incRefCount())
      }
    }
    val intPartNoNeg = withResource(intPart) { _ =>
      removeNegSign(intPart)
    }
    val decPartPad = withResource(decPart) { _ =>
      decPart.pad(d, PadSide.RIGHT, "0")
    }
    (intPartNoNeg, decPartPad)
  }

  private def expDoubleSplit(cv: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // handle special case: 1.234567e+7 or 1.234567e-6
    // get three parts first:
    val replaceDelimToE = withResource(Scalar.fromString("e")) { e =>
      withResource(Scalar.fromString(".")) { p =>
        cv.stringReplace(e, p)
      }
    }
    // get three parts: 1.234567e+7 -> 1, 234567, +7
    val (intPartSign, decPart, expPart) = withResource(replaceDelimToE) { _ =>
      withResource(replaceDelimToE.stringSplit(".", 3)) { intDecExp =>
        (intDecExp.getColumn(0).incRefCount(),
          intDecExp.getColumn(1).incRefCount(),
          intDecExp.getColumn(2).incRefCount())
      }
    }
    val intPart = withResource(intPartSign) { _ =>
      removeNegSign(intPartSign)  
    }
    val exp = withResource(expPart) { _ =>
      expPart.castTo(DType.INT32)
    }
    val (intPartPosExp, decPartPosExp) = closeOnExcept(intPart) { _ =>
      closeOnExcept(decPart) { _ =>
        closeOnExcept(exp) { _ =>
          handleDoublePosExp(cv, intPart, decPart, exp, d)
        }
      }
    }
    val (intPartNegExp, decPartNegExp) = withResource(intPart) { _ =>
      withResource(decPart) { _ =>
        closeOnExcept(exp) { _ =>
          handleDoubleNegExp(cv, intPart, decPart, exp, d)
        }
      }
    }
    val expPos = withResource(exp) { _ =>
      withResource(Scalar.fromInt(0)) { zero =>
        exp.greaterOrEqualTo(zero)
      }
    }
    withResource(expPos) { _ =>
      val intPartExp = withResource(intPartPosExp) { _ =>
        withResource(intPartNegExp) { _ =>
          expPos.ifElse(intPartPosExp, intPartNegExp)
        }
      }
      val decPartExp = withResource(decPartPosExp) { _ =>
        withResource(decPartNegExp) { _ =>
          expPos.ifElse(decPartPosExp, decPartNegExp)
        }
      }
      (intPartExp, decPartExp)
    }
  }

  private def getPartsFromDouble(cv: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // handle normal case: 1234.567
    val (normalInt, normalDec) = normalDoubleSplit(cv, d)
    // first check special case
    val cvStr = withResource(cv.castTo(DType.STRING)) { cvStr =>
      cvStr.incRefCount()
    }
    val containsE = closeOnExcept(cvStr) { _ =>
      withResource(Scalar.fromString("e")) { e =>
        cvStr.stringContains(e)
      }
    }
    withResource(containsE) { _ =>
      // if no special case, return normal case directly
      val anyExp = closeOnExcept(cvStr) { _ =>
        withResource(containsE.any()) { any =>
          any.isValid && any.getBoolean
        }
      }
      anyExp match {
        case false => {
          cvStr.safeClose()
          (normalInt, normalDec)
        }
        case true => {
          val noEReplaced = withResource(cvStr) { _ =>
            // replace normal case with 0e0 to avoid error
            withResource(Scalar.fromString("0.0e0")) { default =>
              containsE.ifElse(cvStr, default)
            }
          }
          // handle scientific notation case:
          val (expInt, expDec) = withResource(noEReplaced) { _ =>
            expDoubleSplit(noEReplaced, d)
          }
          // combine results
          val intPart = withResource(expInt) { _ =>
            withResource(normalInt) { _ =>
              containsE.ifElse(expInt, normalInt)
            }
          }
          val decPart = withResource(expDec) { _ =>
            withResource(normalDec) { _ =>
              containsE.ifElse(expDec, normalDec)
            }
          }
          (intPart, decPart)
        }
      }
    }
  }

  private def getPartsFromDecimal(cv: ColumnVector, d: Int, scale: Int): 
      (ColumnVector, ColumnVector) = {
    val roundingScale = scale.min(d)
    val appendZeroNum = (d - scale).max(0).min(d)
    val (intPart, decTemp) = if (roundingScale <= 0) {
      val intPart = withResource(cv.round(roundingScale, RoundMode.HALF_EVEN)) { rounded =>
        rounded.castTo(DType.STRING)
      }
      // if intString starts with 0, it must be "00000...", replace it with "0"
      val (isZero, zeroCv) = withResource(Scalar.fromString("0")) { zero =>
        withResource(intPart.startsWith(zero)) { isZero =>
          (isZero.incRefCount(), ColumnVector.fromScalar(zero, cv.getRowCount.toInt))
        }
      }
      val intPartZeroHandled = withResource(isZero) { isZero =>
        withResource(zeroCv) { zeroCv =>
          isZero.ifElse(zeroCv, intPart)
        }
      }
      val decPart = withResource(Scalar.fromString("")) { emptyString =>
        ColumnVector.fromScalar(emptyString, cv.getRowCount.toInt)
      }
      (intPartZeroHandled, decPart)
    } else {
      withResource(cv.round(roundingScale, RoundMode.HALF_EVEN)) { rounded =>
        withResource(rounded.castTo(DType.STRING)) { roundedStr =>
          withResource(roundedStr.stringSplit(".", 2)) { intAndDec =>
            (intAndDec.getColumn(0).incRefCount(), intAndDec.getColumn(1).incRefCount())
          }
        }
      }
    }
    val intPartPos = closeOnExcept(decTemp) { _ =>
      withResource(intPart) { _ =>
        withResource(Scalar.fromString("-")) { negativeSign =>
          withResource(Scalar.fromString("")) { emptyString =>
            intPart.stringReplace(negativeSign, emptyString)
          }
        }
      }
    }
    val appendZeros = "0" * appendZeroNum
    val appendZerosCv = closeOnExcept(decTemp) { _ =>
      withResource(Scalar.fromString(appendZeros)) { zeroString =>
        ColumnVector.fromScalar(zeroString, cv.getRowCount.toInt)
      }
    }
    val decPart = withResource(decTemp) { _ =>
      withResource(appendZerosCv) { _ =>
        ColumnVector.stringConcatenate(Array(decTemp, appendZerosCv))
      }
    }
    (intPartPos, decPart)
  }

  private def getParts(cv: ColumnVector, d: Int): (ColumnVector, ColumnVector) = {
    // get int part and dec part from a column vector, int part will be set to positive
    x.dataType match {
      case FloatType | DoubleType => {
        getPartsFromDouble(cv, d)
      }
      case DecimalType.Fixed(_, scale) => {
        getPartsFromDecimal(cv, d, scale)
      }
      case IntegerType | LongType | ShortType | ByteType => {
        val intPartPos = withResource(cv.castTo(DType.STRING)) { intPart =>
          withResource(Scalar.fromString("-")) { negativeSign =>
            withResource(Scalar.fromString("")) { emptyString =>
              intPart.stringReplace(negativeSign, emptyString)
            }
          }
        }
        val dzeros = "0" * d
        val decPart = withResource(Scalar.fromString(dzeros)) { zeroString =>
          ColumnVector.fromScalar(zeroString, cv.getRowCount.toInt)
        }
        (intPartPos, decPart)
      }
      case _ => {
        throw new UnsupportedOperationException(s"format_number doesn't support type ${x.dataType}")
      }
    }
  }

  private def negativeCheck(cv: ColumnVector): ColumnVector = {
    withResource(cv.castTo(DType.STRING)) { cvStr =>
      withResource(Scalar.fromString("-")) { negativeSign =>
        cvStr.startsWith(negativeSign)
      }
    }
  }

  private def removeExtraCommas(str: ColumnVector): ColumnVector = {
    withResource(Scalar.fromString(",")) { comma =>
      str.rstrip(comma)
    }
  }

  private def addCommas(str: ColumnVector): ColumnVector = {
    val maxstrlen = withResource(str.getCharLengths()) { strlen =>
      withResource(strlen.max()) { maxlen =>
        maxlen.isValid match {
          case true => maxlen.getInt
          case false => 0
        }
      }
    }
    val substrs = (0 until maxstrlen by 3).map { i =>
      val endIndex = i + 3
      str.substring(i, endIndex).asInstanceOf[ColumnView]
    }.toArray
    val sepCol = withResource(Scalar.fromString(",")) { sep =>
      ColumnVector.fromScalar(sep, str.getRowCount.toInt)
    }
    withResource(substrs) { _ =>
      withResource(sepCol) { _ =>
        withResource(ColumnVector.stringConcatenate(substrs, sepCol)) { res =>
          removeExtraCommas(res)
        }
      }
    }
  }

  private def handleInfAndNan(cv: ColumnVector, res: ColumnVector): ColumnVector = {
    val symbols = DecimalFormatSymbols.getInstance(Locale.US)
    val nanSymbol = symbols.getNaN
    val infSymbol = symbols.getInfinity
    val negInfSymbol = "-" + infSymbol
    val isInf = x.dataType match {
      case DoubleType => {
        withResource(Scalar.fromDouble(Double.PositiveInfinity)) { inf =>
          cv.equalTo(inf)
        }
      }
      case FloatType => {
        withResource(Scalar.fromFloat(Float.PositiveInfinity)) { inf =>
          cv.equalTo(inf)
        }
      }
    }
    val isNegInf = x.dataType match {
      case DoubleType => {
        withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negInf =>
          cv.equalTo(negInf)
        }
      }
      case FloatType => {
        withResource(Scalar.fromFloat(Float.NegativeInfinity)) { negInf =>
          cv.equalTo(negInf)
        }
      }
    }
    val handleNan = withResource(cv.isNan()) { isNan =>
      withResource(Scalar.fromString(nanSymbol)) { nan =>
        isNan.ifElse(nan, res)
      }
    }
    val handleInf = withResource(isInf) { _ =>
      withResource(Scalar.fromString(infSymbol)) { inf =>
        withResource(handleNan) { _ =>
          isInf.ifElse(inf, handleNan)
        }
      }
    }
    val handleNegInf = withResource(isNegInf) { _ =>
      withResource(Scalar.fromString(negInfSymbol)) { negInf =>
        withResource(handleInf) { _ =>
          isNegInf.ifElse(negInf, handleInf)
        }
      }
    }
    handleNegInf
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // get int d from rhs
    if (!rhs.isValid || rhs.getValue.asInstanceOf[Int] < 0) {
      return GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, StringType)
    }
    val d = rhs.getValue.asInstanceOf[Int]
    val isNegative = negativeCheck(lhs.getBase)
    val (integerPart, decimalPart) = getParts(lhs.getBase, d)
    // reverse integer part for adding commas
    val reversedIntegerPart = withResource(integerPart) { intPart =>
      intPart.reverseStringsOrLists()
    }
    val res = withResource(reversedIntegerPart) { _ =>
      addCommas(reversedIntegerPart)
    }
    // reverse result back
    val reverseBack = withResource(res) { r =>
      r.reverseStringsOrLists()
    }
    val resWithDecimalPart = d match {
      case 0 => {
        // d == 0, only return integer part
        decimalPart.safeClose()
        reverseBack
      }
      case _ => {
        // d > 0, append decimal part to result
        val pointCv = withResource(Scalar.fromString(".")) { point =>
          ColumnVector.fromScalar(point, lhs.getRowCount.toInt)
        }
        withResource(reverseBack) { _ =>
          withResource(decimalPart) { _ =>
            withResource(pointCv) { _ =>
              ColumnVector.stringConcatenate(Array(reverseBack, pointCv, decimalPart))
            }
          }
        }
      }
    }
    // add negative sign back
    val negCv = withResource(Scalar.fromString("-")) { negativeSign =>
      ColumnVector.fromScalar(negativeSign, lhs.getRowCount.toInt)
    }
    val formated = withResource(resWithDecimalPart) { _ =>
      val resWithNeg = withResource(negCv) { _ =>
        ColumnVector.stringConcatenate(Array(negCv, resWithDecimalPart))
      }
      withResource(isNegative) { _ =>
        withResource(resWithNeg) { _ =>
          isNegative.ifElse(resWithNeg, resWithDecimalPart)
        }
      }
    }
    // handle null case
    val formatedWithNull = nullable match {
      case true => {
        withResource(formated) { _ =>
          withResource(lhs.getBase.isNull) { isNull =>
            withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
              isNull.ifElse(nullScalar, formated)
            }
          }
        }
      }
      case false => {
        formated
      }
    }
    // handle inf and nan
    x.dataType match {
      case FloatType | DoubleType => {
        withResource(formatedWithNull) { _ =>
          handleInfAndNan(lhs.getBase, formatedWithNull)
        }
      }
      case _ => {
        formatedWithNull
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new UnsupportedOperationException()
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, dataType)) { col =>
      doColumnar(col, rhs)
    }
  }
}
