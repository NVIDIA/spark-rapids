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

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.DecimalUtil.createCudfDecimal
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuUnaryMinus(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  override def sql: String = s"(- ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : ColumnVector = {
    dataType match {
      case dt: DecimalType =>
        val scale = dt.scale
        if (DecimalType.is32BitDecimalType(dt)) {
          withResource(Scalar.fromDecimal(-scale, 0)) { scalar =>
            scalar.sub(input.getBase)
          }
        } else {
          withResource(Scalar.fromDecimal(-scale, 0L)) { scalar =>
            scalar.sub(input.getBase)
          }
        }
      case _ =>
        withResource(Scalar.fromByte(0.toByte)) { scalar =>
          scalar.sub(input.getBase)
        }
    }
  }
}

case class GpuUnaryPositive(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : ColumnVector = input.getBase.incRefCount()
}

case class GpuAbs(child: Expression) extends CudfUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  override def unaryOp: UnaryOp = UnaryOp.ABS
}

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

case class GpuAdd(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

case class GpuSubtract(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

object GpuMultiplyUtil {
  def decimalDataType(l: DecimalType, r: DecimalType): DecimalType = {
    val p = l.precision + r.precision + 1
    val s = l.scale + r.scale
    // TODO once we support 128-bit decimal support we should match the config for precision loss.
    DecimalType(math.min(p, 38), math.min(s, 38))
  }
}

case class GpuMultiply(
    left: Expression,
    right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  override def binaryOp: BinaryOp = BinaryOp.MUL

  // Override the output type as a special case for decimal
  override def dataType: DataType = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) =>  GpuMultiplyUtil.decimalDataType(l, r)
    case _ => super.dataType
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    import DecimalUtil._
    (left.dataType, right.dataType) match {
      case (l: DecimalType, r: DecimalType)
        if !DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(l) &&
          DecimalType.is32BitDecimalType(r) => {
        // we are casting to the smallest 64-bit decimal so the answer doesn't overflow
        val decimalType = createCudfDecimal(10, Math.max(l.scale, r.scale))
        val cudfOutputType = GpuColumnVector.getNonNestedRapidsType(dataType)
        withResource(lhs.getBase.castTo(decimalType)) { decimalLhs =>
          withResource(rhs.getBase.castTo(decimalType)) { decimalRhs =>
            val tmp = decimalLhs.mul(decimalRhs, cudfOutputType)
            if (tmp.getType != cudfOutputType) {
              withResource(tmp) { tmp =>
                tmp.castTo(cudfOutputType)
              }
            } else {
              tmp
            }
          }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (l: DecimalType, r: DecimalType)
        if !DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(l) &&
          DecimalType.is32BitDecimalType(r) => {
        // we are casting to the smallest 64-bit decimal so the answer doesn't overflow
        val sparkDecimalType = DecimalType(10, Math.max(l .scale, r.scale))
        val decimalType = GpuColumnVector.getNonNestedRapidsType(sparkDecimalType)
        val cudfOutputType = GpuColumnVector.getNonNestedRapidsType(dataType)
        withResource(GpuScalar.from(rhs.getBigDecimal().intValue(), sparkDecimalType)) {
          decimalRhs =>
            withResource(lhs.getBase.castTo(decimalType)) { decimalLhs =>
              val tmp = decimalLhs.mul(decimalRhs, cudfOutputType)
              if (tmp.getType != cudfOutputType) {
                withResource(tmp) { tmp =>
                  tmp.castTo(cudfOutputType)
                }
              } else {
                tmp
              }
            }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (l: DecimalType, r: DecimalType)
        if !DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(l) &&
          DecimalType.is32BitDecimalType(r) => {
        // we are casting to the smallest 64-bit decimal so the answer doesn't overflow
        val sparkDecimalType = DecimalType(10, Math.max(l .scale, r.scale))
        val decimalType = GpuColumnVector.getNonNestedRapidsType(sparkDecimalType)
        val cudfOutputType = GpuColumnVector.getNonNestedRapidsType(dataType)
        withResource(GpuScalar.from(lhs.getBigDecimal().intValue(), sparkDecimalType)) {
          decimalLhs =>
            withResource(rhs.getBase.castTo(decimalType)) { decimalRhs =>
              val tmp = decimalLhs.mul(decimalRhs, cudfOutputType)
              if (tmp.getType != cudfOutputType) {
                withResource(tmp) { tmp =>
                  tmp.castTo(cudfOutputType)
                }
              } else {
                tmp
              }
            }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }
}

object GpuDivModLike {
  def replaceZeroWithNull(v: GpuColumnVector): ColumnVector = {
    var zeroScalar: Scalar = null
    var nullScalar: Scalar = null
    var zeroVec: ColumnVector = null
    var nullVec: ColumnVector = null
    try {
      val dtype = v.getBase.getType
      zeroScalar = makeZeroScalar(dtype)
      nullScalar = Scalar.fromNull(dtype)
      zeroVec = ColumnVector.fromScalar(zeroScalar, 1)
      nullVec = ColumnVector.fromScalar(nullScalar, 1)
      v.getBase.findAndReplaceAll(zeroVec, nullVec)
    } finally {
      if (zeroScalar != null) {
        zeroScalar.close()
      }
      if (nullScalar != null) {
        nullScalar.close()
      }
      if (zeroVec != null) {
        zeroVec.close()
      }
      if (nullVec != null) {
        nullVec.close()
      }
    }
  }

  def isScalarZero(s: Scalar): Boolean = {
    s.getType match {
      case DType.INT8 => s.getByte == 0
      case DType.INT16 => s.getShort == 0
      case DType.INT32 => s.getInt == 0
      case DType.INT64 => s.getLong == 0
      case DType.FLOAT32 => s.getFloat == 0f
      case DType.FLOAT64 => s.getDouble == 0
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL64 => s.getLong == 0
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL32 => s.getInt == 0
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }

  def makeZeroScalar(dtype: DType): Scalar = {
    dtype match {
      case DType.INT8 => Scalar.fromByte(0.toByte)
      case DType.INT16 => Scalar.fromShort(0.toShort)
      case DType.INT32 => Scalar.fromInt(0)
      case DType.INT64 => Scalar.fromLong(0L)
      case DType.FLOAT32 => Scalar.fromFloat(0f)
      case DType.FLOAT64 => Scalar.fromDouble(0)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL64 => Scalar.fromDecimal(d.getScale, 0L)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL32 => Scalar.fromDecimal(d.getScale, 0)
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }
}

trait GpuDivModLike extends CudfBinaryArithmetic {
  lazy val failOnError: Boolean =
    ShimLoader.getSparkShims.shouldFailDivByZero()

  override def nullable: Boolean = true

  import GpuDivModLike._

  private def divByZeroError(): Nothing = {
    throw new ArithmeticException("divide by zero")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    if (failOnError) {
      withResource(makeZeroScalar(rhs.getBase.getType)) { zeroScalar =>
        if (rhs.getBase.contains(zeroScalar)) {
          divByZeroError()
        } else {
          super.doColumnar(lhs, rhs)
        }
      }
    } else {
      withResource(replaceZeroWithNull(rhs)) { replaced =>
        super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
      }
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(replaceZeroWithNull(rhs)) { replaced =>
      super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    if (isScalarZero(rhs)) {
      if (failOnError) {
        divByZeroError()
      } else {
        withResource(Scalar.fromNull(outputType(lhs.getBase, rhs))) { nullScalar =>
          ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

object GpuDivideUtil {
  def decimalDataType(l: DecimalType, r: DecimalType): DecimalType = {
    // Spark does
    // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
    // Scale: max(6, s1 + p2 + 1)
    // But ... We need to do rounding at the end so we need one more than that.
    val s = math.max(6, l.scale + r.precision + 1) + 1
    val p = l.precision - l.scale + r.scale + s
    // TODO once we have 128-bit decimal support we should match the config for precision loss.
    DecimalType(math.min(p, 38), math.min(s, 38))
  }
}

// This is for doubles and floats...
case class GpuDivide(left: Expression, right: Expression,
    failOnErrorOverride: Boolean = ShimLoader.getSparkShims.shouldFailDivByZero())
      extends GpuDivModLike {

  override lazy val failOnError: Boolean = failOnErrorOverride

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = (left.dataType, right.dataType) match {
    case (_: DecimalType, _: DecimalType) => BinaryOp.DIV
    case _ => BinaryOp.TRUE_DIV
  }

  override def outputTypeOverride: DType =
    GpuColumnVector.getNonNestedRapidsType(dataType)

  // Override the output type as a special case for decimal
  override def dataType: DataType = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) => GpuDivideUtil.decimalDataType(l, r)
    case _ => super.dataType
  }

  private def getIntermediaryType(r: DecimalType): DecimalType = {
    val outType = dataType.asInstanceOf[DecimalType]
    // We should never hit a case where the newType hits precision > Decimal.MAX_LONG_DIGITS
    // as we have check for it in tagExprForGpu.
    DecimalType(outType.precision, outType.scale + r.scale)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (_: DecimalType, r: DecimalType) => {
        val (upcastedLhs, upcastedRhs) = if (!DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(r)) {
          // we are casting to the smallest 64-bit decimal so the answer doesn't exceed 64-bit
          val decimalType = createCudfDecimal(10, r.scale)
          (lhs.getBase.castTo(decimalType), rhs.getBase.castTo(decimalType))
        } else {
          (lhs.getBase(), rhs.getBase())
        }
        val newType = getIntermediaryType(r)
        withResource(upcastedLhs) { upcastedLhs =>
          withResource(upcastedRhs) { upcastedRhs =>
            withResource(upcastedLhs.castTo(GpuColumnVector.getNonNestedRapidsType(newType))) {
              modLhs =>
                super.doColumnar(GpuColumnVector.from(modLhs, newType),
                  GpuColumnVector.from(upcastedRhs, DecimalType(10, r.scale)))
            }
          }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (_: DecimalType, r: DecimalType) => {
        val (upcastedLhs, upcastedRhs) = if (!DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(r)) {
          // we are casting to the smallest 64-bit decimal so the answer doesn't overflow
          val sparkDecimalType = DecimalType(10, r.scale)
          val decimalType = GpuColumnVector.getNonNestedRapidsType(sparkDecimalType)
          (lhs.getBase.castTo(decimalType),
            GpuScalar.from(rhs.getBigDecimal().intValue().toLong, sparkDecimalType))
        } else {
          (lhs.getBase(), rhs)
        }
        val newType = getIntermediaryType(r)
        withResource(upcastedLhs) { upcastedLhs =>
          withResource(upcastedRhs) { upcastedRhs =>
            withResource(upcastedLhs.castTo(GpuColumnVector.getNonNestedRapidsType(newType))) {
              modLhs => super.doColumnar(GpuColumnVector.from(modLhs, newType), upcastedRhs)
            }
          }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (_: DecimalType, r: DecimalType) => {
        val (upcastedLhs, upcastedRhs) = if (!DecimalType.is32BitDecimalType(dataType) &&
          DecimalType.is32BitDecimalType(r)) {
          // we are casting to the smallest 64-bit decimal so the answer doesn't overflow
          val sparkDecimalType = DecimalType(10, r.scale)
          val decimalType = GpuColumnVector.getNonNestedRapidsType(sparkDecimalType)
          (GpuScalar.from(lhs.getBigDecimal().intValue().toLong, sparkDecimalType),
            rhs.getBase.castTo(decimalType))
        } else {
          (lhs, rhs.getBase())
        }
        val newType = getIntermediaryType(r)
        withResource(upcastedRhs) { upcastedRhs =>
          withResource(upcastedLhs) { upcastedLhs =>
            withResource(GpuScalar.from(upcastedLhs.getBigDecimal.longValue(), newType)) { modLhs =>
              super.doColumnar(modLhs, upcastedRhs)
            }
          }
        }
      }
      case _ => super.doColumnar(lhs, rhs)
    }
  }
}

case class GpuIntegralDivide(left: Expression, right: Expression) extends GpuDivModLike {
  override def inputType: AbstractDataType = TypeCollection(IntegralType, DecimalType)

  override def dataType: DataType = LongType
  override def outputTypeOverride: DType = DType.INT64
  // CUDF does not support casting output implicitly for decimal binary ops, so we work around
  // it here where we want to force the output to be a Long.
  override def castOutputAtEnd: Boolean = true

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.DIV

  override def sqlOperator: String = "div"
}

case class GpuRemainder(left: Expression, right: Expression) extends GpuDivModLike {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"

  override def binaryOp: BinaryOp = BinaryOp.MOD
}


case class GpuPmod(left: Expression, right: Expression) extends GpuDivModLike {
  override def inputType: AbstractDataType = NumericType

  override def binaryOp: BinaryOp = BinaryOp.PMOD

  override def symbol: String = "pmod"

  override def dataType: DataType = left.dataType
}

trait GpuGreatestLeastBase extends ComplexTypeMergingExpression with GpuExpression {
  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  /**
   * The binary operation that should be performed when combining two values together.
   */
  def binaryOp: BinaryOp

  /**
   * In the case of floating point values should NaN win and become the output if NaN is
   * the value for either input, or lose and not be the output unless the other choice is
   * null.
   */
  def shouldNanWin: Boolean

  private[this] def isFp = dataType == FloatType || dataType == DoubleType
  // TODO need a better way to do this for nested types
  protected lazy val dtype: DType = GpuColumnVector.getNonNestedRapidsType(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(
        s"input to function $prettyName requires at least two arguments")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
            s" got LEAST(${children.map(_.dataType.catalogString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, s"function $prettyName")
    }
  }

  /**
   * Convert the input into either a ColumnVector or a Scalar
   * @param a what to convert
   * @param expandScalar if we get a scalar should we expand it out to a ColumnVector to avoid
   *                     scalar scalar math.
   * @param rows If we expand a scalar how many rows should we do?
   * @return the resulting ColumnVector or Scalar
   */
  private[this] def convertAndCloseIfNeeded(
      a: Any,
      expandScalar: Boolean,
      rows: Int): AutoCloseable =
    a match {
      case gcv: GpuColumnVector => gcv.getBase
      case cv: ColumnVector => cv
      case s: Scalar =>
        if (expandScalar) {
          withResource(s) { s =>
            ColumnVector.fromScalar(s, rows)
          }
        } else {
          s
        }
      case a =>
        if (expandScalar) {
          withResource(GpuScalar.from(a, dataType)) { s =>
            ColumnVector.fromScalar(s, rows)
          }
        } else {
          GpuScalar.from(a, dataType)
        }
    }

  /**
   * Take 2 inputs that are either a Scalar or a ColumnVector and combine them with the correct
   * operator. This will blow up if both of the values are scalars though.
   * @param r first value
   * @param c second value
   * @return the combined value
   */
  private[this] def combineButNoClose(r: Any, c: Any): Any = (r, c) match {
    case (r: ColumnVector, c: ColumnVector) =>
      r.binaryOp(binaryOp, c, dtype)
    case (r: ColumnVector, c: Scalar) =>
      r.binaryOp(binaryOp, c, dtype)
    case (r: Scalar, c: ColumnVector) =>
      r.binaryOp(binaryOp, c, dtype)
    case _ => throw new IllegalStateException(s"Unexpected inputs: $r, $c")
  }

  private[this] def makeNanWin(checkForNans: ColumnVector, result: ColumnVector): ColumnVector = {
    withResource(checkForNans.isNan) { shouldReplace =>
      shouldReplace.ifElse(checkForNans, result)
    }
  }

  private[this] def makeNanWin(checkForNans: Scalar, result: ColumnVector): ColumnVector = {
    if (GpuScalar.isNan(checkForNans)) {
      ColumnVector.fromScalar(checkForNans, result.getRowCount.toInt)
    } else {
      result.incRefCount()
    }
  }

  private[this] def makeNanLose(resultIfNotNull: ColumnVector,
      checkForNans: ColumnVector): ColumnVector = {
    withResource(checkForNans.isNan) { isNan =>
      withResource(resultIfNotNull.isNotNull) { isNotNull =>
        withResource(isNan.and(isNotNull)) { shouldReplace =>
          shouldReplace.ifElse(resultIfNotNull, checkForNans)
        }
      }
    }
  }

  private[this] def makeNanLose(resultIfNotNull: Scalar,
      checkForNans: ColumnVector): ColumnVector = {
    if (resultIfNotNull.isValid) {
      withResource(checkForNans.isNan) { shouldReplace =>
        shouldReplace.ifElse(resultIfNotNull, checkForNans)
      }
    } else {
      // Nothing to replace because the scalar is null
      checkForNans.incRefCount()
    }
  }

  /**
   * Cudf does not handle floating point like Spark wants when it comes to NaN values.
   * Spark wants NaN > anything except for null, and null is either the smallest value when used
   * with the greatest operator or the largest value when used with the least value.
   * This does more computation, but gets the right answer in those cases.
   * @param r first value
   * @param c second value
   * @return the combined value
   */
  private[this] def combineButNoCloseFp(r: Any, c: Any): Any = (r, c) match {
    case (r: ColumnVector, c: ColumnVector) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case (r: ColumnVector, c: Scalar) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case (r: Scalar, c: ColumnVector) =>
      withResource(r.binaryOp(binaryOp, c, dtype)) { tmp =>
        if (shouldNanWin) {
          withResource(makeNanWin(r, tmp)) { tmp2 =>
            makeNanWin(c, tmp2)
          }
        } else {
          withResource(makeNanLose(r, tmp)) { tmp2 =>
            makeNanLose(c, tmp2)
          }
        }
      }
    case _ => throw new IllegalStateException(s"Unexpected inputs: $r, $c")
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val numRows = batch.numRows()

    val result = children.foldLeft[Any](null) { (r, c) =>
      withResource(
        convertAndCloseIfNeeded(c.columnarEval(batch), false, numRows)) { cVal =>
        withResource(convertAndCloseIfNeeded(r, cVal.isInstanceOf[Scalar], numRows)) { rVal =>
          if (isFp) {
            combineButNoCloseFp(rVal, cVal)
          } else {
            combineButNoClose(rVal, cVal)
          }
        }
      }
    }
    // The result should always be a ColumnVector at this point
    GpuColumnVector.from(result.asInstanceOf[ColumnVector], dataType)
  }
}

case class GpuLeast(children: Seq[Expression]) extends GpuGreatestLeastBase {
  override def binaryOp: BinaryOp = BinaryOp.NULL_MIN
  override def shouldNanWin: Boolean = false
}

case class GpuGreatest(children: Seq[Expression]) extends GpuGreatestLeastBase {
  override def binaryOp: BinaryOp = BinaryOp.NULL_MAX
  override def shouldNanWin: Boolean = true
}
