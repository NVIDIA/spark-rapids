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

import java.math.BigInteger

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.DecimalUtil.createCudfDecimal
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuAnsi extends Arm {
  def needBasicOpOverflowCheck(dt: DataType): Boolean =
    dt.isInstanceOf[IntegralType]

  def minValueScalar(dt: DataType): Scalar = dt match {
    case ByteType => Scalar.fromByte(Byte.MinValue)
    case ShortType => Scalar.fromShort(Short.MinValue)
    case IntegerType => Scalar.fromInt(Int.MinValue)
    case LongType => Scalar.fromLong(Long.MinValue)
    case other =>
      throw new IllegalArgumentException(s"$other does not need an ANSI check for this operator")
  }

  def assertMinValueOverflow(cv: GpuColumnVector, op: String): Unit = {
    withResource(minValueScalar(cv.dataType())) { minVal =>
      withResource(cv.getBase.equalToNullAware(minVal)) { isMinVal =>
        withResource(isMinVal.any()) { anyFound =>
          if (anyFound.isValid && anyFound.getBoolean) {
            throw new ArithmeticException(s"One or more rows overflow for $op operation.")
          }
        }
      }
    }
  }
}

case class GpuUnaryMinus(child: Expression, failOnError: Boolean) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  override def sql: String = s"(- ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : ColumnVector = {
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Because of 2s compliment we need to only worry about the min value for integer types.
      GpuAnsi.assertMinValueOverflow(input, "minus")
    }
    dataType match {
      case dt: DecimalType =>
        val scale = dt.scale
        if (DecimalType.is32BitDecimalType(dt)) {
          withResource(Scalar.fromDecimal(-scale, 0)) { scalar =>
            scalar.sub(input.getBase)
          }
        } else if (DecimalType.is64BitDecimalType(dt)) {
          withResource(Scalar.fromDecimal(-scale, 0L)) { scalar =>
            scalar.sub(input.getBase)
          }
        } else { // Decimal-128
          withResource(Scalar.fromDecimal(-scale, BigInteger.ZERO)) { scalar =>
            scalar.sub(input.getBase)
          }
        }
      case _ =>
        withResource(Scalar.fromByte(0.toByte)) { scalar =>
          scalar.sub(input.getBase)
        }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    val literalZero = dataType match {
      case LongType => ast.Literal.ofLong(0)
      case FloatType => ast.Literal.ofFloat(0)
      case DoubleType => ast.Literal.ofDouble(0)
      case IntegerType => ast.Literal.ofInt(0)
    }
    new ast.BinaryOperation(ast.BinaryOperator.SUB, literalZero,
      child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns))
  }
}

case class GpuUnaryPositive(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : ColumnVector = input.getBase.incRefCount()

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)
  }
}

case class GpuAbs(child: Expression, failOnError: Boolean) extends CudfUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  override def unaryOp: UnaryOp = UnaryOp.ABS

  override def doColumnar(input: GpuColumnVector) : ColumnVector = {
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Because of 2s compliment we need to only worry about the min value for integer types.
      GpuAnsi.assertMinValueOverflow(input, "abs")
    }
    super.doColumnar(input)
  }
}

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

case class GpuAdd(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Check overflow. It is true when both arguments have the opposite sign of the result.
      // Which is equal to "((x ^ r) & (y ^ r)) < 0" in the form of arithmetic.
      closeOnExcept(ret) { r =>
        val signCV = withResource(r.bitXor(lhs)) { lXor =>
          withResource(r.bitXor(rhs)) { rXor =>
            lXor.bitAnd(rXor)
          }
        }
        val signDiffCV = withResource(signCV) { sign =>
          withResource(Scalar.fromInt(0)) { zero =>
            sign.lessThan(zero)
          }
        }
        withResource(signDiffCV) { signDiff =>
          withResource(signDiff.any()) { any =>
            if (any.isValid && any.getBoolean) {
              throw new ArithmeticException("One or more rows overflow for Add operation.")
            }
          }
        }
      }
    }
    ret
  }
}

case class GpuSubtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
    if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType)) {
      // Check overflow. It is true if the arguments have different signs and
      // the sign of the result is different from the sign of x.
      // Which is equal to "((x ^ y) & (x ^ r)) < 0" in the form of arithmetic.
      closeOnExcept(ret) { r =>
        val signCV = withResource(lhs.bitXor(rhs)) { xyXor =>
          withResource(lhs.bitXor(r)) { xrXor =>
            xyXor.bitAnd(xrXor)
          }
        }
        val signDiffCV = withResource(signCV) { sign =>
          withResource(Scalar.fromInt(0)) { zero =>
            sign.lessThan(zero)
          }
        }
        withResource(signDiffCV) { signDiff =>
          withResource(signDiff.any()) { any =>
            if (any.isValid && any.getBoolean) {
              throw new ArithmeticException("One or more rows overflow for Subtract operation.")
            }
          }
        }
      }
    }
    ret
  }
}

object GpuMultiplyUtil {
  def decimalPrecision(l: DecimalType, r: DecimalType): Int =
    l.precision + r.precision + 1

  def decimalDataType(l: DecimalType, r: DecimalType): DecimalType = {
    val p = decimalPrecision(l, r)
    val s = l.scale + r.scale
    DecimalType(p, s)
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

  @transient private[this] lazy val decimalResultDType =
    createCudfDecimal(dataType.asInstanceOf[DecimalType])
  @transient private[this] lazy val decimalInputDType =
    createCudfDecimal(left.dataType.asInstanceOf[DecimalType])
  @transient private[this] lazy val decimalUpcastType =
    DType.create(decimalResultDType.getTypeId, decimalInputDType.getScale)
  @transient private[this] lazy val sparkDecimalUpcastType = decimalResultDType.getTypeId match {
    case DType.DTypeEnum.DECIMAL64 =>
      DecimalType(DType.DECIMAL32_MAX_PRECISION + 1, -decimalInputDType.getScale)
    case DType.DTypeEnum.DECIMAL128 =>
      DecimalType(DType.DECIMAL64_MAX_PRECISION + 1, -decimalInputDType.getScale)
    case id =>
      throw new IllegalArgumentException(s"Unexpected type found $id")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    if (left.dataType.isInstanceOf[DecimalType] &&
        decimalResultDType.getTypeId != decimalInputDType.getTypeId) {
      // need to upcast inputs so we don't possibly overflow.
      withResource(lhs.getBase.castTo(decimalUpcastType)) { decimalLhs =>
        withResource(rhs.getBase.castTo(decimalUpcastType)) { decimalRhs =>
          val tmp = decimalLhs.mul(decimalRhs, decimalResultDType)
          if (tmp.getType != decimalResultDType) {
            withResource(tmp) { tmp =>
              tmp.castTo(decimalResultDType)
            }
          } else {
            tmp
          }
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (left.dataType.isInstanceOf[DecimalType] &&
        decimalResultDType.getTypeId != decimalInputDType.getTypeId) {
      // need to upcast inputs so we don't possibly overflow.
      withResource(lhs.getBase.castTo(decimalUpcastType)) { decimalLhs =>
        withResource(GpuScalar.from(rhs.getValue, sparkDecimalUpcastType)) { decimalRhs =>
          val tmp = decimalLhs.mul(decimalRhs, decimalResultDType)
          if (tmp.getType != decimalResultDType) {
            withResource(tmp) { tmp =>
              tmp.castTo(decimalResultDType)
            }
          } else {
            tmp
          }
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if (left.dataType.isInstanceOf[DecimalType] &&
        decimalResultDType.getTypeId != decimalInputDType.getTypeId) {
      // need to upcast inputs so we don't possibly overflow.
      withResource(GpuScalar.from(lhs.getValue, sparkDecimalUpcastType)) { decimalLhs =>
        withResource(rhs.getBase.castTo(decimalUpcastType)) { decimalRhs =>
          val tmp = decimalLhs.mul(decimalRhs, decimalResultDType)
          if (tmp.getType != decimalResultDType) {
            withResource(tmp) { tmp =>
              tmp.castTo(decimalResultDType)
            }
          } else {
            tmp
          }
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

object GpuDivModLike extends Arm {
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
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL128 =>
        s.getBigDecimal.toBigInteger.equals(BigInteger.ZERO)
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
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL128 =>
        Scalar.fromDecimal(d.getScale, BigInteger.ZERO)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL64 =>
        Scalar.fromDecimal(d.getScale, 0L)
      case d if d.getTypeId == DType.DTypeEnum.DECIMAL32 =>
        Scalar.fromDecimal(d.getScale, 0)
      case t => throw new IllegalArgumentException(s"Unexpected type: $t")
    }
  }

  /**
   * This is for the case as below.
   *
   *   left : [1,  2,  Long.MinValue,  3, Long.MinValue]
   *   right: [2, -1,             -1, -1,             6]
   *
   * The 3rd row (Long.MinValue, -1) will cause an overflow of the integral division.
   */
  def isDivOverflow(left: GpuColumnVector, right: GpuColumnVector): Boolean = {
    left.dataType() match {
      case LongType =>
        withResource(Scalar.fromLong(Long.MinValue)) { minLong =>
          withResource(left.getBase.equalTo(minLong)) { eqToMinLong =>
            withResource(Scalar.fromInt(-1)) { minusOne =>
              withResource(right.getBase.equalTo(minusOne)) { eqToMinusOne =>
                withResource(eqToMinLong.and(eqToMinusOne)) { overFlowVector =>
                  withResource(overFlowVector.any()) { isOverFlow =>
                    isOverFlow.isValid && isOverFlow.getBoolean
                  }
                }
              }
            }
          }
        }
      case _ => false
    }
  }

  def isDivOverflow(left: GpuColumnVector, right: GpuScalar): Boolean = {
    left.dataType() match {
      case LongType =>
        (right.isValid && right.getValue == -1) && {
          withResource(Scalar.fromLong(Long.MinValue)) { minLong =>
            left.getBase.contains(minLong)
          }
        }
      case _ => false
    }
  }

  def isDivOverflow(left: GpuScalar, right: GpuColumnVector): Boolean = {
    (left.isValid && left.getValue == Long.MinValue) && {
      withResource(Scalar.fromInt(-1)) { minusOne =>
        right.getBase.contains(minusOne)
      }
    }
  }
}

trait GpuDivModLike extends CudfBinaryArithmetic {
  lazy val failOnError: Boolean =
    ShimLoader.getSparkShims.shouldFailDivByZero()

  override def nullable: Boolean = true

  // Whether we should check overflow or not in ANSI mode.
  protected def checkDivideOverflow: Boolean = false

  import GpuDivModLike._

  private def divByZeroError(): Nothing = {
    throw new ArithmeticException("divide by zero")
  }

  private def divOverflowError(): Nothing = {
    throw new ArithmeticException("Overflow in integral divide.")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    if (failOnError) {
      withResource(makeZeroScalar(rhs.getBase.getType)) { zeroScalar =>
        if (rhs.getBase.contains(zeroScalar)) {
          divByZeroError()
        }
        if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
          divOverflowError()
        }
        super.doColumnar(lhs, rhs)
      }
    } else {
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        divOverflowError()
      }
      withResource(replaceZeroWithNull(rhs)) { replaced =>
        super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
      divOverflowError()
    }
    withResource(replaceZeroWithNull(rhs)) { replaced =>
      super.doColumnar(lhs, GpuColumnVector.from(replaced, rhs.dataType))
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (isScalarZero(rhs.getBase)) {
      if (failOnError) {
        divByZeroError()
      } else {
        withResource(Scalar.fromNull(outputType(lhs.getBase, rhs.getBase))) { nullScalar =>
          ColumnVector.fromScalar(nullScalar, lhs.getRowCount.toInt)
        }
      }
    } else {
      if (checkDivideOverflow && isDivOverflow(lhs, rhs)) {
        divOverflowError()
      }
      super.doColumnar(lhs, rhs)
    }
  }
}

object GpuDivideUtil {
  // For Spark the final desired output is
  // new_scale = max(6, lhs.scale + rhs.precision + 1)
  // new_precision = lhs.precision - lhs.scale + rhs.scale + new_scale
  // But Spark will round the final result, so we need at least one more
  // decimal place on the scale to be able to do the rounding too.
  // That rounding happens in `GpuCheckOverflow`

  def outputDecimalScale(l: DecimalType, r: DecimalType): Int =
    math.max(6, l.scale + r.precision + 1) + 1

  def outputDecimalPrecision(l: DecimalType, r: DecimalType, outputScale: Int): Int =
    l.precision - l.scale + r.scale + outputScale

  def outputDecimalType(l: DecimalType, r: DecimalType): DataType = {
    val outputScale = outputDecimalScale(l, r)
    DecimalType(outputDecimalPrecision(l, r, outputScale), outputScale)
  }

  // In CUDF a divide's output is the same precision as the input, but the scale
  // is lhs.scale - rhs.scale.

  // Spark casts the inputs to the same wider type, but we do not
  // know what the original lhs and rhs were. We need to make sure that we are going to provide
  // enough information to CUDF without overflowing to get the desired output scale and
  // precision based off of the inputs.
  //
  // To do this we get the output scale, and add it to the precision and scale for the
  // LHS, as an intermediate value. The RHS intermediate just needs to make sure that it matches
  // the same precision as the LHS so that CUDF is happy.

  def intermediateDecimalScale(l: DecimalType, outputScale: Int): Int = l.scale + outputScale

  def intermediateDecimalPrecision(l: DecimalType, r: DecimalType, outputScale: Int): Int = {
    // In practice r.precision == l.precision, but we want to future proof it a bit.
    math.max(l.precision + outputScale, r.precision)
  }

  def intermediateDecimalType(l: DecimalType, r: DecimalType, outputScale: Int): DecimalType =
    new DecimalType(
      intermediateDecimalPrecision(l, r, outputScale),
      intermediateDecimalScale(l, outputScale))
}

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

  // Override the output type as a special case for decimal
  override def dataType: DataType = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) => GpuDivideUtil.outputDecimalType(l, r)
    case _ => super.dataType
  }

  override def outputTypeOverride: DType =
    GpuColumnVector.getNonNestedRapidsType(dataType)

  @transient private[this] lazy val lhsDec = left.dataType.asInstanceOf[DecimalType]
  @transient private[this] lazy val rhsDec = right.dataType.asInstanceOf[DecimalType]
  @transient private[this] lazy val outputScale =
    GpuDivideUtil.outputDecimalScale(lhsDec, rhsDec)

  @transient private[this] lazy val intermediateLhsType: DecimalType =
    GpuDivideUtil.intermediateDecimalType(lhsDec, rhsDec, outputScale)

  @transient private[this] lazy val intermediateLhsCudfType =
    GpuColumnVector.getNonNestedRapidsType(intermediateLhsType)

  @transient private[this] lazy val intermediateRhsType =
    DecimalType(intermediateLhsType.precision, rhsDec.scale)

  @transient private[this] lazy val intermediateRhsCudfType =
    GpuColumnVector.getNonNestedRapidsType(intermediateRhsType)

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    // LHS and RHS are the same general
    if (left.dataType.isInstanceOf[DecimalType]) {
      // This will always be an upcast for the LHS (because at a minimum need to shift it over)
      withResource(lhs.getBase.castTo(intermediateLhsCudfType)) { upcastLhs =>
        val upcastRhs = if (right.dataType.equals(intermediateRhsType)) {
          rhs.getBase.incRefCount()
        } else {
          rhs.getBase.castTo(intermediateRhsCudfType)
        }
        withResource(upcastRhs) { upcastRhs =>
          super.doColumnar(GpuColumnVector.from(upcastLhs, intermediateLhsType),
            GpuColumnVector.from(upcastRhs, intermediateRhsType))
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // LHS and RHS are the same type
    if (left.dataType.isInstanceOf[DecimalType]) {
      // This will always be an upcast for the LHS (because at a minimum need to shift it over)
      withResource(lhs.getBase.castTo(intermediateLhsCudfType)) { upcastLhs =>
        withResource(GpuScalar(rhs.getValue, intermediateRhsType)) { upcastRhs =>
          super.doColumnar(GpuColumnVector.from(upcastLhs, intermediateLhsType), upcastRhs)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    // LHS and RHS are the same type
    if (left.dataType.isInstanceOf[DecimalType]) {
      // This will always be an upcast for the LHS (because at a minimum need to shift it over)
      withResource(GpuScalar(lhs.getValue, intermediateLhsType)) { upcastLhs =>
        val upcastRhs = if (right.dataType.equals(intermediateRhsType)) {
          rhs.getBase.incRefCount()
        } else {
          rhs.getBase.castTo(intermediateRhsCudfType)
        }
        withResource(upcastRhs) { upcastRhs =>
          super.doColumnar(upcastLhs, GpuColumnVector.from(upcastRhs, intermediateRhsType))
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

case class GpuIntegralDivide(left: Expression, right: Expression) extends GpuDivModLike {
  override def inputType: AbstractDataType = TypeCollection(IntegralType, DecimalType)

  lazy val failOnOverflow: Boolean =
    ShimLoader.getSparkShims.shouldFailDivOverflow

  override def checkDivideOverflow: Boolean = left.dataType match {
    case LongType if failOnOverflow => true
    case _ => false
  }

  override def dataType: DataType = LongType
  override def outputTypeOverride: DType = DType.INT64
  // CUDF does not support casting output implicitly for decimal binary ops, so we work around
  // it here where we want to force the output to be a Long.
  override def castOutputAtEnd: Boolean = left.dataType.isInstanceOf[DecimalType]

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

trait GpuGreatestLeastBase extends ComplexTypeMergingExpression with GpuExpression
  with ShimExpression {

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
      case cv: ColumnVector => cv
      case gcv: GpuColumnVector => gcv.getBase
      case gs: GpuScalar => withResource(gs) { s =>
          if (expandScalar) {
            ColumnVector.fromScalar(s.getBase, rows)
          } else {
            gs.getBase.incRefCount()
          }
      }
      case null =>
        if (expandScalar) {
          GpuColumnVector.columnVectorFromNull(rows, dataType)
        } else {
          GpuScalar.from(null, dataType)
        }
      case o =>
        // It should not be here. since other things here should be converted to a GpuScalar
        throw new IllegalStateException(s"Unexpected inputs: $o")
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
