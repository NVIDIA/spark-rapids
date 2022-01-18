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

import java.io.Serializable

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression

import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

abstract class CudfUnaryMathExpression(name: String) extends GpuUnaryMathExpression(name)
  with CudfUnaryExpression

abstract class GpuUnaryMathExpression(name: String) extends GpuUnaryExpression
  with Serializable with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"$name($child)"
  override def prettyName: String = name
}

case class GpuAcos(child: Expression) extends CudfUnaryMathExpression("ACOS") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuToDegrees(child: Expression) extends GpuUnaryMathExpression("DEGREES") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(180d / Math.PI)) { multiplier =>
      input.getBase.mul(multiplier)
    }
  }
}

case class GpuToRadians(child: Expression) extends GpuUnaryMathExpression("RADIANS") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(Math.PI / 180d)) { multiplier =>
      input.getBase.mul(multiplier)
    }
  }
}

case class GpuAcoshImproved(child: Expression) extends CudfUnaryMathExpression("ACOSH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOSH
}

case class GpuAcoshCompat(child: Expression) extends GpuUnaryMathExpression("ACOSH") {
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    // StrictMath.log(x + math.sqrt(x * x - 1.0))
    val base = input.getBase
    withResource(base.mul(base)) { squared =>
      withResource(Scalar.fromDouble(1.0)) { one =>
        withResource(squared.sub(one)) { squaredMinOne =>
          withResource(squaredMinOne.sqrt()) { sqrt =>
            withResource(base.add(sqrt)) { sum =>
              sum.log()
            }
          }
        }
      }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    // StrictMath.log(x + math.sqrt(x * x - 1.0))
    val x = child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)
    new ast.UnaryOperation(ast.UnaryOperator.LOG,
      new ast.BinaryOperation(ast.BinaryOperator.ADD, x,
        new ast.UnaryOperation(ast.UnaryOperator.SQRT,
          new ast.BinaryOperation(ast.BinaryOperator.SUB,
            new ast.BinaryOperation(ast.BinaryOperator.MUL, x, x), ast.Literal.ofDouble(1)))))
  }
}

case class GpuAsin(child: Expression) extends CudfUnaryMathExpression("ASIN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAsinhImproved(child: Expression) extends CudfUnaryMathExpression("ASINH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSINH
}

case class GpuAsinhCompat(child: Expression) extends GpuUnaryMathExpression("ASINH") {
  override def outputTypeOverride: DType = DType.FLOAT64

  def computeBasic(input: ColumnVector): ColumnVector =
    withResource(input.mul(input)) { squared =>
      withResource(Scalar.fromDouble(1.0)) { one =>
        withResource(squared.add(one)) { squaredPlusOne =>
          withResource(squaredPlusOne.sqrt()) { sqrt =>
            withResource(input.add(sqrt)) { sum =>
              sum.log()
            }
          }
        }
      }
    }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Typically we would just use UnaryOp.ARCSINH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    //  x match {
    //    case Double.NegativeInfinity => Double.NegativeInfinity
    //    case _ => StrictMath.log(x + math.sqrt(x * x + 1.0)) }
    val base = input.getBase
    withResource(computeBasic(base)) { basic =>
      withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negInf =>
        withResource(base.equalTo(negInf)) { eqNegInf =>
          eqNegInf.ifElse(negInf, basic)
        }
      }
    }
  }
}

case class GpuAtan(child: Expression) extends CudfUnaryMathExpression("ATAN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAtanh(child: Expression) extends CudfUnaryMathExpression("ATANH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

object GpuFloorCeil {
  def unboundedOutputPrecision(dt: DecimalType): Int = {
    if (dt.scale == 0) {
      dt.precision
    } else {
      dt.precision - dt.scale + 1
    }
  }
}

case class GpuCeil(child: Expression) extends CudfUnaryMathExpression("CEIL") {
  override def dataType: DataType = child.dataType match {
    case dt: DecimalType =>
      DecimalType.bounded(GpuFloorCeil.unboundedOutputPrecision(dt), 0)
    case _ => LongType
  }

  override def hasSideEffects: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.CEIL
  override def outputTypeOverride: DType =
    dataType match {
      case dt: DecimalType =>
        DecimalUtil.createCudfDecimal(dt.precision, dt.scale)
      case _ =>
        DType.INT64
    }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.dataType() match {
      case DoubleType =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          super.doColumnar(GpuColumnVector.from(inputWithNansToZero, DoubleType))
        }
      case LongType =>
        // Long is a noop in spark, but for cudf it is not.
        input.getBase.incRefCount()
      case dt: DecimalType =>
        val outputType = dataType.asInstanceOf[DecimalType]
        // check for out of bound values when output precision is constrained by MAX_PRECISION
        if (outputType.precision == DecimalType.MAX_PRECISION && dt.scale < 0) {
          withResource(DecimalUtil.outOfBounds(input.getBase, outputType)) { outOfBounds =>
            withResource(outOfBounds.any()) { isAny =>
              if (isAny.isValid && isAny.getBoolean) {
                throw new ArithmeticException(s"Some data cannot be represented as $outputType")
              }
            }
          }
        }
        super.doColumnar(input)
    }
  }
}

case class GpuCos(child: Expression) extends CudfUnaryMathExpression("COS") {
  override def unaryOp: UnaryOp = UnaryOp.COS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExp(child: Expression) extends CudfUnaryMathExpression("EXP") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExpm1(child: Expression) extends CudfUnaryMathExpression("EXPM1") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(input.getBase.unaryOp(unaryOp)) { cv =>
      withResource(Scalar.fromInt(1)) { sc =>
        cv.binaryOp(BinaryOp.SUB, sc, outputTypeOverride)
      }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    new ast.BinaryOperation(ast.BinaryOperator.SUB,
      super.convertToAst(numFirstTableColumns),
      ast.Literal.ofDouble(1))
  }
}

case class GpuFloor(child: Expression) extends CudfUnaryMathExpression("FLOOR") {
  override def dataType: DataType = child.dataType match {
    case dt: DecimalType =>
      DecimalType.bounded(GpuFloorCeil.unboundedOutputPrecision(dt), 0)
    case _ => LongType
  }

  override def hasSideEffects: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.FLOOR

  override def outputTypeOverride: DType =
    dataType match {
      case dt: DecimalType =>
        DecimalUtil.createCudfDecimal(dt.precision, dt.scale)
      case _ =>
        DType.INT64
    }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    input.dataType() match {
      case DoubleType =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          super.doColumnar(GpuColumnVector.from(inputWithNansToZero, DoubleType))
        }
      case LongType =>
        // Long is a noop in spark, but for cudf it is not.
        input.getBase.incRefCount()
      case dt: DecimalType =>
        val outputType = dataType.asInstanceOf[DecimalType]
        // check for out of bound values when output precision is constrained by MAX_PRECISION
        if (outputType.precision == DecimalType.MAX_PRECISION && dt.scale < 0) {
          withResource(DecimalUtil.outOfBounds(input.getBase, outputType)) { outOfBounds =>
            withResource(outOfBounds.any()) { isAny =>
              if (isAny.isValid && isAny.getBoolean) {
                throw new ArithmeticException(s"Some data cannot be represented as $outputType")
              }
            }
          }
        }
        super.doColumnar(input)
    }
  }
}

case class GpuLog(child: Expression) extends CudfUnaryMathExpression("LOG") {
  override def unaryOp: UnaryOp = UnaryOp.LOG
  override def outputTypeOverride: DType = DType.FLOAT64
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(input)) { normalized =>
      super.doColumnar(GpuColumnVector.from(normalized, child.dataType))
    }
  }
}

object GpuLogarithm extends Arm {

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned GpuColumnVector.
   */
  def fixUpLhs(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(0)) { zero =>
      withResource(input.getBase.binaryOp(BinaryOp.LESS_EQUAL, zero, DType.BOOL8)) { zeroOrLess =>
        withResource(Scalar.fromNull(DType.FLOAT64)) { nullScalar =>
          zeroOrLess.ifElse(nullScalar, input.getBase)
        }
      }
    }
  }

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned Scalar.
   */
  def fixUpLhs(input: GpuScalar): GpuScalar = {
    if (input.isValid && input.getValue.asInstanceOf[Double] <= 0) {
      GpuScalar(null, DoubleType)
    } else {
      input.incRefCount
    }
  }
}

case class GpuLogarithm(left: Expression, right: Expression)
  extends CudfBinaryMathExpression("LOG_BASE") {

  override def binaryOp: BinaryOp = BinaryOp.LOG_BASE
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(GpuColumnVector.from(fixedLhs, left.dataType), rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(fixedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(GpuColumnVector.from(fixedLhs, left.dataType), rhs)
    }
  }
}

case class GpuSin(child: Expression) extends CudfUnaryMathExpression("SIN") {
  override def unaryOp: UnaryOp = UnaryOp.SIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSignum(child: Expression) extends GpuUnaryMathExpression("SIGNUM") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromDouble(0)) { num =>
      withResource(Scalar.fromDouble(1)) { hiReplace =>
        withResource(Scalar.fromDouble(-1)) { loReplace =>
          input.getBase.clamp(num, loReplace, num, hiReplace)
        }
      }
    }
  }

  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTanh(child: Expression) extends CudfUnaryMathExpression("TANH") {
  override def unaryOp: UnaryOp = UnaryOp.TANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCosh(child: Expression) extends CudfUnaryMathExpression("COSH") {
  override def unaryOp: UnaryOp = UnaryOp.COSH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSinh(child: Expression) extends CudfUnaryMathExpression("SINH") {
  override def unaryOp: UnaryOp = UnaryOp.SINH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSqrt(child: Expression) extends CudfUnaryMathExpression("SQRT") {
  override def unaryOp: UnaryOp = UnaryOp.SQRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCbrt(child: Expression) extends CudfUnaryMathExpression("CBRT") {
  override def unaryOp: UnaryOp = UnaryOp.CBRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTan(child: Expression) extends CudfUnaryMathExpression("TAN") {
  override def unaryOp: UnaryOp = UnaryOp.TAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCot(child: Expression) extends GpuUnaryMathExpression("COT") {

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromInt(1)) { one =>
      withResource(input.getBase.unaryOp(UnaryOp.TAN)) { tan =>
        one.div(tan)
      }
    }
  }

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    new ast.BinaryOperation(ast.BinaryOperator.DIV, ast.Literal.ofDouble(1),
      new ast.UnaryOperation(ast.UnaryOperator.TAN,
        child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns)))
  }
}

case class GpuHypot(left: Expression, right: Expression) extends CudfBinaryMathExpression("HYPOT") {

  override def binaryOp: BinaryOp = BinaryOp.ADD

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    // naive implementation (needs to handle overflow/underflow)
    withResource(lhs.mul(lhs)) { lhsSquared =>
      withResource(rhs.mul(rhs)) { rhsSquared =>
        withResource(lhsSquared.add(rhsSquared)) { sumSquares =>
          sumSquares.sqrt()
        }
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    doColumnar(ColumnVector.fromScalar(lhs.getBase, rhs.getRowCount.toInt), rhs.getBase)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    doColumnar(lhs.getBase, ColumnVector.fromScalar(rhs.getBase, lhs.getRowCount.toInt))
  }
}

abstract class CudfBinaryMathExpression(name: String) extends CudfBinaryExpression
    with Serializable with ImplicitCastInputTypes {
  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType)
  override def toString: String = s"$name($left, $right)"
  override def prettyName: String = name
  override def dataType: DataType = DoubleType
}

abstract class GpuRoundBase(child: Expression, scale: Expression) extends GpuBinaryExpression
  with Serializable with ImplicitCastInputTypes {

  override def left: Expression = child
  override def right: Expression = scale

  def roundMode: RoundMode

  override lazy val dataType: DataType = child.dataType match {
    // if the new scale is bigger which means we are scaling up,
    // keep the original scale as `Decimal` does
    case DecimalType.Fixed(p, s) => DecimalType(p, if (_scale > s) s else _scale)
    case t => t
  }

  // Avoid repeated evaluation since `scale` is a constant int,
  // avoid unnecessary `child` evaluation in both codegen and non-codegen eval
  // by checking if scaleV == null as well.
  private lazy val scaleV: Any = scale match {
    case _: GpuExpression =>
      withResource(scale.columnarEval(null).asInstanceOf[GpuScalar]) { s =>
        s.getValue
      }
    case _ => scale.eval(EmptyRow)
  }
  private lazy val _scale: Int = scaleV.asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, IntegerType)

  override def doColumnar(value: GpuColumnVector, scale: GpuScalar): ColumnVector = {

    val lhsValue = value.getBase
    val scaleVal = scale.getValue.asInstanceOf[Int]

    dataType match {
      case DecimalType.Fixed(_, scaleVal) =>
        DecimalUtil.round(lhsValue, scaleVal, roundMode)
      case ByteType =>
        fixUpOverflowInts(() => Scalar.fromByte(0.toByte), scaleVal, lhsValue)
      case ShortType =>
        fixUpOverflowInts(() => Scalar.fromShort(0.toShort), scaleVal, lhsValue)
      case IntegerType =>
        fixUpOverflowInts(() => Scalar.fromInt(0), scaleVal, lhsValue)
      case LongType =>
        fixUpOverflowInts(() => Scalar.fromLong(0L), scaleVal, lhsValue)
      case FloatType =>
        fpZeroReplacement(
          () => Scalar.fromFloat(0.0f),
          () => Scalar.fromFloat(Float.PositiveInfinity),
          () => Scalar.fromFloat(Float.NegativeInfinity),
          scaleVal, lhsValue)
      case DoubleType =>
        fpZeroReplacement(
          () => Scalar.fromDouble(0.0),
          () => Scalar.fromDouble(Double.PositiveInfinity),
          () => Scalar.fromDouble(Double.NegativeInfinity),
          scaleVal, lhsValue)
      case _ =>
        throw new IllegalArgumentException(s"Round operator doesn't support $dataType")
    }
  }

  // Fixes up integral values rounded by a scale exceeding/reaching the max digits of data
  // type. Under this circumstance, cuDF may produce different results to Spark.
  //
  // In this method, we handle round overflow, aligning the inconsistent results to Spark.
  //
  // For scales exceeding max digits, we can simply return zero values.
  //
  // For scales equaling to max digits, we need to perform round. Fortunately, round up
  // will NOT occur on the max digits of numeric types except LongType. Therefore, we only
  // need to handle round down for most of types, through returning zero values.
  private def fixUpOverflowInts(zeroFn: () => Scalar,
      scale: Int,
      lhs: ColumnVector): ColumnVector = {
    // Rounding on the max digit of long values, which should be specialized handled since
    // it may be needed to round up, which will produce inconsistent results because of
    // overflow. Otherwise, we only need to handle round down situations.
    if (-scale == 19 && lhs.getType == DType.INT64) {
      fixUpInt64OnBounds(lhs)
    } else if (-scale >= DecimalUtil.getPrecisionForIntegralType(lhs.getType)) {
      withResource(zeroFn()) { s =>
        withResource(ColumnVector.fromScalar(s, lhs.getRowCount.toInt)) { zero =>
          // set null mask if necessary
          if (lhs.hasNulls) {
            zero.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs)
          } else {
            zero.incRefCount()
          }
        }
      }
    } else {
      lhs.round(scale, roundMode)
    }
  }

  // Compared to other non-decimal numeric types, Int64(LongType) is a bit special in terms of
  // rounding by the max digit. Because the bound values of LongType can be rounded up, while
  // other numeric types can only be rounded down:
  //
  //  the max value of Byte: 127
  //  The first digit is up to 1, which can't be rounded up.
  //  the max value of Short: 32767
  //  The first digit is up to 3, which can't be rounded up.
  //  the max value of Int32: 2147483647
  //  The first digit is up to 2, which can't be rounded up.
  //  the max value of Float32: 3.4028235E38
  //  The first digit is up to 3, which can't be rounded up.
  //  the max value of Float64: 1.7976931348623157E308
  //  The first digit is up to 1, which can't be rounded up.
  //  the max value of Int64: 9223372036854775807
  //  The first digit is up to 9, which can be rounded up.
  //
  // When rounding up 19-digits long values on the first digit, the result can be 1e19 or -1e19.
  // Since LongType can not hold these two values, the 1e19 overflows as -8446744073709551616L,
  // and the -1e19 overflows as 8446744073709551616L. The overflow happens in the same way for
  // HALF_UP (round) and HALF_EVEN (bround).
  private def fixUpInt64OnBounds(lhs: ColumnVector): ColumnVector = {
    // Builds predicates on whether there is a round up on the max digit or not
    val litForCmp = Seq(Scalar.fromLong(1000000000000000000L),
                        Scalar.fromLong(4L),
                        Scalar.fromLong(-4L))
    val (needRep, needNegRep) = withResource(litForCmp) { case Seq(base, four, minusFour) =>
      withResource(lhs.div(base)) { headDigit =>
        closeOnExcept(headDigit.greaterThan(four)) { posRep =>
          closeOnExcept(headDigit.lessThan(minusFour)) { negRep =>
            posRep -> negRep
          }
        }
      }
    }
    // Replaces with corresponding literals
    val litForRep = Seq(Scalar.fromLong(0L),
                        Scalar.fromLong(8446744073709551616L),
                        Scalar.fromLong(-8446744073709551616L))
    val repVal = withResource(litForRep) { case Seq(zero, upLit, negUpLit) =>
      withResource(needRep) { _ =>
        withResource(needNegRep) { _ =>
          withResource(needNegRep.ifElse(upLit, zero)) { negBranch =>
            needRep.ifElse(negUpLit, negBranch)
          }
        }
      }
    }
    // Handles null values
    withResource(repVal) { _ =>
      if (lhs.hasNulls) {
        repVal.mergeAndSetValidity(BinaryOp.BITWISE_AND, lhs)
      } else {
        repVal.incRefCount()
      }
    }
  }

  // Fixes up float points rounded by a scale exceeding the max digits of data type. Under this
  // circumstance, cuDF produces different results to Spark.
  // Compared to integral values, fixing up round overflow of float points needs to take care
  // of some special values: nan, inf, -inf.
  def fpZeroReplacement(zeroFn: () => Scalar,
      infFn: () => Scalar,
      negInfFn: () => Scalar,
      scale: Int,
      lhs: ColumnVector): ColumnVector = {
    val maxDigits = if (dataType == FloatType) 39 else 309
    if (-scale >= maxDigits) {
      // replaces common values (!Null AND !Nan AND !Inf And !-Inf) with zero, while keeps
      // all the special values unchanged
      withResource(Seq(zeroFn(), infFn(), negInfFn())) { case Seq(zero, inf, negInf) =>
        // builds joined predicate: !Null AND !Nan AND !Inf And !-Inf
        val joinedPredicate = {
          val conditions = Seq(() => lhs.isNotNan,
                               () => lhs.notEqualTo(inf),
                               () => lhs.notEqualTo(negInf))
          conditions.foldLeft(lhs.isNotNull) { case (buffer, builder) =>
            withResource(buffer) { _ =>
              withResource(builder()) { predicate =>
                buffer.and(predicate)
              }
            }
          }
        }
        withResource(joinedPredicate) { cond =>
          cond.ifElse(zero, lhs)
        }
      }
    } else if (scale >= maxDigits) {
      // just returns the original values
      lhs.incRefCount()
    } else {
      lhs.round(scale, roundMode)
    }
  }

  override def doColumnar(value: GpuColumnVector, scale: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the round operator to work")
  }

  override def doColumnar(value: GpuScalar, scale: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the round operator to work")
  }

  override def doColumnar(numRows: Int, value: GpuScalar, scale: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(value, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, scale)
    }
  }
}

case class GpuBRound(child: Expression, scale: Expression) extends
  GpuRoundBase(child, scale) {
  override def roundMode: RoundMode = RoundMode.HALF_EVEN
}

case class GpuRound(child: Expression, scale: Expression) extends
  GpuRoundBase(child, scale) {
  override def roundMode: RoundMode = RoundMode.HALF_UP
}

case class GpuPow(left: Expression, right: Expression)
    extends CudfBinaryMathExpression("POWER") {
  override def binaryOp: BinaryOp = BinaryOp.POW
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuRint(child: Expression) extends CudfUnaryMathExpression("ROUND") {
  override def unaryOp: UnaryOp = UnaryOp.RINT
  override def outputTypeOverride: DType = DType.FLOAT64
}
