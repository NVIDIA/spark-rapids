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

case class GpuCeil(child: Expression) extends CudfUnaryMathExpression("CEIL") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

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
      case _: DecimalType =>
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
}

case class GpuFloor(child: Expression) extends CudfUnaryMathExpression("FLOOR") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

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
      case _: DecimalType =>
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
    val scaleVal = dataType match {
      case DecimalType.Fixed(_, s) => s
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
        scale.getValue.asInstanceOf[Int]
      case _ => throw new IllegalArgumentException(s"Round operator doesn't support $dataType")
    }
    val lhsValue = value.getBase
    lhsValue.round(scaleVal, roundMode)
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
