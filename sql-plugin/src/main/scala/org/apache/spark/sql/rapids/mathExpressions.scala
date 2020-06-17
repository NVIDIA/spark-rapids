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

import java.io.Serializable

import ai.rapids.cudf.{BinaryOp, ColumnVector, DType, Scalar, UnaryOp}
import com.nvidia.spark.rapids.{Arm, CudfBinaryExpression, CudfUnaryExpression, FloatUtils, GpuColumnVector, GpuExpression, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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

case class GpuAcos(child: GpuExpression) extends CudfUnaryMathExpression("ACOS") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuToDegrees(child: GpuExpression) extends GpuUnaryMathExpression("DEGREES") {

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromDouble(180d / Math.PI)) { multiplier =>
      GpuColumnVector.from(input.getBase.mul(multiplier))
    }
  }
}

case class GpuToRadians(child: GpuExpression) extends GpuUnaryMathExpression("RADIANS") {

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromDouble(Math.PI / 180d)) { multiplier =>
      GpuColumnVector.from(input.getBase.mul(multiplier))
    }
  }
}

case class GpuAcoshImproved(child: GpuExpression) extends CudfUnaryMathExpression("ACOSH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOSH
}

case class GpuAcoshCompat(child: GpuExpression) extends GpuUnaryMathExpression("ACOSH") {
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    // Typically we would just use UnaryOp.ARCCOSH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    // StrictMath.log(x + math.sqrt(x * x - 1.0))
    val base = input.getBase
    val ret = withResource(base.mul(base)) { squared =>
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
    GpuColumnVector.from(ret)
  }
}

case class GpuAsin(child: GpuExpression) extends CudfUnaryMathExpression("ASIN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAsinhImproved(child: GpuExpression) extends CudfUnaryMathExpression("ASINH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCSINH
}

case class GpuAsinhCompat(child: GpuExpression) extends GpuUnaryMathExpression("ASINH") {
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

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    // Typically we would just use UnaryOp.ARCSINH, but there are corner cases where cudf
    // produces a better result (it does not overflow) than spark does, but our goal is
    // to match Spark's
    //  x match {
    //    case Double.NegativeInfinity => Double.NegativeInfinity
    //    case _ => StrictMath.log(x + math.sqrt(x * x + 1.0)) }
    val base = input.getBase
    val ret = withResource(computeBasic(base)) { basic =>
      withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negInf =>
        withResource(base.equalTo(negInf)) { eqNegInf =>
          eqNegInf.ifElse(negInf, basic)
        }
      }
    }
    GpuColumnVector.from(ret)
  }
}

case class GpuAtan(child: GpuExpression) extends CudfUnaryMathExpression("ATAN") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuAtanh(child: GpuExpression) extends CudfUnaryMathExpression("ATANH") {
  override def unaryOp: UnaryOp = UnaryOp.ARCTANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCeil(child: GpuExpression) extends CudfUnaryMathExpression("CEIL") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.CEIL
  override def outputTypeOverride: DType = DType.INT64

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    if (input.dataType() == DoubleType) {
      withResource(FloatUtils.nanToZero(input.getBase())) { inputWithNansToZero =>
        super.doColumnar(GpuColumnVector.from(inputWithNansToZero))
      }
    } else {
      // Long is a noop in spark, but for cudf it is not.
      input.incRefCount()
    }
  }
}

case class GpuCos(child: GpuExpression) extends CudfUnaryMathExpression("COS") {
  override def unaryOp: UnaryOp = UnaryOp.COS
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExp(child: GpuExpression) extends CudfUnaryMathExpression("EXP") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuExpm1(child: GpuExpression) extends CudfUnaryMathExpression("EXPM1") {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val cv = input.getBase.unaryOp(unaryOp)
    try {
      val sc = Scalar.fromInt(1)
      try {
        GpuColumnVector.from(cv.binaryOp(BinaryOp.SUB, sc, outputTypeOverride))
      } finally {
        sc.close()
      }
    } finally {
      cv.close()
    }
  }
}

case class GpuFloor(child: GpuExpression) extends CudfUnaryMathExpression("FLOOR") {
  override def dataType: DataType = child.dataType match {
    case dt @ DecimalType.Fixed(_, 0) => dt
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision - scale + 1, 0)
    case _ => LongType
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DecimalType, LongType))

  override def unaryOp: UnaryOp = UnaryOp.FLOOR

  override def outputTypeOverride: DType = DType.INT64

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    if (input.dataType() == DoubleType) {
      withResource(FloatUtils.nanToZero(input.getBase())) { inputWithNansToZero =>
        super.doColumnar(GpuColumnVector.from(inputWithNansToZero))
      }
    } else {
      // Long is a noop in spark, but for cudf it is not.
      input.incRefCount()
    }
  }
}

case class GpuLog(child: GpuExpression) extends CudfUnaryMathExpression("LOG") {
  override def unaryOp: UnaryOp = UnaryOp.LOG
  override def outputTypeOverride: DType = DType.FLOAT64
  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(input)) { normalized =>
      super.doColumnar(normalized)
    }
  }
}

object GpuLogarithm extends Arm {

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned GpuColumnVector.
   */
  def fixUpLhs(input: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromDouble(0)) { zero =>
      withResource(input.getBase.binaryOp(BinaryOp.LESS_EQUAL, zero, DType.BOOL8)) { zeroOrLess =>
        withResource(Scalar.fromNull(DType.FLOAT64)) { nullScalar =>
          GpuColumnVector.from(zeroOrLess.ifElse(nullScalar, input.getBase))
        }
      }
    }
  }

  /**
   * Replace negative values with nulls. Note that the caller is responsible for closing the
   * returned Scalar.
   */
  def fixUpLhs(input: Scalar): Scalar = {
    if (input.isValid && input.getDouble <= 0) {
      Scalar.fromNull(DType.FLOAT64)
    } else {
      input.incRefCount()
    }
  }
}

case class GpuLogarithm(left: GpuExpression, right: GpuExpression)
  extends CudfBinaryMathExpression("LOG_BASE") {

  override def binaryOp: BinaryOp = BinaryOp.LOG_BASE
  override def outputTypeOverride: DType = DType.FLOAT64

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(fixedLhs, rhs)
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(fixedLhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    withResource(GpuLogarithm.fixUpLhs(lhs)) { fixedLhs =>
      super.doColumnar(fixedLhs, rhs)
    }
  }
}

case class GpuSin(child: GpuExpression) extends CudfUnaryMathExpression("SIN") {
  override def unaryOp: UnaryOp = UnaryOp.SIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSignum(child: GpuExpression) extends GpuUnaryMathExpression("SIGNUM") {

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
      val num = Scalar.fromDouble(0)
      try {
        val hiReplace = Scalar.fromDouble(1)
        try {
          val loReplace = Scalar.fromDouble(-1)
          try {
            GpuColumnVector.from(input.getBase.clamp(num, loReplace, num, hiReplace))
          } finally {
            loReplace.close
          }
        } finally {
          hiReplace.close
        }
      } finally {
        num.close
      }
  }

  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTanh(child: GpuExpression) extends CudfUnaryMathExpression("TANH") {
  override def unaryOp: UnaryOp = UnaryOp.TANH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCosh(child: GpuExpression) extends CudfUnaryMathExpression("COSH") {
  override def unaryOp: UnaryOp = UnaryOp.COSH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSinh(child: GpuExpression) extends CudfUnaryMathExpression("SINH") {
  override def unaryOp: UnaryOp = UnaryOp.SINH
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuSqrt(child: GpuExpression) extends CudfUnaryMathExpression("SQRT") {
  override def unaryOp: UnaryOp = UnaryOp.SQRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCbrt(child: GpuExpression) extends CudfUnaryMathExpression("CBRT") {
  override def unaryOp: UnaryOp = UnaryOp.CBRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuTan(child: GpuExpression) extends CudfUnaryMathExpression("TAN") {
  override def unaryOp: UnaryOp = UnaryOp.TAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuCot(child: GpuExpression) extends GpuUnaryMathExpression("COT") {

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromInt(1)) { one =>
      withResource(input.getBase.unaryOp(UnaryOp.TAN)) { tan =>
        GpuColumnVector.from(one.div(tan))
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

case class GpuPow(left: GpuExpression, right: GpuExpression)
    extends CudfBinaryMathExpression("POWER") {
  override def binaryOp: BinaryOp = BinaryOp.POW
  override def outputTypeOverride: DType = DType.FLOAT64
}

case class GpuRint(child: GpuExpression) extends CudfUnaryMathExpression("ROUND") {
  override def unaryOp: UnaryOp = UnaryOp.RINT
  override def outputTypeOverride: DType = DType.FLOAT64
}
