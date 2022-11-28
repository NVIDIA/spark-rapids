/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.math.{max, min}

import ai.rapids.cudf._
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.GpuTypeShims

import org.apache.spark.sql.catalyst.analysis.{DecimalPrecision, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {

  protected def allowPrecisionLoss = SQLConf.get.decimalOperationsAllowPrecisionLoss

  // arithmetic operations can overflow and throw exceptions in ANSI mode
  override def hasSideEffects: Boolean = super.hasSideEffects || SQLConf.get.ansiEnabled

  override def dataType: DataType = (left.dataType, right.dataType) match {
    case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
      resultDecimalType(p1, s1, p2, s2)
    case _ => left.dataType
  }

  protected def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    throw new IllegalStateException(
      s"${getClass.getSimpleName} must override `resultDecimalType`.")
  }

  override def checkInputDataTypes(): TypeCheckResult = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) if inputType.acceptsType(l) && inputType.acceptsType(r) =>
      // We allow decimal type inputs with different precision and scale, and use special formulas
      // to calculate the result precision and scale.
      TypeCheckResult.TypeCheckSuccess
    case _ => super.checkInputDataTypes()
  }
}

case class GpuAdd(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD

  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.ADD)

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    withResource(ret) { ret =>
      // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
      if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType) ||
          GpuTypeShims.isSupportedDayTimeType(dataType) ||
          GpuTypeShims.isSupportedYearMonthType(dataType)) {
        // For day time interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        AddOverflowChecks.basicOpOverflowCheck(lhs, rhs, ret)
      }

      if (dataType.isInstanceOf[DecimalType]) {
        val sparkDecimal = dataType.asInstanceOf[DecimalType]
        val cudfDecimal = DecimalUtil.createCudfDecimal(sparkDecimal)
        withResource {
          if (!cudfDecimal.equals(ret.getType)) {
            withResource(ret.round(sparkDecimal.scale)) { rounded =>
              rounded.castTo(cudfDecimal)
            }
          } else {
            ret.incRefCount()
          }
        } { ret =>
          AddOverflowChecks.decimalOpOverflowCheck(lhs, rhs, ret, failOnError)
        }
      } else {
        ret.incRefCount()
      }
    }
  }

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

case class GpuSubtract(
    left: Expression,
    right: Expression,
    failOnError: Boolean) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB
  override def astOperator: Option[BinaryOperator] = Some(ast.BinaryOperator.SUB)

  private[this] def basicOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector): Unit = {
    // Check overflow. It is true if the arguments have different signs and
    // the sign of the result is different from the sign of x.
    // Which is equal to "((x ^ y) & (x ^ r)) < 0" in the form of arithmetic.

    val signCV = withResource(lhs.bitXor(rhs)) { xyXor =>
      withResource(lhs.bitXor(ret)) { xrXor =>
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
          throw RapidsErrorUtils.arithmeticOverflowError(
            "One or more rows overflow for Subtract operation."
          )
        }
      }
    }
  }

  private[this] def decimalOpOverflowCheck(
      lhs: BinaryOperable,
      rhs: BinaryOperable,
      ret: ColumnVector): ColumnVector = {
    // We need a special overflow check for decimal because CUDF does not support INT128 so we
    // cannot reuse the same code for the other types.
    // Overflow happens if the arguments have different signs and the sign of the result is
    // different from the sign of subtractend (RHS).
    val numRows = ret.getRowCount.toInt
    val zero = BigDecimal(0).bigDecimal
    val overflow = withResource(DecimalUtils.lessThan(rhs, zero, numRows)) { rhsLz =>
      val argsSignDifferent = withResource(DecimalUtils.lessThan(lhs, zero, numRows)) { lhsLz =>
        lhsLz.notEqualTo(rhsLz)
      }
      withResource(argsSignDifferent) { argsSignDifferent =>
        val resultAndSubtrahendSameSign =
          withResource(DecimalUtils.lessThan(ret, zero)) { resultLz =>
            rhsLz.equalTo(resultLz)
          }
        withResource(resultAndSubtrahendSameSign) { resultAndSubtrahendSameSign =>
          resultAndSubtrahendSameSign.and(argsSignDifferent)
        }
      }
    }
    withResource(overflow) { overflow =>
      if (failOnError) {
        withResource(overflow.any()) { any =>
          if (any.isValid && any.getBoolean) {
            throw new ArithmeticException("One or more rows overflow for Subtract operation.")
          }
        }
        ret.incRefCount()
      } else {
        withResource(GpuScalar.from(null, dataType)) { nullVal =>
          overflow.ifElse(nullVal, ret)
        }
      }
    }
  }

  override def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val ret = super.doColumnar(lhs, rhs)
    withResource(ret) { ret =>
      // No shims are needed, because it actually supports ANSI mode from Spark v3.0.1.
      if (failOnError && GpuAnsi.needBasicOpOverflowCheck(dataType) ||
          GpuTypeShims.isSupportedDayTimeType(dataType) ||
          GpuTypeShims.isSupportedYearMonthType(dataType)) {
        // For day time interval, Spark throws an exception when overflow,
        // regardless of whether `SQLConf.get.ansiEnabled` is true or false
        basicOpOverflowCheck(lhs, rhs, ret)
      }

      if (dataType.isInstanceOf[DecimalType]) {
        decimalOpOverflowCheck(lhs, rhs, ret)
      } else {
        ret.incRefCount()
      }
    }
  }

  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

trait GpuDivModLikeWithPromote extends GpuDivModLike with DecimalWithPromote

trait DecimalWithPromote extends CudfBinaryOperator {
  private def getCastedVectorIfNeeded(
      vector: GpuColumnVector,
      d: DecimalType, c: DType): GpuColumnVector = {
    if (!vector.dataType.sameType(d)) {
      GpuColumnVector.from(vector.getBase.castTo(c), d)
    } else {
      vector.incRefCount()
    }
  }

  private def getCastedScalarIfNeeded(
      scalar: GpuScalar,
      d: DecimalType): GpuScalar = {
    if (!scalar.dataType.sameType(d)) {
      GpuScalar(GpuScalar.from(scalar.getValue, d), d)
    } else {
      scalar.incRefCount
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (ltype: DecimalType, rtype: DecimalType) =>
        // should match precision
        val widerDecimalType = DecimalPrecision.widerDecimalType(ltype, rtype)
        val widerDType = DecimalUtil.createCudfDecimal(widerDecimalType)
        withResource(getCastedVectorIfNeeded(lhs, widerDecimalType, widerDType)) { newLeft =>
          withResource(getCastedVectorIfNeeded(rhs, widerDecimalType, widerDType)) { newRight =>
            super.doColumnar(newLeft, newRight)
          }
        }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (ltype: DecimalType, rtype: DecimalType) =>
        // should match precision
        val widerDecimalType = DecimalPrecision.widerDecimalType(ltype, rtype)
        val widerDType = DecimalUtil.createCudfDecimal(widerDecimalType)
        withResource(getCastedScalarIfNeeded(lhs, widerDecimalType)) { newLhs =>
          withResource(getCastedVectorIfNeeded(rhs, widerDecimalType, widerDType)) { newRhs =>
            super.doColumnar(newLhs, newRhs)
          }
        }
      case _ => super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    (left.dataType, right.dataType) match {
      case (ltype: DecimalType, rtype: DecimalType) =>
        // should match precision
        val widerDecimalType = DecimalPrecision.widerDecimalType(ltype, rtype)
        val widerDType = DecimalUtil.createCudfDecimal(widerDecimalType)
        withResource(getCastedVectorIfNeeded(lhs, widerDecimalType, widerDType)) { newLeft =>
          withResource(getCastedScalarIfNeeded(rhs, widerDecimalType)) { newRight =>
            super.doColumnar(newLeft, newRight)
          }
        }
      case _ => super.doColumnar(lhs, rhs)
    }
  }
}

case class GpuIntegralDivide(
    left: Expression,
    right: Expression) extends GpuIntegralDivideParent(left, right) with DecimalWithPromote {
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    // This follows division rule
    val intDig = p1 - s1 + s2
    // No precision loss can happen as the result scale is 0.
    DecimalType.bounded(intDig, 0)
  }
}

case class GpuRemainder(
    left: Expression,
    right: Expression)
    extends GpuRemainderParent(left, right) with DecimalWithPromote {

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: min(p1-s1, p2-s2) + max(s1, s2)
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

case class GpuPmod(
    left: Expression,
    right: Expression) extends GpuPmodParent(left, right) with DecimalWithPromote {
  // This follows Remainder rule
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}


