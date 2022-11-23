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

import ai.rapids.cudf._
import ai.rapids.cudf.ast.BinaryOperator
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{GpuTypeShims, SparkShimImpl}

import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType
  // arithmetic operations can overflow and throw exceptions in ANSI mode
  override def hasSideEffects: Boolean = super.hasSideEffects || SQLConf.get.ansiEnabled
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
        AddOverflowChecks.decimalOpOverflowCheck(lhs, rhs, ret, failOnError)
      } else {
        ret.incRefCount()
      }
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
}

case class GpuIntegralDivide(
    left: Expression,
    right: Expression) extends GpuIntegralDivideParent(left, right)

case class GpuRemainder(left: Expression, right: Expression) extends GpuRemainderParent(left, right)

case class GpuPmod(left: Expression, right: Expression) extends GpuPmodParent(left, right)