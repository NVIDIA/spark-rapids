/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import ai.rapids.cudf
import ai.rapids.cudf.DType

import org.apache.spark.sql.types.{ByteType, DataType, Decimal, DecimalType, IntegerType, LongType, ShortType}

object DecimalUtil extends Arm {

  def createCudfDecimal(dt: DecimalType): DType = {
    createCudfDecimal(dt.precision, dt.scale)
  }

  def createCudfDecimal(precision: Int, scale: Int): DType = {
    if (precision <= DType.DECIMAL32_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL32, -scale)
    } else if (precision <= DType.DECIMAL64_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL64, -scale)
    } else if (precision <= DType.DECIMAL128_MAX_PRECISION) {
      DType.create(DType.DTypeEnum.DECIMAL128, -scale)
    } else {
      throw new IllegalArgumentException(s"precision overflow: $precision")
    }
  }

  def getMaxPrecision(dt: DType): Int = dt.getTypeId match {
    case DType.DTypeEnum.DECIMAL32 => DType.DECIMAL32_MAX_PRECISION
    case DType.DTypeEnum.DECIMAL64 => DType.DECIMAL64_MAX_PRECISION
    case _ if dt.isDecimalType => DType.DECIMAL128_MAX_PRECISION
    case _ => throw new IllegalArgumentException(s"not a decimal type: $dt")
  }

  /**
   * Returns two BigDecimals that are exactly the
   * (smallest value `toType` can hold, largest value `toType` can hold).
   *
   * Be very careful when comparing these CUDF decimal comparisons really only work
   * when both types are already the same precision and scale, and when you change the scale
   * you end up losing information.
   */
  def bounds(toType: DecimalType): (BigDecimal, BigDecimal) = {
    val boundStr = ("9" * toType.precision) + "e" + (-toType.scale)
    val toUpperBound = BigDecimal(boundStr)
    val toLowerBound = BigDecimal("-" + boundStr)
    (toLowerBound, toUpperBound)
  }

  /**
   * CUDF can have overflow issues when rounding values. This works around those issues for you.
   * @param input the input data to round.
   * @param decimalPlaces the decimal places to round to
   * @param mode the rounding mode
   * @return the rounded data.
   */
  def round(input: cudf.ColumnView,
      decimalPlaces: Int,
      mode: cudf.RoundMode): cudf.ColumnVector = {
    assert(input.getType.isDecimalType)
    val cudfInputScale = input.getType.getScale
    if (cudfInputScale >= -decimalPlaces) {
      // No issues with overflow for these cases, so just do it.
      input.round(decimalPlaces, mode)
    } else {
      // We actually will need to round because we will be losing some information during the round
      // The DECIMAL type we use needs to be able to hold
      // `std::pow(10, std::abs(decimal_places + input.type().scale()));`
      // in it without overflowing.
      val scaleMovement = Math.abs(decimalPlaces + cudfInputScale)
      val maxInputPrecision = getMaxPrecision(input.getType)
      if (scaleMovement > maxInputPrecision) {
        // This is going to overflow unless we do something else first. But for round to work all
        // we actually need is 1 decimal place more than the target decimalPlaces, so we can cast
        // to this first (which will truncate the extra information), and then round to the desired
        // result
        val intermediateDType = DType.create(input.getType.getTypeId, (-decimalPlaces) + 1)
        withResource(input.castTo(intermediateDType)) { truncated =>
          truncated.round(decimalPlaces, mode)
        }
      } else {
        input.round(decimalPlaces, mode)
      }
    }
  }

  /**
   * Because CUDF can have issues with comparing decimal values that have different precision
   * and scale accurately it takes some special steps to do this. This handles the corner cases
   * for you.
   */
  def lessThan(lhs: cudf.ColumnView, rhs: BigDecimal): cudf.ColumnVector = {
    assert(lhs.getType.isDecimalType)
    val cudfScale = lhs.getType.getScale
    val cudfPrecision = getMaxPrecision(lhs.getType)

    // First we have to round the scalar (rhs) to the same scale as lhs.  Because this is a
    // less than and it is rhs that we are rounding, we will round away from 0 (UP)
    // to make sure we always return the correct value.
    // For example:
    //      100.1 < 100.19
    // If we rounded down the rhs 100.19 would become 100.1, and now 100.1 is not < 100.1

    val roundedRhs = rhs.setScale(-cudfScale, BigDecimal.RoundingMode.UP)

    if (roundedRhs.precision > cudfPrecision) {
      // converting rhs to the same precision as lhs would result in an overflow/error, but
      // the scale is the same so we can still figure this out. For example if LHS precision is
      // 4 and RHS precision is 5 we get the following...
      //  9999 <  99999 => true
      // -9999 <  99999 => true
      //  9999 < -99999 => false
      // -9999 < -99999 => false
      // so the result should be the same as RHS > 0
      withResource(cudf.Scalar.fromBool(roundedRhs > 0)) { rhsGtZero =>
        cudf.ColumnVector.fromScalar(rhsGtZero, lhs.getRowCount.toInt)
      }
    } else {
      val sparkType = DecimalType(cudfPrecision, -cudfScale)
      withResource(GpuScalar.from(roundedRhs, sparkType)) { scalarRhs =>
        lhs.lessThan(scalarRhs)
      }
    }
  }

  def lessThan(lhs: cudf.BinaryOperable, rhs: BigDecimal, numRows: Int): cudf.ColumnVector =
    lhs match {
      case cv: cudf.ColumnVector =>
        lessThan(cv, rhs)
      case s: cudf.Scalar =>
        if (s.isValid) {
          val isLess = (s.getBigDecimal.compareTo(rhs) < 0)
          withResource(cudf.Scalar.fromBool(isLess)) { n =>
            cudf.ColumnVector.fromScalar(n, numRows)
          }
        } else {
          withResource(cudf.Scalar.fromNull(DType.BOOL8)) { n =>
            cudf.ColumnVector.fromScalar(n, numRows)
          }
        }
    }

  /**
   * Because CUDF can have issues with comparing decimal values that have different precision
   * and scale accurately it takes some special steps to do this. This handles the corner cases
   * for you.
   */
  def greaterThan(lhs: cudf.ColumnView, rhs: BigDecimal): cudf.ColumnVector = {
    assert(lhs.getType.isDecimalType)
    val cudfScale = lhs.getType.getScale
    val cudfPrecision = getMaxPrecision(lhs.getType)

    // First we have to round the scalar (rhs) to the same scale as lhs.  Because this is a
    // greater than and it is rhs that we are rounding, we will round towards 0 (DOWN)
    // to make sure we always return the correct value.
    // For example:
    //      100.2 > 100.19
    // If we rounded up the rhs 100.19 would become 100.2, and now 100.2 is not > 100.2

    val roundedRhs = rhs.setScale(-cudfScale, BigDecimal.RoundingMode.DOWN)

    if (roundedRhs.precision > cudfPrecision) {
      // converting rhs to the same precision as lhs would result in an overflow/error, but
      // the scale is the same so we can still figure this out. For example if LHS precision is
      // 4 and RHS precision is 5 we get the following...
      //  9999 >  99999 => false
      // -9999 >  99999 => false
      //  9999 > -99999 => true
      // -9999 > -99999 => true
      // so the result should be the same as RHS < 0
      withResource(cudf.Scalar.fromBool(roundedRhs < 0)) { rhsLtZero =>
        cudf.ColumnVector.fromScalar(rhsLtZero, lhs.getRowCount.toInt)
      }
    } else {
      val sparkType = DecimalType(cudfPrecision, -cudfScale)
      withResource(GpuScalar.from(roundedRhs, sparkType)) { scalarRhs =>
        lhs.greaterThan(scalarRhs)
      }
    }
  }

  def outOfBounds(input: cudf.ColumnView, to: DecimalType): cudf.ColumnVector = {
    val (lowerBound, upperBound) = bounds(to)

    withResource(greaterThan(input, upperBound)) { over =>
      withResource(lessThan(input, lowerBound)) { under =>
        over.or(under)
      }
    }
  }

  /**
   * Return the size in bytes of the Fixed-width data types.
   * WARNING: Do not use this method for variable-width data types
   */
  private[rapids] def getDataTypeSize(dt: DataType): Int = {
    dt match {
      case d: DecimalType if d.precision <= Decimal.MAX_INT_DIGITS => 4
      case t => t.defaultSize
    }
  }

  /**
   * Get the number of decimal places needed to hold the integral type held by this column
   */
  def getPrecisionForIntegralType(input: DType): Int = input match {
    case DType.INT8 =>  3 // -128 to 127
    case DType.INT16 => 5 // -32768 to 32767
    case DType.INT32 => 10 // -2147483648 to 2147483647
    case DType.INT64 => 19 // -9223372036854775808 to 9223372036854775807
    case t => throw new IllegalArgumentException(s"Unsupported type $t")
  }

  def asDecimalType(t: DataType): DecimalType = t match {
    case dt: DecimalType => dt
    case ByteType | ShortType | IntegerType | LongType =>
      val prec = DecimalUtil.getPrecisionForIntegralType(GpuColumnVector.getNonNestedRapidsType(t))
      DecimalType(prec, 0)
    case _ =>
      throw new IllegalArgumentException(
        s"Internal Error: type $t cannot automatically be cast to a supported DecimalType")
  }
}
