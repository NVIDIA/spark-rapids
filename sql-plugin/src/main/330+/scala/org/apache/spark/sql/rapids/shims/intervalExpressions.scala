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

package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{BinaryOperable, ColumnVector, DType, RoundMode, Scalar}
import com.nvidia.spark.rapids.{Arm, BoolUtils, GpuBinaryExpression, GpuColumnVector, GpuScalar}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types._

object IntervalUtils extends Arm {

  /**
   * Convert long cv to int cv, throws exception if any value in `longCv` exceeds the int limits.
   * Check (int)(long_value) == long_value
   */
  def castLongToIntWithOverflowCheck(longCv: ColumnVector): ColumnVector = {
    withResource(longCv.castTo(DType.INT32)) { intResult =>
      withResource(longCv.notEqualTo(intResult)) { notEquals =>
        if (BoolUtils.isAnyValidTrue(notEquals)) {
          throw new ArithmeticException("overflow occurs")
        } else {
          intResult.incRefCount()
        }
      }
    }
  }

  def checkDecimal128CvInRange(decimal128Cv: ColumnVector, minValue: Long, maxValue: Long): Unit = {
    // check min
    withResource(Scalar.fromLong(minValue)) { minScalar =>
      withResource(decimal128Cv.lessThan(minScalar)) { lessThanMin =>
        if (BoolUtils.isAnyValidTrue(lessThanMin)) {
          throw new ArithmeticException("overflow occurs")
        }
      }
    }

    // check max
    withResource(Scalar.fromLong(maxValue)) { maxScalar =>
      withResource(decimal128Cv.greaterThan(maxScalar)) { greaterThanMax =>
        if (BoolUtils.isAnyValidTrue(greaterThanMax)) {
          throw new ArithmeticException("overflow occurs")
        }
      }
    }
  }

  /**
   * Multiple with overflow check, then cast to long
   * Equivalent to Math.multiplyExact
   *
   * @param left   cv or scalar
   * @param right  cv or scalar, will not be scalar if left is scalar
   * @return the long result of left * right
   */
  def multipleToLongWithOverflowCheck(left: BinaryOperable, right: BinaryOperable): ColumnVector = {
    val decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, 0)
    withResource(left.mul(right, decimal128Type)) { ret =>
      checkDecimal128CvInRange(ret, Long.MinValue, Long.MaxValue)
      ret.castTo(DType.INT64)
    }
  }

  /**
   * Multiple with overflow check, then cast to int
   * Equivalent to Math.multiplyExact
   *
   * @param left   cv or scalar
   * @param right  cv or scalar, will not be scalar if left is scalar
   * @return the int result of left * right
   */
  def multipleToIntWithOverflowCheck(left: BinaryOperable, right: BinaryOperable): ColumnVector = {
    val decimal128Type = DType.create(DType.DTypeEnum.DECIMAL128, 0)
    withResource(left.mul(right, decimal128Type)) { ret =>
      checkDecimal128CvInRange(ret, Int.MinValue, Int.MaxValue)
      ret.castTo(DType.INT32)
    }
  }

  def checkDoubleInfNan(doubleCv: ColumnVector): Unit = {
    // check infinity
    withResource(Scalar.fromDouble(Double.PositiveInfinity)) { positiveInfScalar =>
      withResource(doubleCv.equalTo(positiveInfScalar)) { equalsInfinity =>
        if (BoolUtils.isAnyValidTrue(equalsInfinity)) {
          throw new ArithmeticException("Has infinity")
        }
      }
    }

    // check -infinity
    withResource(Scalar.fromDouble(Double.NegativeInfinity)) { negativeInfScalar =>
      withResource(doubleCv.equalTo(negativeInfScalar)) { equalsInfinity =>
        if (BoolUtils.isAnyValidTrue(equalsInfinity)) {
          throw new ArithmeticException("Has -infinity")
        }
      }
    }

    // check NaN
    withResource(doubleCv.isNan) { isNan =>
      if (BoolUtils.isAnyValidTrue(isNan)) {
        throw new ArithmeticException("Has NaN")
      }
    }
  }

  /**
   * Round double cv to int with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundDoubleToIntWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    withResource(doubleCv.round(RoundMode.HALF_UP)) { roundedDouble =>
      // throws exception if the result exceeds int limits
      withResource(roundedDouble.castTo(DType.INT64)) { long =>
        castLongToIntWithOverflowCheck(long)
      }
    }
  }

  /**
   * round double cv to long with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundDoubleToLongWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    roundToLongWithCheck(doubleCv)
  }

  /**
   * Check if double cv exceeds long limits
   * Rewrite from
   * com.google.common.math.DoubleMath.roundToLong:
   * z = roundIntermediate(x, mode)
   * checkInRange(MIN_LONG_AS_DOUBLE - z < 1.0 & z < MAX_LONG_AS_DOUBLE_PLUS_ONE)
   * return z.toLong
   */
  def roundToLongWithCheck(doubleCv: ColumnVector): ColumnVector = {
    val MIN_LONG_AS_DOUBLE: Double = -9.223372036854776E18
    val MAX_LONG_AS_DOUBLE_PLUS_ONE: Double = 9.223372036854776E18

    withResource(doubleCv.round(RoundMode.HALF_UP)) { z =>
      withResource(Scalar.fromDouble(MAX_LONG_AS_DOUBLE_PLUS_ONE)) { max =>
        withResource(z.greaterOrEqualTo(max)) { invalid =>
          if (BoolUtils.isAnyValidTrue(invalid)) {
            throw new ArithmeticException("Round double to long overflow")
          }
        }
      }

      withResource(Scalar.fromDouble(MIN_LONG_AS_DOUBLE)) { min =>
        withResource(min.sub(z)) { diff =>
          withResource(Scalar.fromDouble(1.0d)) { one =>
            withResource(diff.greaterOrEqualTo(one)) { invalid =>
              if (BoolUtils.isAnyValidTrue(invalid)) {
                throw new ArithmeticException("Round double to long overflow")
              }
            }
          }
        }
      }

      z.castTo(DType.INT64)
    }
  }
}

/**
 * Multiply a year-month interval by a numeric:
 * year-month interval * number(byte, short, int, long, float, double)
 * Note not support year-month interval * decimal
 * Year-month interval's internal type is int, the value of int is 12 * year + month
 * left expression is interval, right expression is number
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L506
 *
 */
case class GpuMultiplyYMInterval(
    interval: Expression,
    num: Expression) extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnarImp(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows, interval.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnarImp(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numType match {
      case ByteType | ShortType | IntegerType => // num is byte, short or int
        // compute interval * num
        // interval and num are both in the range: [Int.MinValue, Int.MaxValue],
        // so long fits the result and no need to check overflow
        val longResultCv: ColumnVector = interval.mul(numOperable, DType.INT64)

        withResource(longResultCv) { longResult =>
          // throws exception if exceeds int limits
          IntervalUtils.castLongToIntWithOverflowCheck(longResult)
        }

      case LongType => // num is long
        // The following is equivalent to Math.toIntExact(Math.multiplyExact(months, num))
        IntervalUtils.multipleToIntWithOverflowCheck(interval, numOperable)

      case FloatType | DoubleType => // num is float or double
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)

        withResource(doubleResultCv) { doubleResult =>
          // round to long with overflow check
          IntervalUtils.roundDoubleToIntWithOverflowCheck(doubleResult)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuMultiplyYMInterval")
    }
  }

  override def toString: String = s"$interval * $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)

  override def dataType: DataType = YearMonthIntervalType()
}

/**
 * Multiply a day-time interval by a numeric
 * day-time interval * number(byte, short, int, long, float, double)
 * Note not support day-time interval * decimal
 * Day-time interval's interval type is long, the value of long is the total microseconds
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L558
 */
case class GpuMultiplyDTInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnarImp(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnarImp(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows, interval.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnarImp(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    numType match {
      case ByteType | ShortType | IntegerType | LongType =>
        // interval is long type; num is byte, short, int or long
        IntervalUtils.multipleToLongWithOverflowCheck(interval, numOperable)
      case _: FloatType | DoubleType =>
        // interval is long type; num is float or double
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)
        withResource(doubleResultCv) { doubleResult =>
          // round to long with overflow check
          IntervalUtils.roundDoubleToLongWithOverflowCheck(doubleResult)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in MultiplyDTInterval")
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)

  override def dataType: DataType = DayTimeIntervalType()

}
