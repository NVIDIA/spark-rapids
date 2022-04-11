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
  def castLongToIntWithCheck(longCv: ColumnVector): ColumnVector = {
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

  /**
   * Check if the long result of long * long is overflow
   * See Math.multiplyExact:
   * r = x * y;
   * if (y != 0) && (r/y != x) throw exception;
   * if(x == Long.MIN_VALUE && y == -1) throw exception
   *
   * @param leftLong  long cv or scalar
   * @param rightLong long cv or scalar, will not be scalar if leftLong is scalar
   * @param result    is the result of x * y
   */
  def checkMultiplyOverflow(leftLong: BinaryOperable, rightLong: BinaryOperable,
      result: ColumnVector): Unit = {
    rightLong match {
      case rightVector: ColumnVector =>
        checkMultiplyOverflowWithRightCv(leftLong, rightVector, result)
      case _ =>
        checkMultiplyOverflowWithRightCv(rightLong, leftLong.asInstanceOf[ColumnVector], result)
    }
  }

  /**
   * Check if the long result of long * long is overflow
   * See Math.multiplyExact:
   * r = x * y;
   * if (y != 0) && (r/y != x) throw exception;
   * if(x == Long.MIN_VALUE && y == -1) throw exception
   *
   * @param x long cv or scalar
   * @param y long cv
   * @param r is the result of x * y
   */
  def checkMultiplyOverflowWithRightCv(x: BinaryOperable, y: ColumnVector,
      r: ColumnVector): Unit = {
    // check (y != 0) && (r/y != x)
    checkMultiplyOverflowSimple(x, y, r)

    // check (x == Long.MIN_VALUE && y == -1)
    x match {
      case scalar: Scalar =>
        if (getLong(scalar) == Long.MinValue) { // x == Long.MIN_VALUE
          withResource(Scalar.fromLong(-1L)) { negOne =>
            withResource(y.equalTo(negOne)) { negOneBool =>
              if (BoolUtils.isAnyValidTrue(negOneBool)) { // y = -1L
                throw new ArithmeticException("overflow occurs")
              }
            }
          }
        }
      case _ =>
        // check (x == Long.MIN_VALUE && y == -1)
        withResource(Scalar.fromLong(Long.MinValue)) { minLong =>
          withResource(Scalar.fromLong(-1L)) { negOneLong =>
            withResource(x.equalTo(minLong)) { minBool =>
              withResource(y.equalTo(negOneLong)) { oneBool =>
                withResource(minBool.and(oneBool)) { overflow =>
                  if (BoolUtils.isAnyValidTrue(overflow)) {
                    throw new ArithmeticException("overflow occurs")
                  }
                }
              }
            }
          }
        }
    }
  }

  /**
   * Check if the long result of long * long is overflow
   * See Math.multiplyExact:
   * r = x * y;
   * if (y != 0) && (r/y != x) throw exception;
   *
   * Here is a little different with `Math.multiplyExact`,
   * omits the `check x == Long.MIN_VALUE && y == -1`,
   * Because of the `toIntWithCheck` will throw exception
   * for the value of `Long.MIN_VALUE * -1` exceeds int limits
   *
   * @param leftLong  long cv or scalar
   * @param rightLong long cv or scalar, will not be scalar if leftLong is scalar
   * @param result    is the result of x * y
   */
  def checkMultiplyOverflowSimple(leftLong: BinaryOperable, rightLong: BinaryOperable,
      result: ColumnVector): Unit = {
    rightLong match {
      case rightVector: ColumnVector =>
        checkMultiplyOverflowSimpleWithRightCv(leftLong, rightVector, result)
      case _ =>
        checkMultiplyOverflowSimpleWithRightCv(rightLong, leftLong.asInstanceOf[ColumnVector],
          result)
    }
  }

  /**
   * Check if the long result of long * long is overflow
   * See Math.multiplyExact:
   * r = x * y;
   * if (y != 0) && (r/y != x) throw exception;
   * if(x == Long.MIN_VALUE && y == -1) throw exception
   *
   * Here is a little different with `Math.multiplyExact`,
   * omits the `check x == Long.MIN_VALUE && y == -1`,
   * Because of the `toIntWithCheck` will throw exception
   * for the value of `Long.MIN_VALUE * -1` exceeds int limits
   *
   * @param x long cv or scalar
   * @param y long cv or scalar, will not be scalar if x is scalar
   * @param r is the result of x * y
   */
  def checkMultiplyOverflowSimpleWithRightCv(x: BinaryOperable, y: ColumnVector,
      r: ColumnVector): Unit = {
    // throws exception if multipy(long, long) is overflow
    // See Math.multiplyExact:
    //   r = x * y; check (y != 0) && (r/y != x); check x == Long.MIN_VALUE && y == -1
    // Here is a little different with `Math.multiplyExact`,
    // omits the `check x == Long.MIN_VALUE && y == -1`,
    // because of the `toIntWithCheck` will throw exception
    // for the value of `Long.MIN_VALUE * -1` that is not in the int range
    withResource(Scalar.fromLong(0L)) { zeroScalar =>
      withResource(y.notEqualTo(zeroScalar)) { numIsNotZero => // y != 0
        withResource(r.div(y)) { expected => // (r/y)
          withResource(expected.notEqualTo(x)) { notEquals => // x != (r/y)
            withResource(numIsNotZero.and(notEquals)) { wrong => // x != (r/y) and y != 0
              if (BoolUtils.isAnyValidTrue(wrong)) {
                throw new ArithmeticException("overflow occurs")
              }
            }
          }
        }
      }
    }
  }

  private def getLong(s: Scalar): Long = {
    s.getType match {
      case DType.INT8 => s.getByte.toLong
      case DType.INT16 => s.getShort.toLong
      case DType.INT32 => s.getInt.toLong
      case DType.INT64 => s.getLong
      case _ => throw new IllegalArgumentException()
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
   * round double cv to int with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundToIntWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    withResource(doubleCv.round(RoundMode.HALF_UP)) { roundedDouble =>
      // throws exception if the result exceeds int limits
      withResource(roundedDouble.castTo(DType.INT64)) { long =>
        castLongToIntWithCheck(long)
      }
    }
  }

  /**
   * round double cv to long with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToInt(double value, RoundingMode.HALF_UP)
   */
  def roundToLongWithCheckForDouble(doubleCv: ColumnVector): ColumnVector = {
    // check Inf, -Inf, NaN
    checkDoubleInfNan(doubleCv)

    roundToLongWithCheck(doubleCv)
  }

  /**
   * check if double Cv exceeds long limits
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
        // compute interval.asLong * num
        // interval and num are both in the range: [Int.MinValue, Int.MaxValue],
        // so long fits the result and no need to check the overflow
        val longResultCv: ColumnVector = interval.mul(numOperable, DType.INT64)

        withResource(longResultCv) { longResult =>
          // throws exception if exceeds int limits
          IntervalUtils.castLongToIntWithCheck(longResult)
        }

      case LongType => // num is long
        // The following is equivalent to Math.toIntExact(Math.multiplyExact(months, long num))

        // compute interval.asLong * num
        // should check the overflow
        val longResultCv: ColumnVector = numOperable.mul(interval)

        withResource(longResultCv) { longResult =>
          // check overflow, skipped a check: `check x == Long.MIN_VALUE && y == -1`
          // for r = x * y;
          // if (y != 0) && (r/y != x) throw exception;
          // if(x == Long.MIN_VALUE && y == -1) throw exception
          // here skipped the check: `x == Long.MIN_VALUE && y == -1`,
          // because of `castLongToIntWithCheck` will cover it
          IntervalUtils.checkMultiplyOverflowSimple(interval, numOperable, longResult)

          // throws exception if exceeds int limits
          // Note: here covers: `check x == Long.MIN_VALUE && y == -1`, it exceeds int limits
          IntervalUtils.castLongToIntWithCheck(longResult)
        }

      case FloatType | DoubleType => // num is float or double
        // compute interval.toDouble * num.toDouble
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)

        withResource(doubleResultCv) { doubleResult =>
          // check overflow for (interval.toDouble * num.toDouble), then round to int
          IntervalUtils.roundToIntWithOverflowCheck(doubleResult)
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
      case ByteType | ShortType | IntegerType | LongType => // num is byte, short, int or long
        // compute interval.asLong * num
        // interval and num are both in the range: [Int.MinValue, Int.MaxValue],
        // so long fits the result and no need to check the overflow
        val longResultCv: ColumnVector = interval.mul(numOperable, DType.INT64)

        withResource(longResultCv) { longResult =>
          // check overflow
          IntervalUtils.checkMultiplyOverflow(interval, numOperable, longResult)

          longResult.incRefCount()
        }
      case _: FloatType | DoubleType => // num is float
        // compute interval * num
        val doubleResultCv = interval.mul(numOperable, DType.FLOAT64)
        withResource(doubleResultCv) { doubleResult =>
          // check overflow for (interval * num.toDouble), then round to long
          IntervalUtils.roundToLongWithCheckForDouble(doubleResult)
        }

      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in MultiplyDTInterval")
    }
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)

  override def dataType: DataType = DayTimeIntervalType()

}
