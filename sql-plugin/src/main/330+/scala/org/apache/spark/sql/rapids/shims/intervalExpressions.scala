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
   * throws exception if any value in `longCv` exceeds the int limits
   */
  def toIntWithCheck(longCv: ColumnVector): ColumnVector = {
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

  def getLong(s: Scalar): Long = {
    s.getType match {
      case DType.INT8 => s.getByte.toLong
      case DType.INT16 => s.getShort.toLong
      case DType.INT32 => s.getInt.toLong
      case DType.INT64 => s.getLong
      case _ => throw new IllegalArgumentException()
    }
  }

  /**
   * check infinity, -infinity and NaN for double cv
   */
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
    // check Inf -Inf NaN
    checkDoubleInfNan(doubleCv)

    withResource(doubleCv.round(RoundMode.HALF_UP)) { roundedDouble =>
      // throws exception if the result exceeds int limits
      withResource(roundedDouble.castTo(DType.INT64)) { long =>
        toIntWithCheck(long)
      }
    }
  }

  /**
   * round double cv to long with overflow check
   * equivalent to
   * com.google.common.math.DoubleMath.roundToLong(double value, RoundingMode.HALF_UP)
   */
  def roundToLongWithOverflowCheck(doubleCv: ColumnVector): ColumnVector = {

    // check Inf -Inf NaN
    checkDoubleInfNan(doubleCv)

    roundToLongWithCheck(doubleCv)
  }

  /**
   * check if double Cv exceeds long limits
   * Rewrite from
   * com.google.common.math.DoubleMath.roundToLong:
   * z = roundIntermediate(double value, mode)
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

  /**
   * Returns the result of dividing p by q, rounding using the HALF_UP RoundingMode.
   *
   * It's equivalent to
   * com.google.common.math.LongMath.divide(p, q, RoundingMode.HALF_UP):
   * long div = p / q; // throws if q == 0
   * long rem = p - q * div; // equals p % q
   * // signum is 1 if p and q are both nonnegative or both negative, and -1 otherwise.
   * int signum = 1 | (int) ((p ^ q) >> (Long.SIZE - 1));
   * long absRem = abs(rem);
   * long cmpRemToHalfDivisor = absRem - (abs(q) - absRem);
   * increment = cmpRemToHalfDivisor >= 0
   * return increment ? div + signum : div;
   *
   * Note a different from LongMath.divide:
   * LongMath.divide(Long.MIN_VALUE, -1L) = Long.MIN_VALUE, it's incorrect,
   * Negative / Negative should be positive.
   * Should throw when Long.MIN_VALUE / -1L
   *
   * @param p long cv or scalar
   * @param q long cv or Scalar
   * @return q / q with HALF_UP rounding mode
   */
  def div(p: BinaryOperable, q: BinaryOperable): ColumnVector = {
    // check (p == Long.MIN_VALUE && q == -1)
    withResource(Scalar.fromLong(Long.MinValue)) { minScalar =>
      withResource(Scalar.fromLong(-1L)) { negOneScalar =>
        (p, q) match {
          case (lCv: ColumnVector, rCv: ColumnVector) =>
            withResource(lCv.equalTo(minScalar)) { isMin =>
              withResource(rCv.equalTo(negOneScalar)) { isOne =>
                if (BoolUtils.isAnyValidTrue(isMin) && BoolUtils.isAnyValidTrue(isOne)) {
                  throw new ArithmeticException("overflow occurs")
                }
              }
            }
          case (lCv: ColumnVector, rS: Scalar) =>
            withResource(lCv.equalTo(minScalar)) { isMin =>
              if (rS.getLong == -1L && BoolUtils.isAnyValidTrue(isMin)) {
                throw new ArithmeticException("overflow occurs")
              }
            }
          case (lS: Scalar, rCv: ColumnVector) =>
            withResource(rCv.equalTo(negOneScalar)) { isOne =>
              if (lS.getLong == Long.MinValue && BoolUtils.isAnyValidTrue(isOne)) {
                throw new ArithmeticException("overflow occurs")
              }
            }
          case (lS: Scalar, rS: Scalar) =>
            lS.getLong == Long.MinValue && rS.getLong == -1L
        }
      }
    }

    val absRemCv = withResource(p.mod(q)) { rem =>
      rem.abs()
    }

    // cmpRemToHalfDivisor = absRem - (abs(q) - absRem);
    // increment = (2rem >= q), 2rem == q means exactly on the half mark
    // subtracting two nonnegative longs can't overflow,
    // cmpRemToHalfDivisor has the same sign as compare(abs(rem), abs(q) / 2).
    // Note: can handle special case is: 1L / Long.MinValue
    val cmpRemToHalfDivisorCv = withResource(absRemCv) { absRem =>
      val absQCv = q match {
        case qCv: ColumnVector => qCv.abs
        case qS: Scalar => Scalar.fromLong(math.abs(IntervalUtils.getLong(qS)))
      }
      withResource(absQCv) { absQ =>
        withResource(absQ.sub(absRem)) { qSubRem =>
          absRem.sub(qSubRem)
        }
      }
    }

    val incrementCv = withResource(cmpRemToHalfDivisorCv) { cmpRemToHalfDivisor =>
      withResource(Scalar.fromLong(0L)) { zero =>
        cmpRemToHalfDivisor.greaterOrEqualTo(zero)
      }
    }

    withResource(incrementCv) { increment =>
      // signum is 1 if p and q are both nonnegative or both negative, and -1 otherwise.
      // val signum = 1 | ((p ^ q) >> (Long.SIZE - 1)).toInt
      val signNumCv = withResource(p.bitXor(q)) { pXorQ =>
        withResource(Scalar.fromInt(63)) { shift =>
          withResource(pXorQ.shiftRight(shift)) { signNum =>
            withResource(Scalar.fromLong(1L)) { one =>
              one.bitOr(signNum)
            }
          }
        }
      }

      // round the result
      withResource(signNumCv) { signNum =>
        withResource(p.div(q)) { div =>
          withResource(div.add(signNum)) { incremented =>
            increment.ifElse(incremented, div)
          }
        }
      }
    }
  }

  def hasZero(num: BinaryOperable, dataType: DataType): Boolean = {
    dataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        num match {
          case s: Scalar => IntervalUtils.getLong(s) == 0L
          case cv: ColumnVector =>
            withResource(Scalar.fromInt(0)) { zero =>
              withResource(cv.equalTo(zero)) { equals =>
                BoolUtils.isAnyValidTrue(equals)
              }
            }
        }
      case FloatType =>
        num match {
          case s: Scalar => s.getFloat == 0.0f
          case cv: ColumnVector =>
            withResource(Scalar.fromFloat(0.0f)) { zero =>
              withResource(cv.equalTo(zero)) { equals =>
                BoolUtils.isAnyValidTrue(equals)
              }
            }
        }
      case DoubleType =>
        num match {
          case s: Scalar => s.getFloat == 0.0d
          case cv: ColumnVector =>
            withResource(Scalar.fromDouble(0.0d)) { zero =>
              withResource(cv.equalTo(zero)) { equals =>
                BoolUtils.isAnyValidTrue(equals)
              }
            }
        }
      case other => throw new IllegalStateException(s"unexpected type $other")
    }
  }
}

/**
 * Divide a day-time interval by a numeric:
 * year-month interval / number(byte, short, int, long, float, double)
 * Note not support year-month interval / decimal
 * Year-month interval's internal type is int, the value of int is 12 * year + month
 * left expression is interval, right expression is number
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L615
 *
 */
case class GpuDivideYMInterval(
    interval: Expression,
    num: Expression) extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnar(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnar(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnar(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows, interval.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnar(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    if (IntervalUtils.hasZero(numOperable, numType)) {
      throw new ArithmeticException("overflow: interval / 0")
    }

    numType match {
      case ByteType | ShortType | IntegerType | LongType => // num is byte, short, int or long
        // interval.toLong / num.toLong
        val longRetCv = (interval, numOperable) match {
          case (pCv: ColumnVector, qCv: ColumnVector) =>
            withResource(pCv.castTo(DType.INT64)) { p =>
              withResource(qCv.castTo(DType.INT64)) { q =>
                IntervalUtils.div(p, q)
              }
            }
          case (pCv: ColumnVector, s: Scalar) =>
            withResource(pCv.castTo(DType.INT64)) { p =>
              withResource(Scalar.fromLong(IntervalUtils.getLong(s))) { q =>
                IntervalUtils.div(p, q)
              }
            }
          case (s: Scalar, qCv: ColumnVector) =>
            withResource(Scalar.fromLong(IntervalUtils.getLong(s))) { p =>
              withResource(qCv.castTo(DType.INT64)) { q =>
                IntervalUtils.div(p, q)
              }
            }
        }

        withResource(longRetCv) { longRet =>
          // check overflow
          IntervalUtils.toIntWithCheck(longRet)
        }

      case FloatType | DoubleType => // num is float or double
        // compute interval.asDouble / num
        val doubleCv = interval match {
          case intervalVector: ColumnVector =>
            withResource(intervalVector.castTo(DType.FLOAT64)) { intervalD =>
              intervalD.div(numOperable)
            }
          case intervalScalar: Scalar =>
            withResource(Scalar.fromDouble(intervalScalar.getInt.toDouble)) { intervalD =>
              intervalD.div(numOperable)
            }
        }

        withResource(doubleCv) { double =>
          // check overflow, then round to int
          IntervalUtils.roundToIntWithOverflowCheck(double)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuDivideYMInterval")
    }
  }

  override def toString: String = s"$interval / $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(YearMonthIntervalType, NumericType)

  override def dataType: DataType = YearMonthIntervalType()
}

/**
 * Divide a day-time interval by a numeric
 * day-time interval / number(byte, short, int, long, float, double)
 * Note not support day-time interval / decimal
 * Day-time interval's interval type is long, the value of long is the total microseconds
 * Rewrite from Spark code:
 * https://github.com/apache/spark/blob/v3.2.1/sql/catalyst/src/main/scala/
 * org/apache/spark/sql/catalyst/expressions/intervalExpressions.scala#L693
 */
case class GpuDivideDTInterval(
    interval: Expression,
    num: Expression)
    extends GpuBinaryExpression with ImplicitCastInputTypes with NullIntolerant {

  override def left: Expression = interval

  override def right: Expression = num

  override def doColumnar(interval: GpuColumnVector, numScalar: GpuScalar): ColumnVector = {
    doColumnar(interval.getBase, numScalar.getBase, num.dataType)
  }

  override def doColumnar(interval: GpuColumnVector, num: GpuColumnVector): ColumnVector = {
    doColumnar(interval.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(intervalScalar: GpuScalar, num: GpuColumnVector): ColumnVector = {
    doColumnar(intervalScalar.getBase, num.getBase, num.dataType)
  }

  override def doColumnar(numRows: Int, intervalScalar: GpuScalar,
      numScalar: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(intervalScalar, numRows, interval.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, numScalar)
    }
  }

  private def doColumnar(interval: BinaryOperable, numOperable: BinaryOperable,
      numType: DataType): ColumnVector = {
    if (IntervalUtils.hasZero(numOperable, numType)) {
      throw new ArithmeticException("overflow: interval / 0")
    }

    numType match {
      case ByteType | ShortType | IntegerType | LongType => // num is byte, short, int or long
        (interval, numOperable) match {
          case (pCv: ColumnVector, qCv: ColumnVector) =>
            withResource(pCv.castTo(DType.INT64)) { p =>
              withResource(qCv.castTo(DType.INT64)) { q =>
                IntervalUtils.div(p, q)
              }
            }
          case (pCv: ColumnVector, s: Scalar) =>
            withResource(pCv.castTo(DType.INT64)) { p =>
              withResource(Scalar.fromLong(IntervalUtils.getLong(s))) { q =>
                IntervalUtils.div(p, q)
              }
            }
          case (s: Scalar, qCv: ColumnVector) =>
            withResource(Scalar.fromLong(IntervalUtils.getLong(s))) { p =>
              withResource(qCv.castTo(DType.INT64)) { q =>
                IntervalUtils.div(p, q)
              }
            }
        }
      case FloatType | DoubleType => // num is float or double
        // compute interval.asDouble / num
        val doubleCv = interval match {
          case intervalVector: ColumnVector =>
            withResource(intervalVector.castTo(DType.FLOAT64)) { intervalD =>
              intervalD.div(numOperable)
            }
          case intervalScalar: Scalar =>
            withResource(Scalar.fromDouble(intervalScalar.getLong.toDouble)) { intervalD =>
              intervalD.div(numOperable)
            }
        }

        withResource(doubleCv) { double =>
          // check overflow, then round to long
          IntervalUtils.roundToLongWithOverflowCheck(double)
        }
      case _ => throw new IllegalArgumentException(
        s"Not support num type $numType in GpuDivideDTInterval")
    }
  }

  override def toString: String = s"$interval / $num"

  override def inputTypes: Seq[AbstractDataType] = Seq(DayTimeIntervalType, NumericType)

  override def dataType: DataType = DayTimeIntervalType()

}
