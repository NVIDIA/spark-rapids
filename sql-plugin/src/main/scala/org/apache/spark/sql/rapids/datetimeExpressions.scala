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

import java.util.concurrent.TimeUnit

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{Arm, BinaryExprMeta, DataFromReplacementRule, DateUtils, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.DateUtils.TimestampFormatConversionException
import com.nvidia.spark.rapids.GpuOverrides.{extractStringLit, getTimeParserPolicy}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

trait GpuDateUnaryExpression extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride = DType.INT32
}

trait GpuTimeUnaryExpression extends GpuUnaryExpression with TimeZoneAwareExpression
   with ImplicitCastInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride = DType.INT32

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

case class GpuWeekDay(child: Expression)
    extends GpuDateUnaryExpression {

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(Scalar.fromShort(1.toShort)) { one =>
      withResource(input.getBase.weekDay()) { weekday => // We want Monday = 0, CUDF Monday = 1
        weekday.sub(one)
      }
    }
  }
}

case class GpuDayOfWeek(child: Expression)
    extends GpuDateUnaryExpression {

  override protected def doColumnar(input: GpuColumnVector): ColumnVector = {
    // Cudf returns Monday = 1, ...
    // We want Sunday = 1, ..., so add a day before we extract the day of the week
    val nextInts = withResource(Scalar.fromInt(1)) { one =>
      withResource(input.getBase.asInts()) { ints =>
        ints.add(one)
      }
    }
    withResource(nextInts) { nextInts =>
      withResource(nextInts.asTimestampDays()) { daysAgain =>
        daysAgain.weekDay()
      }
    }
  }
}

case class GpuMinute(child: Expression, timeZoneId: Option[String] = None)
    extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.minute()
}

case class GpuSecond(child: Expression, timeZoneId: Option[String] = None)
    extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.second()
}

case class GpuHour(child: Expression, timeZoneId: Option[String] = None)
  extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.hour()
}

case class GpuYear(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.year()
}

abstract class GpuTimeMath(
    start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None)
   extends BinaryExpression with GpuExpression with TimeZoneAwareExpression with ExpectsInputTypes
   with Serializable {

  def this(start: Expression, interval: Expression) = this(start, interval, None)

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left - $right"
  override def sql: String = s"${left.sql} - ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  val microSecondsInOneDay: Long = TimeUnit.DAYS.toMicros(1)

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEval(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, intvl: CalendarInterval) =>
            if (intvl.months != 0) {
              throw new UnsupportedOperationException("Months aren't supported at the moment")
            }
            val usToSub = intvl.days * microSecondsInOneDay + intvl.microseconds
            if (usToSub != 0) {
              withResource(Scalar.fromLong(usToSub)) { us_s =>
                withResource(l.getBase.bitCastTo(DType.INT64)) { us =>
                  withResource(intervalMath(us_s, us)) { longResult =>
                    GpuColumnVector.from(longResult.castTo(DType.TIMESTAMP_MICROSECONDS), dataType)
                  }
                }
              }
            } else {
              l.incRefCount()
            }
          case _ =>
            throw new UnsupportedOperationException("GpuTimeSub takes column and interval as an " +
              "argument only")
        }
      }
    }
  }

  def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector
}

case class GpuTimeAdd(start: Expression,
                      interval: Expression,
                      timeZoneId: Option[String] = None)
  extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.add(us_s)
  }
}

case class GpuTimeSub(start: Expression,
                       interval: Expression,
                       timeZoneId: Option[String] = None)
  extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.sub(us_s)
  }
}

case class GpuDateAddInterval(start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None)
    extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.add(us_s)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, CalendarIntervalType)

  override def dataType: DataType = DateType

  override def columnarEval(batch: ColumnarBatch): Any = {

    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEval(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, intvl: CalendarInterval) =>
            if (intvl.months != 0) {
              throw new UnsupportedOperationException("Months aren't supported at the moment")
            }
            val microSecToDays = if (intvl.microseconds < 0) {
              // This is to calculate when subtraction is performed. Need to take into account the
              // interval( which are less than days). Convert it into days which needs to be
              // subtracted along with intvl.days(if provided).
              (intvl.microseconds.abs.toDouble / microSecondsInOneDay).ceil.toInt * -1
            } else {
              (intvl.microseconds.toDouble / microSecondsInOneDay).toInt
            }
            val daysToAdd = intvl.days + microSecToDays
            if (daysToAdd != 0) {
              withResource(Scalar.fromInt(daysToAdd)) { us_s =>
                withResource(l.getBase.bitCastTo(DType.INT32)) { us =>
                  withResource(intervalMath(us_s, us)) { intResult =>
                    GpuColumnVector.from(intResult.castTo(DType.TIMESTAMP_DAYS), dataType)
                  }
                }
              }
            } else {
              l.incRefCount()
            }
          case _ =>
            throw new UnsupportedOperationException("GpuDateAddInterval takes column and " +
              "interval as an argument only")
        }
      }
    }
  }
}

case class GpuDateDiff(endDate: Expression, startDate: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate

  override def right: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    withResource(lhs.getBase.asInts()) { lhsDays =>
      withResource(rhs.getBase.asInts()) { rhsDays =>
        lhsDays.sub(rhsDays)
      }
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.castDateScalarToInt(lhs)) { intScalar =>
      withResource(rhs.getBase.asInts()) { intVector =>
        intScalar.sub(intVector)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.castDateScalarToInt(rhs)) { intScalar =>
      withResource(lhs.getBase.asInts()) { intVector =>
        intVector.sub(intScalar)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuQuarter(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    val tmp = withResource(Scalar.fromInt(2)) { two =>
      withResource(input.getBase.month()) { month =>
        month.add(two)
      }
    }
    withResource(tmp) { tmp =>
      withResource(Scalar.fromInt(3)) { three =>
        tmp.div(three)
      }
    }
  }
}

case class GpuMonth(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.month()
}

case class GpuDayOfMonth(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.day()
}

case class GpuDayOfYear(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.dayOfYear()
}

abstract class UnixTimeExprMeta[A <: BinaryExpression with TimeZoneAwareExpression]
   (expr: A, conf: RapidsConf,
   parent: Option[RapidsMeta[_, _, _]],
   rule: DataFromReplacementRule)
  extends BinaryExprMeta[A](expr, conf, parent, rule) {
  var sparkFormat: String = _
  var strfFormat: String = _
  override def tagExprForGpu(): Unit = {
    checkTimeZoneId(expr.timeZoneId)

    // Date and Timestamp work too
    if (expr.right.dataType == StringType) {
      try {
        extractStringLit(expr.right) match {
          case Some(rightLit) =>
            sparkFormat = rightLit
            if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy) {
              willNotWorkOnGpu("legacyTimeParserPolicy LEGACY is not supported")
            } else if (GpuToTimestamp.COMPATIBLE_FORMATS.contains(sparkFormat) ||
                conf.incompatDateFormats) {
              strfFormat = DateUtils.toStrf(sparkFormat,
                expr.left.dataType == DataTypes.StringType)
            } else {
              willNotWorkOnGpu(s"incompatible format '$sparkFormat'. Set " +
                  s"spark.rapids.sql.incompatibleDateFormats.enabled=true to force onto GPU.")
            }
          case None =>
            willNotWorkOnGpu("format has to be a string literal")
        }
      } catch {
        case x: TimestampFormatConversionException =>
          willNotWorkOnGpu(s"Failed to convert ${x.reason} ${x.getMessage()}")
      }
    }
  }
}

sealed trait TimeParserPolicy extends Serializable
object LegacyTimeParserPolicy extends TimeParserPolicy
object ExceptionTimeParserPolicy extends TimeParserPolicy
object CorrectedTimeParserPolicy extends TimeParserPolicy

object GpuToTimestamp extends Arm {
  /** We are compatible with Spark for these formats */
  val COMPATIBLE_FORMATS = Seq(
    "yyyy-MM-dd",
    "yyyy-MM",
    "yyyy/MM/dd",
    "yyyy/MM",
    "dd/MM/yyyy",
    "yyyy-MM-dd HH:mm:ss",
    "MM-dd",
    "MM/dd",
    "dd-MM",
    "dd/MM"
  )

  def daysScalarSeconds(name: String): Scalar = {
    Scalar.timestampFromLong(DType.TIMESTAMP_SECONDS, DateUtils.specialDatesSeconds(name))
  }

  def daysScalarMicros(name: String): Scalar = {
    Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, DateUtils.specialDatesMicros(name))
  }

  def daysEqual(col: ColumnVector, name: String): ColumnVector = {
    withResource(Scalar.fromString(name)) { scalarName =>
      col.equalTo(scalarName)
    }
  }

  def isTimestamp(col: ColumnVector, sparkFormat: String, strfFormat: String) : ColumnVector = {
    if (COMPATIBLE_FORMATS.contains(sparkFormat)) {
      // the cuDF `is_timestamp` function is less restrictive than Spark's behavior for UnixTime
      // and ToUnixTime and will support parsing a subset of a string so we check the length of
      // the string as well which works well for fixed-length formats but if/when we want to
      // support variable-length formats (such as timestamps with milliseconds) then we will need
      // to use regex instead.
      withResource(col.getCharLengths) { actualLen =>
        withResource(Scalar.fromInt(sparkFormat.length)) { expectedLen =>
          withResource(actualLen.equalTo(expectedLen)) { lengthOk =>
            withResource(col.isTimestamp(strfFormat)) { isTimestamp =>
              isTimestamp.and(lengthOk)
            }
          }
        }
      }
    } else {
      // this is the incompatibleDateFormats case where we do not guarantee compatibility with
      // Spark and assume that all non-null inputs are valid
      ColumnVector.fromScalar(Scalar.fromBool(true), col.getRowCount.toInt)
    }
  }

  def parseStringAsTimestamp(
      lhs: GpuColumnVector,
      sparkFormat: String,
      strfFormat: String,
      timeParserPolicy: TimeParserPolicy,
      dtype: DType,
      daysScalar: String => Scalar,
      asTimestamp: (ColumnVector, String) => ColumnVector): ColumnVector = {

    val isTimestamp = GpuToTimestamp.isTimestamp(lhs.getBase, sparkFormat, strfFormat)

    // in addition to date/timestamp strings, we also need to check for special dates and null
    // values, since anything else is invalid and should throw an error or be converted to null
    // depending on the policy
    withResource(isTimestamp) { isTimestamp =>
      withResource(daysEqual(lhs.getBase, DateUtils.EPOCH)) { isEpoch =>
        withResource(daysEqual(lhs.getBase, DateUtils.NOW)) { isNow =>
          withResource(daysEqual(lhs.getBase, DateUtils.TODAY)) { isToday =>
            withResource(daysEqual(lhs.getBase, DateUtils.YESTERDAY)) { isYesterday =>
              withResource(daysEqual(lhs.getBase, DateUtils.TOMORROW)) { isTomorrow =>
                withResource(lhs.getBase.isNull) { isNull =>
                  withResource(Scalar.fromNull(dtype)) { nullValue =>
                    withResource(asTimestamp(lhs.getBase, strfFormat)) { converted =>
                      withResource(daysScalar(DateUtils.EPOCH)) { epoch =>
                        withResource(daysScalar(DateUtils.NOW)) { now =>
                          withResource(daysScalar(DateUtils.TODAY)) { today =>
                            withResource(daysScalar(DateUtils.YESTERDAY)) { yesterday =>
                              withResource(daysScalar(DateUtils.TOMORROW)) { tomorrow =>
                                withResource(isTomorrow.ifElse(tomorrow, nullValue)) { a =>
                                  withResource(isYesterday.ifElse(yesterday, a)) { b =>
                                    withResource(isToday.ifElse(today, b)) { c =>
                                      withResource(isNow.ifElse(now, c)) { d =>
                                        withResource(isEpoch.ifElse(epoch, d)) { e =>
                                          isTimestamp.ifElse(converted, e)
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

/**
 * A direct conversion of Spark's ToTimestamp class which converts time to UNIX timestamp by
 * first converting to microseconds and then dividing by the downScaleFactor
 */
abstract class GpuToTimestamp
  extends GpuBinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  import GpuToTimestamp._

  def downScaleFactor = DateUtils.ONE_SECOND_MICROSECONDS

  def sparkFormat: String
  def strfFormat: String

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  val timeParserPolicy = getTimeParserPolicy

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the unixtimestamp to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the unixtimestamp to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      parseStringAsTimestamp(
        lhs,
        sparkFormat,
        strfFormat,
        timeParserPolicy,
        DType.TIMESTAMP_MICROSECONDS,
        daysScalarMicros,
        (col, strfFormat) => col.asTimestampMicroseconds(strfFormat))
    } else { // Timestamp or DateType
      lhs.getBase.asTimestampMicroseconds()
    }
    // Return Timestamp value if dataType it is expecting is of TimestampType
    if (dataType.equals(TimestampType)) {
      tmp
    } else {
      withResource(tmp) { tmp =>
        // The type we are returning is a long not an actual timestamp
        withResource(Scalar.fromInt(downScaleFactor)) { downScaleFactor =>
          withResource(tmp.asLongs()) { longMicroSecs =>
            longMicroSecs.div(downScaleFactor)
          }
        }
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

/**
 * An improved version of GpuToTimestamp conversion which converts time to UNIX timestamp without
 * first converting to microseconds
 */
abstract class GpuToTimestampImproved extends GpuToTimestamp {
  import GpuToTimestamp._

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      parseStringAsTimestamp(
        lhs,
        sparkFormat,
        strfFormat,
        timeParserPolicy,
        DType.TIMESTAMP_SECONDS,
        daysScalarSeconds,
        (col, strfFormat) => col.asTimestampSeconds(strfFormat))
    } else if (lhs.dataType() == DateType){
      lhs.getBase.asTimestampSeconds()
    } else { // Timestamp
      // https://github.com/rapidsai/cudf/issues/5166
      // The time is off by 1 second if the result is < 0
      val longSecs = withResource(lhs.getBase.asTimestampSeconds()) { secs =>
        secs.asLongs()
      }
      withResource(longSecs) { secs =>
        val plusOne = withResource(Scalar.fromLong(1)) { one =>
          secs.add(one)
        }
        withResource(plusOne) { plusOne =>
          withResource(Scalar.fromLong(0)) { zero =>
            withResource(secs.lessThan(zero)) { neg =>
              neg.ifElse(plusOne, secs)
            }
          }
        }
      }
    }
    withResource(tmp) { r =>
      // The type we are returning is a long not an actual timestamp
      r.asLongs()
    }
  }
}

case class GpuUnixTimestamp(strTs: Expression,
    format: Expression,
    sparkFormat: String,
    strf: String,
    timeZoneId: Option[String] = None) extends GpuToTimestamp {
  override def strfFormat = strf
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def left: Expression = strTs
  override def right: Expression = format

}

case class GpuToUnixTimestamp(strTs: Expression,
    format: Expression,
    sparkFormat: String,
    strf: String,
    timeZoneId: Option[String] = None) extends GpuToTimestamp {
  override def strfFormat = strf
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def left: Expression = strTs
  override def right: Expression = format

}

case class GpuUnixTimestampImproved(strTs: Expression,
    format: Expression,
    sparkFormat: String,
    strf: String,
    timeZoneId: Option[String] = None) extends GpuToTimestampImproved {
  override def strfFormat = strf
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def left: Expression = strTs
  override def right: Expression = format

}

case class GpuToUnixTimestampImproved(strTs: Expression,
    format: Expression,
    sparkFormat: String,
    strf: String,
    timeZoneId: Option[String] = None) extends GpuToTimestampImproved {
  override def strfFormat = strf
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def left: Expression = strTs
  override def right: Expression = format

}

case class GpuGetTimestamp(
    strTs: Expression,
    format: Expression,
    sparkFormat: String,
    strf: String,
    timeZoneId: Option[String] = None) extends GpuToTimestamp {

  override def strfFormat = strf
  override val downScaleFactor = 1
  override def dataType: DataType = TimestampType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def left: Expression = strTs
  override def right: Expression = format
}

case class GpuFromUnixTime(
    sec: Expression,
    format: Expression,
    strfFormat: String,
    timeZoneId: Option[String] = None)
  extends GpuBinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the from_unixtime to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the from_unixtime to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    withResource(lhs.getBase.asTimestampSeconds) { tsVector =>
      tsVector.asStrings(strfFormat)
    }
  }

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def left: Expression = sec

  // we aren't using this "right" GpuExpression, as it was already converted in the GpuOverrides
  // while creating the expressions map and passed down here as strfFormat
  override def right: Expression = format

  override def dataType: DataType = StringType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

trait GpuDateMathBase extends GpuBinaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, TypeCollection(IntegerType, ShortType, ByteType))

  override def dataType: DataType = DateType

  def binaryOp: BinaryOp

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    withResource(lhs.getBase.castTo(DType.INT32)) { daysSinceEpoch =>
      withResource(daysSinceEpoch.binaryOp(binaryOp, rhs.getBase, daysSinceEpoch.getType)) {
        daysAsInts => daysAsInts.castTo(DType.TIMESTAMP_DAYS)
      }
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuScalar.castDateScalarToInt(lhs)) { daysAsInts =>
      withResource(daysAsInts.binaryOp(binaryOp, rhs.getBase, daysAsInts.getType)) { ints =>
        ints.castTo(DType.TIMESTAMP_DAYS)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): ColumnVector = {
    withResource(lhs.getBase.castTo(DType.INT32)) { daysSinceEpoch =>
      withResource(daysSinceEpoch.binaryOp(binaryOp, rhs, daysSinceEpoch.getType)) { daysAsInts =>
        daysAsInts.castTo(DType.TIMESTAMP_DAYS)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: Scalar, rhs: Scalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuDateSub(startDate: Expression, days: Expression)
  extends GpuDateMathBase {

  override def left: Expression = startDate
  override def right: Expression = days

  override def prettyName: String = "date_sub"

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

case class GpuDateAdd(startDate: Expression, days: Expression) extends GpuDateMathBase {

  override def left: Expression = startDate
  override def right: Expression = days

  override def prettyName: String = "date_add"

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

case class GpuLastDay(startDate: Expression)
    extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def prettyName: String = "last_day"

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.lastDayOfMonth()
}
