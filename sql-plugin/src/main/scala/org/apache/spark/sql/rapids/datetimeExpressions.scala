/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import java.time.ZoneId
import java.util.concurrent.TimeUnit

import ai.rapids.cudf.{BinaryOp, CaptureGroups, ColumnVector, ColumnView, DType, RegexProgram, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, BoolUtils, DataFromReplacementRule, DateUtils, GpuBinaryExpression, GpuBinaryExpressionArgsAnyScalar, GpuCast, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.GpuOverrides.{extractStringLit, getTimeParserPolicy}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, FromUnixTime, FromUTCTimestamp, ImplicitCastInputTypes, NullIntolerant, TimeZoneAwareExpression, ToUTCTimestamp}
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

trait GpuDateUnaryExpression extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride: DType = DType.INT32
}

trait GpuTimeUnaryExpression extends GpuUnaryExpression with TimeZoneAwareExpression
   with ImplicitCastInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride: DType = DType.INT32

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
    if (GpuOverrides.isUTCTimezone(zoneId)) {
      input.getBase.minute()
    } else {
      // Non-UTC time zone
      withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(input.getBase, zoneId)) {
        shifted => shifted.minute()
      }
    }
}

case class GpuSecond(child: Expression, timeZoneId: Option[String] = None)
    extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    if (GpuOverrides.isUTCTimezone(zoneId)) {
      input.getBase.second()
    } else {
      // Non-UTC time zone
      withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(input.getBase, zoneId)) {
        shifted => shifted.second()
      }
    }
}

case class GpuHour(child: Expression, timeZoneId: Option[String] = None)
  extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): ColumnVector =
    if (GpuOverrides.isUTCTimezone(zoneId)) {
      input.getBase.hour()
    } else {
      // Non-UTC time zone
      withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(input.getBase, zoneId)) {
        shifted => shifted.hour()
      }
    }
}

case class GpuYear(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.year()
}

abstract class GpuTimeMath(
    start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None)
   extends ShimBinaryExpression
       with GpuExpression
       with TimeZoneAwareExpression
       with ExpectsInputTypes
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

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) { rhs =>
        (lhs, rhs) match {
          case (l, intvlS: GpuScalar)
              if intvlS.dataType.isInstanceOf[CalendarIntervalType] =>
            // Scalar does not support 'CalendarInterval' now, so use
            // the Scala value instead.
            // Skip the null check because it wll be detected by the following calls.
            val intvl = intvlS.getValue.asInstanceOf[CalendarInterval]
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
            throw new UnsupportedOperationException("only column and interval arguments " +
              s"are supported, got left: ${lhs.getClass} right: ${rhs.getClass}")
        }
      }
    }
  }

  def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector
}

case class GpuDateAddInterval(start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None,
    ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
    extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.add(us_s)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, CalendarIntervalType)

  override def dataType: DataType = DateType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {

    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) {
        case intvlS: GpuScalar if intvlS.dataType.isInstanceOf[CalendarIntervalType] =>
          // Scalar does not support 'CalendarInterval' now, so use
          // the Scala value instead.
          // Skip the null check because it will be detected by the following calls.
          val intvl = intvlS.getValue.asInstanceOf[CalendarInterval]

          // ANSI mode checking
          if (ansiEnabled && intvl.microseconds != 0) {
            val msg = "IllegalArgumentException: Cannot add hours, minutes or seconds" +
                ", milliseconds, microseconds to a date. " +
                "If necessary set spark.sql.ansi.enabled to false to bypass this error."
            throw new IllegalArgumentException(msg)
          }

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
              withResource(lhs.getBase.bitCastTo(DType.INT32)) { us =>
                withResource(intervalMath(us_s, us)) { intResult =>
                  GpuColumnVector.from(intResult.castTo(DType.TIMESTAMP_DAYS), dataType)
                }
              }
            }
          } else {
            lhs.incRefCount()
          }
        case _ =>
          throw new UnsupportedOperationException("GpuDateAddInterval requires a scalar " +
              "for the interval")
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

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.from(lhs.getValue, IntegerType)) { intScalar =>
      withResource(rhs.getBase.asInts()) { intVector =>
        intScalar.sub(intVector)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.from(rhs.getValue, IntegerType)) { intScalar =>
      withResource(lhs.getBase.asInts()) { intVector =>
        intVector.sub(intScalar)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

case class GpuDateFormatClass(timestamp: Expression,
    format: Expression,
    strfFormat: String,
    timeZoneId: Option[String] = None)
  extends GpuBinaryExpressionArgsAnyScalar
      with TimeZoneAwareExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def left: Expression = timestamp

  // we aren't using this "right" GpuExpression, as it was already converted in the GpuOverrides
  // while creating the expressions map and passed down here as strfFormat
  override def right: Expression = format
  override def prettyName: String = "date_format"

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    withResource(lhs.getBase.asTimestampMicroseconds()) { tsVector =>
      if (GpuOverrides.isUTCTimezone(zoneId)) {
        // UTC time zone
        tsVector.asStrings(strfFormat)
      } else {
        // Non-UTC TZ
        withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(tsVector, zoneId.normalized())) {
          shifted => shifted.asStrings(strfFormat)
        }
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
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
    // Date and Timestamp work too
    if (expr.right.dataType == StringType) {
      extractStringLit(expr.right) match {
        case Some(rightLit) =>
          sparkFormat = rightLit
          strfFormat = DateUtils.tagAndGetCudfFormat(this,
            sparkFormat, expr.left.dataType == DataTypes.StringType)
        case None =>
          willNotWorkOnGpu("format has to be a string literal")
      }
    }
  }
}

trait GpuNumberToTimestampUnaryExpression extends GpuUnaryExpression {
  
  override def dataType: DataType = TimestampType
  override def outputTypeOverride: DType = DType.TIMESTAMP_MICROSECONDS

  /**
   * Test whether if input * multiplier will cause Long-overflow. In Math.multiplyExact, 
   * if there is an integer-overflow, then it will throw an ArithmeticException "long overflow"
   */
  def checkLongMultiplicationOverflow(input: ColumnVector, multiplier: Long): Unit = {
    withResource(input.max()) { maxValue =>
      if (maxValue.isValid) {
        Math.multiplyExact(maxValue.getLong, multiplier)
      }
    }
    withResource(input.min()) { minValue =>
      if (minValue.isValid) {
        Math.multiplyExact(minValue.getLong, multiplier)
      }
    }
  }

  protected val convertTo : GpuColumnVector => ColumnVector
  
  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    convertTo(input)
  }
}

case class GpuSecondsToTimestamp(child: Expression) extends GpuNumberToTimestampUnaryExpression {

  override def nullable: Boolean = child.dataType match {
    case _: FloatType | _: DoubleType => true
    case _ => child.nullable
  }

  private def checkRoundingNecessary(input: ColumnVector, dt: DecimalType): Unit = {
    // SecondsToTimestamp supports decimals with a scale of 6 or less, which can be represented
    // as microseconds. An exception will be thrown if the scale is more than 6.
    val decimalTypeSupported = DType.create(DType.DTypeEnum.DECIMAL128, -6)
    if (dt.scale > 6) {
      // Match the behavior of `BigDecimal.longValueExact()`, if decimal is equal to the value
      // casted to decimal128 with scale 6, then no rounding is necessary.
      val decimalTypeAllScale = DType.create(DType.DTypeEnum.DECIMAL128, -dt.scale)
      val castedAllScale = withResource(input.castTo(decimalTypeSupported)) { casted =>
        casted.castTo(decimalTypeAllScale)
      }
      val isEqual = withResource(castedAllScale) { _ =>
        withResource(input.castTo(decimalTypeAllScale)) { original =>
          original.equalTo(castedAllScale)
        }
      }
      val roundUnnecessity = withResource(isEqual) { _ =>
        withResource(isEqual.all()) { all =>
          all.isValid && all.getBoolean()
        }
      }
      if (!roundUnnecessity) {
        throw new ArithmeticException("Rounding necessary")
      }
    }
  }

  @transient
  protected lazy val convertTo: GpuColumnVector => ColumnVector = child.dataType match {
    case LongType =>
      (input: GpuColumnVector) => {
        checkLongMultiplicationOverflow(input.getBase, DateTimeConstants.MICROS_PER_SECOND)
        val mul = withResource(Scalar.fromLong(DateTimeConstants.MICROS_PER_SECOND)) { scalar =>
          input.getBase.mul(scalar)
        }
        withResource(mul) { _ =>
          mul.asTimestampMicroseconds()
        }
      }
    case DoubleType | FloatType =>
      (input: GpuColumnVector) => {
        GpuCast.doCast(input.getBase, input.dataType, TimestampType)
      }
    case dt: DecimalType =>
      (input: GpuColumnVector) => {
        checkRoundingNecessary(input.getBase, dt)
        // Cast to decimal128 to avoid overflow, scala of 6 is enough after rounding check.
        val decimalTypeSupported = DType.create(DType.DTypeEnum.DECIMAL128, -6)
        val mul = withResource(input.getBase.castTo(decimalTypeSupported)) { decimal =>
          withResource(Scalar.fromLong(DateTimeConstants.MICROS_PER_SECOND)) { scalar =>
            decimal.mul(scalar, decimalTypeSupported)
          }
        }
        // Match the behavior of `BigDecimal.longValueExact()`:
        closeOnExcept(mul) { _ =>
          val greaterThanMax = withResource(Scalar.fromLong(Long.MaxValue)) { longMax =>
              mul.greaterThan(longMax)
          }
          val largerThanLongMax = withResource(greaterThanMax) { greaterThanMax =>
            withResource(greaterThanMax.any()) { any =>
              any.isValid && any.getBoolean()
            }
          }
          lazy val smallerThanLongMin: Boolean = {
            val lessThanMin = withResource(Scalar.fromLong(Long.MinValue)) { longMin =>
                mul.lessThan(longMin)
            }
            withResource(lessThanMin) { lessThanMin =>
              withResource(lessThanMin.any()) { any =>
                any.isValid && any.getBoolean()
              }
            }
          }
          if (largerThanLongMax || smallerThanLongMin) {
            throw new java.lang.ArithmeticException("Overflow")
          }
        }
        val longs = withResource(mul) { _ =>
          mul.castTo(DType.INT64)
        }
        withResource(longs) { _ =>
          longs.asTimestampMicroseconds()
        }
      }
    case IntegerType | ShortType | ByteType =>
      (input: GpuColumnVector) =>
        withResource(input.getBase.castTo(DType.INT64)) { longs =>
          // Not possible to overflow for Int, Short and Byte
          longs.asTimestampSeconds()
        }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupport type ${child.dataType} " + 
          s"for SecondsToTimestamp ")
  }
}

case class GpuMillisToTimestamp(child: Expression) extends GpuNumberToTimestampUnaryExpression {
  protected lazy val convertTo: GpuColumnVector => ColumnVector = child.dataType match {
    case LongType => 
      (input: GpuColumnVector) => {
        checkLongMultiplicationOverflow(input.getBase, DateTimeConstants.MICROS_PER_MILLIS)
        input.getBase.asTimestampMilliseconds()
      }
    case IntegerType | ShortType | ByteType =>
      (input: GpuColumnVector) => {
        withResource(input.getBase.castTo(DType.INT64)) { longs =>
          checkLongMultiplicationOverflow(longs, DateTimeConstants.MICROS_PER_MILLIS)
          longs.asTimestampMilliseconds()
        }
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupport type ${child.dataType} " + 
          s"for MillisToTimestamp ")
  }
}

case class GpuMicrosToTimestamp(child: Expression) extends GpuNumberToTimestampUnaryExpression {
  protected lazy val convertTo: GpuColumnVector => ColumnVector = child.dataType match {
    case LongType =>
      (input: GpuColumnVector) => {
        input.getBase.asTimestampMicroseconds()
      }
    case IntegerType | ShortType | ByteType =>
      (input: GpuColumnVector) => {
        withResource(input.getBase.castTo(DType.INT64)) { longs =>
          longs.asTimestampMicroseconds()
        }
      }
    case _ =>
      throw new UnsupportedOperationException(s"Unsupport type ${child.dataType} " + 
          s"for MicrosToTimestamp ")
  }
}

sealed trait TimeParserPolicy extends Serializable
object LegacyTimeParserPolicy extends TimeParserPolicy
object ExceptionTimeParserPolicy extends TimeParserPolicy
object CorrectedTimeParserPolicy extends TimeParserPolicy

object GpuToTimestamp {
  // We are compatible with Spark for these formats when the timeParserPolicy is CORRECTED
  // or EXCEPTION. It is possible that other formats may be supported but these are the only
  // ones that we have tests for.
  val CORRECTED_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{4}-\d{2}-\d{2}\Z"),
    "yyyy/MM/dd" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}\Z"),
    "yyyy-MM" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{4}-\d{2}\Z"),
    "yyyy/MM" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{4}/\d{2}\Z"),
    "dd/MM/yyyy" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{2}/\d{2}/\d{4}\Z"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta(Option('-'), isTimestamp = true,
      raw"\A\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\Z"),
    "MM-dd" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "MM/dd" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z"),
    "dd-MM" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "dd/MM" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z"),
    "MM/yyyy" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{2}/\d{4}\Z"),
    "MM-yyyy" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{2}-\d{4}\Z"),
    "MM/dd/yyyy" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{2}/\d{2}/\d{4}\Z"),
    "MM-dd-yyyy" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{2}-\d{2}-\d{4}\Z"),
    "MMyyyy" -> ParseFormatMeta(Option.empty, isTimestamp = false,
      raw"\A\d{6}\Z")
  )

  // We are compatible with Spark for these formats when the timeParserPolicy is LEGACY. It
  // is possible that other formats may be supported but these are the only ones that we have
  // tests for.
  val LEGACY_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{4}-\d{1,2}-\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}(\D|\s|\Z)"),
    "dd-MM-yyyy" -> ParseFormatMeta(Option('-'), isTimestamp = false,
      raw"\A\d{1,2}-\d{1,2}-\d{4}(\D|\s|\Z)"),
    "dd/MM/yyyy" -> ParseFormatMeta(Option('/'), isTimestamp = false,
      raw"\A\d{1,2}/\d{1,2}/\d{4}(\D|\s|\Z)"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta(Option('-'), isTimestamp = true,
      raw"\A\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd HH:mm:ss" -> ParseFormatMeta(Option('/'), isTimestamp = true,
      raw"\A\d{4}/\d{1,2}/\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)"),
    "yyyyMMdd" -> ParseFormatMeta(None, isTimestamp = false,
      raw"\A\d{8}(\D|\s|\Z)"),
    "yyyymmdd" -> ParseFormatMeta(None, isTimestamp = false,
      raw"\A\d{8}(\D|\s|\Z)")
  )

  /** remove whitespace before month and day */
  val REMOVE_WHITESPACE_FROM_MONTH_DAY: RegexReplace =
    RegexReplace(raw"(\A\d+)-([ \t]*)(\d+)-([ \t]*)(\d+)", raw"\1-\3-\5")

  def daysEqual(col: ColumnVector, name: String): ColumnVector = {
    withResource(Scalar.fromString(name)) { scalarName =>
      col.equalTo(scalarName)
    }
  }

  /**
   * Replace special date strings such as "now" with timestampDays. This method does not
   * close the `stringVector`.
   */
  def replaceSpecialDates(
      stringVector: ColumnVector,
      chronoVector: ColumnVector,
      specialDates: Map[String, () => Scalar]): ColumnVector = {
    specialDates.foldLeft(chronoVector) { case (buffer, (name, scalarBuilder)) =>
      withResource(buffer) { bufVector =>
        withResource(daysEqual(stringVector, name)) { isMatch =>
          withResource(scalarBuilder()) { scalar =>
            isMatch.ifElse(scalar, bufVector)
          }
        }
      }
    }
  }

  def isTimestamp(col: ColumnVector, sparkFormat: String, strfFormat: String) : ColumnVector = {
    CORRECTED_COMPATIBLE_FORMATS.get(sparkFormat) match {
      case Some(fmt) =>
        // the cuDF `is_timestamp` function is less restrictive than Spark's behavior for UnixTime
        // and ToUnixTime and will support parsing a subset of a string so we check the length of
        // the string as well which works well for fixed-length formats but if/when we want to
        // support variable-length formats (such as timestamps with milliseconds) then we will need
        // to use regex instead.
        val prog = new RegexProgram(fmt.validRegex, CaptureGroups.NON_CAPTURE)
        val isTimestamp = withResource(col.matchesRe(prog)) { matches =>
          withResource(col.isTimestamp(strfFormat)) { isTimestamp =>
            isTimestamp.and(matches)
          }
        }
        withResource(isTimestamp) { _ =>
          withResource(col.getCharLengths) { len =>
            withResource(Scalar.fromInt(sparkFormat.length)) { expectedLen =>
              withResource(len.equalTo(expectedLen)) { lenMatches =>
                lenMatches.and(isTimestamp)
              }
            }
          }
        }
      case _ =>
        // this is the incompatibleDateFormats case where we do not guarantee compatibility with
        // Spark and assume that all non-null inputs are valid
        ColumnVector.fromScalar(Scalar.fromBool(true), col.getRowCount.toInt)
    }
  }

  def parseStringAsTimestamp(
      lhs: GpuColumnVector,
      sparkFormat: String,
      strfFormat: String,
      dtype: DType,
      failOnError: Boolean): ColumnVector = {

    // `tsVector` will be closed in replaceSpecialDates
    val tsVector = withResource(isTimestamp(lhs.getBase, sparkFormat, strfFormat)) { isTs =>
      if(failOnError && !BoolUtils.isAllValidTrue(isTs)) {
        // ANSI mode and has invalid value.
        // CPU may throw `DateTimeParseException`, `DateTimeException` or `ParseException`
        throw new IllegalArgumentException("Exception occurred when parsing timestamp in ANSI mode")
      }

      withResource(Scalar.fromNull(dtype)) { nullValue =>
        withResource(lhs.getBase.asTimestamp(dtype, strfFormat)) { tsVec =>
          isTs.ifElse(tsVec, nullValue)
        }
      }
    }

    // in addition to date/timestamp strings, we also need to check for special dates and null
    // values, since anything else is invalid and should throw an error or be converted to null
    // depending on the policy
    closeOnExcept(tsVector) { tsVector =>
      DateUtils.fetchSpecialDates(dtype) match {
        case specialDates if specialDates.nonEmpty =>
          // `tsVector` will be closed in replaceSpecialDates
          replaceSpecialDates(lhs.getBase, tsVector, specialDates)
        case _ =>
          tsVector
      }
    }
  }

  /**
   * Parse string to timestamp when timeParserPolicy is LEGACY. This was the default behavior
   * prior to Spark 3.0
   */
  def parseStringAsTimestampWithLegacyParserPolicy(
      lhs: GpuColumnVector,
      sparkFormat: String,
      strfFormat: String,
      dtype: DType,
      asTimestamp: (ColumnVector, String) => ColumnVector): ColumnVector = {

    val format = LEGACY_COMPATIBLE_FORMATS.getOrElse(sparkFormat,
      throw new IllegalStateException(s"Unsupported format $sparkFormat"))

    val regexReplaceRules = Seq(REMOVE_WHITESPACE_FROM_MONTH_DAY)

    // we support date formats using either `-` or `/` to separate year, month, and day and the
    // regex rules are written with '-' so we need to replace '-' with '/' here as necessary
    val rulesWithSeparator = format.separator match {
      case Some('/') =>
        regexReplaceRules.map {
          case RegexReplace(pattern, backref) =>
            RegexReplace(pattern.replace('-', '/'), backref.replace('-', '/'))
        }
      case Some('-') | Some(_) =>
        regexReplaceRules
      case None =>
        // For formats like `yyyyMMdd` that do not contains separator,
        // do not need to do regexp replacement rules
        // Note: here introduced the following inconsistent behavior compared to Spark
        // Spark's behavior:
        //   to_date('20240101', 'yyyyMMdd')  = 2024-01-01
        //   to_date('202401 01', 'yyyyMMdd') = 2024-01-01
        //   to_date('2024 0101', 'yyyyMMdd') = null
        // GPU behavior:
        //   to_date('20240101', 'yyyyMMdd')  = 2024-01-01
        //   to_date('202401 01', 'yyyyMMdd') = null
        //   to_date('2024 0101', 'yyyyMMdd') = null
        Seq()
    }

    // apply each rule in turn to the data
    val fixedUp = rulesWithSeparator
      .foldLeft(rejectLeadingNewlineThenStrip(lhs))((cv, regexRule) => {
        withResource(cv) {
          _.stringReplaceWithBackrefs(new RegexProgram(regexRule.search), regexRule.replace)
        }
      })

    // check the final value against a regex to determine if it is valid or not, so we produce
    // null values for any invalid inputs
    withResource(Scalar.fromNull(dtype)) { nullValue =>
      val prog = new RegexProgram(format.validRegex, CaptureGroups.NON_CAPTURE)
      withResource(fixedUp.matchesRe(prog)) { isValidDate =>
        withResource(asTimestampOrNull(fixedUp, dtype, strfFormat, asTimestamp)) { timestamp =>
          isValidDate.ifElse(timestamp, nullValue)
        }
      }
    }
  }

  /**
   * Filter out strings that have a newline before the first non-whitespace character
   * and then strip all leading and trailing whitespace.
   */
  private def rejectLeadingNewlineThenStrip(lhs: GpuColumnVector) = {
    val prog = new RegexProgram("\\A[ \\t]*[\\n]+", CaptureGroups.NON_CAPTURE)
    withResource(lhs.getBase.matchesRe(prog)) { hasLeadingNewline =>
      withResource(Scalar.fromNull(DType.STRING)) { nullValue =>
        withResource(lhs.getBase.strip()) { stripped =>
          hasLeadingNewline.ifElse(nullValue, stripped)
        }
      }
    }
  }

  /**
   * Parse a string column to timestamp.
   */
  def asTimestampOrNull(
      cv: ColumnVector,
      dtype: DType,
      strfFormat: String,
      asTimestamp: (ColumnVector, String) => ColumnVector): ColumnVector = {
    withResource(cv) { _ =>
      withResource(Scalar.fromNull(dtype)) { nullValue =>
        withResource(cv.isTimestamp(strfFormat)) { isTimestamp =>
          withResource(asTimestamp(cv, strfFormat)) { timestamp =>
            isTimestamp.ifElse(timestamp, nullValue)
          }
        }
      }
    }
  }

}

case class ParseFormatMeta(separator: Option[Char], isTimestamp: Boolean, validRegex: String)

case class RegexReplace(search: String, replace: String)

/**
 * A direct conversion of Spark's ToTimestamp class which converts time to UNIX timestamp by
 * first converting to microseconds and then dividing by the downScaleFactor
 */
abstract class GpuToTimestamp
  extends GpuBinaryExpressionArgsAnyScalar with TimeZoneAwareExpression with ExpectsInputTypes {

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

  val failOnError: Boolean = SQLConf.get.ansiEnabled

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val tmp = lhs.dataType match {
      case _: StringType =>
        // rhs is ignored we already parsed the format
        val res = if (getTimeParserPolicy == LegacyTimeParserPolicy) {
          parseStringAsTimestampWithLegacyParserPolicy(
            lhs,
            sparkFormat,
            strfFormat,
            DType.TIMESTAMP_MICROSECONDS,
            (col, strfFormat) => col.asTimestampMicroseconds(strfFormat))
        } else {
          parseStringAsTimestamp(
            lhs,
            sparkFormat,
            strfFormat,
            DType.TIMESTAMP_MICROSECONDS,
            failOnError)
        }
        if (GpuOverrides.isUTCTimezone(zoneId)) {
          res
        } else {
          withResource(res) { _ =>
            GpuTimeZoneDB.fromTimestampToUtcTimestamp(res, zoneId)
          }
        }
      case _: DateType =>
        timeZoneId match {
          case Some(_) =>
            if (GpuOverrides.isUTCTimezone(zoneId)) {
              lhs.getBase.asTimestampMicroseconds()
            } else {
              assert(GpuTimeZoneDB.isSupportedTimeZone(zoneId))
              withResource(lhs.getBase.asTimestampMicroseconds) { tsInMs =>
                GpuTimeZoneDB.fromTimestampToUtcTimestamp(tsInMs, zoneId)
              }
            }
          case None => lhs.getBase.asTimestampMicroseconds()
        }
      case _ =>
        // Consistent with Spark's behavior which ignores timeZone for other types like timestamp
        // and timestampNtp.
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

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
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

class FromUnixTimeMeta(a: FromUnixTime,
    override val conf: RapidsConf,
    val p: Option[RapidsMeta[_, _, _]],
    r: DataFromReplacementRule) extends UnixTimeExprMeta[FromUnixTime](a, conf, p, r) {

  private type FmtConverter = ColumnView => ColumnVector

  private var colConverter: Option[FmtConverter] = None

  /**
   * More supported formats by post conversions. The idea is
   *  1) Map the unsupported target format to a supported format as
   *     the intermediate format,
   *  2) Call into cuDF with this intermediate format,
   *  3) Run a post conversion to get the right output for the target format.
   *
   * NOTE: Need to remove the entry if the key format is supported by cuDF.
   */
  private val FORMATS_BY_CONVERSION: Map[String, (String, FmtConverter)] = Map(
    // spark format -> (intermediate format, converter)
    "yyyyMMdd" -> (("yyyy-MM-dd",
      col => {
        withResource(Scalar.fromString("-")) { dashStr =>
          withResource(Scalar.fromString("")) { emptyStr =>
            col.stringReplace(dashStr, emptyStr)
          }
        }
      }
    ))
  )

  override def tagExprForGpu(): Unit = {
    extractStringLit(a.right) match {
      case Some(rightLit) =>
        sparkFormat = rightLit
        var inputFormat: Option[String] = None
        FORMATS_BY_CONVERSION.get(sparkFormat).foreach { case (tempFormat, converter) =>
            colConverter = Some(converter)
            inputFormat = Some(tempFormat)
        }
        strfFormat = DateUtils.tagAndGetCudfFormat(this, sparkFormat,
          a.left.dataType == DataTypes.StringType, inputFormat)
      case None =>
        willNotWorkOnGpu("format has to be a string literal")
    }
  }

  override def isTimeZoneSupported = true
  override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression = {
    // passing the already converted strf string for a little optimization
    GpuFromUnixTime(lhs, rhs, strfFormat, colConverter, a.timeZoneId)
  }
}

case class GpuFromUnixTime(
    sec: Expression,
    format: Expression,
    strfFormat: String,
    colConverter: Option[ColumnView => ColumnVector],
    timeZoneId: Option[String])
  extends GpuBinaryExpressionArgsAnyScalar
    with TimeZoneAwareExpression
    with ImplicitCastInputTypes {

  // To avoid duplicated "if...else" for each input batch
  private val convertFunc: ColumnVector => ColumnVector = {
    if (colConverter.isDefined) {
      col => withResource(col)(colConverter.get.apply)
    } else {
      identity[ColumnVector]
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    val ret = withResource(lhs.getBase.asTimestampSeconds) { secondCV =>
      if (GpuOverrides.isUTCTimezone(zoneId)) {
        // UTC time zone
        secondCV.asStrings(strfFormat)
      } else {
        // Non-UTC TZ
        withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(secondCV, zoneId.normalized())) {
          shifted => shifted.asStrings(strfFormat)
        }
      }
    }
    convertFunc(ret)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
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

abstract class ConvertUTCTimestampExprMetaBase[INPUT <: BinaryExpression](
    expr: INPUT,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends BinaryExprMeta[INPUT](expr, conf, parent, rule) {

  protected[this] var timezoneId: ZoneId = null  

  override def tagExprForGpu(): Unit = {
    extractStringLit(expr.right) match {
      case None =>
        willNotWorkOnGpu("timezone input must be a literal string")
      case Some(timezoneShortID) =>
        if (timezoneShortID != null) {
          timezoneId = GpuTimeZoneDB.getZoneId(timezoneShortID)
          if (!GpuTimeZoneDB.isSupportedTimeZone(timezoneId)) {
            willNotWorkOnGpu(s"Not supported timezone type $timezoneShortID.")
          }
        }
    }
  }
}

class FromUTCTimestampExprMeta(
    expr: FromUTCTimestamp,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ConvertUTCTimestampExprMetaBase[FromUTCTimestamp](expr, conf, parent, rule) {

  override def convertToGpu(timestamp: Expression, timezone: Expression): GpuExpression =
    GpuFromUTCTimestamp(timestamp, timezone, timezoneId)
}

case class GpuFromUTCTimestamp(
    timestamp: Expression, timezone: Expression, zoneId: ZoneId)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerant {

  override def left: Expression = timestamp
  override def right: Expression = timezone
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (rhs.getBase.isValid) {
      if (GpuOverrides.isUTCTimezone(zoneId)) {
        // For UTC timezone, just a no-op bypassing GPU computation.
        lhs.getBase.incRefCount()
      } else {
        GpuTimeZoneDB.fromUtcTimestampToTimestamp(lhs.getBase, zoneId)
      }
    } else {
      // All-null output column.
      GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { lhsCol =>
      doColumnar(lhsCol, rhs)
    }
  }
}

class ToUTCTimestampExprMeta(
    expr: ToUTCTimestamp,
    override val conf: RapidsConf,
    override val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ConvertUTCTimestampExprMetaBase[ToUTCTimestamp](expr, conf, parent, rule) {

  override def convertToGpu(timestamp: Expression, timezone: Expression): GpuExpression =
    GpuToUTCTimestamp(timestamp, timezone, timezoneId)
}

case class GpuToUTCTimestamp(
    timestamp: Expression, timezone: Expression, zoneId: ZoneId)
  extends GpuBinaryExpressionArgsAnyScalar
      with ImplicitCastInputTypes
      with NullIntolerant {

  override def left: Expression = timestamp
  override def right: Expression = timezone
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if (rhs.getBase.isValid) {
      if (GpuOverrides.isUTCTimezone(zoneId)) {
        // For UTC timezone, just a no-op bypassing GPU computation.
        lhs.getBase.incRefCount()
      } else {
        GpuTimeZoneDB.fromTimestampToUtcTimestamp(lhs.getBase, zoneId)
      }
    } else {
      // All-null output column.
      GpuColumnVector.columnVectorFromNull(lhs.getRowCount.toInt, dataType)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { lhsCol =>
      doColumnar(lhsCol, rhs)
    }
  }
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

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    withResource(GpuScalar.from(lhs.getValue, IntegerType)) { daysAsInts =>
      withResource(daysAsInts.binaryOp(binaryOp, rhs.getBase, daysAsInts.getType)) { ints =>
        ints.castTo(DType.TIMESTAMP_DAYS)
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    withResource(lhs.getBase.castTo(DType.INT32)) { daysSinceEpoch =>
      withResource(daysSinceEpoch.binaryOp(binaryOp, rhs.getBase, daysSinceEpoch.getType)) {
        daysAsInts => daysAsInts.castTo(DType.TIMESTAMP_DAYS)
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
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
