/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.{Arm, BinaryExprMeta, DataFromReplacementRule, DateUtils, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.GpuOverrides.{extractStringLit, getTimeParserPolicy}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, TimeZoneAwareExpression}
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

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEval(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, intvlS: GpuScalar)
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
            throw new UnsupportedOperationException("GpuTimeSub takes column and interval as an " +
              "argument only")
        }
      }
    }
  }

  def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector
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
          case (l: GpuColumnVector, intvlS: GpuScalar)
              if intvlS.dataType.isInstanceOf[CalendarIntervalType] =>
            // Scalar does not support 'CalendarInterval' now, so use
            // the Scala value instead.
            // Skip the null check because it wll be detected by the following calls.
            val intvl = intvlS.getValue.asInstanceOf[CalendarInterval]
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
  extends GpuBinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {

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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the date_format to work")
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
        "the date_format to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    withResource(lhs.getBase.asTimestampSeconds) { tsVector =>
      tsVector.asStrings(strfFormat)
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

  def shouldFallbackOnAnsiTimestamp: Boolean

  var sparkFormat: String = _
  var strfFormat: String = _
  override def tagExprForGpu(): Unit = {
    checkTimeZoneId(expr.timeZoneId)

    if (shouldFallbackOnAnsiTimestamp) {
      willNotWorkOnGpu("ANSI mode is not supported")
    }

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

sealed trait TimeParserPolicy extends Serializable
object LegacyTimeParserPolicy extends TimeParserPolicy
object ExceptionTimeParserPolicy extends TimeParserPolicy
object CorrectedTimeParserPolicy extends TimeParserPolicy

object GpuToTimestamp extends Arm {
  // We are compatible with Spark for these formats when the timeParserPolicy is CORRECTED
  // or EXCEPTION. It is possible that other formats may be supported but these are the only
  // ones that we have tests for.
  val CORRECTED_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{2}-\d{2}\Z"),
    "yyyy/MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}\Z"),
    "yyyy-MM" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{2}\Z"),
    "yyyy/MM" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{2}\Z"),
    "dd/MM/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}/\d{4}\Z"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta('-', isTimestamp = true,
      raw"\A\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\Z"),
    "MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z"),
    "dd-MM" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "dd/MM" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z"),
    "MM/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{4}\Z"),
    "MM-yyyy" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{4}\Z"),
    "MM/dd/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}/\d{4}\Z"),
    "MM-dd-yyyy" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{2}-\d{4}\Z")
  )

  // We are compatible with Spark for these formats when the timeParserPolicy is LEGACY. It
  // is possible that other formats may be supported but these are the only ones that we have
  // tests for.
  val LEGACY_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{1,2}-\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}(\D|\s|\Z)"),
    "dd-MM-yyyy" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{1,2}-\d{1,2}-\d{4}(\D|\s|\Z)"),
    "dd/MM/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{1,2}/\d{1,2}/\d{4}(\D|\s|\Z)"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta('-', isTimestamp = true,
      raw"\A\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd HH:mm:ss" -> ParseFormatMeta('/', isTimestamp = true,
      raw"\A\d{4}/\d{1,2}/\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)")
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
        withResource(col.matchesRe(fmt.validRegex)) { matches =>
          withResource(col.isTimestamp(strfFormat)) { isTimestamp =>
            isTimestamp.and(matches)
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
      dtype: DType): ColumnVector = {

    // `tsVector` will be closed in replaceSpecialDates
    val tsVector = withResource(isTimestamp(lhs.getBase, sparkFormat, strfFormat)) { isTs =>
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
      case '/' =>
        regexReplaceRules.map {
          case RegexReplace(pattern, backref) =>
            RegexReplace(pattern.replace('-', '/'), backref.replace('-', '/'))
        }
      case '-' =>
        regexReplaceRules
    }

    // apply each rule in turn to the data
    val fixedUp = rulesWithSeparator
      .foldLeft(rejectLeadingNewlineThenStrip(lhs))((cv, regexRule) => {
        withResource(cv) {
          _.stringReplaceWithBackrefs(regexRule.search, regexRule.replace)
        }
      })

    // check the final value against a regex to determine if it is valid or not, so we produce
    // null values for any invalid inputs
    withResource(Scalar.fromNull(dtype)) { nullValue =>
      withResource(fixedUp.matchesRe(format.validRegex)) { isValidDate =>
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
    withResource(lhs.getBase.matchesRe("\\A[ \\t]*[\\n]+")) { hasLeadingNewline =>
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

case class ParseFormatMeta(separator: Char, isTimestamp: Boolean, validRegex: String)

case class RegexReplace(search: String, replace: String)

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

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the unixtimestamp to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      if (getTimeParserPolicy == LegacyTimeParserPolicy) {
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
          DType.TIMESTAMP_MICROSECONDS)
      }
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

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      if (getTimeParserPolicy == LegacyTimeParserPolicy) {
        parseStringAsTimestampWithLegacyParserPolicy(
          lhs,
          sparkFormat,
          strfFormat,
          DType.TIMESTAMP_SECONDS,
          (col, strfFormat) => col.asTimestampSeconds(strfFormat))
      } else {
        parseStringAsTimestamp(
          lhs,
          sparkFormat,
          strfFormat,
          DType.TIMESTAMP_SECONDS)
      }
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

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the from_unixtime to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    withResource(lhs.getBase.asTimestampSeconds) { tsVector =>
      tsVector.asStrings(strfFormat)
    }
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
