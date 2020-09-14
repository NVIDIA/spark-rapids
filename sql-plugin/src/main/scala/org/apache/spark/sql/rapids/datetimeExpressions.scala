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

import java.time.ZoneId

import ai.rapids.cudf.{BinaryOp, ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.{BinaryExprMeta, ConfKeysAndIncompat, DateUtils, GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuOverrides, GpuScalar, GpuUnaryExpression, RapidsConf, RapidsMeta}
import com.nvidia.spark.rapids.DateUtils.TimestampFormatConversionException
import com.nvidia.spark.rapids.GpuOverrides.extractStringLit
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

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromShort(1.toShort)) { one =>
      withResource(input.getBase.weekDay()) { weekday => // We want Monday = 0, CUDF Monday = 1
        GpuColumnVector.from(weekday.sub(one))
      }
    }
  }
}

case class GpuDayOfWeek(child: Expression)
    extends GpuDateUnaryExpression {

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    // Cudf returns Monday = 1, ...
    // We want Sunday = 1, ..., so add a day before we extract the day of the week
    val nextInts = withResource(Scalar.fromInt(1)) { one =>
      withResource(input.getBase.asInts()) { ints =>
        ints.add(one)
      }
    }
    withResource(nextInts) { nextInts =>
      withResource(nextInts.asTimestampDays()) { daysAgain =>
        GpuColumnVector.from(daysAgain.weekDay())
      }
    }
  }
}

case class GpuMinute(child: Expression, timeZoneId: Option[String] = None)
    extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.minute())
  }
}

case class GpuSecond(child: Expression, timeZoneId: Option[String] = None)
    extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.second())
  }
}

case class GpuHour(child: Expression, timeZoneId: Option[String] = None)
  extends GpuTimeUnaryExpression {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.hour())
  }
}

case class GpuYear(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.year())
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

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      (lhs, rhs) match {
        case (l: GpuColumnVector, intvl: CalendarInterval) =>
          if (intvl.months != 0) {
            throw new UnsupportedOperationException("Months aren't supported at the moment")
          }
          val usToSub = intvl.days.toLong * 24 * 60 * 60 * 1000 * 1000 + intvl.microseconds
          if (usToSub != 0) {
            withResource(Scalar.fromLong(usToSub)) { us_s =>
              withResource(l.getBase.castTo(DType.INT64)) { us =>
                withResource(intervalMath(us_s, us)) { longResult =>
                  GpuColumnVector.from(longResult.castTo(DType.TIMESTAMP_MICROSECONDS))
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
    } finally {
      if (lhs.isInstanceOf[AutoCloseable]) {
        lhs.asInstanceOf[AutoCloseable].close()
      }
      if (rhs.isInstanceOf[AutoCloseable]) {
        rhs.asInstanceOf[AutoCloseable].close()
      }
    }
  }

  def intervalMath(us_s: Scalar, us: ColumnVector): ColumnVector
}

case class GpuTimeAdd(start: Expression,
                      interval: Expression,
                      timeZoneId: Option[String] = None)
  extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnVector): ColumnVector = {
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

  def intervalMath(us_s: Scalar, us: ColumnVector): ColumnVector = {
    us.sub(us_s)
  }
}

case class GpuDateDiff(endDate: Expression, startDate: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate

  override def right: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    withResource(lhs.getBase.asInts()) { lhsDays =>
      withResource(rhs.getBase.asInts()) { rhsDays =>
        GpuColumnVector.from(lhsDays.sub(rhsDays))
      }
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.castDateScalarToInt(lhs)) { intScalar =>
      withResource(rhs.getBase.asInts()) { intVector =>
        GpuColumnVector.from(intScalar.sub(intVector))
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    withResource(GpuScalar.castDateScalarToInt(rhs)) { intScalar =>
      withResource(lhs.getBase.asInts()) { intVector =>
        GpuColumnVector.from(intVector.sub(intScalar))
      }
    }
  }
}

case class GpuQuarter(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val tmp = withResource(Scalar.fromInt(2)) { two =>
      withResource(input.getBase.month()) { month =>
        month.add(two)
      }
    }
    withResource(tmp) { tmp =>
      withResource(Scalar.fromInt(3)) { three =>
        GpuColumnVector.from(tmp.div(three))
      }
    }
  }
}

case class GpuMonth(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.month())
}

case class GpuDayOfMonth(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.day())
}

case class GpuDayOfYear(child: Expression) extends GpuDateUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.dayOfYear())
}

abstract class UnixTimeExprMeta[A <: BinaryExpression with TimeZoneAwareExpression]
   (expr: A, conf: RapidsConf,
   parent: Option[RapidsMeta[_, _, _]],
   rule: ConfKeysAndIncompat) extends BinaryExprMeta[A](expr, conf, parent, rule) {
  var strfFormat: String = _
  override def tagExprForGpu(): Unit = {
    if (ZoneId.of(expr.timeZoneId.get).normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
      willNotWorkOnGpu("Only UTC zone id is supported")
    }
    // Date and Timestamp work too
    if (expr.right.dataType == StringType) {
      try {
        val rightLit = extractStringLit(expr.right)
        if (rightLit.isDefined) {
          strfFormat = DateUtils.toStrf(rightLit.get)
        } else {
          willNotWorkOnGpu("format has to be a string literal")
        }
      } catch {
        case x: TimestampFormatConversionException =>
          willNotWorkOnGpu(s"Failed to convert ${x.reason} ${x.getMessage()}")
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

  def downScaleFactor = 1000000 // MICROS IN SECOND

  def strfFormat: String

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the unixtimestamp to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the unixtimestamp to work")
  }
  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      lhs.getBase.asTimestampMicroseconds(strfFormat)
    } else { // Timestamp or DateType
      lhs.getBase.asTimestampMicroseconds()
    }
    withResource(tmp) { r =>
      // The type we are returning is a long not an actual timestamp
      withResource(Scalar.fromInt(downScaleFactor)) { downScaleFactor =>
        withResource(tmp.asLongs()) { longMicroSecs =>
          GpuColumnVector.from(longMicroSecs.div(downScaleFactor))
        }
      }
    }
  }
}

/**
 * An improved version of GpuToTimestamp conversion which converts time to UNIX timestamp without
 * first converting to microseconds
 */
abstract class GpuToTimestampImproved extends GpuToTimestamp {
  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    val tmp = if (lhs.dataType == StringType) {
      // rhs is ignored we already parsed the format
      lhs.getBase.asTimestampSeconds(strfFormat)
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
      GpuColumnVector.from(r.asLongs())
    }
  }
}

case class GpuUnixTimestamp(strTs: Expression,
   format: Expression,
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
   strf: String,
   timeZoneId: Option[String] = None) extends GpuToTimestampImproved {
  override def strfFormat = strf
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def left: Expression = strTs
  override def right: Expression = format

}

case class GpuFromUnixTime(
    sec: Expression,
    format: Expression,
    strfFormat: String,
    timeZoneId: Option[String] = None)
  extends GpuBinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the from_unixtime to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for " +
      "the from_unixtime to work")
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    // we aren't using rhs as it was already converted in the GpuOverrides while creating the
    // expressions map and passed down here as strfFormat
    var tsVector: ColumnVector = null
    try {
      tsVector = lhs.getBase.asTimestampSeconds
      val strVector = tsVector.asStrings(strfFormat)
      GpuColumnVector.from(strVector)
    } finally {
      if (tsVector != null) {
        tsVector.close()
      }
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    withResource(lhs.getBase.castTo(DType.INT32)) { daysSinceEpoch =>
      withResource(daysSinceEpoch.binaryOp(binaryOp, rhs.getBase, daysSinceEpoch.getType)) {
        daysAsInts => GpuColumnVector.from(daysAsInts.castTo(DType.TIMESTAMP_DAYS))
      }
    }
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    withResource(GpuScalar.castDateScalarToInt(lhs)) { daysAsInts =>
     withResource(daysAsInts.binaryOp(binaryOp, rhs.getBase, daysAsInts.getType)) { ints =>
       GpuColumnVector.from(ints.castTo(DType.TIMESTAMP_DAYS))
     }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    withResource(lhs.getBase.castTo(DType.INT32)) { daysSinceEpoch =>
      withResource(daysSinceEpoch.binaryOp(binaryOp, rhs, daysSinceEpoch.getType)) { daysAsInts =>
        GpuColumnVector.from(daysAsInts.castTo(DType.TIMESTAMP_DAYS))
      }
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

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.lastDayOfMonth())
}
