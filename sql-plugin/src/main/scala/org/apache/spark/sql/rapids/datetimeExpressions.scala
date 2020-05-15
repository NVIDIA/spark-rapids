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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import ai.rapids.spark.{GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuUnaryExpression}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression, ImplicitCastInputTypes, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

trait GpuDateTimeUnaryExpression extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride = DType.INT32
}

case class GpuYear(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.year())
}

case class GpuTimeSub(start: GpuExpression, interval: GpuExpression, timeZoneId: Option[String] = None)
  extends BinaryExpression with GpuExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  def this(start: GpuExpression, interval: GpuExpression) = this(start, interval, None)

  override def left: GpuExpression = start
  override def right: GpuExpression = interval

  override def toString: String = s"$left - $right"
  override def sql: String = s"${left.sql} - ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Option(timeZoneId))

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
          val usToSub = intvl.days * 24 * 60 * 60 * 1000 * 1000L + intvl.microseconds
          if (usToSub > 0) {
            withResource(Scalar.fromLong(usToSub)) { us_s =>
              withResource(l.getBase.castTo(DType.INT64)) { us =>
                withResource(us.sub(us_s)) {longResult =>
                  GpuColumnVector.from(longResult.castTo(DType.TIMESTAMP_MICROSECONDS))
                }
              }
            }
          }
        case _ => throw new UnsupportedOperationException("GpuTimeSub takes column and interval as an argument only")
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
    val intScalar = Scalar.fromInt(lhs.getInt)
    try {
      val intVector = rhs.getBase.asInts()
      try {
        GpuColumnVector.from(intScalar.sub(intVector))
      } finally {
        intVector.close()
      }
    } finally {
      intScalar.close()
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    // if one of the operands is a scalar, they have to be explicitly casted by the caller
    // before the operation can be run. This is an issue being tracked by
    // https://github.com/rapidsai/cudf/issues/4180
    val intScalar = Scalar.fromInt(rhs.getInt)
    try {
      val intVector = lhs.getBase.asInts()
      try {
        GpuColumnVector.from(intVector.sub(intScalar))
      } finally {
        intVector.close()
      }
    } finally {
      intScalar.close()
    }
  }
}

case class GpuMonth(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.month())
}

case class GpuDayOfMonth(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.day())
}

case class GpuUnixTimestamp(strTs: GpuExpression, format: GpuExpression, strfFormat: String, timeZoneId: Option[String] = None)
  extends GpuBinaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the unixtimestamp to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for the unixtimestamp to work")
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Option(timeZoneId))
  override def left: GpuExpression = strTs
  override def right: GpuExpression = format
  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

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

case class GpuFromUnixTime(sec: GpuExpression, format: GpuExpression, strfFormat: String, timeZoneId: Option[String] = None)
  extends GpuBinaryExpression with TimeZoneAwareExpression with ImplicitCastInputTypes {
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("rhs has to be a scalar for the from_unixtime to work")
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    throw new IllegalArgumentException("lhs has to be a vector and rhs has to be a scalar for the from_unixtime to work")
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

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = copy(timeZoneId = Option(timeZoneId))

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  override def left: GpuExpression = sec

  // we aren't using this "right" GpuExpression, as it was already converted in the GpuOverrides while creating the
  // expressions map and passed down here as strfFormat
  override def right: GpuExpression = format

  override def dataType: DataType = StringType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}