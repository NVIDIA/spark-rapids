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

import ai.rapids.cudf.{DType, Scalar}
import ai.rapids.spark._
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.util.TimestampFormatter
import org.apache.spark.sql.types._

trait GpuDateTimeUnaryExpression extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride = DType.INT32
}

case class GpuYear(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.year())
}

case class GpuDateDiff(endDate: Expression, startDate: Expression)
  extends GpuBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate
  override def right: Expression = startDate
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = IntegerType

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    // the result type has to be TIMESTAMP_DAYS and casted separately. This is an issue that's being tracked by
    // https://github.com/rapidsai/cudf/issues/4181
    val vector = lhs.getBase.sub(rhs.getBase, DType.TIMESTAMP_DAYS)
    try {
      GpuColumnVector.from(vector.asInts())
    } finally {
      vector.close()
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
    GpuColumnVector.from(lhs.getBase.asTimestampSeconds.asStrings(strfFormat))
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