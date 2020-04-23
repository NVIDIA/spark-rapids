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

package ai.rapids.spark

import ai.rapids.cudf.{BinaryOp, DType, Scalar}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

object GpuCast {
  /**
   * Returns true iff we can cast `from` to `to` using the GPU.
   *
   * Eventually we will need to match what is supported by spark proper
   * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala#L37-L95
   */
  def canCast(from: DataType, to: DataType): Boolean =
    (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (ByteType|ShortType|IntegerType|LongType, _: StringType) => true
    case (FloatType|DoubleType, _: StringType) => true

    case (BooleanType, _: NumericType) => true

    case (_: NumericType, BooleanType) => true
    case (_: NumericType, _: NumericType) => true
    case (_: NumericType, TimestampType) => true

    case (DateType, BooleanType) => true
    case (DateType, _: NumericType) => true
    case (DateType, TimestampType) => true

    case (TimestampType, BooleanType) => true
    case (TimestampType, _: NumericType) => true
    case (TimestampType, DateType) => true

    case _ => false
  }
}

/**
 * Casts using the GPU
 */
case class GpuCast(child: GpuExpression, dataType: DataType, timeZoneId: Option[String] = None)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  override def toString: String = s"cast($child as ${dataType.simpleString})"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (Cast.canCast(child.dataType, dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"cannot cast ${child.dataType.catalogString} to ${dataType.catalogString}")
    }
  }

  override def nullable: Boolean = Cast.forceNullable(child.dataType, dataType) || child.nullable

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  // When this cast involves TimeZone, it's only resolved if the timeZoneId is set;
  // Otherwise behave like Expression.resolved.
  override lazy val resolved: Boolean =
    childrenResolved && checkInputDataTypes().isSuccess && (!needsTimeZone || timeZoneId.isDefined)

  private[this] def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

  override def sql: String = dataType match {
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL, this
    // type of casting can only be introduced by the analyzer, and can be omitted when converting
    // back to SQL query string.
    case _: ArrayType | _: MapType | _: StructType => child.sql
    case _ => s"CAST(${child.sql} AS ${dataType.sql})"
  }

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val cudfType = GpuColumnVector.getRapidsType(dataType)

    (input.dataType(), dataType) match {
      case (DateType, BooleanType | _: NumericType) =>
        // casts from date type to numerics are always null
        val scalar = GpuScalar.from(null, dataType)
        try {
          GpuColumnVector.from(scalar, input.getBase.getRowCount.toInt)
        } finally {
          scalar.close()
        }
      case (TimestampType, FloatType | DoubleType) =>
        val asLongs = input.getBase.castTo(DType.INT64)
        try {
          val microsPerSec = Scalar.fromDouble(1000000)
          try {
            // Use trueDiv to ensure cast to double before division for full precision
            GpuColumnVector.from(asLongs.trueDiv(microsPerSec, cudfType))
          } finally {
            microsPerSec.close()
          }
        } finally {
          asLongs.close()
        }
      case (TimestampType, ByteType | ShortType | IntegerType) =>
        // normally we would just do a floordiv here, but cudf downcasts the operands to
        // the output type before the divide.  https://github.com/rapidsai/cudf/issues/2574
        val asLongs = input.getBase.castTo(DType.INT64)
        try {
          val microsPerSec = Scalar.fromInt(1000000)
          try {
            val cv = asLongs.floorDiv(microsPerSec, DType.INT64)
            try {
              GpuColumnVector.from(cv.castTo(cudfType))
            } finally {
              cv.close()
            }
          } finally {
            microsPerSec.close()
          }
        } finally {
          asLongs.close()
        }
      case (TimestampType, _: NumericType) =>
        val asLongs = input.getBase.castTo(DType.INT64)
        try {
          val microsPerSec = Scalar.fromInt(1000000)
          try {
            GpuColumnVector.from(asLongs.floorDiv(microsPerSec, cudfType))
          } finally {
            microsPerSec.close()
          }
        } finally {
          asLongs.close()
        }
      case (FloatType | DoubleType, TimestampType) =>
        // Spark casting to timestamp from double assumes value is in microseconds
        withResource(Scalar.fromInt(1000000)) { microsPerSec =>
          withResource(FloatUtils.nansToNulls(input.getBase)) { inputWithNansToNull =>
            withResource(FloatUtils.infinityToNulls(inputWithNansToNull)) { inputWithoutNanAndInfinity =>
              withResource(inputWithoutNanAndInfinity.mul(microsPerSec)) { inputTimesMicrosCv =>
                GpuColumnVector.from(inputTimesMicrosCv.castTo(DType.TIMESTAMP_MICROSECONDS))
              }
            }
          }
        }
      case (_: NumericType, TimestampType) =>
        // Spark casting to timestamp assumes value is in seconds, but timestamps
        // are tracked in microseconds.
        val timestampSecs = input.getBase.castTo(DType.TIMESTAMP_SECONDS)
        try {
          GpuColumnVector.from(timestampSecs.castTo(cudfType))
        } finally {
          timestampSecs.close();
        }
        // Float.NaN => Int is casted to a zero but float.NaN => Long returns a small negative number
        // Double.NaN => Int | Long, returns a small negative number so Nans have to be converted to zero first
      case (FloatType, LongType) | (DoubleType, IntegerType | LongType) =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          GpuColumnVector.from(inputWithNansToZero.castTo(cudfType))
        }
      case (FloatType|DoubleType, StringType) =>
        castFloatingTypeToString(input)
      case _ =>
        GpuColumnVector.from(input.getBase.castTo(cudfType))
    }
  }

  private def castFloatingTypeToString(input: GpuColumnVector): GpuColumnVector = {
    withResource(input.getBase.castTo(DType.STRING)) { cudfCast =>

      // replace "e+" with "E"
      val replaceExponent = withResource(Scalar.fromString("e+")) { cudfExponent =>
        withResource(Scalar.fromString("E")) { sparkExponent =>
          cudfCast.stringReplace(cudfExponent, sparkExponent)
        }
      }

      // replace "Inf" with "Infinity"
      withResource(replaceExponent) { replaceExponent =>
        withResource(Scalar.fromString("Inf")) { cudfInf =>
          withResource(Scalar.fromString("Infinity")) { sparkInfinity =>
            GpuColumnVector.from(replaceExponent.stringReplace(cudfInf, sparkInfinity))
          }
        }
      }
    }
  }
}
