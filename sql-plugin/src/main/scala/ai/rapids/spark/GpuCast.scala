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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
class CastExprMeta[INPUT <: CastBase](
    cast: INPUT,
    ansiEnabled: Boolean,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends UnaryExprMeta[INPUT](cast, conf, parent, rule) {

  private val castExpr = if (ansiEnabled) "ansi_cast" else "cast"
  private val fromType = cast.child.dataType
  private val toType = cast.dataType

  override def tagExprForGpu(): Unit = {
    if (!GpuCast.canCast(fromType, toType, ansiEnabled)) {
      willNotWorkOnGpu(s"$castExpr from $fromType " +
        s"to $toType is not currently supported on the GPU")
    }
    if (!conf.isCastToFloatEnabled && toType == DataTypes.StringType &&
      (fromType == DataTypes.FloatType || fromType == DataTypes.DoubleType)) {
      willNotWorkOnGpu("the GPU will use different precision than Java's toString method when " +
        "converting floating point data types to strings and this can produce results that " +
        "differ from the default behavior in Spark.  To enable this operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_FLOAT_TO_STRING} to true.")
    }
    if (!conf.isCastTimestampToStringEnabled && fromType == DataTypes.TimestampType &&
      toType == DataTypes.StringType) {
      willNotWorkOnGpu("the GPU will add trailing zeros for the millisecond portion of the " +
        "timestamp, which differs from the default behavior in Spark. To enable this operation " +
        s"on the GPU, set ${RapidsConf.ENABLE_CAST_TIMESTAMP_TO_STRING} to true.")
    }

    if (!conf.isCastStringToIntegerEnabled && cast.child.dataType == DataTypes.StringType &&
    Seq(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType)
      .contains(cast.dataType)) {
      willNotWorkOnGpu("the GPU will return incorrect results for strings representing" +
        "values greater than Long.MaxValue or less than Long.MinValue.  To enable this " +
        "operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_STRING_TO_INTEGER} to true.")
    }
  }

  override def convertToGpu(child: GpuExpression): GpuExpression =
    GpuCast(child, toType, ansiEnabled, cast.timeZoneId)
}

object GpuCast {

  /**
   * Regex for identifying strings that contain numeric values that can be casted to integral
   * types. This includes floating point numbers but not numbers containing exponents.
   */
  private val CASTABLE_TO_INT_REGEX = "\\s*[+\\-]?[0-9]*(\\.)?[0-9]+\\s*$"

  /**
   * Regex for identifying strings that contain numeric values that can be casted to integral
   * types when ansi is enabled.
   */
  private val ANSI_CASTABLE_TO_INT_REGEX = "\\s*[+\\-]?[0-9]+\\s*$"

  private val INVALID_INPUT_MESSAGE = "Column contains at least one value that is not in the " +
    "required range"
  /**
   * Returns true iff we can cast `from` to `to` using the GPU.
   *
   * Eventually we will need to match what is supported by spark proper
   * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala#L37-L95
   */
  def canCast(from: DataType, to: DataType, ansiEnabled: Boolean = false): Boolean = {
    if (from == to) {
      return true
    }
    from match {
      case BooleanType => to match {
        case ByteType | ShortType | IntegerType | LongType => true
        case FloatType | DoubleType => true
        case TimestampType => true
        case StringType => true
        case _ => false
      }
      case ByteType | ShortType | IntegerType | LongType => to match {
        case BooleanType => true
        case ByteType | ShortType | IntegerType | LongType => true
        case FloatType | DoubleType => true
        case StringType => true
        case TimestampType => true
        case _ => false
      }
      case FloatType | DoubleType => to match {
        case BooleanType => true
        case ByteType | ShortType | IntegerType | LongType => true
        case FloatType | DoubleType => true
        case TimestampType => true
        case StringType => true
        case _ => false
      }
      case DateType => to match {
        case BooleanType => true
        case ByteType | ShortType | IntegerType | LongType => true
        case FloatType | DoubleType => true
        case TimestampType => true
        case StringType => true
        case _ => false
      }
      case TimestampType => to match {
        case BooleanType => true
        case ByteType | ShortType | IntegerType => true
        case LongType => true
        case FloatType | DoubleType => true
        case DateType => true
        case StringType => true
        case _ => false
      }
      case StringType => to match {
        case BooleanType => true
        case ByteType | ShortType | IntegerType | LongType => true
        case _ => false
      }
    }
  }
}

/**
 * Casts using the GPU
 */
case class GpuCast(
    child: GpuExpression,
    dataType: DataType,
    ansiMode: Boolean = false,
    timeZoneId: Option[String] = None)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  override def toString: String = if (ansiMode) {
    s"ansi_cast($child as ${dataType.simpleString})"
  } else {
    s"cast($child as ${dataType.simpleString})"
  }

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
    // HiveQL doesn't allow casting to complex types. For logical plans translated from HiveQL,
    // this type of casting can only be introduced by the analyzer, and can be omitted when
    // converting back to SQL query string.
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
      case (DateType, StringType) =>
        GpuColumnVector.from(input.getBase.asStrings("%Y-%m-%d"))
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
              if (ansiMode) {
                dataType match {
                  case IntegerType =>
                    assertValuesInRange(cv, Scalar.fromInt(Int.MinValue),
                      Scalar.fromInt(Int.MaxValue))
                  case ShortType =>
                    assertValuesInRange(cv, Scalar.fromShort(Short.MinValue),
                      Scalar.fromShort(Short.MaxValue))
                  case ByteType =>
                    assertValuesInRange(cv, Scalar.fromByte(Byte.MinValue),
                      Scalar.fromByte(Byte.MaxValue))
                }
              }
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
      case (TimestampType, _: LongType) =>
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
      case (TimestampType, StringType) =>
        GpuColumnVector.from(input.getBase.asStrings("%Y-%m-%d %H:%M:%S.%3f"))

      // ansi cast from larger-than-integer integral types, to integer
      case (LongType, IntegerType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from larger-than-short integral types, to short
      case (LongType|IntegerType, ShortType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from larger-than-byte integral types, to byte
      case (LongType|IntegerType|ShortType, ByteType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from floating-point types, to byte
      case (FloatType|DoubleType, ByteType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from floating-point types, to short
      case (FloatType|DoubleType, ShortType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from floating-point types, to integer
      case (FloatType|DoubleType, IntegerType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      // ansi cast from floating-point types, to long
      case (FloatType|DoubleType, LongType) if ansiMode =>
        assertValuesInRange(input.getBase, Scalar.fromLong(Long.MinValue),
          Scalar.fromLong(Long.MaxValue))
        GpuColumnVector.from(input.getBase.castTo(cudfType))

      case (FloatType | DoubleType, TimestampType) =>
        // Spark casting to timestamp from double assumes value is in microseconds
        withResource(Scalar.fromInt(1000000)) { microsPerSec =>
          withResource(input.getBase.nansToNulls()) { inputWithNansToNull =>
            withResource(FloatUtils.infinityToNulls(inputWithNansToNull)) {
              inputWithoutNanAndInfinity =>
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
        // Float.NaN => Int is casted to a zero but float.NaN => Long returns a small negative
        // number Double.NaN => Int | Long, returns a small negative number so Nans have to be
        // converted to zero first
      case (FloatType, LongType) | (DoubleType, IntegerType | LongType) =>
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          GpuColumnVector.from(inputWithNansToZero.castTo(cudfType))
        }
      case (FloatType|DoubleType, StringType) =>
        castFloatingTypeToString(input)
      case (StringType, BooleanType) =>
        castStringToBool(input, ansiMode)
      case (StringType, ByteType|ShortType|IntegerType|LongType) =>
        // filter out values that are not valid longs or nulls
        val regex = if (ansiMode) {
          GpuCast.ANSI_CASTABLE_TO_INT_REGEX
        } else {
          GpuCast.CASTABLE_TO_INT_REGEX
        }
        val longStrings = withResource(input.getBase.strip()) { trimmed =>
          withResource(trimmed.matchesRe(regex)) { regexMatches =>
            if (ansiMode) {
              withResource(regexMatches.all()) { allRegexMatches => {
                if (!allRegexMatches.getBoolean) {
                  throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
                }
              }}
            }
            withResource(Scalar.fromNull(DType.STRING)) { nullString =>
              regexMatches.ifElse(trimmed, nullString)
            }
          }
        }
        // cast to specific integral type after filtering out values that are not in range for that
        // type note that the scalar values here are named parameters so are not created until they
        // are needed
        withResource(longStrings) { longStrings =>
          cudfType match {
            case DType.INT8 =>
              castStringToIntegralType(longStrings, DType.INT8,
                Scalar.fromInt(Byte.MinValue), Scalar.fromInt(Byte.MaxValue))
            case DType.INT16 =>
              castStringToIntegralType(longStrings, DType.INT16,
                Scalar.fromInt(Short.MinValue), Scalar.fromInt(Short.MaxValue))
            case DType.INT32 =>
              castStringToIntegralType(longStrings, DType.INT32,
                Scalar.fromInt(Int.MinValue), Scalar.fromInt(Int.MaxValue))
            case DType.INT64 =>
              GpuColumnVector.from(longStrings.castTo(DType.INT64))
            case _ =>
              throw new IllegalStateException("Invalid integral type")
          }
        }

      case _ =>
        GpuColumnVector.from(input.getBase.castTo(cudfType))
    }
  }

  /**
   * Asserts that all values in a column are within the specfied range.
   *
   * @param values ColumnVector
   * @param minValue Named parameter for function to create Scalar representing range minimum value
   * @param maxValue Named parameter for function to create Scalar representing range maximum value
   * @throws IllegalStateException if any values in the column are not within the specified range
   */
  private def assertValuesInRange(values: ColumnVector,
    minValue: => Scalar,
    maxValue: => Scalar): Unit = {

    def throwIfAny(cv: ColumnVector): Unit = {
      withResource(cv) { cv =>
        withResource(cv.any()) { isAny =>
          if (isAny.getBoolean) {
            throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
      }
    }

    withResource(minValue) { minValue =>
      throwIfAny(values.lessThan(minValue))
    }

    withResource(maxValue) { maxValue =>
      throwIfAny(values.greaterThan(maxValue))
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

  private def castStringToBool(input: GpuColumnVector, ansiEnabled: Boolean): GpuColumnVector = {
    val trueStrings = Seq("t", "true", "y", "yes", "1")
    val falseStrings = Seq("f", "false", "n", "no", "0")
    val boolStrings = trueStrings ++ falseStrings

    // remove whitespace first
    withResource(input.getBase().strip()) { input =>
      // determine which values are valid bool strings
      withResource(ColumnVector.fromStrings(boolStrings: _*)) { boolStrings =>
        withResource(input.contains(boolStrings)) { validBools =>
          // in ansi mode, fail if any values are not valid bool strings
          if (ansiEnabled) {
            withResource(validBools.all()) { isAllBool =>
              if (!isAllBool.getBoolean) {
                throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
              }
            }
          }
          // replace non-boolean values with null
          withResource(Scalar.fromNull(DType.STRING)) { nullString =>
            withResource(validBools.ifElse(input, nullString)) { sanitizedInput =>
              // return true, false, or null, as appropriate
              withResource(ColumnVector.fromStrings(trueStrings: _*)) { cvTrue =>
                GpuColumnVector.from(sanitizedInput.contains(cvTrue))
              }
            }
          }
        }
      }
    }
  }

  /**
   * Cast column of long values to a smaller integral type (bytes, short, int).
   *
   * @param longStrings Long values in string format
   * @param castToType Type to cast to
   * @param minValue Named parameter for function to create Scalar representing range minimum value
   * @param maxValue Named parameter for function to create Scalar representing range maximum value
   * @return Values cast to specified integral type
   */
  private def castStringToIntegralType(longStrings: ColumnVector,
      castToType: DType,
      minValue: => Scalar,
      maxValue: => Scalar): GpuColumnVector = {

    // evaluate min and max named parameters once since they are used in multiple places
    withResource(minValue) { minValue: Scalar =>
      withResource(maxValue) { maxValue: Scalar =>
        withResource(Scalar.fromNull(DType.INT64)) { nulls =>
          withResource(longStrings.castTo(DType.INT64)) { values =>

            // replace values less than minValue with null
            val gtEqMinOrNull = withResource(values.greaterOrEqualTo(minValue)) { isGtEqMin =>
              if (ansiMode) {
                withResource(isGtEqMin.all()) { all =>
                  if (!all.getBoolean) {
                    throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
                  }
                }
              }
              isGtEqMin.ifElse(values, nulls)
            }

            // replace values greater than maxValue with null
            val ltEqMaxOrNull = withResource(gtEqMinOrNull) { gtEqMinOrNull =>
              withResource(gtEqMinOrNull.lessOrEqualTo(maxValue)) { isLtEqMax =>
                if (ansiMode) {
                  withResource(isLtEqMax.all()) { all =>
                    if (!all.getBoolean) {
                      throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
                    }
                  }
                }
                isLtEqMax.ifElse(gtEqMinOrNull, nulls)
              }
            }

            // cast the final values
            withResource(ltEqMaxOrNull) { ltEqMaxOrNull =>
              GpuColumnVector.from(ltEqMaxOrNull.castTo(castToType))
            }
          }
        }
      }

    }
  }

}
