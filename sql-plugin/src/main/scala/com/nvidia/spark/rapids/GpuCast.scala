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

package com.nvidia.spark.rapids

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Calendar, TimeZone}

import ai.rapids.cudf.{ColumnVector, DType, Scalar}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
    if (!GpuCast.canCast(fromType, toType)) {
      willNotWorkOnGpu(s"$castExpr from $fromType " +
        s"to $toType is not currently supported on the GPU")
    }
    if (!conf.isCastFloatToStringEnabled && toType == DataTypes.StringType &&
      (fromType == DataTypes.FloatType || fromType == DataTypes.DoubleType)) {
      willNotWorkOnGpu("the GPU will use different precision than Java's toString method when " +
        "converting floating point data types to strings and this can produce results that " +
        "differ from the default behavior in Spark.  To enable this operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_FLOAT_TO_STRING} to true.")
    }
    if (!conf.isCastStringToFloatEnabled && cast.child.dataType == DataTypes.StringType &&
      Seq(DataTypes.FloatType, DataTypes.DoubleType).contains(cast.dataType)) {
      willNotWorkOnGpu("Currently hex values aren't supported on the GPU. Also note " +
        "that casting from string to float types on the GPU returns incorrect results when the " +
        "string represents any number \"1.7976931348623158E308\" <= x < " +
        "\"1.7976931348623159E308\" and \"-1.7976931348623159E308\" < x <= " +
        "\"-1.7976931348623158E308\" in both these cases the GPU returns Double.MaxValue while " +
        "CPU returns \"+Infinity\" and \"-Infinity\" respectively. To enable this operation on " +
        "the GPU, set" + s" ${RapidsConf.ENABLE_CAST_STRING_TO_FLOAT} to true.")
    }
    if (!conf.isCastStringToIntegerEnabled && cast.child.dataType == DataTypes.StringType &&
    Seq(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType)
      .contains(cast.dataType)) {
      willNotWorkOnGpu("the GPU will return incorrect results for strings representing" +
        "values greater than Long.MaxValue or less than Long.MinValue.  To enable this " +
        "operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_STRING_TO_INTEGER} to true.")
    }
    if (!conf.isCastStringToTimestampEnabled && fromType == DataTypes.StringType
      && toType == DataTypes.TimestampType) {
      willNotWorkOnGpu("the GPU only supports a subset of formats " +
        "when casting strings to timestamps. Refer to the CAST documentation " +
        "for more details. To enable this operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP} to true.")
    }
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuCast(child, toType, ansiEnabled, cast.timeZoneId)
}

object GpuCast {

  private val DATE_REGEX_YYYY = "\\A\\d{4}\\Z"
  private val DATE_REGEX_YYYY_MM = "\\A\\d{4}\\-\\d{2}\\Z"
  private val DATE_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{2}\\-\\d{2}([ T](:?[\\r\\n]|.)*)?\\Z"

  private val TIMESTAMP_REGEX_YYYY = "\\A\\d{4}\\Z"
  private val TIMESTAMP_REGEX_YYYY_MM = "\\A\\d{4}\\-\\d{2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{2}\\-\\d{2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_FULL =
    "\\A\\d{4}\\-\\d{2}\\-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z\\Z"
  private val TIMESTAMP_REGEX_NO_DATE = "\\A[T]?(\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z)\\Z"

  private val ONE_DAY_MICROSECONDS = 86400000000L

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

  /**
   * Regex to match timestamps with or without trailing zeros.
   */
  private val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
    "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
    "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?$"

  val INVALID_INPUT_MESSAGE = "Column contains at least one value that is not in the " +
    "required range"

  val INVALID_FLOAT_CAST_MSG = "At least one value is either null or is an invalid number"

  /**
   * Returns true iff we can cast `from` to `to` using the GPU.
   */
  def canCast(from: DataType, to: DataType): Boolean = {
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
        case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
        case DateType => true
        case TimestampType => true
        case _ => false
      }
      case _ => false
    }
  }
}

/**
 * Casts using the GPU
 */
case class GpuCast(
    child: Expression,
    dataType: DataType,
    ansiMode: Boolean = false,
    timeZoneId: Option[String] = None)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  import GpuCast._

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

  /**
   * Under certain conditions during hash partitioning, Spark will attempt to replace casts
   * with semantically equivalent expressions. This method is overridden to prevent Spark
   * from substituting non-GPU expressions.
   */
  override def semanticEquals(other: Expression): Boolean = other match {
    case g: GpuExpression =>
      if (this == g) {
        true
      } else {
        super.semanticEquals(g)
      }
    case _ => false
  }

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
        castTimestampToString(input)

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
                withResource(inputWithoutNanAndInfinity.mul(microsPerSec, DType.INT64)) {
                  inputTimesMicrosCv =>
                    GpuColumnVector.from(inputTimesMicrosCv.castTo(DType.TIMESTAMP_MICROSECONDS))
                }
            }
          }
        }
      case (BooleanType, TimestampType) =>
        // cudf requires casting to a long first.
        withResource(input.getBase.castTo(DType.INT64)) { longs =>
          GpuColumnVector.from(longs.castTo(cudfType))
        }
      case (BooleanType | ByteType | ShortType | IntegerType, TimestampType) =>
        // cudf requires casting to a long first
        withResource(input.getBase.castTo(DType.INT64)) { longs =>
          withResource(longs.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
            GpuColumnVector.from(timestampSecs.castTo(cudfType))
          }
        }
      case (_: NumericType, TimestampType) =>
        // Spark casting to timestamp assumes value is in seconds, but timestamps
        // are tracked in microseconds.
        withResource(input.getBase.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
          GpuColumnVector.from(timestampSecs.castTo(cudfType))
        }
      case (FloatType, LongType) | (DoubleType, IntegerType | LongType) =>
        // Float.NaN => Int is casted to a zero but float.NaN => Long returns a small negative
        // number Double.NaN => Int | Long, returns a small negative number so Nans have to be
        // converted to zero first
        withResource(FloatUtils.nanToZero(input.getBase)) { inputWithNansToZero =>
          GpuColumnVector.from(inputWithNansToZero.castTo(cudfType))
        }
      case (FloatType|DoubleType, StringType) =>
        castFloatingTypeToString(input)
      case (StringType, BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType
                        | DoubleType | DateType | TimestampType) =>
        withResource(input.getBase.strip()) { trimmed =>
          dataType match {
            case BooleanType =>
              castStringToBool(trimmed, ansiMode)
            case DateType =>
              castStringToDate(trimmed)
            case TimestampType =>
              castStringToTimestamp(trimmed)
            case FloatType | DoubleType =>
              castStringToFloats(trimmed, ansiMode, cudfType)
            case ByteType | ShortType | IntegerType | LongType =>
              // filter out values that are not valid longs or nulls
              val regex = if (ansiMode) {
                GpuCast.ANSI_CASTABLE_TO_INT_REGEX
              } else {
                GpuCast.CASTABLE_TO_INT_REGEX
              }
              val longStrings = withResource(trimmed.matchesRe(regex)) { regexMatches =>
                if (ansiMode) {
                  withResource(regexMatches.all()) { allRegexMatches =>
                    if (!allRegexMatches.getBoolean) {
                      throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
                    }
                  }
                }
                withResource(Scalar.fromNull(DType.STRING)) { nullString =>
                  regexMatches.ifElse(trimmed, nullString)
                }
              }
              // cast to specific integral type after filtering out values that are not in range
              // for that type. Note that the scalar values here are named parameters so are not
              // created until they are needed
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

  private def castTimestampToString(input: GpuColumnVector) = {
    GpuColumnVector.from(
      withResource(input.getBase.asStrings("%Y-%m-%d %H:%M:%S.%3f")) { cv =>
        cv.stringReplaceWithBackrefs(GpuCast.TIMESTAMP_TRUNCATE_REGEX,"\\1\\2\\3")
      })
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

  private def castStringToBool(input: ColumnVector, ansiEnabled: Boolean): GpuColumnVector = {
    val trueStrings = Seq("t", "true", "y", "yes", "1")
    val falseStrings = Seq("f", "false", "n", "no", "0")
    val boolStrings = trueStrings ++ falseStrings

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

  def castStringToFloats(
      input: ColumnVector,
      ansiEnabled: Boolean, dType: DType): GpuColumnVector = {

    // TODO: since cudf doesn't support case-insensitive regex, we have to generate all
    //  possible strings. But these should cover most of the cases
    val POS_INF_REGEX = "^[+]?(?:infinity|inf|Infinity|Inf|INF|INFINITY)$"
    val NEG_INF_REGEX = "^[\\-](?:infinity|inf|Infinity|Inf|INF|INFINITY)$"
    val NAN_REGEX = "^(?:nan|NaN|NAN)$"


//      1. convert the different infinities to "Inf"/"-Inf" which is the only variation cudf
//         understands
//      2. identify the nans
//      3. identify the floats. "nan", "null" and letters are not considered floats
//      4. if ansi is enabled we want to throw and exception if the string is neither float nor nan
//      5. convert everything thats not floats to null
//      6. set the indices where we originally had nans to Float.NaN
//
//      NOTE Limitation: "1.7976931348623159E308" and "-1.7976931348623159E308" are not considered
//      Inf even though spark does

    if (ansiEnabled && input.hasNulls()) {
      throw new NumberFormatException(GpuCast.INVALID_FLOAT_CAST_MSG)
    }
    // First replace different spellings/cases of infinity with Inf and -Infinity with -Inf
    val posInfReplaced = withResource(input.matchesRe(POS_INF_REGEX)) { containsInf =>
      withResource(Scalar.fromString("Inf")) { inf =>
        containsInf.ifElse(inf, input)
      }
    }
    val withPosNegInfinityReplaced = withResource(posInfReplaced) { withPositiveInfinityReplaced =>
      withResource(withPositiveInfinityReplaced.matchesRe(NEG_INF_REGEX)) { containsNegInf =>
        withResource(Scalar.fromString("-Inf")) { negInf =>
          containsNegInf.ifElse(negInf, withPositiveInfinityReplaced)
        }
      }
    }
    withResource(withPosNegInfinityReplaced) { withPosNegInfinityReplaced =>
      //Now identify the different variations of nans
      withResource(withPosNegInfinityReplaced.matchesRe(NAN_REGEX)) { isNan =>
        // now check if the values are floats
        withResource(withPosNegInfinityReplaced.isFloat()) { isFloat =>
          if (ansiEnabled) {
            withResource(isNan.not()) { notNan =>
              withResource(isFloat.not()) { notFloat =>
                withResource(notFloat.and(notNan)) { notFloatAndNotNan =>
                  withResource(notFloatAndNotNan.any()) { notNanAndNotFloat =>
                    if (notNanAndNotFloat.getBoolean()) {
                      throw new NumberFormatException(GpuCast.INVALID_FLOAT_CAST_MSG)
                    }
                  }
                }
              }
            }
          }
          withResource(withPosNegInfinityReplaced.castTo(dType)) { casted =>
            withResource(Scalar.fromNull(dType)) { nulls =>
              withResource(isFloat.ifElse(casted, nulls)) { floatsOnly =>
                withResource(FloatUtils.getNanScalar(dType)) { nan =>
                  GpuColumnVector.from(isNan.ifElse(nan, floatsOnly))
                }
              }
            }
          }
        }
      }
    }
  }

  private def castStringToDate(input: ColumnVector): GpuColumnVector = {

    /**
     * Replace special date strings such as "now" with timestampDays. This method does not
     * close the `input` ColumnVector.
     */
    def specialDateOr(
        input: ColumnVector,
        special: String,
        value: Int,
        orColumnVector: ColumnVector): ColumnVector = {

      withResource(orColumnVector) { other =>
        withResource(Scalar.fromString(special)) { str =>
          withResource(input.equalTo(str)) { isStr =>
            withResource(Scalar.timestampDaysFromInt(value)) { date =>
              isStr.ifElse(date, other)
            }
          }
        }
      }
    }

    /**
     * Parse dates that match the provided regex. This method does not close the `input`
     * ColumnVector.
     */
    def convertDateOrNull(
        input: ColumnVector,
        regex: String,
        cudfFormat: String): ColumnVector = {

      withResource(Scalar.fromNull(DType.TIMESTAMP_DAYS)) { nullScalar =>
        withResource(input.matchesRe(regex)) { isMatch =>
          withResource(input.asTimestampDays(cudfFormat)) { asDays =>
            isMatch.ifElse(asDays, nullScalar)
          }
        }
      }
    }

    /** This method does not close the `input` ColumnVector. */
    def convertDateOr(
        input: ColumnVector,
        regex: String,
        cudfFormat: String,
        orElse: ColumnVector): ColumnVector = {

      withResource(input.matchesRe(regex)) { isMatch =>
        withResource(input.asTimestampDays(cudfFormat)) { asDays =>
          withResource(orElse) { orElse =>
            isMatch.ifElse(asDays, orElse)
          }
        }
      }
    }

    // special dates
    val now = DateTimeUtils.currentDate(ZoneId.of("UTC"))
    val specialDates: Map[String, Int] = Map(
      "epoch" -> 0,
      "now" -> now,
      "today" -> now,
      "yesterday" -> (now - 1),
      "tomorrow" -> (now + 1)
    )

    var sanitizedInput = input.incRefCount()

    // replace partial months
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])-", "-0\\1-")
    }

    // replace partial month or day at end of string
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])([ T](:?[\\r\\n]|.)*)?\\Z", "-0\\1")
    }

    val result = withResource(sanitizedInput) { sanitizedInput =>

      // convert dates that are in valid formats yyyy, yyyy-mm, yyyy-mm-dd
      val converted = convertDateOr(sanitizedInput, DATE_REGEX_YYYY_MM_DD, "%Y-%m-%d",
        convertDateOr(sanitizedInput, DATE_REGEX_YYYY_MM, "%Y-%m",
          convertDateOrNull(sanitizedInput, DATE_REGEX_YYYY, "%Y")))

      // handle special dates like "epoch", "now", etc.
      specialDates.foldLeft(converted)((prev, specialDate) =>
        specialDateOr(sanitizedInput, specialDate._1, specialDate._2, prev))
    }

    GpuColumnVector.from(result)
  }

  private def castStringToTimestamp(input: ColumnVector): GpuColumnVector = {

    /**
     * Replace special date strings such as "now" with timestampMicros. This method does not
     * close the `input` ColumnVector.
     */
    def specialTimestampOr(
        input: ColumnVector,
        special: String,
        value: Long,
        orColumnVector: ColumnVector): ColumnVector = {

      withResource(orColumnVector) { other =>
        withResource(Scalar.fromString(special)) { str =>
          withResource(input.equalTo(str)) { isStr =>
            withResource(Scalar.timestampFromLong(DType.TIMESTAMP_MICROSECONDS, value)) { date =>
              isStr.ifElse(date, other)
            }
          }
        }
      }
    }

    /**
     * Parse dates that match the the provided regex. This method does not close the `input`
     * ColumnVector.
     */
    def convertTimestampOrNull(
        input: ColumnVector,
        regex: String,
        cudfFormat: String): ColumnVector = {

      withResource(Scalar.fromNull(DType.TIMESTAMP_MICROSECONDS)) { nullScalar =>
        withResource(input.matchesRe(regex)) { isMatch =>
          withResource(input.asTimestampMicroseconds(cudfFormat)) { asDays =>
            isMatch.ifElse(asDays, nullScalar)
          }
        }
      }
    }

    /** This method does not close the `input` ColumnVector. */
    def convertTimestampOr(
        input: ColumnVector,
        regex: String,
        cudfFormat: String,
        orElse: ColumnVector): ColumnVector = {

      withResource(input.matchesRe(regex)) { isMatch =>
        withResource(input.asTimestampMicroseconds(cudfFormat)) { asDays =>
          withResource(orElse) { orElse =>
            isMatch.ifElse(asDays, orElse)
          }
        }
      }
    }

    // special timestamps
    val cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")))
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    val today: Long = cal.getTimeInMillis * 1000
    val todayStr = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val specialDates: Map[String, Long] = Map(
      "epoch" -> 0,
      "now" -> today,
      "today" -> today,
      "yesterday" -> (today - ONE_DAY_MICROSECONDS),
      "tomorrow" -> (today + ONE_DAY_MICROSECONDS)
    )

    var sanitizedInput = input.incRefCount()

    // replace partial months
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])-", "-0\\1-")
    }

    // replace partial month or day at end of string
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])[ ]?\\Z", "-0\\1")
    }

    // replace partial day in timestamp formats without dates
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs(
        "-([0-9])([ T]\\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{6}Z)\\Z", "-0\\1\\2")
    }

    // prepend today's date to timestamp formats without dates
    sanitizedInput = withResource(sanitizedInput) { cv =>
      sanitizedInput.stringReplaceWithBackrefs(TIMESTAMP_REGEX_NO_DATE, s"${todayStr}T\\1")
    }

    val result = withResource(sanitizedInput) { sanitizedInput =>

      // convert dates that are in valid timestamp formats
      val converted =
        convertTimestampOr(sanitizedInput, TIMESTAMP_REGEX_FULL, "%Y-%m-%dT%H:%M:%SZ%f",
          convertTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM_DD, "%Y-%m-%d",
            convertTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM, "%Y-%m",
              convertTimestampOrNull(sanitizedInput, TIMESTAMP_REGEX_YYYY, "%Y"))))

      // handle special dates like "epoch", "now", etc.
      specialDates.foldLeft(converted)((prev, specialDate) =>
        specialTimestampOr(sanitizedInput, specialDate._1, specialDate._2, prev))
    }

    GpuColumnVector.from(result)
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
