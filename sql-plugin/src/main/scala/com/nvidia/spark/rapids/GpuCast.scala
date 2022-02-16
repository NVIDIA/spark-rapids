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

package com.nvidia.spark.rapids

import java.text.SimpleDateFormat
import java.time.DateTimeException

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar}
import ai.rapids.cudf
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.YearParseUtil

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.rapids.GpuToTimestamp.replaceSpecialDates
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
final class CastExprMeta[INPUT <: CastBase](
    cast: INPUT,
    val ansiEnabled: Boolean,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule,
    doFloatToIntCheck: Boolean,
    // stringToDate supports ANSI mode from Spark v3.2.0.  Here is the details.
    //     https://github.com/apache/spark/commit/6e862792fb
    // We do not want to create a shim class for this small change
    stringToAnsiDate: Boolean,
    toTypeOverride: Option[DataType] = None)
  extends UnaryExprMeta[INPUT](cast, conf, parent, rule) {

  def withToTypeOverride(newToType: DecimalType): CastExprMeta[INPUT] =
    new CastExprMeta[INPUT](cast, ansiEnabled, conf, parent, rule,
      doFloatToIntCheck, stringToAnsiDate, Some(newToType))

  val fromType: DataType = cast.child.dataType
  val toType: DataType = toTypeOverride.getOrElse(cast.dataType)
  val legacyCastToString: Boolean = ShimLoader.getSparkShims.getLegacyComplexTypeToString()

  override def tagExprForGpu(): Unit = recursiveTagExprForGpuCheck()

  private def recursiveTagExprForGpuCheck(
      fromDataType: DataType = fromType,
      toDataType: DataType = toType,
      depth: Int = 0): Unit = {
    val checks = rule.getChecks.get.asInstanceOf[CastChecks]
    if (depth > 0 &&
        !checks.gpuCanCast(fromDataType, toDataType)) {
      willNotWorkOnGpu(s"Casting child type $fromDataType to $toDataType is not supported")
    }

    (fromDataType, toDataType) match {
      case (FloatType | DoubleType, ByteType | ShortType | IntegerType | LongType) if
          doFloatToIntCheck && !conf.isCastFloatToIntegralTypesEnabled =>
        willNotWorkOnGpu(buildTagMessage(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES))
      case (dt: DecimalType, _: StringType) =>
        if (!conf.isCastDecimalToStringEnabled) {
          willNotWorkOnGpu("the GPU does not produce the exact same string as Spark produces, " +
              s"set ${RapidsConf.ENABLE_CAST_DECIMAL_TO_STRING} to true if semantically " +
              s"equivalent decimal strings are sufficient for your application.")
        }
        if (dt.precision > DType.DECIMAL128_MAX_PRECISION) {
          willNotWorkOnGpu(s"decimal to string with a " +
              s"precision > ${DType.DECIMAL128_MAX_PRECISION} is not supported yet")
        }
      case ( _: DecimalType, _: FloatType | _: DoubleType) if !conf.isCastDecimalToFloatEnabled =>
        willNotWorkOnGpu("the GPU will use a different strategy from Java's BigDecimal " +
            "to convert decimal data types to floating point and this can produce results that " +
            "slightly differ from the default behavior in Spark.  To enable this operation on " +
            s"the GPU, set ${RapidsConf.ENABLE_CAST_DECIMAL_TO_FLOAT} to true.")
      case (_: FloatType | _: DoubleType, _: DecimalType) if !conf.isCastFloatToDecimalEnabled =>
        willNotWorkOnGpu("the GPU will use a different strategy from Java's BigDecimal " +
            "to convert floating point data types to decimals and this can produce results that " +
            "slightly differ from the default behavior in Spark.  To enable this operation on " +
            s"the GPU, set ${RapidsConf.ENABLE_CAST_FLOAT_TO_DECIMAL} to true.")
      case (_: FloatType | _: DoubleType, _: StringType) if !conf.isCastFloatToStringEnabled =>
        willNotWorkOnGpu("the GPU will use different precision than Java's toString method when " +
            "converting floating point data types to strings and this can produce results that " +
            "differ from the default behavior in Spark.  To enable this operation on the GPU, set" +
            s" ${RapidsConf.ENABLE_CAST_FLOAT_TO_STRING} to true.")
      case (_: StringType, dt: DecimalType) if dt.precision + 1 > DecimalType.MAX_PRECISION =>
        willNotWorkOnGpu(s"Because of rounding requirements we cannot support $dt on the GPU")
      case (_: StringType, _: FloatType | _: DoubleType) if !conf.isCastStringToFloatEnabled =>
        willNotWorkOnGpu("Currently hex values aren't supported on the GPU. Also note " +
            "that casting from string to float types on the GPU returns incorrect results when " +
            "the string represents any number \"1.7976931348623158E308\" <= x < " +
            "\"1.7976931348623159E308\" and \"-1.7976931348623159E308\" < x <= " +
            "\"-1.7976931348623158E308\" in both these cases the GPU returns Double.MaxValue " +
            "while CPU returns \"+Infinity\" and \"-Infinity\" respectively. To enable this " +
            s"operation on the GPU, set ${RapidsConf.ENABLE_CAST_STRING_TO_FLOAT} to true.")
      case (_: StringType, _: TimestampType) =>
        if (!conf.isCastStringToTimestampEnabled) {
          willNotWorkOnGpu("the GPU only supports a subset of formats " +
              "when casting strings to timestamps. Refer to the CAST documentation " +
              "for more details. To enable this operation on the GPU, set" +
              s" ${RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP} to true.")
        }
        YearParseUtil.tagParseStringAsDate(conf, this)
      case (_: StringType, _: DateType) =>
        YearParseUtil.tagParseStringAsDate(conf, this)
      case (_: StringType, dt:DecimalType) =>
        if (dt.scale < 0 && !ShimLoader.getSparkShims.isCastingStringToNegDecimalScaleSupported) {
          willNotWorkOnGpu("RAPIDS doesn't support casting string to decimal for " +
              "negative scale decimal in this version of Spark because of SPARK-37451")
        }
      case (structType: StructType, StringType) =>
        structType.foreach { field =>
          recursiveTagExprForGpuCheck(field.dataType, StringType, depth + 1)
        }
      case (fromStructType: StructType, toStructType: StructType) =>
        fromStructType.zip(toStructType).foreach {
          case (fromChild, toChild) =>
            recursiveTagExprForGpuCheck(fromChild.dataType, toChild.dataType, depth + 1)
        }
      case (ArrayType(elementType, _), StringType) =>
        recursiveTagExprForGpuCheck(elementType, StringType, depth + 1)

      case (ArrayType(nestedFrom, _), ArrayType(nestedTo, _)) =>
        recursiveTagExprForGpuCheck(nestedFrom, nestedTo, depth + 1)

      case (MapType(keyFrom, valueFrom, _), MapType(keyTo, valueTo, _)) =>
        recursiveTagExprForGpuCheck(keyFrom, keyTo, depth + 1)
        recursiveTagExprForGpuCheck(valueFrom, valueTo, depth + 1)

      case (MapType(keyFrom, valueFrom, _), StringType) =>
        recursiveTagExprForGpuCheck(keyFrom, StringType, depth + 1)
        recursiveTagExprForGpuCheck(valueFrom, StringType, depth + 1)

      case _ =>
    }
  }

  def buildTagMessage(entry: ConfEntry[_]): String = {
    s"${entry.doc}. To enable this operation on the GPU, set ${entry.key} to true."
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuCast(child, toType, ansiEnabled, cast.timeZoneId, legacyCastToString,
      stringToAnsiDate)
}

object GpuCast extends Arm {

  private val DATE_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{1,2}\\-\\d{1,2}([ T](:?[\\r\\n]|.)*)?\\Z"
  private val DATE_REGEX_YYYY_MM = "\\A\\d{4}\\-\\d{1,2}\\Z"
  private val DATE_REGEX_YYYY = "\\A\\d{4}\\Z"

  private val TIMESTAMP_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{1,2}\\-\\d{1,2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_YYYY_MM = "\\A\\d{4}\\-\\d{1,2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_YYYY = "\\A\\d{4}[ ]?\\Z"
  private val TIMESTAMP_REGEX_FULL =
    "\\A\\d{4}\\-\\d{1,2}\\-\\d{1,2}[ T]?(\\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{6}Z)\\Z"
  private val TIMESTAMP_REGEX_NO_DATE = "\\A[T]?(\\d{1,2}:\\d{1,2}:\\d{1,2}\\.\\d{6}Z)\\Z"

  /**
   * Regex to match timestamps with or without trailing zeros.
   */
  private val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
    "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
    "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?$"

  val INVALID_INPUT_MESSAGE: String = "Column contains at least one value that is not in the " +
    "required range"

  val INVALID_NUMBER_MSG: String = "At least one value is either null or is an invalid number"

  def sanitizeStringToFloat(
      input: ColumnVector,
      ansiEnabled: Boolean): ColumnVector = {

    // This regex is just strict enough to filter out known edge cases that would result
    // in incorrect values. We further filter out invalid values using the cuDF isFloat method.
    val VALID_FLOAT_REGEX =
      "^" +                             // start of line
        "[Nn][Aa][Nn]" +                // NaN
        "|" +
        "(" +
          "[+\\-]?" +                   // optional sign preceding Inf or numeric
          "(" +
            "([Ii][Nn][Ff]" +           // Inf, Infinity
            "([Ii][Nn][Ii][Tt][Yy])?)" +
            "|" +
            "(" +
              "(" +
                "([0-9]+)|" +           // digits, OR
                "([0-9]*\\.[0-9]+)|" +  // decimal with optional leading and mandatory trailing, OR
                "([0-9]+\\.[0-9]*)" +   // decimal with mandatory leading and optional trailing
              ")" +
              "([eE][+\\-]?[0-9]+)?" +  // exponent
              "[fFdD]?" +               // floating-point designator
            ")" +
          ")" +
        ")" +
      "$"                               // end of line

    withResource(input.lstrip()) { stripped =>
      withResource(GpuScalar.from(null, DataTypes.StringType)) { nullString =>
        // filter out strings containing breaking whitespace
        val withoutWhitespace = withResource(ColumnVector.fromStrings("\r", "\n")) {
          verticalWhitespace =>
            withResource(stripped.contains(verticalWhitespace)) {
              _.ifElse(nullString, stripped)
            }
        }
        // filter out any strings that are not valid floating point numbers according
        // to the regex pattern
        val floatOrNull = withResource(withoutWhitespace) { _ =>
          withResource(withoutWhitespace.matchesRe(VALID_FLOAT_REGEX)) { isFloat =>
            if (ansiEnabled) {
              withResource(isFloat.all()) { allMatch =>
                // Check that all non-null values are valid floats.
                if (allMatch.isValid && !allMatch.getBoolean) {
                  throw new NumberFormatException(GpuCast.INVALID_NUMBER_MSG)
                }
                withoutWhitespace.incRefCount()
              }
            } else {
              isFloat.ifElse(withoutWhitespace, nullString)
            }
          }
        }
        // strip floating-point designator 'f' or 'd' but don't strip the 'f' from 'Inf'
        withResource(floatOrNull) {
          _.stringReplaceWithBackrefs("([^nN])[fFdD]$", "\\1")
        }
      }
    }
  }

  def sanitizeStringToIntegralType(input: ColumnVector, ansiEnabled: Boolean): ColumnVector = {
    // Convert any strings containing whitespace to null values. The input is assumed to already
    // have been stripped of leading and trailing whitespace
    val sanitized = withResource(input.containsRe("\\s")) { hasWhitespace =>
      withResource(hasWhitespace.any()) { any =>
        if (any.isValid && any.getBoolean) {
          if (ansiEnabled) {
            throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
          } else {
            withResource(GpuScalar.from(null, DataTypes.StringType)) { nullVal =>
              hasWhitespace.ifElse(nullVal, input)
            }
          }
        } else {
          input.incRefCount()
        }
      }
    }

    withResource(sanitized) { _ =>
      if (ansiEnabled) {
        // ansi mode only supports simple integers, so no exponents or decimal places
        val regex = "^[+\\-]?[0-9]+$"
        withResource(sanitized.matchesRe(regex)) { isInt =>
          withResource(isInt.all()) { allInts =>
            // Check that all non-null values are valid integers.
            if (allInts.isValid && !allInts.getBoolean) {
              throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
            }
          }
          sanitized.incRefCount()
        }
      } else {
        // truncate strings that represent decimals to just look at the string before the dot
        withResource(Scalar.fromString(".")) { dot =>
          withResource(sanitized.stringContains(dot)) { hasDot =>
            // only do the decimal sanitization if any strings do contain dot
            withResource(hasDot.any(DType.BOOL8)) { anyDot =>
              if (anyDot.getBoolean) {
                // Special handling for strings that have no numeric value before the dot, such
                // as "." and ".1" because extractsRe returns null for the capture group
                // for these values and it also returns null for invalid inputs so we need this
                // explicit check
                withResource(sanitized.matchesRe("^[+\\-]?\\.[0-9]*$")) { startsWithDot =>
                  withResource(sanitized.extractRe("^([+\\-]?[0-9]*)\\.[0-9]*$")) { table =>
                    withResource(Scalar.fromString("0")) { zero =>
                      withResource(startsWithDot.ifElse(zero, table.getColumn(0))) {
                        decimal => hasDot.ifElse(decimal, sanitized)
                      }
                    }
                  }
                }
              } else {
                sanitized.incRefCount()
              }
            }
          }
        }
      }
    }
  }

  def doCast(
      input: ColumnView,
      fromDataType: DataType,
      toDataType: DataType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    if (DataType.equalsStructurally(fromDataType, toDataType)) {
      return input.copyToColumnVector()
    }

    (fromDataType, toDataType) match {
      case (NullType, to) =>
        GpuColumnVector.columnVectorFromNull(input.getRowCount.toInt, to)

      case (DateType, BooleanType | _: NumericType) =>
        // casts from date type to numerics are always null
        GpuColumnVector.columnVectorFromNull(input.getRowCount.toInt, toDataType)
      case (DateType, StringType) =>
        input.asStrings("%Y-%m-%d")

      case (TimestampType, FloatType | DoubleType) =>
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromDouble(1000000)) { microsPerSec =>
            // Use trueDiv to ensure cast to double before division for full precision
            asLongs.trueDiv(microsPerSec, GpuColumnVector.getNonNestedRapidsType(toDataType))
          }
        }
      case (TimestampType, ByteType | ShortType | IntegerType) =>
        // normally we would just do a floordiv here, but cudf downcasts the operands to
        // the output type before the divide.  https://github.com/rapidsai/cudf/issues/2574
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromInt(1000000)) { microsPerSec =>
            withResource(asLongs.floorDiv(microsPerSec, DType.INT64)) { cv =>
              if (ansiMode) {
                toDataType match {
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
              cv.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
            }
          }
        }
      case (TimestampType, _: LongType) =>
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromInt(1000000)) { microsPerSec =>
            asLongs.floorDiv(microsPerSec, GpuColumnVector.getNonNestedRapidsType(toDataType))
          }
        }
      case (TimestampType, StringType) =>
        castTimestampToString(input)

      case (StructType(fields), StringType) =>
        castStructToString(input, fields, ansiMode, legacyCastToString,
          stringToDateAnsiModeEnabled)

      // ansi cast from larger-than-long integral-like types, to long
      case (dt: DecimalType, LongType) if ansiMode =>
        // This is a work around for https://github.com/rapidsai/cudf/issues/9282
        val min = BigDecimal(Long.MinValue)
            .setScale(dt.scale, BigDecimal.RoundingMode.DOWN).bigDecimal
        val max = BigDecimal(Long.MaxValue)
            .setScale(dt.scale, BigDecimal.RoundingMode.DOWN).bigDecimal
        assertValuesInRange(input, Scalar.fromDecimal(min), Scalar.fromDecimal(max))
        if (dt.precision <= DType.DECIMAL32_MAX_PRECISION && dt.scale < 0) {
          // This is a work around for https://github.com/rapidsai/cudf/issues/9281
          withResource(input.castTo(DType.create(DType.DTypeEnum.DECIMAL64, -dt.scale))) { tmp =>
            tmp.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
          }
        } else {
          input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }

      case (dt: DecimalType, LongType) if dt.precision <= DType.DECIMAL32_MAX_PRECISION &&
          dt.scale < 0 =>
        // This is a work around for https://github.com/rapidsai/cudf/issues/9281
        withResource(input.castTo(DType.create(DType.DTypeEnum.DECIMAL64, -dt.scale))) { tmp =>
          tmp.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }

      // ansi cast from larger-than-integer integral-like types, to integer
      case (t @(LongType | _: DecimalType), IntegerType) if ansiMode =>
        assertValuesInRange(input, GpuScalar.from(Int.MinValue, t),
          GpuScalar.from(Int.MaxValue, t))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-short integral-like types, to short
      case (t @(LongType | IntegerType | _: DecimalType), ShortType) if ansiMode =>
        assertValuesInRange(input, GpuScalar.from(Short.MinValue, t),
          GpuScalar.from(Short.MaxValue, t))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-byte integral-like types, to byte
      case (t @ (LongType | IntegerType | ShortType | _: DecimalType), ByteType) if ansiMode =>
        assertValuesInRange(input, GpuScalar.from(Byte.MinValue, t),
          GpuScalar.from(Byte.MaxValue, t))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to byte
      case (FloatType | DoubleType, ByteType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to short
      case (FloatType | DoubleType, ShortType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to integer
      case (FloatType | DoubleType, IntegerType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to long
      case (FloatType | DoubleType, LongType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromLong(Long.MinValue),
          Scalar.fromLong(Long.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      case (FloatType | DoubleType, TimestampType) =>
        // Spark casting to timestamp from double assumes value is in microseconds
        withResource(Scalar.fromInt(1000000)) { microsPerSec =>
          withResource(input.nansToNulls()) { inputWithNansToNull =>
            withResource(FloatUtils.infinityToNulls(inputWithNansToNull)) {
              inputWithoutNanAndInfinity =>
                if (fromDataType == FloatType &&
                    ShimLoader.getSparkShims.hasCastFloatTimestampUpcast) {
                  withResource(inputWithoutNanAndInfinity.castTo(DType.FLOAT64)) { doubles =>
                    withResource(doubles.mul(microsPerSec, DType.INT64)) {
                      inputTimesMicrosCv =>
                        inputTimesMicrosCv.castTo(DType.TIMESTAMP_MICROSECONDS)
                    }
                  }
                } else {
                  withResource(inputWithoutNanAndInfinity.mul(microsPerSec, DType.INT64)) {
                    inputTimesMicrosCv =>
                      inputTimesMicrosCv.castTo(DType.TIMESTAMP_MICROSECONDS)
                  }
                }
            }
          }
        }
      case (FloatType | DoubleType, dt: DecimalType) =>
        castFloatsToDecimal(input, dt, ansiMode)
      case (from: DecimalType, to: DecimalType) =>
        castDecimalToDecimal(input, from, to, ansiMode)
      case (BooleanType, TimestampType) =>
        // cudf requires casting to a long first.
        withResource(input.castTo(DType.INT64)) { longs =>
          longs.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }
      case (BooleanType | ByteType | ShortType | IntegerType, TimestampType) =>
        // cudf requires casting to a long first
        withResource(input.castTo(DType.INT64)) { longs =>
          withResource(longs.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
            timestampSecs.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
          }
        }
      case (_: NumericType, TimestampType) =>
        // Spark casting to timestamp assumes value is in seconds, but timestamps
        // are tracked in microseconds.
        withResource(input.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
          timestampSecs.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }
      case (FloatType, LongType) | (DoubleType, IntegerType | LongType) =>
        // Float.NaN => Int is casted to a zero but float.NaN => Long returns a small negative
        // number Double.NaN => Int | Long, returns a small negative number so Nans have to be
        // converted to zero first
        withResource(FloatUtils.nanToZero(input)) { inputWithNansToZero =>
          inputWithNansToZero.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }
      case (FloatType | DoubleType, StringType) =>
        castFloatingTypeToString(input)
      case (StringType, BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType
                        | DoubleType | DateType | TimestampType) =>
        withResource(input.strip()) { trimmed =>
          toDataType match {
            case BooleanType =>
              castStringToBool(trimmed, ansiMode)
            case DateType =>
              if (stringToDateAnsiModeEnabled) {
                castStringToDateAnsi(trimmed, ansiMode)
              } else {
                castStringToDate(trimmed)
              }
            case TimestampType =>
              castStringToTimestamp(trimmed, ansiMode)
            case FloatType | DoubleType =>
              castStringToFloats(trimmed, ansiMode,
                GpuColumnVector.getNonNestedRapidsType(toDataType))
            case ByteType | ShortType | IntegerType | LongType =>
              castStringToInts(trimmed, ansiMode,
                GpuColumnVector.getNonNestedRapidsType(toDataType))
          }
        }
      case (StringType, dt: DecimalType) =>
        castStringToDecimal(input, ansiMode, dt)

      case (ByteType | ShortType | IntegerType | LongType, dt: DecimalType) =>
        castIntegralsToDecimal(input, dt, ansiMode)

      case (ShortType | IntegerType | LongType | ByteType | StringType, BinaryType) =>
        input.asByteList(true)

      case (_: DecimalType, StringType) =>
        input.castTo(DType.STRING)

      case (ArrayType(nestedFrom, _), ArrayType(nestedTo, _)) =>
        withResource(input.getChildColumnView(0)) { childView =>
          withResource(doCast(childView, nestedFrom, nestedTo,
            ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)) { childColumnVector =>
            withResource(input.replaceListChild(childColumnVector))(_.copyToColumnVector())
          }
        }

      case (ArrayType(elementType, _), StringType) =>
        castArrayToString(
          input, elementType, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled
        )

      case (from: StructType, to: StructType) =>
        castStructToStruct(from, to, input, ansiMode, legacyCastToString,
          stringToDateAnsiModeEnabled)

      case (from: MapType, to: MapType) =>
        castMapToMap(from, to, input, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)

      case (from: MapType, _: StringType) =>
        castMapToString(input, from, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)

      case _ =>
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
    }
  }

  /**
   * Asserts that all values in a column are within the specific range.
   *
   * @param values ColumnVector to be performed with range check
   * @param minValue Named parameter for function to create Scalar representing range minimum value
   * @param maxValue Named parameter for function to create Scalar representing range maximum value
   * @param inclusiveMin Whether the min value is included in the valid range or not
   * @param inclusiveMax Whether the max value is included in the valid range or not
   * @throws IllegalStateException if any values in the column are not within the specified range
   */
  private def assertValuesInRange(values: ColumnView,
      minValue: => Scalar,
      maxValue: => Scalar,
      inclusiveMin: Boolean = true,
      inclusiveMax: Boolean = true): Unit = {

    def throwIfAny(cv: ColumnView): Unit = {
      withResource(cv) { cv =>
        withResource(cv.any()) { isAny =>
          if (isAny.isValid && isAny.getBoolean) {
            throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
      }
    }

    withResource(minValue) { minValue =>
      throwIfAny(if (inclusiveMin) {
        values.lessThan(minValue)
      } else {
        values.lessOrEqualTo(minValue)
      })
    }

    withResource(maxValue) { maxValue =>
      throwIfAny(if (inclusiveMax) {
        values.greaterThan(maxValue)
      } else {
        values.greaterOrEqualTo(maxValue)
      })
    }
  }

  /**
   * Detects outlier values of a column given with specific range, and replaces them with
   * a inputted substitution value.
   *
   * @param values ColumnVector to be performed with range check
   * @param minValue Named parameter for function to create Scalar representing range minimum value
   * @param maxValue Named parameter for function to create Scalar representing range maximum value
   * @param replaceValue Named parameter for function to create scalar to substitute outlier value
   * @param inclusiveMin Whether the min value is included in the valid range or not
   * @param inclusiveMax Whether the max value is included in the valid range or not
   */
  private def replaceOutOfRangeValues(values: ColumnView,
      minValue: => Scalar,
      maxValue: => Scalar,
      replaceValue: => Scalar,
      inclusiveMin: Boolean = true,
      inclusiveMax: Boolean = true): ColumnVector = {

    withResource(minValue) { minValue =>
      withResource(maxValue) { maxValue =>
        val minPredicate = if (inclusiveMin) {
          values.lessThan(minValue)
        } else {
          values.lessOrEqualTo(minValue)
        }
        withResource(minPredicate) { minPredicate =>
          val maxPredicate = if (inclusiveMax) {
            values.greaterThan(maxValue)
          } else {
            values.greaterOrEqualTo(maxValue)
          }
          withResource(maxPredicate) { maxPredicate =>
            withResource(maxPredicate.or(minPredicate)) { rangePredicate =>
              withResource(replaceValue) { nullScalar =>
                rangePredicate.ifElse(nullScalar, values)
              }
            }
          }
        }
      }
    }
  }

  private def castTimestampToString(input: ColumnView): ColumnVector = {
    withResource(input.castTo(DType.TIMESTAMP_MICROSECONDS)) { micros =>
      withResource(micros.asStrings("%Y-%m-%d %H:%M:%S.%6f")) { cv =>
        cv.stringReplaceWithBackrefs(GpuCast.TIMESTAMP_TRUNCATE_REGEX, "\\1\\2\\3")
      }
    }
  }

  /**
   * A 5 steps solution for concatenating string array column. <p>
   * Giving an input with 3 rows:
   * `[ ["1", "2", null, "3"], [], null]` <p>
   * When `legacyCastToString = true`: <p>
   * Step 1: add space char in the front of all not-null elements:
   * `[ [" 1", " 2", null, " 3"], [], null]` <p>
   * step 2: cast `null` elements to their string representation :
   * `[ [" 1", " 2", "", " 3"], [], null]`(here we use "" to represent null) <p>
   * step 3: concatenate list elements, seperated by `","`:
   * `[" 1, 2,, 3", null, null]` <p>
   * step 4: remove the first char, if it is an `' '`:
   * `["1, 2,, 3", null, null]` <p>
   * step 5: replace nulls with empty string:
   * `["1, 2,, 3", "", ""]` <p>
   *
   * when `legacyCastToString = false`, step 1, 4 are skipped
   */
  private def concatenateStringArrayElements(
      input: ColumnView,
      legacyCastToString: Boolean): ColumnVector = {
    val emptyStr = ""
    val spaceStr = " "
    val nullStr = if (legacyCastToString) ""  else "null"
    val sepStr = if (legacyCastToString) "," else ", "
    val numRows = input.getRowCount.toInt

    withResource(
      Seq(emptyStr, spaceStr, nullStr, sepStr).safeMap(Scalar.fromString)
    ){ case Seq(empty, space, nullRep, sep) =>

      // add `' '` to all not null elements when `legacyCastToString = true`
      def addSpaces(strChildContainsNull: ColumnView): ColumnView = {
        withResource(strChildContainsNull) { strChildContainsNull =>
          withResource(strChildContainsNull.replaceNulls(nullRep)) { strChild =>
            if (legacyCastToString) {// add a space string to each non-null element
              val numElements = strChildContainsNull.getRowCount.toInt
              withResource(ColumnVector.fromScalar(space, numElements)) { spaceVec =>
                withResource(ColumnVector.stringConcatenate(Array(spaceVec, strChild))
                ) { hasSpaces =>
                  withResource(strChildContainsNull.isNotNull) {_.ifElse(hasSpaces, strChild)}
                }
              }
            }
            else { strChild.incRefCount }
          }
        }
      }

      // If the first char of a string is ' ', remove it (only for legacyCastToString = true)
      def removeFirstSpace(strCol: ColumnVector): ColumnVector = {
        if (legacyCastToString){
          withResource(strCol.substring(0,1)) { firstChars =>
            withResource(strCol.substring(1)) { remain =>
              withResource(firstChars.equalTo(space)) {_.ifElse(remain, strCol)}
            }
          }
        }
        else {strCol.incRefCount}
      }

      withResource(ColumnVector.fromScalar(sep, numRows)) {sepCol =>
        withResource(input.getChildColumnView(0)) { childCol =>
          withResource(addSpaces(childCol)) {strChildCol =>
            withResource(input.replaceListChild(strChildCol)) {strArrayCol =>
              withResource(
                strArrayCol.stringConcatenateListElements(sepCol)) { strColContainsNull =>
                withResource(strColContainsNull.replaceNulls(empty)) {strCol =>
                  removeFirstSpace(strCol)
                }
              }
            }
          }
        }
      }
    }
  }

  private def castArrayToString(input: ColumnView,
      elementType: DataType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val (leftStr, rightStr) =  ("[", "]")
    val emptyStr = ""
    val nullStr = if (legacyCastToString) ""  else "null"
    val numRows = input.getRowCount.toInt

    withResource(
      Seq(leftStr, rightStr, emptyStr, nullStr).safeMap(Scalar.fromString)
    ){ case Seq(left, right, empty, nullRep) =>

      /*
       * Add brackets to each string. Ex: ["1, 2, 3", "4, 5"] => ["[1, 2, 3]", "[4, 5]"]
       */
      def addBrackets(strVec: ColumnVector): ColumnVector = {
        withResource(
          Seq(left, right).safeMap(s => ColumnVector.fromScalar(s, numRows))
        ) { case Seq(leftColumn, rightColumn) =>
          ColumnVector.stringConcatenate(empty, nullRep, Array(leftColumn, strVec, rightColumn))
        }
      }

      val strChildContainsNull = withResource(input.getChildColumnView(0)) {child =>
        doCast(
          child, elementType, StringType, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)
      }

      withResource(strChildContainsNull) {strChildContainsNull =>
        withResource(input.replaceListChild(strChildContainsNull)) {strArrayCol =>
          withResource(concatenateStringArrayElements(strArrayCol, legacyCastToString)) {strCol =>
            withResource(addBrackets(strCol)) {
              _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
            }
          }
        }
      }
    }
  }

  private def castMapToString(
      input: ColumnView,
      from: MapType,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val numRows = input.getRowCount.toInt
    val (arrowStr, emptyStr, spaceStr) = ("->", "", " ")
    val (leftStr, rightStr, nullStr) =
      if (legacyCastToString) ("[", "]", "") else ("{", "}", "null")

    // cast the key column and value column to string columns
    val (strKey, strValue) = withResource(input.getChildColumnView(0)) { kvStructColumn =>
      val strKey = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
        doCast(
          keyColumn, from.keyType, StringType, ansiMode,
          legacyCastToString, stringToDateAnsiModeEnabled)
      }
      val strValue = withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
        closeOnExcept(strKey) {_ =>
          doCast(
            valueColumn, from.valueType, StringType, ansiMode,
            legacyCastToString, stringToDateAnsiModeEnabled)
        }
      }
      (strKey, strValue)
    }

    // concatenate the key-value pairs to string
    // Example: ("key", "value") -> "key -> value"
    withResource(
      Seq(leftStr, rightStr, arrowStr, emptyStr, nullStr, spaceStr).safeMap(Scalar.fromString)
    ) { case Seq(leftScalar, rightScalar, arrowScalar, emptyScalar, nullScalar, spaceScalar) =>
      val strElements = withResource(Seq(strKey, strValue)) { case Seq(strKey, strValue) =>
        val numElements = strKey.getRowCount.toInt
        withResource(Seq(spaceScalar, arrowScalar).safeMap(ColumnVector.fromScalar(_, numElements))
        ) {case Seq(spaceCol, arrowCol) =>
          if (legacyCastToString) {
            withResource(
              spaceCol.mergeAndSetValidity(BinaryOp.BITWISE_AND, strValue)
            ) {spaceBetweenSepAndVal =>
              ColumnVector.stringConcatenate(
                emptyScalar, nullScalar,
                Array(strKey, spaceCol, arrowCol, spaceBetweenSepAndVal, strValue))
            }
          } else {
            ColumnVector.stringConcatenate(
              emptyScalar, nullScalar, Array(strKey, spaceCol, arrowCol, spaceCol, strValue))
          }
        }
      }

      // concatenate elements
      withResource(strElements) {strElements =>
        withResource(input.replaceListChild(strElements)) {strArrayCol =>
          withResource(concatenateStringArrayElements(strArrayCol, legacyCastToString)) {strCol =>
            withResource(
              Seq(leftScalar, rightScalar).safeMap(ColumnVector.fromScalar(_, numRows))
            ) {case Seq(leftCol, rightCol) =>
              withResource(ColumnVector.stringConcatenate(
                emptyScalar, nullScalar, Array(leftCol, strCol, rightCol))) {
                _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
              }
            }
          }
        }
      }
    }
  }

  private def castStructToString(
      input: ColumnView,
      inputSchema: Array[StructField],
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {

    val (leftStr, rightStr) = if (legacyCastToString) ("[", "]") else ("{", "}")
    val emptyStr = ""
    val nullStr = if (legacyCastToString) "" else "null"
    val separatorStr = if (legacyCastToString) "," else ", "
    val spaceStr = " "
    val numRows = input.getRowCount.toInt
    val numInputColumns = input.getNumChildren

    def doCastStructToString(
        emptyScalar: Scalar,
        nullScalar: Scalar,
        sepColumn: ColumnVector,
        spaceColumn: ColumnVector,
        leftColumn: ColumnVector,
        rightColumn: ColumnVector): ColumnVector = {
      withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
        // legacy: [firstCol
        //   3.1+: {firstCol
        columns += leftColumn.incRefCount()
        withResource(input.getChildColumnView(0)) { firstColumnView =>
          columns += doCast(firstColumnView, inputSchema.head.dataType, StringType,
            ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)
        }
        for (nonFirstIndex <- 1 until numInputColumns) {
          withResource(input.getChildColumnView(nonFirstIndex)) { nonFirstColumnView =>
            // legacy: ","
            //   3.1+: ", "
            columns += sepColumn.incRefCount()
            val nonFirstColumn = doCast(nonFirstColumnView,
              inputSchema(nonFirstIndex).dataType, StringType, ansiMode, legacyCastToString,
                stringToDateAnsiModeEnabled)
            if (legacyCastToString) {
              // " " if non-null
              columns += spaceColumn.mergeAndSetValidity(BinaryOp.BITWISE_AND, nonFirstColumnView)
            }
            columns += nonFirstColumn
          }
        }

        columns += rightColumn.incRefCount()
        withResource(ColumnVector.stringConcatenate(emptyScalar, nullScalar, columns.toArray))(
          _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input) // original whole row is null
        )
      }
    }

    withResource(Seq(emptyStr, nullStr, separatorStr, spaceStr, leftStr, rightStr)
        .safeMap(Scalar.fromString)) {
      case Seq(emptyScalar, nullScalar, columnScalars@_*) =>

        withResource(
          columnScalars.safeMap(s => ColumnVector.fromScalar(s, numRows))
        ) { case Seq(sepColumn, spaceColumn, leftColumn, rightColumn) =>

          doCastStructToString(emptyScalar, nullScalar, sepColumn,
            spaceColumn, leftColumn, rightColumn)
        }
    }
  }

  private def castFloatingTypeToString(input: ColumnView): ColumnVector = {
    withResource(input.castTo(DType.STRING)) { cudfCast =>

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
            replaceExponent.stringReplace(cudfInf, sparkInfinity)
          }
        }
      }
    }
  }

  private def castStringToBool(input: ColumnVector, ansiEnabled: Boolean): ColumnVector = {
    val trueStrings = Seq("t", "true", "y", "yes", "1")
    val falseStrings = Seq("f", "false", "n", "no", "0")
    val boolStrings = trueStrings ++ falseStrings

    // determine which values are valid bool strings
    withResource(ColumnVector.fromStrings(boolStrings: _*)) { boolStrings =>
      withResource(input.strip()) { stripped =>
        withResource(stripped.lower()) { lower =>
          withResource(lower.contains(boolStrings)) { validBools =>
            // in ansi mode, fail if any values are not valid bool strings
            if (ansiEnabled) {
              withResource(validBools.all()) { isAllBool =>
                if (isAllBool.isValid && !isAllBool.getBoolean) {
                  throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
                }
              }
            }
            // replace non-boolean values with null
            withResource(Scalar.fromNull(DType.STRING)) { nullString =>
              withResource(validBools.ifElse(lower, nullString)) { sanitizedInput =>
                // return true, false, or null, as appropriate
                withResource(ColumnVector.fromStrings(trueStrings: _*)) { cvTrue =>
                  sanitizedInput.contains(cvTrue)
                }
              }
            }
          }
        }
      }
    }
  }

  def castStringToInts(
      input: ColumnVector,
      ansiEnabled: Boolean,
      dType: DType): ColumnVector = {

    withResource(GpuCast.sanitizeStringToIntegralType(input, ansiEnabled)) { sanitized =>
      withResource(sanitized.isInteger(dType)) { isInt =>
        if (ansiEnabled) {
          withResource(isInt.all()) { allInts =>
            // Check that all non-null values are valid integers.
            if (allInts.isValid && !allInts.getBoolean) {
              throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
            }
          }
        }
        withResource(sanitized.castTo(dType)) { parsedInt =>
          withResource(Scalar.fromNull(dType)) { nullVal =>
            isInt.ifElse(parsedInt, nullVal)
          }
        }
      }
    }
  }

  def castStringToDecimal(
      input: ColumnView,
      ansiEnabled: Boolean,
      dt: DecimalType): ColumnVector = {
    // 1. Sanitize strings to make sure all are fixed points
    // 2. Identify all fixed point values
    // 3. Cast String to newDt (newDt = dt. precision + 1, dt.scale + 1). Promote precision if
    //    needed. This step is required so we can round up if needed in the final step
    // 4. Now cast newDt to dt (Decimal to Decimal)
    def getInterimDecimalPromoteIfNeeded(dt: DecimalType): DecimalType = {
      if (dt.precision + 1 > DecimalType.MAX_PRECISION) {
        throw new IllegalArgumentException("One or more values exceed the maximum supported " +
            "Decimal precision while conversion")
      }
      DecimalType(dt.precision + 1, dt.scale + 1)
    }

    val interimSparkDt = getInterimDecimalPromoteIfNeeded(dt)
    val interimDt = DecimalUtil.createCudfDecimal(interimSparkDt)
    val isFixedPoints = withResource(input.strip()) {
      // We further filter out invalid values using the cuDF isFixedPoint method.
      _.isFixedPoint(interimDt)
    }

    withResource(isFixedPoints) { isFixedPoints =>
      if (ansiEnabled) {
        withResource(isFixedPoints.all()) { allFixedPoints =>
          if (allFixedPoints.isValid && !allFixedPoints.getBoolean) {
            throw new ArithmeticException(s"One or more values cannot be " +
                s"represented as Decimal(${dt.precision}, ${dt.scale})")
          }
        }
      }
      // intermediate step needed so we can make sure we can round up
      withResource(input.castTo(interimDt)) { interimDecimals =>
        withResource(Scalar.fromNull(interimDt)) { nulls =>
          withResource(isFixedPoints.ifElse(interimDecimals, nulls)) { decimals =>
            // cast Decimal to the Decimal that's needed
            castDecimalToDecimal(decimals, interimSparkDt, dt, ansiEnabled)
          }
        }
      }
    }
  }

  def castStringToFloats(
      input: ColumnVector,
      ansiEnabled: Boolean,
      dType: DType): ColumnVector = {
    // 1. identify the nans
    // 2. identify the floats. "null" and letters are not considered floats
    // 3. if ansi is enabled we want to throw an exception if the string is neither float nor nan
    // 4. convert everything that's not floats to null
    // 5. set the indices where we originally had nans to Float.NaN
    //
    // NOTE Limitation: "1.7976931348623159E308" and "-1.7976931348623159E308" are not considered
    // Inf even though Spark does

    val NAN_REGEX = "^[nN][aA][nN]$"

    withResource(GpuCast.sanitizeStringToFloat(input, ansiEnabled)) { sanitized =>
      //Now identify the different variations of nans
      withResource(sanitized.matchesRe(NAN_REGEX)) { isNan =>
        // now check if the values are floats
        withResource(sanitized.isFloat) { isFloat =>
          if (ansiEnabled) {
            withResource(isNan.or(isFloat)) { nanOrFloat =>
              withResource(nanOrFloat.all()) { allNanOrFloat =>
                // Check that all non-null values are valid floats or NaN.
                if (allNanOrFloat.isValid && !allNanOrFloat.getBoolean) {
                  throw new NumberFormatException(GpuCast.INVALID_NUMBER_MSG)
                }
              }
            }
          }
          withResource(sanitized.castTo(dType)) { casted =>
            withResource(Scalar.fromNull(dType)) { nulls =>
              withResource(isFloat.ifElse(casted, nulls)) { floatsOnly =>
                withResource(FloatUtils.getNanScalar(dType)) { nan =>
                  isNan.ifElse(nan, floatsOnly)
                }
              }
            }
          }
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  def convertDateOrNull(
      input: ColumnVector,
      regex: String,
      cudfFormat: String): ColumnVector = {

    val isValidDate = withResource(input.matchesRe(regex)) { isMatch =>
      withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
        isMatch.and(isTimestamp)
      }
    }

    withResource(isValidDate) { _ =>
      withResource(Scalar.fromNull(DType.TIMESTAMP_DAYS)) { orElse =>
        withResource(input.asTimestampDays(cudfFormat)) { asDays =>
          isValidDate.ifElse(asDays, orElse)
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

    val isValidDate = withResource(input.matchesRe(regex)) { isMatch =>
      withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
        isMatch.and(isTimestamp)
      }
    }

    withResource(isValidDate) { _ =>
      withResource(orElse) { _ =>
        withResource(input.asTimestampDays(cudfFormat)) { asDays =>
          isValidDate.ifElse(asDays, orElse)
        }
      }
    }
  }

  private def checkResultForAnsiMode(input: ColumnVector, result: ColumnVector,
      errMessage: String): ColumnVector = {
    closeOnExcept(result) { finalResult =>
      withResource(input.isNotNull) { wasNotNull =>
        withResource(finalResult.isNull) { isNull =>
          withResource(wasNotNull.and(isNull)) { notConverted =>
            withResource(notConverted.any()) { notConvertedAny =>
              if (notConvertedAny.isValid && notConvertedAny.getBoolean) {
                throw new DateTimeException(errMessage)
              }
            }
          }
        }
      }
    }
    result
  }

  /**
   * Trims and parses a given UTF8 date string to a corresponding [[Int]] value.
   * The return type is [[Option]] in order to distinguish between 0 and null. The following
   * formats are allowed:
   *
   * `yyyy`
   * `yyyy-[m]m`
   * `yyyy-[m]m-[d]d`
   * `yyyy-[m]m-[d]d `
   * `yyyy-[m]m-[d]d *`
   * `yyyy-[m]m-[d]dT*`
   */
  private def castStringToDate(sanitizedInput: ColumnVector): ColumnVector = {

    // convert dates that are in valid formats yyyy, yyyy-mm, yyyy-mm-dd
    val converted = convertDateOr(sanitizedInput, DATE_REGEX_YYYY_MM_DD, "%Y-%m-%d",
      convertDateOr(sanitizedInput, DATE_REGEX_YYYY_MM, "%Y-%m",
        convertDateOrNull(sanitizedInput, DATE_REGEX_YYYY, "%Y")))

    // handle special dates like "epoch", "now", etc.
    closeOnExcept(converted) { tsVector =>
      DateUtils.fetchSpecialDates(DType.TIMESTAMP_DAYS) match {
        case specialDates if specialDates.nonEmpty =>
          // `tsVector` will be closed in replaceSpecialDates
          replaceSpecialDates(sanitizedInput, tsVector, specialDates)
        case _ =>
          tsVector
      }
    }
  }

  private def castStringToDateAnsi(input: ColumnVector, ansiMode: Boolean): ColumnVector = {
    val result = castStringToDate(input)
    if (ansiMode) {
      // When ANSI mode is enabled, we need to throw an exception if any values could not be
      // converted
      checkResultForAnsiMode(input, result,
        "One or more values could not be converted to DateType")
    } else {
      result
    }
  }

  /** This method does not close the `input` ColumnVector. */
  private def convertTimestampOrNull(
      input: ColumnVector,
      regex: String,
      cudfFormat: String): ColumnVector = {

    withResource(Scalar.fromNull(DType.TIMESTAMP_MICROSECONDS)) { orElse =>
      val isValidTimestamp = withResource(input.matchesRe(regex)) { isMatch =>
        withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
          isMatch.and(isTimestamp)
        }
      }
      withResource(isValidTimestamp) { isValidTimestamp =>
        withResource(input.asTimestampMicroseconds(cudfFormat)) { asDays =>
          isValidTimestamp.ifElse(asDays, orElse)
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  private def convertTimestampOr(
      input: ColumnVector,
      regex: String,
      cudfFormat: String,
      orElse: ColumnVector): ColumnVector = {

    withResource(orElse) { orElse =>
      val isValidTimestamp = withResource(input.matchesRe(regex)) { isMatch =>
        withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
          isMatch.and(isTimestamp)
        }
      }
      withResource(isValidTimestamp) { isValidTimestamp =>
        withResource(input.asTimestampMicroseconds(cudfFormat)) { asDays =>
          isValidTimestamp.ifElse(asDays, orElse)
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  private def convertFullTimestampOr(
      input: ColumnVector,
      orElse: ColumnVector): ColumnVector = {

    val cudfFormat1 = "%Y-%m-%d %H:%M:%S.%f"
    val cudfFormat2 = "%Y-%m-%dT%H:%M:%S.%f"

    withResource(orElse) { orElse =>

      // valid dates must match the regex and either of the cuDF formats
      val isCudfMatch = withResource(input.isTimestamp(cudfFormat1)) { isTimestamp1 =>
        withResource(input.isTimestamp(cudfFormat2)) { isTimestamp2 =>
          isTimestamp1.or(isTimestamp2)
        }
      }

      val isValidTimestamp = withResource(isCudfMatch) { isCudfMatch =>
        withResource(input.matchesRe(TIMESTAMP_REGEX_FULL)) { isRegexMatch =>
          isCudfMatch.and(isRegexMatch)
        }
      }

      // we only need to parse with one of the cuDF formats because the parsing code ignores
      // the ' ' or 'T' between the date and time components
      withResource(isValidTimestamp) { _ =>
        withResource(input.asTimestampMicroseconds(cudfFormat1)) { asDays =>
          isValidTimestamp.ifElse(asDays, orElse)
        }
      }
    }
  }

  private def castStringToTimestamp(input: ColumnVector, ansiMode: Boolean): ColumnVector = {

    // special timestamps
    val today = DateUtils.currentDate()
    val todayStr = new SimpleDateFormat("yyyy-MM-dd")
        .format(today * DateUtils.ONE_DAY_SECONDS * 1000L)

    var sanitizedInput = input.incRefCount()

    // prepend today's date to timestamp formats without dates
    sanitizedInput = withResource(sanitizedInput) { _ =>
      sanitizedInput.stringReplaceWithBackrefs(TIMESTAMP_REGEX_NO_DATE, s"${todayStr}T\\1")
    }

    withResource(sanitizedInput) { sanitizedInput =>
      // convert dates that are in valid timestamp formats
      val converted =
        convertFullTimestampOr(sanitizedInput,
          convertTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM_DD, "%Y-%m-%d",
            convertTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM, "%Y-%m",
              convertTimestampOrNull(sanitizedInput, TIMESTAMP_REGEX_YYYY, "%Y"))))

      // handle special dates like "epoch", "now", etc.
      val finalResult = closeOnExcept(converted) { tsVector =>
        DateUtils.fetchSpecialDates(DType.TIMESTAMP_MICROSECONDS) match {
          case specialDates if specialDates.nonEmpty =>
            // `tsVector` will be closed in replaceSpecialDates.
            replaceSpecialDates(sanitizedInput, tsVector, specialDates)
          case _ =>
            tsVector
        }
      }

      if (ansiMode) {
        // When ANSI mode is enabled, we need to throw an exception if any values could not be
        // converted
        checkResultForAnsiMode(input, finalResult,
          "One or more values could not be converted to TimestampType")
      } else {
        finalResult
      }
    }
  }

  private def castMapToMap(
      from: MapType,
      to: MapType,
      input: ColumnView,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {
    // For cudf a map is a list of (key, value) structs, but lets keep it in ColumnView as much
    // as possible
    withResource(input.getChildColumnView(0)) { kvStructColumn =>
      val castKey = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
        doCast(keyColumn, from.keyType, to.keyType, ansiMode, legacyCastToString,
          stringToDateAnsiModeEnabled)
      }
      withResource(castKey) { castKey =>
        val castValue = withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
          doCast(valueColumn, from.valueType, to.valueType,
            ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)
        }
        withResource(castValue) { castValue =>
          withResource(ColumnView.makeStructView(castKey, castValue)) { castKvStructColumn =>
            // We don't have to worry about null in the key/value struct because they are not
            // allowed for maps in Spark
            withResource(input.replaceListChild(castKvStructColumn)) { replacedView =>
              replacedView.copyToColumnVector()
            }
          }
        }
      }
    }
  }

  private def castStructToStruct(
      from: StructType,
      to: StructType,
      input: ColumnView,
      ansiMode: Boolean,
      legacyCastToString: Boolean,
      stringToDateAnsiModeEnabled: Boolean): ColumnVector = {
    withResource(new ArrayBuffer[ColumnVector](from.length)) { childColumns =>
      from.indices.foreach { index =>
        childColumns += doCast(
          input.getChildColumnView(index),
          from(index).dataType,
          to(index).dataType,
          ansiMode,
          legacyCastToString, stringToDateAnsiModeEnabled)
      }
      withResource(ColumnView.makeStructView(childColumns: _*)) { casted =>
        if (input.getNullCount == 0) {
          casted.copyToColumnVector()
        } else {
          withResource(input.isNull) { isNull =>
            withResource(GpuScalar.from(null, to)) { nullVal =>
              isNull.ifElse(nullVal, casted)
            }
          }
        }
      }
    }
  }

  private def castIntegralsToDecimal(
      input: ColumnView,
      dt: DecimalType,
      ansiMode: Boolean): ColumnVector = {
    val prec = DecimalUtil.getPrecisionForIntegralType(input.getType)
    // Cast input to decimal
    val inputDecimalType = new DecimalType(prec, 0)
    withResource(input.castTo(DecimalUtil.createCudfDecimal(prec, 0))) { castedInput =>
      castDecimalToDecimal(castedInput, inputDecimalType, dt, ansiMode)
    }
  }

  private def castFloatsToDecimal(
      input: ColumnView,
      dt: DecimalType,
      ansiMode: Boolean): ColumnVector = {

    // Approach to minimize difference between CPUCast and GPUCast:
    // step 1. cast input to FLOAT64 (if necessary)
    // step 2. cast FLOAT64 to container DECIMAL (who keeps one more digit for rounding)
    // step 3. perform HALF_UP rounding on container DECIMAL
    val checkedInput = withResource(input.castTo(DType.FLOAT64)) { double =>
      val roundedDouble = double.round(dt.scale, cudf.RoundMode.HALF_UP)
      withResource(roundedDouble) { rounded =>
        // We rely on containerDecimal to perform preciser rounding. So, we have to take extra
        // space cost of container into consideration when we run bound check.
        val containerScaleBound = DType.DECIMAL128_MAX_PRECISION - (dt.scale + 1)
        val bound = math.pow(10, (dt.precision - dt.scale) min containerScaleBound)
        if (ansiMode) {
          assertValuesInRange(rounded,
            minValue = Scalar.fromDouble(-bound),
            maxValue = Scalar.fromDouble(bound),
            inclusiveMin = false,
            inclusiveMax = false)
          rounded.incRefCount()
        } else {
          replaceOutOfRangeValues(rounded,
            minValue = Scalar.fromDouble(-bound),
            maxValue = Scalar.fromDouble(bound),
            inclusiveMin = false,
            inclusiveMax = false,
            replaceValue = Scalar.fromNull(DType.FLOAT64))
        }
      }
    }

    withResource(checkedInput) { checked =>
      val targetType = DecimalUtil.createCudfDecimal(dt.precision, dt.scale)
      // If target scale reaches DECIMAL128_MAX_PRECISION, container DECIMAL can not
      // be created because of precision overflow. In this case, we perform casting op directly.
      val casted = if (DecimalUtil.getMaxPrecision(targetType) == dt.scale) {
        checked.castTo(targetType)
      } else {
        val containerType = DecimalUtil.createCudfDecimal(dt.precision, dt.scale + 1)
        withResource(checked.castTo(containerType)) { container =>
          DecimalUtil.round(container, dt.scale, cudf.RoundMode.HALF_UP)
        }
      }
      // Cast NaN values to nulls
      withResource(casted) { casted =>
        withResource(input.isNan) { inputIsNan =>
          withResource(Scalar.fromNull(targetType)) { nullScalar =>
            inputIsNan.ifElse(nullScalar, casted)
          }
        }
      }
    }
  }

  def fixDecimalBounds(input: ColumnView,
      outOfBounds: ColumnView,
      ansiMode: Boolean): ColumnVector = {
    if (ansiMode) {
      withResource(outOfBounds.any()) { isAny =>
        if (isAny.isValid && isAny.getBoolean) {
          throw new IllegalStateException(GpuCast.INVALID_INPUT_MESSAGE)
        }
      }
      input.copyToColumnVector()
    } else {
      withResource(Scalar.fromNull(input.getType)) { nullVal =>
        outOfBounds.ifElse(nullVal, input)
      }
    }
  }

  def checkNFixDecimalBounds(
      input: ColumnView,
      to: DecimalType,
      ansiMode: Boolean): ColumnVector = {
    assert(input.getType.isDecimalType)
    withResource(DecimalUtil.outOfBounds(input, to)) { outOfBounds =>
      fixDecimalBounds(input, outOfBounds, ansiMode)
    }
  }

  private def castDecimalToDecimal(
      input: ColumnView,
      from: DecimalType,
      to: DecimalType,
      ansiMode: Boolean): ColumnVector = {
    val toDType = DecimalUtil.createCudfDecimal(to.precision, to.scale)
    val fromDType = DecimalUtil.createCudfDecimal(from.precision, from.scale)

    val fromWholeNumPrecision = from.precision - from.scale
    val toWholeNumPrecision = to.precision - to.scale

    // Decimal numbers in general terms have two parts, a part before decimal (whole number)
    // and a part after decimal (fractional number)
    // If we are upcasting the whole number part there is no need to check for out of bound
    // values.
    val isWholeNumUpcast = fromWholeNumPrecision <= toWholeNumPrecision

    // When upcasting the scale (fractional number) part there is no need for rounding.
    val isScaleUpcast = from.scale <= to.scale

    if (toDType.equals(fromDType) && to.precision >= from.precision) {
      // This can happen in some cases when the scale does not change but the precision does. To
      // Spark they are different types, but CUDF sees them as the same, so no need to change
      // anything.
      // If the input is a ColumnVector already this will just inc the reference count
      input.copyToColumnVector()
    } else {
      // We have to round first to match what Spark is doing...
      val rounded = if (!isScaleUpcast) {
        // We have to round the data to the desired scale. Spark uses HALF_UP rounding in
        // this case so we need to also.

        // Rounding up can cause overflow, but if the input is in the proper range for Spark
        // the overflow will fit in the current CUDF type without the need to cast it.
        // Int.MinValue =                       -2147483648
        // DECIMAL32 min unscaled =              -999999999
        // DECIMAL32 min unscaled and rounded = -1000000000 (Which fits)
        // Long.MinValue =                      -9223372036854775808
        // DECIMAL64 min unscaled =              -999999999999999999
        // DECIMAL64 min unscaled and rounded = -1000000000000000000 (Which fits)
        // That means we don't need to cast it to a wider type first, we just need to be sure
        // that we do boundary checks, if we did need to round
        DecimalUtil.round(input, to.scale, cudf.RoundMode.HALF_UP)
      } else {
        input.copyToColumnVector()
      }

      val checked = withResource(rounded) { rounded =>
        if (!isWholeNumUpcast || !isScaleUpcast) {
          // We need to check for out of bound values.
          // The wholeNumberUpcast is obvious why we have to check, but we also have to check it
          // when we rounded, because rounding can add a digit to the effective precision.
          checkNFixDecimalBounds(rounded, to, ansiMode)
        } else {
          rounded.incRefCount()
        }
      }

      withResource(checked) { checked =>
        checked.castTo(toDType)
      }
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
    timeZoneId: Option[String] = None,
    legacyCastToString: Boolean = false,
    stringToDateAnsiModeEnabled: Boolean = false)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  import GpuCast._

  // when ansi mode is enabled, some cast expressions can throw exceptions on invalid inputs
  override def hasSideEffects: Boolean = {
    (child.dataType, dataType) match {
      case (StringType, _) if ansiMode => true
      case (TimestampType, ByteType | ShortType | IntegerType) if ansiMode => true
      case (_: DecimalType, LongType) if ansiMode => true
      case (LongType | _: DecimalType, IntegerType) if ansiMode => true
      case (LongType | IntegerType | _: DecimalType, ShortType) if ansiMode => true
      case (LongType | IntegerType | ShortType | _: DecimalType, ByteType) if ansiMode => true
      case (FloatType | DoubleType, ByteType) if ansiMode => true
      case (FloatType | DoubleType, ShortType) if ansiMode => true
      case (FloatType | DoubleType, IntegerType) if ansiMode => true
      case (FloatType | DoubleType, LongType) if ansiMode => true
      case _ => false
    }
  }

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

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    doCast(input.getBase, input.dataType(), dataType, ansiMode, legacyCastToString,
      stringToDateAnsiModeEnabled)

}
