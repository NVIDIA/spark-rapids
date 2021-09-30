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

package com.nvidia.spark.rapids

import java.text.SimpleDateFormat
import java.time.DateTimeException

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.YearParseUtil

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuToTimestamp.replaceSpecialDates
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
class CastExprMeta[INPUT <: CastBase](
    cast: INPUT,
    ansiEnabled: Boolean,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMeta[INPUT](cast, conf, parent, rule) {

  val fromType: DataType = cast.child.dataType
  val toType: DataType = cast.dataType
  val legacyCastToString: Boolean = ShimLoader.getSparkShims.getLegacyComplexTypeToString()

  // stringToDate supports ANSI mode from Spark v3.2.0.  Here is the details.
  //     https://github.com/apache/spark/commit/6e862792fb
  // We do not want to create a shim class for this small change, so define a method
  // to support both cases in a single class.
  protected def stringToDateAnsiModeEnabled: Boolean = false

  override def tagExprForGpu(): Unit = recursiveTagExprForGpuCheck()

  private def recursiveTagExprForGpuCheck(
      fromDataType: DataType = fromType,
      toDataType: DataType = toType,
      depth: Int = 0): Unit = {
    val checks = rule.getChecks.get.asInstanceOf[CastChecks]
    if (depth > 0 &&
        !checks.gpuCanCast(fromDataType, toDataType, allowDecimal = conf.decimalTypeEnabled)) {
      willNotWorkOnGpu(s"Casting child type $fromDataType to $toDataType is not supported")
    }
    (fromDataType, toDataType) match {
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
      case (_: StringType, _: DecimalType) if !conf.isCastStringToDecimalEnabled =>
        // FIXME: https://github.com/NVIDIA/spark-rapids/issues/2019
        willNotWorkOnGpu("Currently string to decimal type on the GPU might produce " +
            "results which slightly differed from the correct results when the string represents " +
            "any number exceeding the max precision that CAST_STRING_TO_FLOAT can keep. For " +
            "instance, the GPU returns 99999999999999987 when given the input string " +
            "\"99999999999999999\". The cause of divergence is that we can not cast strings " +
            "containing scientific notation to decimal directly. So, we have to cast strings " +
            "to floats firstly. Then, cast floats to decimals. The first step may lead to " +
            "precision loss. To enable this operation on the GPU, set " +
            s" ${RapidsConf.ENABLE_CAST_STRING_TO_FLOAT} to true.")
      case (structType: StructType, StringType) =>
        structType.foreach { field =>
          recursiveTagExprForGpuCheck(field.dataType, StringType, depth + 1)
        }
      case (fromStructType: StructType, toStructType: StructType) =>
        fromStructType.zip(toStructType).foreach {
          case (fromChild, toChild) =>
            recursiveTagExprForGpuCheck(fromChild.dataType, toChild.dataType, depth + 1)
        }
      case (ArrayType(nestedFrom, _), ArrayType(nestedTo, _)) =>
        recursiveTagExprForGpuCheck(nestedFrom, nestedTo, depth + 1)

      case (MapType(keyFrom, valueFrom, _), MapType(keyTo, valueTo, _)) =>
        recursiveTagExprForGpuCheck(keyFrom, keyTo, depth + 1)
        recursiveTagExprForGpuCheck(valueFrom, valueTo, depth + 1)

      case _ =>
    }
  }

  def buildTagMessage(entry: ConfEntry[_]): String = {
    s"${entry.doc}. To enable this operation on the GPU, set ${entry.key} to true."
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuCast(child, toType, ansiEnabled, cast.timeZoneId, legacyCastToString,
      stringToDateAnsiModeEnabled)
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

  val INVALID_FLOAT_CAST_MSG: String = "At least one value is either null or is an invalid number"

  def sanitizeStringToFloat(input: ColumnVector, ansiEnabled: Boolean): ColumnVector = {

    // This regex gets applied after the transformation to normalize use of Inf and is
    // just strict enough to filter out known edge cases that would result in incorrect
    // values. We further filter out invalid values using the cuDF isFloat method.
    val VALID_FLOAT_REGEX =
      "^" +                         // start of line
      "[+\\-]?" +                   // optional + or - at start of string
      "(" +
        "(" +
          "(" +
            "([0-9]+)|" +           // digits, OR
            "([0-9]*\\.[0-9]+)|" +  // decimal with optional leading and mandatory trailing, OR
            "([0-9]+\\.[0-9]*)" +   // decimal with mandatory leading and optional trailing
          ")" +
          "([eE][+\\-]?[0-9]+)?" +  // exponent
          "[fFdD]?" +               // floating-point designator
        ")" +
        "|Inf" +                    // Infinity
        "|[nN][aA][nN]" +           // NaN
      ")" +
      "$"                           // end of line

    withResource(input.lstrip()) { stripped =>
      withResource(GpuScalar.from(null, DataTypes.StringType)) { nullString =>
        // filter out strings containing breaking whitespace
        val withoutWhitespace = withResource(ColumnVector.fromStrings("\r", "\n")) {
            verticalWhitespace =>
          withResource(stripped.contains(verticalWhitespace)) {
            _.ifElse(nullString, stripped)
          }
        }
        // replace all possible versions of "Inf" and "Infinity" with "Inf"
        val inf = withResource(withoutWhitespace) { _ =>
            withoutWhitespace.stringReplaceWithBackrefs(
          "(?:[iI][nN][fF])" + "(?:[iI][nN][iI][tT][yY])?", "Inf")
        }
        // replace "+Inf" with "Inf" because cuDF only supports "Inf" and "-Inf"
        val infWithoutPlus = withResource(inf) { _ =>
          withResource(GpuScalar.from("+Inf", DataTypes.StringType)) { search =>
            withResource(GpuScalar.from("Inf", DataTypes.StringType)) { replace =>
              inf.stringReplace(search, replace)
            }
          }
        }
        // filter out any strings that are not valid floating point numbers according
        // to the regex pattern
        val floatOrNull = withResource(infWithoutPlus) { _ =>
          withResource(infWithoutPlus.matchesRe(VALID_FLOAT_REGEX)) { isFloat =>
            if (ansiEnabled) {
              withResource(isFloat.all()) { allMatch =>
                // Check that all non-null values are valid floats.
                if (allMatch.isValid && !allMatch.getBoolean) {
                  throw new NumberFormatException(GpuCast.INVALID_FLOAT_CAST_MSG)
                }
                infWithoutPlus.incRefCount()
              }
            } else {
              isFloat.ifElse(infWithoutPlus, nullString)
            }
          }
        }
        // strip floating-point designator 'f' or 'd' but don't strip the 'f' from 'Inf'
        withResource(floatOrNull) {
          _.stringReplaceWithBackrefs("([^n])[fFdD]$", "\\1")
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
      case (LongType | _: DecimalType, IntegerType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-short integral-like types, to short
      case (LongType | IntegerType | _: DecimalType, ShortType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-byte integral-like types, to byte
      case (LongType | IntegerType | ShortType | _: DecimalType, ByteType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
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
        // To apply HALF_UP rounding strategy during casting to decimal, we firstly cast
        // string to fp64. Then, cast fp64 to target decimal type to enforce HALF_UP rounding.
        withResource(input.strip()) { trimmed =>
          withResource(castStringToFloats(trimmed, ansiMode, DType.FLOAT64)) { fp =>
            castFloatsToDecimal(fp, dt, ansiMode)
          }
        }
      case (ShortType | IntegerType | LongType, dt: DecimalType) =>
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

      case (from: StructType, to: StructType) =>
        castStructToStruct(from, to, input, ansiMode, legacyCastToString,
          stringToDateAnsiModeEnabled)

      case (from: MapType, to: MapType) =>
        castMapToMap(from, to, input, ansiMode, legacyCastToString, stringToDateAnsiModeEnabled)

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

  def castStringToFloats(
      input: ColumnVector,
      ansiEnabled: Boolean,
      dType: DType): ColumnVector = {

    // 1. convert the different infinities to "Inf"/"-Inf" which is the only variation cudf
    // understands
    // 2. identify the nans
    // 3. identify the floats. "nan", "null" and letters are not considered floats
    // 4. if ansi is enabled we want to throw and exception if the string is neither float nor nan
    // 5. convert everything thats not floats to null
    // 6. set the indices where we originally had nans to Float.NaN
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
                  throw new NumberFormatException(GpuCast.INVALID_FLOAT_CAST_MSG)
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

  private def castIntegralsToDecimalAfterCheck(input: ColumnView, dt: DecimalType): ColumnVector = {
    if (dt.scale < 0) {
      // Rounding is essential when scale is negative,
      // so we apply HALF_UP rounding manually to keep align with CpuCast.
      withResource(input.castTo(DecimalUtil.createCudfDecimal(dt.precision, 0))) {
        scaleZero => scaleZero.round(dt.scale, ai.rapids.cudf.RoundMode.HALF_UP)
      }
    } else if (dt.scale > 0) {
      // Integer will be enlarged during casting if scale > 0, so we cast input to INT64
      // before casting it to decimal in case of overflow.
      withResource(input.castTo(DType.INT64)) { long =>
        long.castTo(DecimalUtil.createCudfDecimal(dt.precision, dt.scale))
      }
    } else {
      input.castTo(DecimalUtil.createCudfDecimal(dt.precision, dt.scale))
    }
  }

  private def castIntegralsToDecimal(
      input: ColumnView,
      dt: DecimalType,
      ansiMode: Boolean): ColumnVector = {
    // Use INT64 bounds instead of FLOAT64 bounds, which enables precise comparison.
    val (lowBound, upBound) = math.pow(10, dt.precision - dt.scale) match {
      case bound if bound > Long.MaxValue => (Long.MinValue, Long.MaxValue)
      case bound => (-bound.toLong + 1, bound.toLong - 1)
    }
    // At first, we conduct overflow check onto input column.
    // Then, we cast checked input into target decimal type.
    if (ansiMode) {
      assertValuesInRange(input,
        minValue = Scalar.fromLong(lowBound),
        maxValue = Scalar.fromLong(upBound))
      castIntegralsToDecimalAfterCheck(input, dt)
    } else {
      val checkedInput = replaceOutOfRangeValues(input,
        minValue = Scalar.fromLong(lowBound),
        maxValue = Scalar.fromLong(upBound),
        replaceValue = Scalar.fromNull(input.getType))
      withResource(checkedInput) { checked =>
        castIntegralsToDecimalAfterCheck(checked, dt)
      }
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
      val roundedDouble = double.round(dt.scale, ai.rapids.cudf.RoundMode.HALF_UP)
      withResource(roundedDouble) { rounded =>
        // We rely on containerDecimal to perform preciser rounding. So, we have to take extra
        // space cost of container into consideration when we run bound check.
        val containerScaleBound = DType.DECIMAL64_MAX_PRECISION - (dt.scale + 1)
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
      // If target scale reaches DECIMAL64_MAX_PRECISION, container DECIMAL can not
      // be created because of precision overflow. In this case, we perform casting op directly.
      val casted = if (DType.DECIMAL64_MAX_PRECISION == dt.scale) {
        checked.castTo(targetType)
      } else {
        val containerType = DecimalUtil.createCudfDecimal(dt.precision, dt.scale + 1)
        withResource(checked.castTo(containerType)) { container =>
          container.round(dt.scale, ai.rapids.cudf.RoundMode.HALF_UP)
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

  private def castDecimalToDecimal(
      input: ColumnView,
      from: DecimalType,
      to: DecimalType,
      ansiMode: Boolean): ColumnVector = {

    val isFrom32Bit = DecimalType.is32BitDecimalType(from)
    val isTo32Bit = DecimalType.is32BitDecimalType(to)
    val cudfDecimal = DecimalUtil.createCudfDecimal(to.precision, to.scale)

    def castCheckedDecimal(checkedInput: ColumnView): ColumnVector = {
      if (to.scale == from.scale) {
        if (isFrom32Bit == isTo32Bit) {
          // If the input is a ColumnVector already this will just inc the reference count
          checkedInput.copyToColumnVector()
        } else {
          // the input is already checked, just cast it
          checkedInput.castTo(cudfDecimal)
        }
      } else if (to.scale > from.scale) {
        checkedInput.castTo(cudfDecimal)
      } else {
        withResource(checkedInput.round(to.scale, ai.rapids.cudf.RoundMode.HALF_UP)) {
          rounded => rounded.castTo(cudfDecimal)
        }
      }
    }

    if (to.scale <= from.scale) {
      if (!isFrom32Bit && isTo32Bit) {
        // check for overflow when 64bit => 32bit
        withResource(checkForOverflow(input, to, isFrom32Bit, ansiMode)) { checkedInput =>
          castCheckedDecimal(checkedInput)
        }
      } else {
        if (to.scale < 0 && !SQLConf.get.allowNegativeScaleOfDecimalEnabled) {
          throw new IllegalStateException(s"Negative scale is not allowed: ${to.scale}. " +
              s"You can use spark.sql.legacy.allowNegativeScaleOfDecimal=true " +
              s"to enable legacy mode to allow it.")
        }
        castCheckedDecimal(input)
      }
    } else {
      //  from.scale > to.scale
      withResource(checkForOverflow(input, to, isFrom32Bit, ansiMode)) { checkedInput =>
        castCheckedDecimal(checkedInput)
      }
    }
  }

  def checkForOverflow(
      input: ColumnView,
      to: DecimalType,
      isFrom32Bit: Boolean,
      ansiMode: Boolean): ColumnVector = {

    // Decimal numbers in general terms have two parts, a part before decimal (whole number)
    // and a part after decimal (fractional number)
    // When moving from a smaller scale to a bigger scale (or 32-bit to 64-bit), the target type is
    // able to hold much more values on the fractional side which leaves less room for the whole
    // number. In the following examples we have kept the precision constant to keep it simple.
    //
    // Ex:
    //  999999.999 => from.scale = 3
    //  9999.99999 => to.scale = 5
    //
    // In the above example the source can have a maximum of 4 digits for the whole number and
    // 3 digits for fractional side. We are not worried about the fractional side as the target can
    // hold more digits than the source can. What we need to make sure is the source
    // doesn't have values that are bigger than the destination whole number side can hold.
    // So we calculate the max number that should be in the input column before we can safely cast
    // the values without overflowing. If we find values bigger, we handle it depending on if we
    // are in ANSI mode or not.
    //
    // When moving from a bigger scale to a smaller scale (or 64-bit to 32-bit), the target type
    // is able to have more digits on the whole number side but less on the fractional
    // side. In this case all we need to do is round the value to the new scale. Only, in case we
    // are moving from 64-bit to a 32-bit do we need to check for overflow
    //
    // Ex:
    // 9999.99999 => from.scale = 5
    // 999999.999 => to.scale = 3
    //
    // Here you can see the "to.scale" can hold less fractional values but more on the whole
    // number side so overflow check is unnecessary when the bases are the same i.e. 32-bit to
    // 32-bit and 64-bit to 64-bit. Only when we go from a 64-bit number to a 32-bit number in this
    // case we need to check for overflow.
    //
    // Therefore the values of absMax and absMin will be calculated based on the absBoundPrecision
    // value to make sure the source has values that don't exceed the upper and lower bounds
    val absBoundPrecision = to.precision - to.scale

    // When we support 128 bit Decimals we should add a check for that
    // if (isFrom32Bit && prec > Decimal.MAX_INT_DIGITS ||
    // !isFrom32Bit && prec > Decimal.MAX_LONG_DIGITS)
    if (isFrom32Bit && absBoundPrecision > Decimal.MAX_INT_DIGITS) {
      return input.copyToColumnVector()
    }
    val (minValueScalar, maxValueScalar) = if (!isFrom32Bit) {
      val absBound = math.pow(10, absBoundPrecision).toLong
      (Scalar.fromDecimal(0, -absBound), Scalar.fromDecimal(0, absBound))
    } else {
      val absBound = math.pow(10, absBoundPrecision).toInt
      (Scalar.fromDecimal(0, -absBound), Scalar.fromDecimal(0, absBound))
    }
    val checkedInput = if (ansiMode) {
      assertValuesInRange(input,
        minValue = minValueScalar,
        maxValue = maxValueScalar,
        inclusiveMin = false, inclusiveMax = false)
      input.copyToColumnVector()
    } else {
      replaceOutOfRangeValues(input,
        minValue = minValueScalar,
        maxValue = maxValueScalar,
        replaceValue = Scalar.fromNull(input.getType),
        inclusiveMin = false, inclusiveMax = false)
    }

    checkedInput
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
