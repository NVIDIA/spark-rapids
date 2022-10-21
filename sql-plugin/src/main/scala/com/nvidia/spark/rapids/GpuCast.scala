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
import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DecimalUtils, DType, Scalar}
import ai.rapids.cudf
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.CastStrings
import com.nvidia.spark.rapids.shims.{AnsiUtil, GpuIntervalUtils, GpuTypeShims, SparkShimImpl, YearParseUtil}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuToTimestamp.replaceSpecialDates
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
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
  val legacyCastToString: Boolean = SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)

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
        if (dt.scale < 0 && !SparkShimImpl.isCastingStringToNegDecimalScaleSupported) {
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
    "\\A\\d{4}\\-\\d{1,2}\\-\\d{1,2}[ T]?(\\d{1,2}:\\d{1,2}:([0-5]\\d|\\d)(\\.\\d{0,6})?Z?)\\Z"
  private val TIMESTAMP_REGEX_NO_DATE =
    "\\A[T]?(\\d{1,2}:\\d{1,2}:([0-5]\\d|\\d)(\\.\\d{0,6})?Z?)\\Z"

  private val BIG_DECIMAL_LONG_MIN = BigDecimal(Long.MinValue)
  private val BIG_DECIMAL_LONG_MAX = BigDecimal(Long.MaxValue)

  val INVALID_INPUT_MESSAGE: String = "Column contains at least one value that is not in the " +
    "required range"

  val OVERFLOW_MESSAGE: String = "overflow occurred"

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
                    assertValuesInRange[Long](cv, Int.MinValue.toLong,
                      Int.MaxValue.toLong, errorMsg = GpuCast.OVERFLOW_MESSAGE)
                  case ShortType =>
                    assertValuesInRange[Long](cv, Short.MinValue.toLong,
                      Short.MaxValue.toLong, errorMsg = GpuCast.OVERFLOW_MESSAGE)
                  case ByteType =>
                    assertValuesInRange[Long](cv, Byte.MinValue.toLong,
                      Byte.MaxValue.toLong, errorMsg = GpuCast.OVERFLOW_MESSAGE)
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
        val min = BIG_DECIMAL_LONG_MIN.setScale(dt.scale, BigDecimal.RoundingMode.DOWN).bigDecimal
        val max = BIG_DECIMAL_LONG_MAX.setScale(dt.scale, BigDecimal.RoundingMode.DOWN).bigDecimal
        // We are going against our convention of calling assertValuesInRange()
        // because the min/max values are a different decimal type i.e. Decimal 128 as opposed to
        // the incoming input column type.
        withResource(input.min()) { minInput =>
          withResource(input.max()) { maxInput =>
            if (minInput.isValid && minInput.getBigDecimal().compareTo(min) == -1 ||
                maxInput.isValid && maxInput.getBigDecimal().compareTo(max) == 1) {
              throw new ArithmeticException(GpuCast.OVERFLOW_MESSAGE)
            }
          }
        }
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
        fromDataType match {
          case LongType =>
            assertValuesInRange[Long](input, Int.MinValue.toLong, Int.MaxValue.toLong)
          case _ =>
            assertValuesInRange[BigDecimal](input, BigDecimal(Int.MinValue),
              BigDecimal(Int.MaxValue))
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-short integral-like types, to short
      case (LongType | IntegerType | _: DecimalType, ShortType) if ansiMode =>
        fromDataType match {
          case LongType =>
            assertValuesInRange[Long](input, Short.MinValue.toLong, Short.MaxValue.toLong)
          case IntegerType =>
            assertValuesInRange[Int](input, Short.MinValue.toInt, Short.MaxValue.toInt)
          case _ =>
            assertValuesInRange[BigDecimal](input, BigDecimal(Short.MinValue),
              BigDecimal(Short.MaxValue))
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from larger-than-byte integral-like types, to byte
      case (LongType | IntegerType | ShortType | _: DecimalType, ByteType) if ansiMode =>
        fromDataType match {
          case LongType =>
            assertValuesInRange[Long](input, Byte.MinValue.toLong, Byte.MaxValue.toLong)
          case IntegerType =>
            assertValuesInRange[Int](input, Byte.MinValue.toInt, Byte.MaxValue.toInt)
          case ShortType =>
            assertValuesInRange[Short](input, Byte.MinValue.toShort, Byte.MaxValue.toShort)
          case _ =>
            assertValuesInRange[BigDecimal](input, BigDecimal(Byte.MinValue),
              BigDecimal(Byte.MaxValue))
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to byte
      case (FloatType | DoubleType, ByteType) if ansiMode =>
        fromDataType match {
          case FloatType =>
            assertValuesInRange[Float](input, Byte.MinValue.toFloat, Byte.MaxValue.toFloat)
          case DoubleType =>
            assertValuesInRange[Double](input, Byte.MinValue.toDouble, Byte.MaxValue.toDouble)
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to short
      case (FloatType | DoubleType, ShortType) if ansiMode =>
        fromDataType match {
          case FloatType =>
            assertValuesInRange[Float](input, Short.MinValue.toFloat, Short.MaxValue.toFloat)
          case DoubleType =>
            assertValuesInRange[Double](input, Short.MinValue.toDouble, Short.MaxValue.toDouble)
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to integer
      case (FloatType | DoubleType, IntegerType) if ansiMode =>
        fromDataType match {
          case FloatType =>
            assertValuesInRange[Float](input, Int.MinValue.toFloat, Int.MaxValue.toFloat)
          case DoubleType =>
            assertValuesInRange[Double](input, Int.MinValue.toDouble, Int.MaxValue.toDouble)
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      // ansi cast from floating-point types, to long
      case (FloatType | DoubleType, LongType) if ansiMode =>
        fromDataType match {
          case FloatType =>
            assertValuesInRange[Float](input, Long.MinValue.toFloat, Long.MaxValue.toFloat)
          case DoubleType =>
            assertValuesInRange[Double](input, Long.MinValue.toDouble, Long.MaxValue.toDouble)
        }
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))

      case (FloatType | DoubleType, TimestampType) =>
        // Spark casting to timestamp from double assumes value is in microseconds
        if (ansiMode && AnsiUtil.supportsAnsiCastFloatToTimestamp()) {
          // We are going through a util class because Spark 3.3.0+ throws an
          // exception if the float value is nan, +/- inf or out-of-range value,
          // where previously it didn't
          AnsiUtil.castFloatToTimestampAnsi(input, toDataType)
        } else {
          // non-Ansi mode, convert nan/inf to null
          withResource(Scalar.fromInt(1000000)) { microsPerSec =>
            withResource(input.nansToNulls()) { inputWithNansToNull =>
              withResource(FloatUtils.infinityToNulls(inputWithNansToNull)) {
                inputWithoutNanAndInfinity =>
                  if (fromDataType == FloatType &&
                      SparkShimImpl.hasCastFloatTimestampUpcast) {
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
      case (_: LongType, TimestampType) =>
        // Spark casting to timestamp assumes value is in seconds, but timestamps
        // are tracked in microseconds.
        castLongToTimestamp(input, toDataType)
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
      case (StringType, ByteType | ShortType | IntegerType | LongType ) =>
        CastStrings.toInteger(input, ansiMode,
          GpuColumnVector.getNonNestedRapidsType(toDataType))
      case (StringType, BooleanType | FloatType | DoubleType | DateType | TimestampType) =>
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
          }
        }
      case (StringType, dt: DecimalType) =>
        CastStrings.toDecimal(input, ansiMode, dt.precision, -dt.scale)

      case (ByteType | ShortType | IntegerType | LongType, dt: DecimalType) =>
        castIntegralsToDecimal(input, dt, ansiMode)

      case (ShortType | IntegerType | LongType | ByteType | StringType, BinaryType) =>
        input.asByteList(true)

      case (BinaryType, StringType) =>
        castBinToString(input)

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

      case (dayTime: DataType, _: StringType) if GpuTypeShims.isSupportedDayTimeType(dayTime) =>
        GpuIntervalUtils.toDayTimeIntervalString(input, dayTime)

      case (_: StringType, dayTime: DataType) if GpuTypeShims.isSupportedDayTimeType(dayTime) =>
        GpuIntervalUtils.castStringToDayTimeIntervalWithThrow(input, dayTime)

      // cast(`day time interval` as integral)
      case (dt: DataType, _: LongType) if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.dayTimeIntervalToLong(input, dt)
      case (dt: DataType, _: IntegerType) if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.dayTimeIntervalToInt(input, dt)
      case (dt: DataType, _: ShortType) if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.dayTimeIntervalToShort(input, dt)
      case (dt: DataType, _: ByteType) if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.dayTimeIntervalToByte(input, dt)

      // cast(integral as `day time interval`)
      case (_: LongType, dt: DataType) if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.longToDayTimeInterval(input, dt)
      case (_: IntegerType | ShortType | ByteType, dt: DataType)
        if GpuTypeShims.isSupportedDayTimeType(dt) =>
        GpuIntervalUtils.intToDayTimeInterval(input, dt)

      // cast(`year month interval` as integral)
      case (ym: DataType, _: LongType) if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.yearMonthIntervalToLong(input, ym)
      case (ym: DataType, _: IntegerType) if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.yearMonthIntervalToInt(input, ym)
      case (ym: DataType, _: ShortType) if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.yearMonthIntervalToShort(input, ym)
      case (ym: DataType, _: ByteType) if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.yearMonthIntervalToByte(input, ym)

      // cast(integral as `year month interval`)
      case (_: LongType, ym: DataType) if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.longToYearMonthInterval(input, ym)
      case (_: IntegerType | ShortType | ByteType, ym: DataType)
        if GpuTypeShims.isSupportedYearMonthType(ym) =>
        GpuIntervalUtils.intToYearMonthInterval(input, ym)
      case _ =>
        input.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
    }
  }

  /**
   * Asserts that all values in a column are within the specific range.
   *
   * @param values ColumnVector to be performed with range check
   * @param minValue Range minimum value of input type T
   * @param maxValue Range maximum value of input type T
   * @param inclusiveMin Whether the min value is included in the valid range or not
   * @param inclusiveMax Whether the max value is included in the valid range or not
   * @param errorMsg Specify the message in the `IllegalStateException`
   * @throws IllegalStateException if any values in the column are not within the specified range
   */
  private def assertValuesInRange[T](values: ColumnView,
      minValue: T,
      maxValue: T,
      inclusiveMin: Boolean = true,
      inclusiveMax: Boolean = true,
      errorMsg:String = GpuCast.OVERFLOW_MESSAGE)
      (implicit ord: Ordering[T]): Unit = {

    def throwIfAnyNan(): Unit = {
      withResource(values.isNan()) { valuesIsNan =>
        withResource(valuesIsNan.any()) { anyNan =>
          if (anyNan.isValid && anyNan.getBoolean) {
            throw RapidsErrorUtils.arithmeticOverflowError(errorMsg)
          }
        }
      }
    }

    def throwIfOutOfRange(minInput: T, maxInput: T): Unit = {
      if (inclusiveMin && ord.compare(minInput, minValue) < 0 ||
          !inclusiveMin && ord.compare(minInput, minValue) <= 0 ||
          inclusiveMax && ord.compare(maxInput, maxValue) > 0 ||
          !inclusiveMax && ord.compare(maxInput, maxValue) >= 0) {
        throw RapidsErrorUtils.arithmeticOverflowError(errorMsg)
      }
    }

    def getValue(s: Scalar): T = (s.getType match {
      case DType.FLOAT64 => s.getDouble
      case DType.FLOAT32 => s.getFloat
      case DType.STRING => s.getJavaString
      case dt if dt.isDecimalType => BigDecimal(s.getBigDecimal)
      case dt if dt.isBackedByLong => s.getLong
      case dt if dt.isBackedByInt => s.getInt
      case dt if dt.isBackedByShort => s.getShort
      case dt if dt.isBackedByByte => s.getByte
      case _ => throw new IllegalArgumentException("Unsupported scalar type")
    }).asInstanceOf[T]

    withResource(values.min()) { minInput =>
      withResource(values.max()) { maxInput =>
        if (values.getType == DType.FLOAT32 || values.getType == DType.FLOAT64) {
          throwIfAnyNan()
        }
        throwIfOutOfRange(getValue(minInput), getValue(maxInput))
      }
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
    // the complexity in this function is due to Spark's rules for truncating
    // the fractional part of the timestamp string. Any trailing decimal place
    // or zeroes should be truncated
    // ".000000" -> ""
    // ".000100" -> ".0001"
    // ".100000" -> ".1"
    // ".101010" -> ".10101"
    withResource(input.castTo(DType.TIMESTAMP_MICROSECONDS)) { micros =>
      withResource(micros.asStrings("%Y-%m-%d %H:%M:%S.%6f")) { cv =>
        // to keep code complexity down, do a first pass that
        // removes ".000000" using simple string replace
        val firstPass = withResource(Scalar.fromString(".000000")) { search =>
          withResource(Scalar.fromString("")) { replace =>
            cv.stringReplace(search, replace)
          }
        }
        // now remove trailing zeroes from any remaining fractional parts
        // the first group captures everything between
        // the decimal point and the last non-zero digit
        // the second group (non-capture) covers the remaining zeroes
        withResource(firstPass) { _ =>
          firstPass.stringReplaceWithBackrefs("(\\.[0-9]*[1-9]+)(?:0+)?$", "\\1")
        }
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
      val strValue = closeOnExcept(strKey) {_ =>
        withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
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

  private[rapids] def castFloatingTypeToString(input: ColumnView): ColumnVector = {
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

  def castStringToFloats(
      input: ColumnVector,
      ansiEnabled: Boolean,
      dType: DType,
      alreadySanitized: Boolean = false): ColumnVector = {
    // 1. identify the nans
    // 2. identify the floats. "null" and letters are not considered floats
    // 3. if ansi is enabled we want to throw an exception if the string is neither float nor nan
    // 4. convert everything that's not floats to null
    // 5. set the indices where we originally had nans to Float.NaN
    //
    // NOTE Limitation: "1.7976931348623159E308" and "-1.7976931348623159E308" are not considered
    // Inf even though Spark does

    val NAN_REGEX = "^[nN][aA][nN]$"

    val sanitized = if (alreadySanitized) {
      input.incRefCount()
    } else {
      GpuCast.sanitizeStringToFloat(input, ansiEnabled)
    }

    withResource(sanitized) { _ =>
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
    val cudfFormat3 = "%Y-%m-%d %H:%M:%S"
    val cudfFormat4 = "%Y-%m-%dT%H:%M:%S"

    withResource(orElse) { orElse =>

      // valid dates must match the regex and either of the cuDF formats
      val isCudfMatch = withResource(input.isTimestamp(cudfFormat1)) { isTimestamp1 =>
        withResource(input.isTimestamp(cudfFormat2)) { isTimestamp2 =>
          withResource(input.isTimestamp(cudfFormat3)) { isTimestamp3 =>
            withResource(input.isTimestamp(cudfFormat4)) { isTimestamp4 =>
              withResource(isTimestamp1.or(isTimestamp2)) { isTimestamp12 =>
                withResource(isTimestamp12.or(isTimestamp3)) { isTimestamp123 =>
                  isTimestamp123.or(isTimestamp4)
                }
              }
            }
          }
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

  private def castBinToString(input: ColumnView): ColumnVector = {
    // Spark interprets the binary as UTF-8 bytes. So the layout of the
    // binary and the layout of the string are the same. We just need to play some games with
    // the CPU side metadata to make CUDF think it is a String.
    // Sadly there is no simple CUDF API to do this, so for now we pull it apart and put
    // it back together again
    withResource(input.getChildColumnView(0)) { dataCol =>
      withResource(new ColumnView(DType.STRING, input.getRowCount,
        Optional.of[java.lang.Long](input.getNullCount),
        dataCol.getData, input.getValid, input.getOffsets)) { cv =>
        cv.copyToColumnVector()
      }
    }
  }

  private def castIntegralsToDecimal(
      input: ColumnView,
      dt: DecimalType,
      ansiMode: Boolean): ColumnVector = {
    val prec = input.getType.getPrecisionForInt
    // Cast input to decimal
    val inputDecimalType = new DecimalType(prec, 0)
    withResource(input.castTo(DecimalUtil.createCudfDecimal(inputDecimalType))) { castedInput =>
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
          assertValuesInRange[Double](rounded,
            minValue = -bound,
            maxValue = bound,
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
      val targetType = DecimalUtil.createCudfDecimal(dt)
      // If target scale reaches DECIMAL128_MAX_PRECISION, container DECIMAL can not
      // be created because of precision overflow. In this case, we perform casting op directly.
      val casted = if (DType.DECIMAL128_MAX_PRECISION == dt.scale) {
        checked.castTo(targetType)
      } else {
        // Increase precision by one along with scale in case of overflow, which may lead to
        // the upcast of cuDF decimal type. If precision already hits the max precision, it is safe
        // to increase the scale solely because we have checked and replaced out of range values.
        val containerType = DecimalUtils.createDecimalType(
          dt.precision + 1 min DType.DECIMAL128_MAX_PRECISION, dt.scale + 1)
        withResource(checked.castTo(containerType)) { container =>
          withResource(container.round(dt.scale, cudf.RoundMode.HALF_UP)) { rd =>
            // The cast here is for cases that cuDF decimal type got promoted as precision + 1.
            // Need to convert back to original cuDF type, to keep align with the precision.
            rd.castTo(targetType)
          }
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
          throw RapidsErrorUtils.arithmeticOverflowError(GpuCast.OVERFLOW_MESSAGE)
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
    val toDType = DecimalUtil.createCudfDecimal(to)
    val fromDType = DecimalUtil.createCudfDecimal(from)

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
        input.round(to.scale, cudf.RoundMode.HALF_UP)
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

  /**
   * return `longInput` * MICROS_PER_SECOND, the input values are seconds.
   * return Long.MaxValue if `long value` * MICROS_PER_SECOND > Long.MaxValue
   * return Long.MinValue if `long value` * MICROS_PER_SECOND < Long.MinValue
   */
  def castLongToTimestamp(longInput: ColumnView, toType: DataType): ColumnVector = {
    // rewrite from `java.util.concurrent.SECONDS.toMicros`
    val maxSeconds = Long.MaxValue / MICROS_PER_SECOND
    val minSeconds = -maxSeconds

    val mulRet = withResource(Scalar.fromLong(MICROS_PER_SECOND)) { microsPerSecondS =>
      longInput.mul(microsPerSecondS)
    }

    val updatedMaxRet = withResource(mulRet) { mulCv =>
      withResource(Scalar.fromLong(maxSeconds)) { maxSecondsS =>
        withResource(longInput.greaterThan(maxSecondsS)) { greaterThanMaxSeconds =>
          withResource(Scalar.fromLong(Long.MaxValue)) { longMaxS =>
            greaterThanMaxSeconds.ifElse(longMaxS, mulCv)
          }
        }
      }
    }

    withResource(updatedMaxRet) { updatedMax =>
      withResource(Scalar.fromLong(minSeconds)) { minSecondsS =>
        withResource(longInput.lessThan(minSecondsS)) { lessThanMinSeconds =>
          withResource(Scalar.fromLong(Long.MinValue)) { longMinS =>
            withResource(lessThanMinSeconds.ifElse(longMinS, updatedMax)) { cv =>
              cv.castTo(GpuColumnVector.getNonNestedRapidsType(toType))
            }
          }
        }
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
  override def hasSideEffects: Boolean = super.hasSideEffects || {
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
      case (_: LongType, dayTimeIntervalType: DataType)
        if GpuTypeShims.isSupportedDayTimeType(dayTimeIntervalType) => true
      case (_: IntegerType, dayTimeIntervalType: DataType)
        if GpuTypeShims.isSupportedDayTimeType(dayTimeIntervalType) =>
        GpuTypeShims.hasSideEffectsIfCastIntToDayTime(dayTimeIntervalType)
      case (dayTimeIntervalType: DataType, _: IntegerType | ShortType | ByteType)
        if GpuTypeShims.isSupportedDayTimeType(dayTimeIntervalType) => true
      case (_: LongType, yearMonthIntervalType: DataType)
        if GpuTypeShims.isSupportedYearMonthType(yearMonthIntervalType) => true
      case (_: IntegerType, yearMonthIntervalType: DataType)
        if GpuTypeShims.isSupportedYearMonthType(yearMonthIntervalType) =>
        GpuTypeShims.hasSideEffectsIfCastIntToYearMonth(yearMonthIntervalType)
      case (yearMonthIntervalType: DataType, _: ShortType | ByteType)
        if GpuTypeShims.isSupportedYearMonthType(yearMonthIntervalType) => true
      case (FloatType | DoubleType, TimestampType) =>
        GpuTypeShims.hasSideEffectsIfCastFloatToTimestamp
      case _ => false
    }
  }

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

  def needsTimeZone: Boolean = Cast.needsTimeZone(child.dataType, dataType)

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