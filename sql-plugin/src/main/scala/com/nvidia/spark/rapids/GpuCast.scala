/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BinaryOp, CaptureGroups, ColumnVector, ColumnView, DType, RegexProgram, Scalar}
import ai.rapids.cudf
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{CastStrings, DecimalUtils, GpuTimeZoneDB}
import com.nvidia.spark.rapids.shims.{AnsiUtil, GpuCastShims, GpuIntervalUtils, GpuTypeShims, SparkShimImpl, YearParseUtil}
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, NullIntolerant, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuToTimestamp.replaceSpecialDates
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
final class CastExprMeta[INPUT <: UnaryExpression with TimeZoneAwareExpression with NullIntolerant](
    cast: INPUT,
    val evalMode: GpuEvalMode.Value,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule,
    doFloatToIntCheck: Boolean,
    // stringToDate supports ANSI mode from Spark v3.2.0.  Here is the details.
    //     https://github.com/apache/spark/commit/6e862792fb
    // We do not want to create a shim class for this small change
    stringToAnsiDate: Boolean,
    toTypeOverride: Option[DataType] = None)
  extends CastExprMetaBase(cast, conf, parent, rule, doFloatToIntCheck) {

  val legacyCastComplexTypesToString: Boolean =
    SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)
  override val toType: DataType = toTypeOverride.getOrElse(cast.dataType)

  override def tagExprForGpu(): Unit = {
    if (evalMode == GpuEvalMode.TRY) {
      willNotWorkOnGpu("try_cast is not supported on the GPU")
    }
    recursiveTagExprForGpuCheck()
  }

  def withToTypeOverride(newToType: DecimalType): CastExprMeta[INPUT] =
    new CastExprMeta[INPUT](cast, evalMode, conf, parent, rule,
      doFloatToIntCheck, stringToAnsiDate, Some(newToType))

  override def convertToGpu(child: Expression): GpuExpression =
    GpuCast(child, toType, evalMode == GpuEvalMode.ANSI, cast.timeZoneId,
      legacyCastComplexTypesToString, stringToAnsiDate)

}

/** Meta-data for cast, ansi_cast and ToPrettyString */
abstract class CastExprMetaBase[INPUT <: UnaryExpression with TimeZoneAwareExpression](
    cast: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule,
    doFloatToIntCheck: Boolean = false)
  extends UnaryExprMeta[INPUT](cast, conf, parent, rule) {

  val fromType: DataType = cast.child.dataType
  val toType: DataType = cast.dataType

  override def isTimeZoneSupported: Boolean = {
    (fromType, toType) match {
      case (TimestampType, DateType) => true // this is for to_date(...)
      case _ => false
    }
  }

  override def tagExprForGpu(): Unit = {
    recursiveTagExprForGpuCheck()
  }

  protected def recursiveTagExprForGpuCheck(
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
}

object CastOptions {
  val DEFAULT_CAST_OPTIONS = new CastOptions(false, false, false)
  val ARITH_ANSI_OPTIONS = new CastOptions(false, true, false)
  val TO_PRETTY_STRING_OPTIONS = ToPrettyStringOptions

  def getArithmeticCastOptions(failOnError: Boolean): CastOptions =
    if (failOnError) ARITH_ANSI_OPTIONS else DEFAULT_CAST_OPTIONS

  object ToPrettyStringOptions extends CastOptions(false, false, false,
      castToJsonString = false) {
    override val leftBracket: String = "{"

    override val rightBracket: String = "}"

    override val nullString: String = "NULL"

    override val useDecimalPlainString: Boolean = true

    override val useHexFormatForBinary: Boolean = true
  }
}

/**
 * This class is used to encapsulate parameters to use to help determine how to
 * cast
 *
 * @param legacyCastComplexTypesToString If we should use legacy casting method
 * @param ansiMode                       Whether the cast should be ANSI compliant
 * @param stringToDateAnsiMode           Whether to cast String to Date using ANSI compliance
 * @param castToJsonString               Whether to use JSON format when casting to String
 * @param ignoreNullFieldsInStructs      Whether to omit null values when converting to JSON
 * @param timeZoneId                     If cast is timezone aware, the timezone needed
 */
class CastOptions(
    legacyCastComplexTypesToString: Boolean,
    ansiMode: Boolean,
    stringToDateAnsiMode: Boolean,
    val castToJsonString: Boolean = false,
    val ignoreNullFieldsInStructs: Boolean = true,
    val timeZoneId: Option[String] = Option.empty[String]) extends Serializable {

  /**
   * Retuns the left bracket to use when surrounding brackets when converting
   * map or struct types to string
   * example:
   * [ "a" -> "b"] when legacyCastComplexTypesToString is enabled
   * otherwise { "a" -> "b" }
   */
  val leftBracket: String = if (legacyCastComplexTypesToString) "[" else "{"

  /**
   * Returns the right bracket to use when surrounding brackets when converting
   * map or struct types to string
   * example:
   * [ "a" -> "b"] when legacyCastComplexTypesToString is enabled
   * otherwise { "a" -> "b" }
   */
  val rightBracket: String = if (legacyCastComplexTypesToString) "]" else "}"

  /**
   * Returns the string value to use to represent null elements in array/struct/map.
   */
  val nullString: String = if (legacyCastComplexTypesToString) "" else "null"

  /**
   * Returns whether a decimal value with exponents should be
   * converted to a plain string, exactly like Java BigDecimal.toPlainString()
   * example:
   * plain string value of decimal 1.23E+7 is 12300000
   */
  val useDecimalPlainString: Boolean = ansiMode

  /**
   * Returns whether the binary data should be printed as hex values
   * instead of ascii values
   */
  val useHexFormatForBinary: Boolean = false

  /**
   * Returns whether we should cast using ANSI compliance
   */
  val isAnsiMode: Boolean = ansiMode

  /**
   * Returns whether we should use ANSI compliance when casting a String
   * to Date
   */
  val useAnsiStringToDateMode: Boolean = stringToDateAnsiMode

  /**
   * Returns whether we should use legacy behavior to convert complex types
   * like structs/maps to a String
   */
  val useLegacyComplexTypesToString: Boolean = legacyCastComplexTypesToString
}

object GpuCast {

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

  def doCast(
      input: ColumnView,
      fromDataType: DataType,
      toDataType: DataType,
      options: CastOptions = CastOptions.DEFAULT_CAST_OPTIONS): ColumnVector = {
    if (DataType.equalsStructurally(fromDataType, toDataType)) {
      return input.copyToColumnVector()
    }

    val ansiMode = options.isAnsiMode

    (fromDataType, toDataType) match {
      case (NullType, to) =>
        GpuColumnVector.columnVectorFromNull(input.getRowCount.toInt, to)

      case (DateType, BooleanType | _: NumericType) =>
        // casts from date type to numerics are always null
        GpuColumnVector.columnVectorFromNull(input.getRowCount.toInt, toDataType)

      // Cast to String
      case (DateType | TimestampType | FloatType | DoubleType | BinaryType |
            _: DecimalType | _: ArrayType | _: MapType | _: StructType, StringType) =>
        castToString(input, fromDataType, options)


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
                      Int.MaxValue.toLong, errorMsg = OVERFLOW_MESSAGE)
                  case ShortType =>
                    assertValuesInRange[Long](cv, Short.MinValue.toLong,
                      Short.MaxValue.toLong, errorMsg = OVERFLOW_MESSAGE)
                  case ByteType =>
                    assertValuesInRange[Long](cv, Byte.MinValue.toLong,
                      Byte.MaxValue.toLong, errorMsg = OVERFLOW_MESSAGE)
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
              throw new ArithmeticException(OVERFLOW_MESSAGE)
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
      case (StringType, ByteType | ShortType | IntegerType | LongType) =>
        CastStrings.toInteger(input, ansiMode,
          GpuColumnVector.getNonNestedRapidsType(toDataType))
      case (StringType, FloatType | DoubleType) =>
        CastStrings.toFloat(input, ansiMode,
          GpuColumnVector.getNonNestedRapidsType(toDataType))
      case (StringType, BooleanType | DateType | TimestampType) =>
        withResource(input.strip()) { trimmed =>
          toDataType match {
            case BooleanType =>
              castStringToBool(trimmed, ansiMode)
            case DateType =>
              if (options.useAnsiStringToDateMode) {
                castStringToDateAnsi(trimmed, ansiMode)
              } else {
                castStringToDate(trimmed)
              }
            case TimestampType =>
              castStringToTimestamp(trimmed, ansiMode)
          }
        }
      case (StringType, dt: DecimalType) =>
        CastStrings.toDecimal(input, ansiMode, dt.precision, -dt.scale)

      case (ByteType | ShortType | IntegerType | LongType, dt: DecimalType) =>
        castIntegralsToDecimal(input, dt, ansiMode)

      case (ShortType | IntegerType | LongType | ByteType | StringType, BinaryType) =>
        input.asByteList(true)

      case (ArrayType(nestedFrom, _), ArrayType(nestedTo, _)) =>
        withResource(input.getChildColumnView(0)) { childView =>
          withResource(doCast(childView, nestedFrom, nestedTo, options)) { childColumnVector =>
            withResource(input.replaceListChild(childColumnVector))(_.copyToColumnVector())
          }
        }

      case (from: StructType, to: StructType) =>
        castStructToStruct(from, to, input, options)

      case (from: MapType, to: MapType) =>
        castMapToMap(from, to, input, options)

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
      case (TimestampType, DateType) if options.timeZoneId.isDefined =>
        val zoneId = DateTimeUtils.getZoneId(options.timeZoneId.get)
        withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(input.asInstanceOf[ColumnVector],
            zoneId.normalized())) {
          shifted => shifted.castTo(GpuColumnVector.getNonNestedRapidsType(toDataType))
        }
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
      errorMsg: String = OVERFLOW_MESSAGE)
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

  def castToString(
      input: ColumnView,
      fromDataType: DataType, options: CastOptions): ColumnVector = fromDataType match {
    case StringType => input.copyToColumnVector()
    case DateType => input.asStrings("%Y-%m-%d")
    case TimestampType if options.castToJsonString => castTimestampToJson(input)
    case TimestampType => castTimestampToString(input)
    case FloatType | DoubleType => CastStrings.fromFloat(input)
    case BinaryType => castBinToString(input, options)
    case _: DecimalType => GpuCastShims.CastDecimalToString(input, options.useDecimalPlainString)
    case StructType(fields) => castStructToString(input, fields, options)

    case ArrayType(elementType, _) =>
      castArrayToString(input, elementType, options)
    case from: MapType =>
      castMapToString(input, from, options)
    case _ =>
      input.castTo(GpuColumnVector.getNonNestedRapidsType(StringType))
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
          val prog = new RegexProgram("(\\.[0-9]*[1-9]+)(?:0+)?$")
          firstPass.stringReplaceWithBackrefs(prog, "\\1")
        }
      }
    }
  }

  private def castTimestampToJson(input: ColumnView): ColumnVector = {
    // we fall back to CPU if the JSON timezone is not UTC, so it is safe
    // to hard-code `Z` here for now, but we should really add a timestamp
    // format to CastOptions when we add support for custom formats in
    // https://github.com/NVIDIA/spark-rapids/issues/9602
    input.asStrings("%Y-%m-%dT%H:%M:%S.%3fZ")
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
      options: CastOptions,
      castingBinaryData: Boolean = false): ColumnVector = {

    import options._

    val emptyStr = ""
    val spaceStr = if (options.castToJsonString) "" else " "

    val sepStr = if (useHexFormatForBinary && castingBinaryData) spaceStr
      else if (useLegacyComplexTypesToString || options.castToJsonString) "," else ", "

    withResource(
      Seq(emptyStr, spaceStr, nullString, sepStr).safeMap(Scalar.fromString)
    ) { case Seq(empty, space, nullRep, sep) =>

      val withSpacesIfLegacy = if (!options.castToJsonString && !useLegacyComplexTypesToString) {
        withResource(input.getChildColumnView(0)) {
          _.replaceNulls(nullRep)
        }
      } else {
        // add a space string to each non-null element
        val (strChild, childNotNull, numElements) =
          withResource(input.getChildColumnView(0)) { childCol =>
            closeOnExcept(childCol.replaceNulls(nullRep)) {
              (_, childCol.isNotNull(), childCol.getRowCount.toInt)
            }
          }
        withResource(Seq(strChild, childNotNull)) { _ =>
          val hasSpaces = withResource(ColumnVector.fromScalar(space, numElements)) { spaceCol =>
            ColumnVector.stringConcatenate(Array(spaceCol, strChild))
          }
          withResource(hasSpaces) {
            childNotNull.ifElse(_, strChild)
          }
        }
      }
      val concatenated = withResource(withSpacesIfLegacy) { strChildCol =>
        withResource(input.replaceListChild(strChildCol)) { strArrayCol =>
          withResource(ColumnVector.fromScalar(sep, input.getRowCount.toInt)) {
            strArrayCol.stringConcatenateListElements
          }
        }
      }
      val strCol = withResource(concatenated) {
        _.replaceNulls(empty)
      }
      if (!useLegacyComplexTypesToString) {
        strCol
      } else {
        // If the first char of a string is ' ', remove it (only for legacyCastToString = true)
        withResource(strCol) { _ =>
          withResource(strCol.startsWith(space)) { startsWithSpace =>
            withResource(strCol.substring(1)) { remain =>
              startsWithSpace.ifElse(remain, strCol)
            }
          }
        }
      }
    }
  }

  private def castArrayToString(input: ColumnView,
      elementType: DataType,
      options: CastOptions,
      castingBinaryData: Boolean = false): ColumnVector = {
    // We use square brackets for arrays regardless
    val (leftStr, rightStr) = ("[", "]")
    val emptyStr = ""
    val numRows = input.getRowCount.toInt

    withResource(
      Seq(leftStr, rightStr, emptyStr, options.nullString).safeMap(Scalar.fromString)
    ){ case Seq(left, right, empty, nullRep) =>
      val strChildContainsNull = withResource(input.getChildColumnView(0)) {child =>
        doCast(
          child, elementType, StringType, options)
      }

      val concatenated = withResource(strChildContainsNull) { _ =>
        withResource(input.replaceListChild(strChildContainsNull)) {
          concatenateStringArrayElements(_, options, castingBinaryData)
        }
      }

      // Add brackets to each string. Ex: ["1, 2, 3", "4, 5"] => ["[1, 2, 3]", "[4, 5]"]
      val hasBrackets = withResource(concatenated) { _ =>
        withResource(
          Seq(left, right).safeMap(ColumnVector.fromScalar(_, numRows))
        ) { case Seq(leftColumn, rightColumn) =>
          ColumnVector.stringConcatenate(empty, nullRep, Array(leftColumn, concatenated,
            rightColumn))
        }
      }
      withResource(hasBrackets) {
        _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
      }
    }
  }

  private def castMapToString(
      input: ColumnView,
      from: MapType,
      options: CastOptions): ColumnVector = {

    val numRows = input.getRowCount.toInt

    /**
     * Create a new column with quotes around the supplied string column. Caller
     * is responsible for closing `column`.
     */
    def addQuotes(column: ColumnVector, rowCount: Int): ColumnVector = {
      withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
        withResource(Scalar.fromString("\"")) { quote =>
          withResource(ColumnVector.fromScalar(quote, rowCount)) {
            quoteScalar =>
              columns += quoteScalar.incRefCount()
              columns += escapeJsonString(column)
              columns += quoteScalar.incRefCount()
          }
        }
        withResource(Scalar.fromString("")) { emptyScalar =>
          ColumnVector.stringConcatenate(emptyScalar, emptyScalar, columns.toArray)
        }
      }
    }

    // cast the key column and value column to string columns
    val (strKey, strValue) = withResource(input.getChildColumnView(0)) { kvStructColumn =>
      if (options.castToJsonString) {
        // keys must have quotes around them in JSON mode
        val strKey: ColumnVector = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
          withResource(castToString(keyColumn, from.keyType, options)) { key =>
            addQuotes(key, keyColumn.getRowCount.toInt)
          }
        }
        // string values must have quotes around them in JSON mode, and null values need
        // to be represented by the string literal `null`
        val strValue = closeOnExcept(strKey) { _ =>
          withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
            val dt = valueColumn.getType
            val valueStr = if (dt == DType.STRING || dt.isDurationType || dt.isTimestampType) {
              withResource(castToString(valueColumn, from.valueType, options)) { valueStr =>
                addQuotes(valueStr, valueColumn.getRowCount.toInt)
              }
            } else {
              castToString(valueColumn, from.valueType, options)
            }
            withResource(valueStr) { _ =>
              withResource(Scalar.fromString("null")) { nullScalar =>
                withResource(valueColumn.isNull) { isNull =>
                  isNull.ifElse(nullScalar, valueStr)
                }
              }
            }
          }
        }
        (strKey, strValue)
      } else {
        val strKey = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
          castToString(keyColumn, from.keyType, options)
        }
        val strValue = closeOnExcept(strKey) { _ =>
          withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
            castToString(valueColumn, from.valueType, options)
          }
        }
        (strKey, strValue)
      }
    }

    val (arrowStr, emptyStr, spaceStr) = if (options.castToJsonString) {
      (":", "", "")
    } else {
      ("->", "", " ")
    }

    import options._
    // concatenate the key-value pairs to string
    // Example: ("key", "value") -> "key -> value"
    withResource(
      Seq(leftBracket,
        rightBracket,
        arrowStr,
        emptyStr,
        nullString,
        spaceStr).safeMap(Scalar.fromString)
    ) { case Seq(leftScalar, rightScalar, arrowScalar, emptyScalar, nullScalar, spaceScalar) =>
      val strElements = withResource(Seq(strKey, strValue)) { case Seq(strKey, strValue) =>
        val numElements = strKey.getRowCount.toInt
        withResource(Seq(spaceScalar, arrowScalar).safeMap(ColumnVector.fromScalar(_, numElements))
        ) { case Seq(spaceCol, arrowCol) =>
          if (useLegacyComplexTypesToString) {
            withResource(
              spaceCol.mergeAndSetValidity(BinaryOp.BITWISE_AND, strValue)
            ) { spaceBetweenSepAndVal =>
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
      val strCol = withResource(strElements) { _ =>
        withResource(input.replaceListChild(strElements)) {
          concatenateStringArrayElements(_, options)
        }
      }
      val resPreValidityFix = withResource(strCol) { _ =>
        withResource(
          Seq(leftScalar, rightScalar).safeMap(ColumnVector.fromScalar(_, numRows))
        ) { case Seq(leftCol, rightCol) =>
          ColumnVector.stringConcatenate(
            emptyScalar, nullScalar, Array(leftCol, strCol, rightCol))
        }
      }
      withResource(resPreValidityFix) {
        _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
      }
    }
  }

  private def castStructToString(
      input: ColumnView,
      inputSchema: Array[StructField],
      options: CastOptions): ColumnVector = {

    import options._

    if (options.castToJsonString) {
      return castStructToJsonString(input, inputSchema, options)
    }

    val emptyStr = ""
    val separatorStr = if (useLegacyComplexTypesToString) "," else ", "
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
          columns += castToString(firstColumnView, inputSchema.head.dataType, options)
        }
        for (nonFirstIndex <- 1 until numInputColumns) {
          withResource(input.getChildColumnView(nonFirstIndex)) { nonFirstColumnView =>
            // legacy: ","
            //   3.1+: ", "
            columns += sepColumn.incRefCount()
            val nonFirstColumn = doCast(nonFirstColumnView,
              inputSchema(nonFirstIndex).dataType, StringType, options)
            if (useLegacyComplexTypesToString) {
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

    withResource(Seq(emptyStr, nullString, separatorStr, spaceStr, leftBracket, rightBracket)
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

  /**
   * This is a specialized version of castStructToString that uses JSON format.
   * The main differences are:
   *
   * - Struct field names are included
   * - Null fields are optionally omitted
   */
  def castStructToJsonString(input: ColumnView,
      inputSchema: Array[StructField],
      options: CastOptions): ColumnVector = {

    val rowCount = input.getRowCount.toInt

    def castToJsonAttribute(fieldIndex: Int,
        colon: ColumnVector,
        quote: ColumnVector): ColumnVector = {
      val jsonName = StringEscapeUtils.escapeJson(inputSchema(fieldIndex).name)
      val dt = inputSchema(fieldIndex).dataType
      val needsQuoting = dt == DataTypes.StringType || dt == DataTypes.DateType ||
        dt == DataTypes.TimestampType
      withResource(input.getChildColumnView(fieldIndex)) { cv =>
        withResource(ArrayBuffer.empty[ColumnVector]) { attrColumns =>
          // prefix with quoted column name followed by colon
          withResource(Scalar.fromString("\"" + jsonName + "\"")) { name =>
            attrColumns += ColumnVector.fromScalar(name, rowCount)
            attrColumns += colon.incRefCount()
          }
          if (options.ignoreNullFieldsInStructs) {
            // write the value
            withResource(castToString(cv, inputSchema(fieldIndex).dataType, options)) {
                attrValue =>
              if (needsQuoting) {
                attrColumns += quote.incRefCount()
                attrColumns += escapeJsonString(attrValue)
                attrColumns += quote.incRefCount()
              } else {
                attrColumns += attrValue.incRefCount()
              }
            }
            // now concatenate
            val jsonAttr = withResource(Scalar.fromString("")) { emptyString =>
              ColumnVector.stringConcatenate(emptyString, emptyString, attrColumns.toArray)
            }
            // add an empty string or the attribute
            withResource(jsonAttr) { _ =>
              withResource(cv.isNull) { isNull =>
                withResource(Scalar.fromNull(DType.STRING)) { nullScalar =>
                  isNull.ifElse(nullScalar, jsonAttr)
                }
              }
            }
          } else {
            val jsonAttr = withResource(ArrayBuffer.empty[ColumnVector]) { attrValues =>
              withResource(castToString(cv, inputSchema(fieldIndex).dataType, options)) {
                  attrValue =>
                if (needsQuoting) {
                  attrValues += quote.incRefCount()
                  attrValues += escapeJsonString(attrValue)
                  attrValues += quote.incRefCount()
                  withResource(Scalar.fromString("")) { emptyString =>
                    ColumnVector.stringConcatenate(emptyString, emptyString, attrValues.toArray)
                  }
                } else {
                  attrValue.incRefCount()
                }
              }
            }
            // add attribute value, or null literal string if value is null
            attrColumns += withResource(jsonAttr) { _ =>
              withResource(cv.isNull) { isNull =>
                withResource(Scalar.fromString("null")) { nullScalar =>
                  isNull.ifElse(nullScalar, jsonAttr)
                }
              }
            }
            // now concatenate
            withResource(Scalar.fromString("")) { emptyString =>
              ColumnVector.stringConcatenate(emptyString, emptyString, attrColumns.toArray)
            }
          }
        }
      }
    }

    withResource(Seq("", ",", ":", "\"", "{", "}").safeMap(Scalar.fromString)) {
      case Seq(emptyScalar, commaScalar, columnScalars@_*) =>
            withResource(columnScalars.safeMap(s => ColumnVector.fromScalar(s, rowCount))) {
        case Seq(colon, quote, leftBrace, rightBrace) =>
          val jsonAttrs = withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
            // create one column per attribute, which will either be in the form `"name":value` or
            // empty string for rows that have null values
            if (input.getNumChildren == 1) {
              castToJsonAttribute(0, colon, quote)
            } else {
              for (i <- 0 until input.getNumChildren) {
                columns += castToJsonAttribute(i, colon, quote)
              }
              // concatenate the columns into one string
              withResource(ColumnVector.stringConcatenate(commaScalar,
                emptyScalar, columns.toArray, false))(
                _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input) // original whole row is null
              )
            }
          }
          // now wrap the string with `{` and `}`
          withResource(jsonAttrs) { _ =>
            withResource(ArrayBuffer.empty[ColumnVector]) { columns =>
              columns += leftBrace.incRefCount()
              columns += jsonAttrs.incRefCount()
              columns += rightBrace.incRefCount()
              withResource(ColumnVector.stringConcatenate(emptyScalar,
                emptyScalar, columns.toArray, false))(
                _.mergeAndSetValidity(BinaryOp.BITWISE_AND, input) // original whole row is null
              )
            }
          }
      }
    }
  }

  /**
   * Escape quotes and newlines in a string column. Caller is responsible for closing `cv`.
   */
  private def escapeJsonString(cv: ColumnVector): ColumnVector = {
    val chars = Seq("\r", "\n", "\\", "\"")
    val escaped = chars.map(StringEscapeUtils.escapeJava)
    withResource(ColumnVector.fromStrings(chars: _*)) { search =>
      withResource(ColumnVector.fromStrings(escaped: _*)) { replace =>
        cv.stringReplace(search, replace)
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
      val lowerStripped = withResource(input.strip()) {
        _.lower()
      }
      val sanitizedInput = withResource(lowerStripped) { _ =>
        withResource(lowerStripped.contains(boolStrings)) { validBools =>
          // in ansi mode, fail if any values are not valid bool strings
          if (ansiEnabled) {
            withResource(validBools.all()) { isAllBool =>
              if (isAllBool.isValid && !isAllBool.getBoolean) {
                throw new IllegalStateException(INVALID_INPUT_MESSAGE)
              }
            }
          }
          // replace non-boolean values with null
          withResource(Scalar.fromNull(DType.STRING)) {
            validBools.ifElse(lowerStripped, _)
          }
        }
      }
      withResource(sanitizedInput) { _ =>
        // return true, false, or null, as appropriate
        withResource(ColumnVector.fromStrings(trueStrings: _*)) {
          sanitizedInput.contains
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  def convertDateOrNull(
      input: ColumnView,
      regex: String,
      cudfFormat: String,
      failOnInvalid: Boolean = false): ColumnVector = {

    val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
    val isValidDate = withResource(input.matchesRe(prog)) { isMatch =>
      withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
        isMatch.and(isTimestamp)
      }
    }

    withResource(isValidDate) { _ =>
      if (failOnInvalid) {
        withResource(isValidDate.all()) { all =>
          if (all.isValid && !all.getBoolean) {
            throw new DateTimeException("One or more values is not a valid date")
          }
        }
      }
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

    val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
    val isValidDate = withResource(input.matchesRe(prog)) { isMatch =>
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
    closeOnExcept(result) { _ =>
      val notConverted = withResource(input.isNotNull()) { inputNotNull =>
        withResource(result.isNull()) { resultIsNull =>
          inputNotNull.and(resultIsNull)
        }
      }
      val notConvertedAny = withResource(notConverted) {
        _.any()
      }
      withResource(notConvertedAny) { _ =>
        if (notConvertedAny.isValid && notConvertedAny.getBoolean) {
          throw new DateTimeException(errMessage)
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
  def castStringToDate(sanitizedInput: ColumnVector): ColumnVector = {

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

  def castStringToDateAnsi(input: ColumnVector, ansiMode: Boolean): ColumnVector = {
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
  def convertTimestampOrNull(
      input: ColumnVector,
      regex: String,
      cudfFormat: String): ColumnVector = {

    withResource(Scalar.fromNull(DType.TIMESTAMP_MICROSECONDS)) { orElse =>
      val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
      val isValidTimestamp = withResource(input.matchesRe(prog)) { isMatch =>
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
      val prog = new RegexProgram(regex, CaptureGroups.NON_CAPTURE)
      val isValidTimestamp = withResource(input.matchesRe(prog)) { isMatch =>
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
      val isCudfMatch = Seq(
        cudfFormat2,
        cudfFormat3,
        cudfFormat4
      ).foldLeft(input.isTimestamp(cudfFormat1)) { (isTimestamp, nextFormat) =>
        withResource(isTimestamp) { _ =>
          withResource(input.isTimestamp(nextFormat)) { nextIsTimeStamp =>
            isTimestamp.or(nextIsTimeStamp)
          }
        }
      }

      val isValidTimestamp = withResource(isCudfMatch) { _ =>
        val prog = new RegexProgram(TIMESTAMP_REGEX_FULL, CaptureGroups.NON_CAPTURE)
        withResource(input.matchesRe(prog)) { isRegexMatch =>
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

  def castStringToTimestamp(input: ColumnVector, ansiMode: Boolean): ColumnVector = {

    // special timestamps
    val today = DateUtils.currentDate()
    val todayStr = new SimpleDateFormat("yyyy-MM-dd")
        .format(today * DateUtils.ONE_DAY_SECONDS * 1000L)

    var sanitizedInput = input.incRefCount()

    // prepend today's date to timestamp formats without dates
    sanitizedInput = withResource(sanitizedInput) { _ =>
      val prog = new RegexProgram(TIMESTAMP_REGEX_NO_DATE)
      sanitizedInput.stringReplaceWithBackrefs(prog, s"${todayStr}T\\1")
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
      options: CastOptions): ColumnVector = {
    // For cudf a map is a list of (key, value) structs, but lets keep it in ColumnView as much
    // as possible
    withResource(input.getChildColumnView(0)) { kvStructColumn =>
      val castKey = withResource(kvStructColumn.getChildColumnView(0)) { keyColumn =>
        doCast(keyColumn, from.keyType, to.keyType, options)
      }
      withResource(castKey) { castKey =>
        val castValue = withResource(kvStructColumn.getChildColumnView(1)) { valueColumn =>
          doCast(valueColumn, from.valueType, to.valueType, options)
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
      options: CastOptions): ColumnVector = {
    withResource(new ArrayBuffer[ColumnVector](from.length)) { childColumns =>
      from.indices.foreach { index =>
        childColumns += doCast(
          input.getChildColumnView(index),
          from(index).dataType,
          to(index).dataType, options)
      }
      withResource(ColumnView.makeStructView(childColumns.toSeq: _*)) { casted =>
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

  private def castBinToString(input: ColumnView, options: CastOptions): ColumnVector = {
    if (options.useHexFormatForBinary) {
      withResource(input.getChildColumnView(0)) { dataCol =>
        withResource(dataCol.toHex()) { stringCol =>
          withResource(input.replaceListChild(stringCol)) { cv =>
            castArrayToString(cv, DataTypes.StringType, options, true)
          }
        }
      }
    } else {
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
    val targetType = DecimalUtil.createCudfDecimal(dt)
    val converted = DecimalUtils.floatingPointToDecimal(input, targetType, dt.precision)
    if (ansiMode && converted.hasFailure) {
      converted.result.close()
      throw RapidsErrorUtils.arithmeticOverflowError(OVERFLOW_MESSAGE)
    }
    converted.result
  }

  def fixDecimalBounds(input: ColumnView,
      outOfBounds: ColumnView,
      ansiMode: Boolean): ColumnVector = {
    if (ansiMode) {
      withResource(outOfBounds.any()) { isAny =>
        if (isAny.isValid && isAny.getBoolean) {
          throw RapidsErrorUtils.arithmeticOverflowError(OVERFLOW_MESSAGE)
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

    val cv = withResource(updatedMaxRet) { updatedMax =>
      withResource(Seq(minSeconds, Long.MinValue).safeMap(Scalar.fromLong)) {
        case Seq(minSecondsS, longMinS) =>
          withResource(longInput.lessThan(minSecondsS)) {
            _.ifElse(longMinS, updatedMax)
          }
      }
    }
    withResource(cv) {
      _.castTo(GpuColumnVector.getNonNestedRapidsType(toType))
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
    legacyCastComplexTypesToString: Boolean = false,
    stringToDateAnsiModeEnabled: Boolean = false)
  extends GpuUnaryExpression with TimeZoneAwareExpression with NullIntolerant {

  import GpuCast._

  private val options: CastOptions =
    new CastOptions(legacyCastComplexTypesToString, ansiMode, stringToDateAnsiModeEnabled,
        timeZoneId = timeZoneId)

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
    doCast(input.getBase, input.dataType(), dataType, options)
}
