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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Cast, CastBase, Expression, NullIntolerant, TimeZoneAwareExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
class CastExprMeta[INPUT <: CastBase](
    cast: INPUT,
    ansiEnabled: Boolean,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends UnaryExprMeta[INPUT](cast, conf, parent, rule) {

  val fromType = cast.child.dataType
  val toType = cast.dataType
  var legacyCastToString = ShimLoader.getSparkShims.getLegacyComplexTypeToString()

  override def tagExprForGpu(): Unit = {
    recursiveTagExprForGpuCheck(fromType)
  }

  def recursiveTagExprForGpuCheck(fromDataType: DataType) {
    if (!conf.isCastFloatToDecimalEnabled && toType.isInstanceOf[DecimalType] &&
      (fromDataType == DataTypes.FloatType || fromDataType == DataTypes.DoubleType)) {
      willNotWorkOnGpu("the GPU will use a different strategy from Java's BigDecimal to convert " +
        "floating point data types to decimals and this can produce results that slightly " +
        "differ from the default behavior in Spark.  To enable this operation on the GPU, set " +
        s"${RapidsConf.ENABLE_CAST_FLOAT_TO_DECIMAL} to true.")
    }
    if (!conf.isCastFloatToStringEnabled && toType == DataTypes.StringType &&
      (fromDataType == DataTypes.FloatType || fromDataType == DataTypes.DoubleType)) {
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
    if (!conf.isCastStringToTimestampEnabled && fromType == DataTypes.StringType
      && toType == DataTypes.TimestampType) {
      willNotWorkOnGpu("the GPU only supports a subset of formats " +
        "when casting strings to timestamps. Refer to the CAST documentation " +
        "for more details. To enable this operation on the GPU, set" +
        s" ${RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP} to true.")
    }
    // FIXME: https://github.com/NVIDIA/spark-rapids/issues/2019
    if (!conf.isCastStringToDecimalEnabled && cast.child.dataType == DataTypes.StringType &&
        cast.dataType.isInstanceOf[DecimalType]) {
      willNotWorkOnGpu("Currently string to decimal type on the GPU might produce results which " +
        "slightly differed from the correct results when the string represents any number " +
        "exceeding the max precision that CAST_STRING_TO_FLOAT can keep. For instance, the GPU " +
        "returns 99999999999999987 given input string \"99999999999999999\". The cause of " +
        "divergence is that we can not cast strings containing scientific notation to decimal " +
        "directly. So, we have to cast strings to floats firstly. Then, cast floats to decimals. " +
        "The first step may lead to precision loss. To enable this operation on the GPU, set " +
        s" ${RapidsConf.ENABLE_CAST_STRING_TO_FLOAT} to true.")
    }
    if (fromDataType.isInstanceOf[StructType]) {
      val checks = rule.getChecks.get.asInstanceOf[CastChecks]
      fromDataType.asInstanceOf[StructType].foreach{field =>
        recursiveTagExprForGpuCheck(field.dataType)
        if (toType == StringType) {
          if (!checks.gpuCanCast(field.dataType, toType)) {
            willNotWorkOnGpu(s"Unsupported type ${field.dataType} found in Struct column. " +
              s"Casting ${field.dataType} to ${toType} not currently supported. Refer to " +
              "CAST documentation for more details.")
          }
        }
      }
    }
  }

  def buildTagMessage(entry: ConfEntry[_]): String = {
    s"${entry.doc}. To enable this operation on the GPU, set ${entry.key} to true."
  }

  override def convertToGpu(child: Expression): GpuExpression =
    GpuCast(child, toType, ansiEnabled, cast.timeZoneId, legacyCastToString)
}

object GpuCast {

  private val DATE_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{2}\\-\\d{2}([ T](:?[\\r\\n]|.)*)?\\Z"

  private val TIMESTAMP_REGEX_YYYY_MM = "\\A\\d{4}\\-\\d{2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_YYYY_MM_DD = "\\A\\d{4}\\-\\d{2}\\-\\d{2}[ ]?\\Z"
  private val TIMESTAMP_REGEX_NO_DATE = "\\A[T]?(\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z)\\Z"

  /**
   * The length of a timestamp with 6 digits for microseconds followed by 'Z', such
   * as "2020-01-01T12:34:56.123456Z".
   */
  private val FULL_TIMESTAMP_LENGTH = 27

  /**
   * Regex to match timestamps with or without trailing zeros.
   */
  private val TIMESTAMP_TRUNCATE_REGEX = "^([0-9]{4}-[0-9]{2}-[0-9]{2} " +
    "[0-9]{2}:[0-9]{2}:[0-9]{2})" +
    "(.[1-9]*(?:0)?[1-9]+)?(.0*[1-9]+)?(?:.0*)?$"

  val INVALID_INPUT_MESSAGE = "Column contains at least one value that is not in the " +
    "required range"

  val INVALID_FLOAT_CAST_MSG = "At least one value is either null or is an invalid number"
}

/**
 * Casts using the GPU
 */
case class GpuCast(
    child: Expression,
    dataType: DataType,
    ansiMode: Boolean = false,
    timeZoneId: Option[String] = None,
    legacyCastToString: Boolean = false)
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

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    (input.dataType(), dataType) match {
      // Filter out casts to Decimal that utilize the ColumnVector to avoid a copy
      case (ShortType | IntegerType | LongType, dt: DecimalType) =>
        castIntegralsToDecimal(input.getBase, dt)

      case (FloatType | DoubleType, dt: DecimalType) =>
        castFloatsToDecimal(input.getBase, dt)

      case (from: DecimalType, to: DecimalType) =>
        castDecimalToDecimal(input.getBase, from, to)

      case _ =>
        doColumnar(input.getBase, input.dataType())
    }
  }

  def doColumnar(input: ColumnView, sparkType: DataType): ColumnVector = {
    (sparkType, dataType) match {
      case (NullType, to) =>
        withResource(GpuScalar.from(null, to)) { scalar =>
          ColumnVector.fromScalar(scalar, input.getRowCount.toInt)
        }
      case (DateType, BooleanType | _: NumericType) =>
        // casts from date type to numerics are always null
        withResource(GpuScalar.from(null, dataType)) { scalar =>
          ColumnVector.fromScalar(scalar, input.getRowCount.toInt)
        }
      case (DateType, StringType) =>
        input.asStrings("%Y-%m-%d")
      case (TimestampType, FloatType | DoubleType) =>
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromDouble(1000000)) { microsPerSec =>
            // Use trueDiv to ensure cast to double before division for full precision
            asLongs.trueDiv(microsPerSec, GpuColumnVector.getNonNestedRapidsType(dataType))
          }
        }
      case (TimestampType, ByteType | ShortType | IntegerType) =>
        // normally we would just do a floordiv here, but cudf downcasts the operands to
        // the output type before the divide.  https://github.com/rapidsai/cudf/issues/2574
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromInt(1000000)) { microsPerSec =>
            withResource(asLongs.floorDiv(microsPerSec, DType.INT64)) { cv =>
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
              cv.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
            }
          }
        }
      case (TimestampType, _: LongType) =>
        withResource(input.castTo(DType.INT64)) { asLongs =>
          withResource(Scalar.fromInt(1000000)) {  microsPerSec =>
            asLongs.floorDiv(microsPerSec, GpuColumnVector.getNonNestedRapidsType(dataType))
          }
        }
      case (TimestampType, StringType) =>
        castTimestampToString(input)
      case (StructType(fields), StringType) =>
        castStructToString(input, legacyCastToString, fields)

      // ansi cast from larger-than-integer integral types, to integer
      case (LongType, IntegerType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from larger-than-short integral types, to short
      case (LongType|IntegerType, ShortType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from larger-than-byte integral types, to byte
      case (LongType|IntegerType|ShortType, ByteType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from floating-point types, to byte
      case (FloatType|DoubleType, ByteType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromByte(Byte.MinValue),
          Scalar.fromByte(Byte.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from floating-point types, to short
      case (FloatType|DoubleType, ShortType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromShort(Short.MinValue),
          Scalar.fromShort(Short.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from floating-point types, to integer
      case (FloatType|DoubleType, IntegerType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromInt(Int.MinValue),
          Scalar.fromInt(Int.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      // ansi cast from floating-point types, to long
      case (FloatType|DoubleType, LongType) if ansiMode =>
        assertValuesInRange(input, Scalar.fromLong(Long.MinValue),
          Scalar.fromLong(Long.MaxValue))
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))

      case (FloatType | DoubleType, TimestampType) =>
        // Spark casting to timestamp from double assumes value is in microseconds
        withResource(Scalar.fromInt(1000000)) { microsPerSec =>
          withResource(input.nansToNulls()) { inputWithNansToNull =>
            withResource(FloatUtils.infinityToNulls(inputWithNansToNull)) {
              inputWithoutNanAndInfinity =>
                if (sparkType == FloatType &&
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
      case (BooleanType, TimestampType) =>
        // cudf requires casting to a long first.
        withResource(input.castTo(DType.INT64)) { longs =>
          longs.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
        }
      case (BooleanType | ByteType | ShortType | IntegerType, TimestampType) =>
        // cudf requires casting to a long first
        withResource(input.castTo(DType.INT64)) { longs =>
          withResource(longs.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
            timestampSecs.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
          }
        }
      case (_: NumericType, TimestampType) =>
        // Spark casting to timestamp assumes value is in seconds, but timestamps
        // are tracked in microseconds.
        withResource(input.castTo(DType.TIMESTAMP_SECONDS)) { timestampSecs =>
          timestampSecs.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
        }
      case (FloatType, LongType) | (DoubleType, IntegerType | LongType) =>
        // Float.NaN => Int is casted to a zero but float.NaN => Long returns a small negative
        // number Double.NaN => Int | Long, returns a small negative number so Nans have to be
        // converted to zero first
        withResource(FloatUtils.nanToZero(input)) { inputWithNansToZero =>
          inputWithNansToZero.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
        }
      case (FloatType|DoubleType, StringType) =>
        castFloatingTypeToString(input)
      case (StringType, BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType
                        | DoubleType | DateType | TimestampType) =>
        withResource(input.strip()) { trimmed =>
          dataType match {
            case BooleanType =>
              castStringToBool(trimmed, ansiMode)
            case DateType =>
              castStringToDate(trimmed)
            case TimestampType =>
              castStringToTimestamp(trimmed)
            case FloatType | DoubleType =>
              castStringToFloats(trimmed, ansiMode,
                GpuColumnVector.getNonNestedRapidsType(dataType))
            case ByteType | ShortType | IntegerType | LongType =>
              castStringToInts(trimmed, ansiMode,
                GpuColumnVector.getNonNestedRapidsType(dataType))
          }
        }
      case (StringType, dt: DecimalType) =>
        // To apply HALF_UP rounding strategy during casting to decimal, we firstly cast
        // string to fp64. Then, cast fp64 to target decimal type to enforce HALF_UP rounding.
        withResource(input.strip()) { trimmed =>
          withResource(castStringToFloats(trimmed, ansiMode, DType.FLOAT64)) { fp =>
            castFloatsToDecimal(fp, dt)
          }
        }

      case (ShortType | IntegerType | LongType | ByteType | StringType, BinaryType) =>
        input.asByteList(true)

      case (ShortType | IntegerType | LongType, dt: DecimalType) =>
        withResource(input.copyToColumnVector()) { inputVector =>
          castIntegralsToDecimal(inputVector, dt)
        }

      case (FloatType | DoubleType, dt: DecimalType) =>
        withResource(input.copyToColumnVector()) { inputVector =>
          castFloatsToDecimal(inputVector, dt)
        }

      case (from: DecimalType, to: DecimalType) =>
        castDecimalToDecimal(input.copyToColumnVector(), from, to)

      case (_: DecimalType, StringType) =>
        input.castTo(DType.STRING)

      case _ =>
        input.castTo(GpuColumnVector.getNonNestedRapidsType(dataType))
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
          if (isAny.getBoolean) {
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
  private def replaceOutOfRangeValues(values: ColumnVector,
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

  private def legacyStructToString(input: ColumnView,
    inputSchema: Array[StructField]): ColumnVector = {
    var separatorColumn: ColumnVector = null
    var spaceColumn: ColumnVector = null
    val columns: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]()
    // coreColumns tracks the casted child columns
    val coreColumns: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]()

    try {
      withResource(GpuScalar.from(",", StringType)) { separatorScalar =>
        separatorColumn = ColumnVector.fromScalar(separatorScalar, input.getRowCount.toInt)
      }
      withResource(GpuScalar.from(" ", StringType)) { separatorScalar =>
        spaceColumn = ColumnVector.fromScalar(separatorScalar, input.getRowCount.toInt)
      }
      withResource(GpuScalar.from("[", StringType)) { bracketScalar =>
        columns += ColumnVector.fromScalar(bracketScalar, input.getRowCount.toInt)
      }

      withResource(input.getChildColumnView(0)) { childView =>
        columns += doColumnar(childView, inputSchema(0).dataType)
        coreColumns += columns.last
      }
      for(childIndex <- 1 until input.getNumChildren()) {
        withResource(input.getChildColumnView(childIndex)) { childView =>
          columns += separatorColumn
          // Copies the whitespace column's validity with the current column's validity.
          // Mimics the Spark null behavior of consecutive commas with no space between them
          columns += spaceColumn.mergeAndSetValidity(BinaryOp.BITWISE_AND, childView)
          columns += doColumnar(childView, inputSchema(childIndex).dataType)
          coreColumns += columns.last
        }
      }
      withResource(GpuScalar.from("]", StringType)) { bracketScalar =>
        columns += ColumnVector.fromScalar(bracketScalar, input.getRowCount.toInt)
      }

      // Merge casted child columns
      withResource(GpuScalar.from("", StringType)) { emptyStrScalar =>
        withResource(ColumnVector.stringConcatenate(emptyStrScalar, emptyStrScalar,
          columns.toArray[ColumnView])) { fullResult =>
          // Merge the validity of all child columns, fully null rows are null in the result
          withResource(fullResult.mergeAndSetValidity(BinaryOp.BITWISE_OR,
            coreColumns: _*)) { nulledResult =>
            // Reflect the struct column's validity vector in the result
            nulledResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, input, nulledResult)
          }
        }
      }
    } finally {
      if (separatorColumn != null) {
        columns.foreach(col =>
          if(col.getNativeView() != separatorColumn.getNativeView()) {
            col.close()
          })
        separatorColumn.close()
      }
      if (spaceColumn != null) {
        spaceColumn.close()
      }
    }
  }

  private def modernStructToString(input: ColumnView,
    inputSchema: Array[StructField]): ColumnVector = {
    var separatorColumn: ColumnVector = null
    var spaceColumn: ColumnVector = null
    val columns: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]()

    try {
      withResource(GpuScalar.from(", ", StringType)) { separatorScalar =>
        separatorColumn = ColumnVector.fromScalar(separatorScalar, input.getRowCount.toInt)
      }
      withResource(GpuScalar.from("{", StringType)) { bracketScalar =>
        columns += ColumnVector.fromScalar(bracketScalar, input.getRowCount.toInt)
      }

      withResource(input.getChildColumnView(0)) { childView =>
        columns += doColumnar(childView, inputSchema(0).dataType)
      }
      for(childIndex <- 1 until input.getNumChildren()) {
        withResource(input.getChildColumnView(childIndex)) { childView =>
          columns += separatorColumn
          columns += doColumnar(childView, inputSchema(childIndex).dataType)
        }
      }
      withResource(GpuScalar.from("}", StringType)) { bracketScalar =>
        columns += ColumnVector.fromScalar(bracketScalar, input.getRowCount.toInt)
      }

      // Merge casted child columns
      withResource(GpuScalar.from("", StringType)) { emptyStrScalar =>
        withResource(GpuScalar.from("null", StringType)) { nullStringScalar =>
          withResource(ColumnVector.stringConcatenate(emptyStrScalar, nullStringScalar,
            columns.toArray[ColumnView])) { fullResult =>
            // Reflect the struct column's validity vector in the result
            fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, input)
          }
        }
      }
    } finally {
      if (separatorColumn != null) {
        columns.foreach(col =>
          if(col.getNativeView() != separatorColumn.getNativeView()) {
            col.close()
          })
        separatorColumn.close()
      }
      if (spaceColumn != null) {
        spaceColumn.close()
      }
    }
  }

  private def castStructToString(input: ColumnView,
    legacyCastToString: Boolean, inputSchema: Array[StructField]): ColumnVector = {
    if (legacyCastToString) {
      legacyStructToString(input, inputSchema)
    } else {
      modernStructToString(input,inputSchema)
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
              sanitizedInput.contains(cvTrue)
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
    val cleaned = if (!ansiEnabled) {
      // TODO would be great to get rid of this regex, but the overflow checks don't work
      //  on the more lenient pattern.
      // To avoid doing the expensive regex all the time, we will first check to see if we need
      // to do it. The only time we do need to do it is when we have a '.' in any of the strings.
      val data = input.getData
      val hasDot = withResource(
        ColumnView.fromDeviceBuffer(data, 0, DType.INT8, data.getLength.toInt)) { childData =>
        withResource(GpuScalar.from('.'.toByte, ByteType)) { dot =>
          childData.contains(dot)
        }
      }
      if (hasDot) {
        withResource(input.extractRe("^([+\\-]?[0-9]+)(?:\\.[0-9]*)?$")) { table =>
          table.getColumn(0).incRefCount()
        }
      } else {
        input.incRefCount()
      }
    } else {
      input.incRefCount()
    }
    withResource(cleaned) { cleaned =>
      withResource(cleaned.isInteger(dType)) { isInt =>
        if (ansiEnabled) {
          withResource(isInt.all()) { allInts =>
            if (!allInts.getBoolean) {
              throw new NumberFormatException(GpuCast.INVALID_INPUT_MESSAGE)
            }
          }
          cleaned.castTo(dType)
        } else {
          withResource(cleaned.castTo(dType)) { parsedInt =>
            withResource(GpuScalar.from(null, dataType)) { nullVal =>
              isInt.ifElse(parsedInt, nullVal)
            }
          }
        }
      }
    }
  }

  def castStringToFloats(
      input: ColumnVector,
      ansiEnabled: Boolean,
      dType: DType): ColumnVector = {

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
                  isNan.ifElse(nan, floatsOnly)
                }
              }
            }
          }
        }
      }
    }
  }

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
   * Parse dates that match the provided length and format. This method does not
   * close the `input` ColumnVector.
   *
   * @param input Input ColumnVector
   * @param len The string length to match against
   * @param cudfFormat The cuDF timestamp format to match against
   * @return ColumnVector containing timestamps for input entries that match both
   *         the length and format, and null for other entries
   */
  def convertFixedLenDateOrNull(
      input: ColumnVector,
      len: Int,
      cudfFormat: String): ColumnVector = {

    withResource(isValidTimestamp(input, len, cudfFormat)) { isValidDate =>
      withResource(input.asTimestampDays(cudfFormat)) { asDays =>
        withResource(Scalar.fromNull(DType.TIMESTAMP_DAYS)) { nullScalar =>
          isValidDate.ifElse(asDays, nullScalar)
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  def convertVarLenDateOr(
      input: ColumnVector,
      regex: String,
      cudfFormat: String,
      orElse: ColumnVector): ColumnVector = {

    withResource(orElse) { orElse =>
      val isValidDate = withResource(input.matchesRe(regex)) { isMatch =>
        withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
          isMatch.and(isTimestamp)
        }
      }
      withResource(isValidDate) { isValidDate =>
        withResource(input.asTimestampDays(cudfFormat)) { asDays =>
          isValidDate.ifElse(asDays, orElse)
        }
      }
    }
  }

  /**
   * Parse dates that match the provided length and format. This method does not
   * close the `input` ColumnVector.
   *
   * @param input Input ColumnVector
   * @param len The string length to match against
   * @param cudfFormat The cuDF timestamp format to match against
   * @return ColumnVector containing timestamps for input entries that match both
   *         the length and format, and null for other entries
   */
  def convertFixedLenDateOr(
      input: ColumnVector,
      len: Int,
      cudfFormat: String,
      orElse: ColumnVector): ColumnVector = {

    withResource(orElse) { orElse =>
      withResource(isValidTimestamp(input, len, cudfFormat)) { isValidDate =>
        withResource(input.asTimestampDays(cudfFormat)) { asDays =>
          isValidDate.ifElse(asDays, orElse)
        }
      }
    }
  }

  private def castStringToDate(input: ColumnVector): ColumnVector = {

    var sanitizedInput = input.incRefCount()

    // replace partial months
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])-", "-0\\1-")
    }

    // replace partial month or day at end of string
    sanitizedInput = withResource(sanitizedInput) { cv =>
      cv.stringReplaceWithBackrefs("-([0-9])([ T](:?[\\r\\n]|.)*)?\\Z", "-0\\1")
    }

    val specialDates = DateUtils.specialDatesDays

    withResource(sanitizedInput) { sanitizedInput =>

      // convert dates that are in valid formats yyyy, yyyy-mm, yyyy-mm-dd
      val converted = convertVarLenDateOr(sanitizedInput, DATE_REGEX_YYYY_MM_DD, "%Y-%m-%d",
        convertFixedLenDateOr(sanitizedInput, 7, "%Y-%m",
          convertFixedLenDateOrNull(sanitizedInput, 4, "%Y")))

      // handle special dates like "epoch", "now", etc.
      specialDates.foldLeft(converted)((prev, specialDate) =>
        specialDateOr(sanitizedInput, specialDate._1, specialDate._2, prev))
    }
  }

  /**
   * Replace special date strings such as "now" with timestampMicros. This method does not
   * close the `input` ColumnVector.
   */
  private def specialTimestampOr(
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
  private def convertFixedLenTimestampOrNull(
      input: ColumnVector,
      len: Int,
      cudfFormat: String): ColumnVector = {

    withResource(isValidTimestamp(input, len, cudfFormat)) { isTimestamp =>
      withResource(Scalar.fromNull(DType.TIMESTAMP_MICROSECONDS)) { nullScalar =>
        withResource(input.asTimestampMicroseconds(cudfFormat)) { asDays =>
          isTimestamp.ifElse(asDays, nullScalar)
        }
      }
    }
  }

  /** This method does not close the `input` ColumnVector. */
  private def convertVarLenTimestampOr(
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
        val isValidLength = withResource(Scalar.fromInt(FULL_TIMESTAMP_LENGTH)) { requiredLen =>
          withResource(input.getCharLengths) { actualLen =>
            requiredLen.equalTo(actualLen)
          }
        }
        withResource(isValidLength) { isValidLength =>
          isValidLength.and(isCudfMatch)
        }
      }

      // we only need to parse with one of the cuDF formats because the parsing code ignores
      // the ' ' or 'T' between the date and time components
      withResource(isValidTimestamp) { isValidTimestamp =>
        withResource(input.asTimestampMicroseconds(cudfFormat1)) { asDays =>
          isValidTimestamp.ifElse(asDays, orElse)
        }
      }
    }
  }

  private def castStringToTimestamp(input: ColumnVector): ColumnVector = {

    // special timestamps
    val today = DateUtils.currentDate()
    val todayStr = new SimpleDateFormat("yyyy-MM-dd")
        .format(today * DateUtils.ONE_DAY_SECONDS * 1000L)
    val specialDates = DateUtils.specialDatesMicros

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
    sanitizedInput = withResource(sanitizedInput) { _ =>
      sanitizedInput.stringReplaceWithBackrefs(TIMESTAMP_REGEX_NO_DATE, s"${todayStr}T\\1")
    }

    withResource(sanitizedInput) { sanitizedInput =>
      // convert dates that are in valid timestamp formats
      val converted =
        convertFullTimestampOr(sanitizedInput,
          convertVarLenTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM_DD, "%Y-%m-%d",
            convertVarLenTimestampOr(sanitizedInput, TIMESTAMP_REGEX_YYYY_MM, "%Y-%m",
              convertFixedLenTimestampOrNull(sanitizedInput, 4, "%Y"))))

      // handle special dates like "epoch", "now", etc.
      val finalResult = specialDates.foldLeft(converted)((prev, specialDate) =>
        specialTimestampOr(sanitizedInput, specialDate._1, specialDate._2, prev))

      // When ANSI mode is enabled, we need to throw an exception if any values could not be
      // converted
      if (ansiMode) {
        closeOnExcept(finalResult) { finalResult =>
          withResource(input.isNotNull) { wasNotNull =>
            withResource(finalResult.isNull) { isNull =>
              withResource(wasNotNull.and(isNull)) { notConverted =>
                if (notConverted.any().getBoolean) {
                  throw new DateTimeException(
                    "One or more values could not be converted to TimestampType")
                }
              }
            }
          }
        }
      }

      finalResult
    }
  }

  /**
   * Determine which timestamps are the specified length and also comply with the specified
   * cuDF format string.
   *
   * @param input Input ColumnVector
   * @param len The string length to match against
   * @param cudfFormat The cuDF timestamp format to match against
   * @return ColumnVector containing booleans representing which entries match both
   *         the length and format
   */
  private def isValidTimestamp(input: ColumnVector, len: Int, cudfFormat: String) = {
    val isCorrectLength = withResource(Scalar.fromInt(len)) { requiredLen =>
      withResource(input.getCharLengths) { actualLen =>
        requiredLen.equalTo(actualLen)
      }
    }
    withResource(isCorrectLength) { isCorrectLength =>
      withResource(input.isTimestamp(cudfFormat)) { isTimestamp =>
        isCorrectLength.and(isTimestamp)
      }
    }
  }

  private def castIntegralsToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {

    // Use INT64 bounds instead of FLOAT64 bounds, which enables precise comparison.
    val (lowBound, upBound) = math.pow(10, dt.precision - dt.scale) match {
      case bound if bound > Long.MaxValue => (Long.MinValue, Long.MaxValue)
      case bound => (-bound.toLong + 1, bound.toLong - 1)
    }
    // At first, we conduct overflow check onto input column.
    // Then, we cast checked input into target decimal type.
    val checkedInput = if (ansiMode) {
      assertValuesInRange(input,
        minValue = Scalar.fromLong(lowBound),
        maxValue = Scalar.fromLong(upBound))
      input.incRefCount()
    } else {
      replaceOutOfRangeValues(input,
        minValue = Scalar.fromLong(lowBound),
        maxValue = Scalar.fromLong(upBound),
        replaceValue = Scalar.fromNull(input.getType))
    }

    withResource(checkedInput) { checked =>
      if (dt.scale < 0) {
        // Rounding is essential when scale is negative,
        // so we apply HALF_UP rounding manually to keep align with CpuCast.
        withResource(checked.castTo(DecimalUtil.createCudfDecimal(dt.precision, 0))) {
          scaleZero => scaleZero.round(dt.scale, ai.rapids.cudf.RoundMode.HALF_UP)
        }
      } else if (dt.scale > 0) {
        // Integer will be enlarged during casting if scale > 0, so we cast input to INT64
        // before casting it to decimal in case of overflow.
        withResource(checked.castTo(DType.INT64)) { long =>
          long.castTo(DecimalUtil.createCudfDecimal(dt.precision, dt.scale))
        }
      } else {
        checked.castTo(DecimalUtil.createCudfDecimal(dt.precision, dt.scale))
      }
    }
  }

  private def castFloatsToDecimal(input: ColumnVector, dt: DecimalType): ColumnVector = {

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
        val containerType = DecimalUtil.createCudfDecimal(dt.precision, (dt.scale + 1))
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

  private def castDecimalToDecimal(input: ColumnVector,
      from: DecimalType,
      to: DecimalType): ColumnVector = {

    val isFrom32Bit = DecimalType.is32BitDecimalType(from)
    val isTo32Bit = DecimalType.is32BitDecimalType(to)
    val cudfDecimal = DecimalUtil.createCudfDecimal(to.precision, to.scale)

    def castCheckedDecimal(checkedInput: ColumnVector): ColumnVector = {
      if (to.scale == from.scale) {
        if (isFrom32Bit == isTo32Bit) {
          checkedInput.incRefCount()
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
        withResource(checkForOverflow(input, to, isFrom32Bit)) { checkedInput =>
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
      withResource(checkForOverflow(input, to, isFrom32Bit)) { checkedInput =>
        castCheckedDecimal(checkedInput)
      }
    }
  }

  def checkForOverflow(
     input: ColumnVector,
     to: DecimalType,
     isFrom32Bit: Boolean): ColumnVector = {

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
      return input.incRefCount()
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
      input.incRefCount()
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
