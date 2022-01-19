/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types._

/** Meta-data for cast and ansi_cast. */
final class CastExprMeta[INPUT <: Cast](
    cast: INPUT,
    val ansiEnabled: Boolean,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
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
  // 2.x doesn't have the SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING config, so set it to true
  val legacyCastToString: Boolean = true

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
        if (dt.precision > GpuOverrides.DECIMAL128_MAX_PRECISION) {
          willNotWorkOnGpu(s"decimal to string with a " +
              s"precision > ${GpuOverrides.DECIMAL128_MAX_PRECISION} is not supported yet")
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
      case (_: StringType, _: DateType) =>
        // NOOP for anything prior to 3.2.0
      case (_: StringType, dt:DecimalType) =>
        // Spark 2.x: removed check for
        // !ShimLoader.getSparkShims.isCastingStringToNegDecimalScaleSupported
        // this dealt with handling a bug fix that is only in newer versions of Spark
        // (https://issues.apache.org/jira/browse/SPARK-37451)
        // Since we don't know what version of Spark 3 they will be using
        // just always say it won't work and they can hopefully figure it out from warning.
        if (dt.scale < 0) {
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

      case _ =>
    }
  }

  def buildTagMessage(entry: ConfEntry[_]): String = {
    s"${entry.doc}. To enable this operation on the GPU, set ${entry.key} to true."
  }
}
