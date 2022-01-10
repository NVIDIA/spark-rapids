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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.DateUtils.TimestampFormatConversionException
import com.nvidia.spark.rapids.GpuOverrides.extractStringLit

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, TimeZoneAwareExpression}
import org.apache.spark.sql.types._

case class ParseFormatMeta(separator: Char, isTimestamp: Boolean, validRegex: String)

case class RegexReplace(search: String, replace: String)

object GpuToTimestamp {
  // We are compatible with Spark for these formats when the timeParserPolicy is CORRECTED
  // or EXCEPTION. It is possible that other formats may be supported but these are the only
  // ones that we have tests for.
  val CORRECTED_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{2}-\d{2}\Z"),
    "yyyy/MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}\Z"),
    "yyyy-MM" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{2}\Z"),
    "yyyy/MM" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{2}\Z"),
    "dd/MM/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}/\d{4}\Z"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta('-', isTimestamp = true,
      raw"\A\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\Z"),
    "MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z"),
    "dd-MM" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{2}-\d{2}\Z"),
    "dd/MM" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{2}/\d{2}\Z")
  )
  // We are compatible with Spark for these formats when the timeParserPolicy is LEGACY. It
  // is possible that other formats may be supported but these are the only ones that we have
  // tests for.
  val LEGACY_COMPATIBLE_FORMATS = Map(
    "yyyy-MM-dd" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{4}-\d{1,2}-\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{4}/\d{1,2}/\d{1,2}(\D|\s|\Z)"),
    "dd-MM-yyyy" -> ParseFormatMeta('-', isTimestamp = false,
      raw"\A\d{1,2}-\d{1,2}-\d{4}(\D|\s|\Z)"),
    "dd/MM/yyyy" -> ParseFormatMeta('/', isTimestamp = false,
      raw"\A\d{1,2}/\d{1,2}/\d{4}(\D|\s|\Z)"),
    "yyyy-MM-dd HH:mm:ss" -> ParseFormatMeta('-', isTimestamp = true,
      raw"\A\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)"),
    "yyyy/MM/dd HH:mm:ss" -> ParseFormatMeta('/', isTimestamp = true,
      raw"\A\d{4}/\d{1,2}/\d{1,2}[ T]\d{1,2}:\d{1,2}:\d{1,2}(\D|\s|\Z)")
  )
}

abstract class UnixTimeExprMeta[A <: BinaryExpression with TimeZoneAwareExpression]
   (expr: A, conf: RapidsConf,
   parent: Option[RapidsMeta[_, _]],
   rule: DataFromReplacementRule)
  extends BinaryExprMeta[A](expr, conf, parent, rule) {

  def shouldFallbackOnAnsiTimestamp: Boolean

  var sparkFormat: String = _
  var strfFormat: String = _
  override def tagExprForGpu(): Unit = {
    checkTimeZoneId(expr.timeZoneId)

    if (shouldFallbackOnAnsiTimestamp) {
      willNotWorkOnGpu("ANSI mode is not supported")
    }

    // Date and Timestamp work too
    if (expr.right.dataType == StringType) {
      extractStringLit(expr.right) match {
        case Some(rightLit) =>
          sparkFormat = rightLit
          if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy) {
            try {
              // try and convert the format to cuDF format - this will throw an exception if
              // the format contains unsupported characters or words
              strfFormat = DateUtils.toStrf(sparkFormat,
                expr.left.dataType == DataTypes.StringType)
              // format parsed ok but we have no 100% compatible formats in LEGACY mode
              if (GpuToTimestamp.LEGACY_COMPATIBLE_FORMATS.contains(sparkFormat)) {
                // LEGACY support has a number of issues that mean we cannot guarantee
                // compatibility with CPU
                // - we can only support 4 digit years but Spark supports a wider range
                // - we use a proleptic Gregorian calender but Spark uses a hybrid Julian+Gregorian
                //   calender in LEGACY mode
                // Spark 2.x - ansi not available
                /*
                if (SQLConf.get.ansiEnabled) {
                  willNotWorkOnGpu("LEGACY format in ANSI mode is not supported on the GPU")
                } else */
                 if (!conf.incompatDateFormats) {
                  willNotWorkOnGpu(s"LEGACY format '$sparkFormat' on the GPU is not guaranteed " +
                    s"to produce the same results as Spark on CPU. Set " +
                    s"${RapidsConf.INCOMPATIBLE_DATE_FORMATS.key}=true to force onto GPU.")
                }
              } else {
                willNotWorkOnGpu(s"LEGACY format '$sparkFormat' is not supported on the GPU.")
              }
            } catch {
              case e: TimestampFormatConversionException =>
                willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
            }
          } else {
            try {
              // try and convert the format to cuDF format - this will throw an exception if
              // the format contains unsupported characters or words
              strfFormat = DateUtils.toStrf(sparkFormat,
                expr.left.dataType == DataTypes.StringType)
              // format parsed ok, so it is either compatible (tested/certified) or incompatible
              if (!GpuToTimestamp.CORRECTED_COMPATIBLE_FORMATS.contains(sparkFormat) &&
                  !conf.incompatDateFormats) {
                willNotWorkOnGpu(s"CORRECTED format '$sparkFormat' on the GPU is not guaranteed " +
                  s"to produce the same results as Spark on CPU. Set " +
                  s"${RapidsConf.INCOMPATIBLE_DATE_FORMATS.key}=true to force onto GPU.")
              }
            } catch {
              case e: TimestampFormatConversionException =>
                willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
            }
          }
        case None =>
          willNotWorkOnGpu("format has to be a string literal")
      }
    }
  }
}

