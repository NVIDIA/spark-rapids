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

import java.time.DateTimeException

import scala.collection.mutable.ListBuffer
import scala.math.max

import com.nvidia.spark.rapids.DateUtils.{toStrf, TimestampFormatConversionException}
import com.nvidia.spark.rapids.shims.GpuTypeShims
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{ExceptionTimeParserPolicy, GpuToTimestamp, LegacyTimeParserPolicy}
import org.apache.spark.sql.types.{DataTypes, DecimalType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuTextBasedDateUtils {

  private val supportedDateFormats = Set(
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "yyyy-MM",
    "yyyy/MM",
    "MM-yyyy",
    "MM/yyyy",
    "MM-dd-yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "dd/MM/yyyy"
  )

  private val supportedTsPortionFormats = Set(
    "HH:mm:ss.SSSXXX",
    "HH:mm:ss[.SSS][XXX]",
    "HH:mm:ss[.SSSXXX]",
    "HH:mm",
    "HH:mm:ss",
    "HH:mm[:ss]",
    "HH:mm:ss.SSS",
    "HH:mm:ss[.SSS]"
  )

  def tagCudfFormat(
      meta: RapidsMeta[_, _],
      sparkFormat: String,
      parseString: Boolean): Unit = {
    if (GpuOverrides.getTimeParserPolicy == LegacyTimeParserPolicy) {
      try {
        // try and convert the format to cuDF format - this will throw an exception if
        // the format contains unsupported characters or words
        toCudfFormats(sparkFormat, parseString)
        // format parsed ok but we have no 100% compatible formats in LEGACY mode
        if (GpuToTimestamp.LEGACY_COMPATIBLE_FORMATS.contains(sparkFormat)) {
          // LEGACY support has a number of issues that mean we cannot guarantee
          // compatibility with CPU
          // - we can only support 4 digit years but Spark supports a wider range
          // - we use a proleptic Gregorian calender but Spark uses a hybrid Julian+Gregorian
          //   calender in LEGACY mode
          //  Spark 2.x doesn't have ansi
          // if (SQLConf.get.ansiEnabled) {
          //   meta.willNotWorkOnGpu("LEGACY format in ANSI mode is not supported on the GPU")
          if (!meta.conf.incompatDateFormats) {
            meta.willNotWorkOnGpu(s"LEGACY format '$sparkFormat' on the GPU is not guaranteed " +
              s"to produce the same results as Spark on CPU. Set " +
              s"${RapidsConf.INCOMPATIBLE_DATE_FORMATS.key}=true to force onto GPU.")
          }
        } else {
          meta.willNotWorkOnGpu(s"LEGACY format '$sparkFormat' is not supported on the GPU.")
        }
      } catch {
        case e: TimestampFormatConversionException =>
          meta.willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
      }
    } else {
      val parts = sparkFormat.split("'T'", 2)
      if (parts.isEmpty) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      if (parts.headOption.exists(h => !supportedDateFormats.contains(h))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      if (parts.length > 1 && !supportedTsPortionFormats.contains(parts(1))) {
        meta.willNotWorkOnGpu(s"the timestamp format '$sparkFormat' is not supported")
      }
      try {
        // try and convert the format to cuDF format - this will throw an exception if
        // the format contains unsupported characters or words
        toCudfFormats(sparkFormat, parseString)
      } catch {
        case e: TimestampFormatConversionException =>
          meta.willNotWorkOnGpu(s"Failed to convert ${e.reason} ${e.getMessage}")
      }
    }
  }

  /**
   * Get the list of all cuDF formats that need to be checked for when parsing timestamps. The
   * returned formats must be ordered such that the first format is the most lenient and the
   * last is the least lenient.
   *
   * For example, the spark format `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]` would result in the
   * following cuDF formats being returned, in this order:
   *
   * - `%Y-%m-%d`
   * - `%Y-%m-%dT%H:%M`
   * - `%Y-%m-%dT%H:%M:%S`
   * - `%Y-%m-%dT%H:%M:%S.%f`
   */
  def toCudfFormats(sparkFormat: String, parseString: Boolean): Seq[String] = {
    val hasZsuffix = sparkFormat.endsWith("Z")
    val formatRoot = if (hasZsuffix) {
      sparkFormat.substring(0, sparkFormat.length-1)
    } else {
      sparkFormat
    }

    // strip off suffixes that cuDF will not recognize
    val cudfSupportedFormat = formatRoot
      .replace("'T'", "T")
      .replace("[.SSSXXX]", "")
      .replace("[.SSS][XXX]", "")
      .replace("[.SSS]", "")
      .replace("[.SSSSSS]", "")
      .replace(".SSSXXX", "")
      .replace(".SSS", "")
      .replace("[:ss]", "")

    val cudfFormat = toStrf(cudfSupportedFormat, parseString)
    val suffix = if (hasZsuffix) "Z" else ""

    val optionalFractional = Seq("[.SSS][XXX]", "[.SSS]", "[.SSSSSS]", "[.SSS][XXX]",
      ".SSSXXX", ".SSS")
    val baseFormats = if (optionalFractional.exists(formatRoot.endsWith)) {
      val cudfFormat1 = cudfFormat + suffix
      val cudfFormat2 = cudfFormat + ".%f" + suffix
      Seq(cudfFormat1, cudfFormat2)
    } else if (formatRoot.endsWith("[:ss]")) {
      Seq(cudfFormat + ":%S" + suffix)
    } else {
      Seq(cudfFormat)
    }

    val pos = baseFormats.head.indexOf('T')
    val formatsIncludingDateOnly = if (pos == -1) {
      baseFormats
    } else {
      Seq(baseFormats.head.substring(0, pos)) ++ baseFormats
    }

    // seconds are always optional in Spark
    val formats = ListBuffer[String]()
    for (fmt <- formatsIncludingDateOnly) {
      if (fmt.contains(":%S") && !fmt.contains("%f")) {
        formats += fmt.replace(":%S", "")
      }
      formats += fmt
    }
    formats
  }

}
