/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{DateUtils, GpuCast, GpuOverrides, RapidsMeta}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.json.GpuJsonUtils
import org.apache.spark.sql.rapids.ExceptionTimeParserPolicy

object GpuJsonToStructsShim {

  def tagDateFormatSupport(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
  }

  def castJsonStringToDate(input: ColumnView, options: Map[String, String]): ColumnVector = {
    GpuJsonUtils.optionalDateFormatInRead(options) match {
      case None =>
        // legacy behavior
        withResource(Scalar.fromString(" ")) { space =>
          withResource(input.strip(space)) { trimmed =>
            GpuCast.castStringToDate(trimmed)
          }
        }
      case Some(f) =>
        // from_json does not respect EXCEPTION policy
        jsonStringToDate(input, f, failOnInvalid = false)
    }
  }

  def tagDateFormatSupportFromScan(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
  }

  def castJsonStringToDateFromScan(input: ColumnView, dt: DType,
      dateFormat: Option[String]): ColumnVector = {
    dateFormat match {
      case None =>
        // legacy behavior
        withResource(input.strip()) { trimmed =>
          GpuCast.castStringToDateAnsi(trimmed, ansiMode =
            GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy)
        }
      case Some(f) =>
        jsonStringToDate(input, f,
          GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy)
    }
  }

  private def jsonStringToDate(input: ColumnView, dateFormatPattern: String,
      failOnInvalid: Boolean): ColumnVector = {
    val regexRoot = dateFormatPattern
      .replace("yyyy", raw"\d{4}")
      .replace("MM", raw"\d{2}")
      .replace("dd", raw"\d{2}")
    val cudfFormat = DateUtils.toStrf(dateFormatPattern, parseString = true)
    GpuCast.convertDateOrNull(input, "^" + regexRoot + "$", cudfFormat, failOnInvalid)
  }

  def tagTimestampFormatSupport(meta: RapidsMeta[_, _, _],
      timestampFormat: Option[String]): Unit = {
    // we only support the case where no format is specified
    timestampFormat.foreach(f => meta.willNotWorkOnGpu(s"Unsupported timestampFormat: $f"))
  }

  def castJsonStringToTimestamp(input: ColumnView,
      options: Map[String, String]): ColumnVector = {
    options.get("timestampFormat") match {
      case None =>
        // legacy behavior
        withResource(Scalar.fromString(" ")) { space =>
          withResource(input.strip(space)) { trimmed =>
            // from_json doesn't respect ansi mode
            GpuCast.castStringToTimestamp(trimmed, ansiMode = false)
          }
        }
      case other =>
        // should be unreachable due to GpuOverrides checks
        throw new IllegalStateException(s"Unsupported timestampFormat $other")
      }
  }
}
