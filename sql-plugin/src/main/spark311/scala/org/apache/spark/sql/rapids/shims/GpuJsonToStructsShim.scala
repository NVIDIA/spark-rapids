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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{GpuCast, GpuOverrides, RapidsMeta}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.json.GpuJsonUtils
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.rapids.ExceptionTimeParserPolicy

object GpuJsonToStructsShim {
  def tagDateFormatSupport(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
    dateFormat match {
      case None | Some("yyyy-MM-dd") =>
      case dateFormat =>
        meta.willNotWorkOnGpu(s"GpuJsonToStructs unsupported dateFormat $dateFormat")
    }
  }

  def castJsonStringToDate(input: ColumnView, options: JSONOptions): ColumnVector = {
    GpuJsonUtils.optionalDateFormatInRead(options) match {
      case None | Some("yyyy-MM-dd") =>
        withResource(Scalar.fromString(" ")) { space =>
          withResource(input.strip(space)) { trimmed =>
            GpuCast.castStringToDate(trimmed)
          }
        }
      case other =>
        // should be unreachable due to GpuOverrides checks
        throw new IllegalStateException(s"Unsupported dateFormat $other")
    }
  }

  def tagDateFormatSupportFromScan(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
    tagDateFormatSupport(meta, dateFormat)
  }

  def castJsonStringToDateFromScan(input: ColumnView, dt: DType,
      dateFormat: Option[String]): ColumnVector = {
    dateFormat match {
      case None | Some("yyyy-MM-dd") =>
        withResource(input.strip()) { trimmed =>
          GpuCast.castStringToDateAnsi(trimmed, ansiMode =
            GpuOverrides.getTimeParserPolicy == ExceptionTimeParserPolicy)
        }
      case other =>
        // should be unreachable due to GpuOverrides checks
        throw new IllegalStateException(s"Unsupported dateFormat $other")
    }
  }


  def castJsonStringToTimestamp(input: ColumnView,
      options: JSONOptions): ColumnVector = {
    withResource(Scalar.fromString(" ")) { space =>
      withResource(input.strip(space)) { trimmed =>
        // from_json doesn't respect ansi mode
        GpuCast.castStringToTimestamp(trimmed, ansiMode = false)
      }
    }
  }
}
