/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.{GpuCast, RapidsMeta}

import org.apache.spark.sql.catalyst.json.GpuJsonUtils

object GpuJsonToStructsShim {
  def tagDateFormatSupport(meta: RapidsMeta[_, _, _], dateFormat: Option[String]): Unit = {
    dateFormat match {
      case None | Some("yyyy-MM-dd") =>
        // this is fine
      case dateFormat =>
        meta.willNotWorkOnGpu(s"GpuJsonToStructs unsupported dateFormat $dateFormat")
    }
  }

  def castJsonStringToDate(input: ColumnVector, options: Map[String, String]): ColumnVector = {
    GpuJsonUtils.dateFormatInRead(options) match {
      case "yyyy-MM-dd" =>
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

  def castJsonStringToDateFromScan(input: ColumnVector, dt: DType, dateFormat: Option[String],
      failOnInvalid: Boolean): ColumnVector = {
    dateFormat match {
      case None | Some("yyyy-MM-dd") =>
        withResource(input.strip()) { trimmed =>
          GpuCast.castStringToDate(trimmed)
        }
      case other =>
        // should be unreachable due to GpuOverrides checks
        throw new IllegalStateException(s"Unsupported dateFormat $other")
    }
  }

  def castJsonStringToTimestamp(input: ColumnVector,
      options: Map[String, String]): ColumnVector = {
    withResource(Scalar.fromString(" ")) { space =>
      withResource(input.strip(space)) { trimmed =>
        // from_json doesn't respect ansi mode
        GpuCast.castStringToTimestamp(trimmed, ansiMode = false)
      }
    }
  }
}