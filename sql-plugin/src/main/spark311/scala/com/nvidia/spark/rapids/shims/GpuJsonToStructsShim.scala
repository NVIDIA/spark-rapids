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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.GpuCast
object GpuJsonToStructsShim {

  def castJsonStringToDate(input: ColumnVector, options: Map[String, String]): ColumnVector = {
    options.getOrElse("dateFormat", "yyyy-MM-dd") match {
      case "yyyy-MM-dd" =>
        GpuCast.castStringToDate(input)
      case other =>
        // should be unreachable due to GpuOverrides checks
        throw new IllegalStateException(s"Unsupported dateFormat $other")
    }
  }

}