/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuExec

import org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec
import org.apache.spark.sql.rapids.ExternalSourceBase

object ExternalSourceShim extends ExternalSourceBase {
  def tagForGpu(
                 cpuExec: ReplaceDataExec,
                 meta: ReplaceDataExecMeta): Unit = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.tagForGpuPlan(cpuExec, meta)
    } else {
      meta.willNotWorkOnGpu(s"Replace data $writeClass is not supported")
    }
  }

  def convertToGpu(
                    cpuExec: ReplaceDataExec,
                    meta: ReplaceDataExecMeta): GpuExec = {
    val writeClass = cpuExec.write.getClass

    if (hasIcebergJar && icebergProvider.isSupportedWrite(writeClass)) {
      icebergProvider.convertToGpuPlan(cpuExec, meta)
    } else {
      throw new IllegalStateException("No GPU conversion")
    }
  }
}

