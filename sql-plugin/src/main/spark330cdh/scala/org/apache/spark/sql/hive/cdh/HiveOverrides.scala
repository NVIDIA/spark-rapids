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
{"spark": "330cdh"}
{"spark": "332cdh"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.cdh

import com.nvidia.spark.rapids.{ExecChecks, GpuExec, GpuOverrides, SparkPlanMeta, TypeSig}

import org.apache.spark.sql.hive.execution.HiveTableScanExec

object HiveOverrides {

  // This is a workaround for the mismatches listed in #7423
  def getHiveTableExecRule() = {
    GpuOverrides.exec[HiveTableScanExec](
      desc = "CDH-specific override to disable HiveTableScanExec, " +
        "to intercept Hive delimited text table reads.",
      ExecChecks(TypeSig.commonCudfTypes + TypeSig.DECIMAL_128,
        TypeSig.all),
      (p, conf, parent, r) => new SparkPlanMeta[HiveTableScanExec](p, conf, parent, r) {
        override def convertToGpu(): GpuExec = null // No substitutions.
        override def tagPlanForGpu(): Unit = {
          willNotWorkOnGpu("HiveTableScanExec disabled on CDH due to text parsing " +
            "incompatibilities.")
        }
      }
    )
  }
}
