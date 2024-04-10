/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

object ScanExecShims {
  def tagGpuFileSourceScanExecSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit =
    GpuFileSourceScanExec.tagSupport(meta)

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[FileSourceScanExec](
      "Reading data from files, often from Hive tables",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
          TypeSig.ARRAY + TypeSig.BINARY + TypeSig.DECIMAL_128).nested(),
        TypeSig.all),
      (fsse, conf, p, r) => new FileSourceScanExecMeta(fsse, conf, p, r)),
    GpuOverrides.exec[BatchScanExec](
      "The backend for most file input",
      ExecChecks(
        (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
          TypeSig.DECIMAL_128 + TypeSig.BINARY).nested(),
        TypeSig.all),
      (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r))
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

}
