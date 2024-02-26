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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

//TODO is this needed? we already have a similar version in spark330db
case class GpuGroupedPythonRunnerFactory(
  conf: org.apache.spark.sql.internal.SQLConf,
  chainedFunc: Seq[ChainedPythonFunctions],
  argOffsets: Array[Array[Int]],
  dedupAttrs: StructType,
  pythonOutputSchema: StructType) {
  // Configs from DB runtime
  val maxBytes = conf.pandasZeroConfConversionGroupbyApplyMaxBytesPerSlice
  val zeroConfEnabled = conf.pandasZeroConfConversionGroupbyApplyEnabled
  val sessionLocalTimeZone = conf.sessionLocalTimeZone
  val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)

  def getRunner(): GpuBasePythonRunner[ColumnarBatch] = {
    if (zeroConfEnabled && maxBytes > 0L) {
      new GpuGroupUDFArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        // The whole group data should be written in a single call, so here is unlimited
        Int.MaxValue,
        pythonOutputSchema)
    } else {
      new GpuArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        argOffsets,
        dedupAttrs,
        sessionLocalTimeZone,
        pythonRunnerConf,
        Int.MaxValue,
        pythonOutputSchema)
    }
  }
}
