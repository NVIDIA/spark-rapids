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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuGroupedPythonRunnerFactory(
    conf: org.apache.spark.sql.internal.SQLConf,
    chainedFunc: Seq[ChainedPythonFunctions],
    argOffsets: Array[Array[Int]],
    dedupAttrs: StructType,
    pythonOutputSchema: StructType) {
  val sessionLocalTimeZone = conf.sessionLocalTimeZone
  val pythonRunnerConf = ArrowUtilsShim.getPythonRunnerConfMap(conf)

  def getRunner(): GpuBasePythonRunner[ColumnarBatch] = {
    new GpuArrowPythonRunner(
      chainedFunc,
      PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
      argOffsets,
      dedupAttrs,
      sessionLocalTimeZone,
      pythonRunnerConf,
      // The whole group data should be written in a single call, so here is unlimited
      Int.MaxValue,
      pythonOutputSchema)
  }
}