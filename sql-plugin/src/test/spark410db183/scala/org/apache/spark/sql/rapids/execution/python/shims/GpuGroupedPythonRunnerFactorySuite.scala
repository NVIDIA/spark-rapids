/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
{"spark": "410db183"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import com.nvidia.spark.rapids.FQSuiteName
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class GpuGroupedPythonRunnerFactorySuite extends AnyFunSuite with FQSuiteName {
  private val zeroConfKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.enabled"
  private val maxBytesKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.maxBytesPerSlice"
  private val arrowSlicingKey =
    "spark.databricks.execution.python.arrowBatchSize.slicing.enabled"

  for {
    zeroConfEnabled <- Seq(false, true)
    maxBytes <- Seq(0L, 1L)
    arrowSlicingEnabled <- Seq(false, true)
  } {
    test(s"runner predicate: zeroConf=$zeroConfEnabled, maxBytes=$maxBytes, " +
        s"arrowSlicing=$arrowSlicingEnabled") {
      val actual = GpuGroupedPythonRunnerFactory.shouldUseGroupUdfRunner(
        zeroConfEnabled, maxBytes, arrowSlicingEnabled)
      assert(actual === (zeroConfEnabled && maxBytes > 0L))
    }
  }

  private def getRunner(
      zeroConfEnabled: Boolean,
      arrowSlicingEnabled: Boolean): GpuBasePythonRunner[_] = {
    val conf = new SQLConf()
    conf.setConfString(zeroConfKey, zeroConfEnabled.toString)
    conf.setConfString(maxBytesKey, "1")
    conf.setConfString(arrowSlicingKey, arrowSlicingEnabled.toString)

    GpuGroupedPythonRunnerFactory(
      conf,
      chainedFunc = Seq.empty,
      argOffsets = Array.empty,
      dedupAttrs = new StructType(),
      pythonOutputSchema = new StructType(),
      evalType = PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF).getRunner()
  }

  test("zero-conf with a positive slice size uses the grouped runner") {
    assert(getRunner(zeroConfEnabled = true, arrowSlicingEnabled = false)
      .isInstanceOf[GpuGroupUDFArrowPythonRunner])
  }

  test("Arrow slicing alone uses the window runner") {
    assert(getRunner(zeroConfEnabled = false, arrowSlicingEnabled = true)
      .isInstanceOf[GpuWindowArrowPythonRunner])
  }
}
