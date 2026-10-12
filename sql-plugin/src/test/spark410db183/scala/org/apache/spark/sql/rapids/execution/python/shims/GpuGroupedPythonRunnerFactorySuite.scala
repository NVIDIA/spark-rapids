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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.nvidia.spark.rapids.FQSuiteName
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.api.python.{PythonEvalType, PythonWorkerUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class GpuGroupedPythonRunnerFactorySuite extends AnyFunSuite with FQSuiteName {
  private val zeroConfKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.enabled"
  private val maxBytesKey =
    "spark.databricks.execution.pandasZeroConfConversion.groupbyApply.maxBytesPerSlice"
  private val arrowSlicingKey =
    "spark.databricks.execution.python.arrowBatchSize.slicing.enabled"
  private val udfLogMaxEntriesKey = "spark.sql.pyspark.udf.logging.maxEntries"
  private val udfLogLevelKey = "spark.sql.pyspark.udf.logging.logLevel"

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
      arrowSlicingEnabled: Boolean,
      udfLogMaxEntries: Int = 0,
      udfLogLevel: String = "WARNING"): GpuBasePythonRunner[_] = {
    val conf = new SQLConf()
    conf.setConfString(zeroConfKey, zeroConfEnabled.toString)
    conf.setConfString(maxBytesKey, "1")
    conf.setConfString(arrowSlicingKey, arrowSlicingEnabled.toString)
    conf.setConfString(udfLogMaxEntriesKey, udfLogMaxEntries.toString)
    conf.setConfString(udfLogLevelKey, udfLogLevel)

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

  test("factory propagates Python UDF logging configuration") {
    val runner = getRunner(
      zeroConfEnabled = true,
      arrowSlicingEnabled = false,
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO").asInstanceOf[GpuGroupUDFArrowPythonRunner]

    assert(runner.udfLogMaxEntries === 37)
    assert(runner.udfLogLevel === "INFO")
  }

  test("non-grouped runners retain Python UDF logging configuration") {
    val arrowRunner = new GpuArrowPythonRunner(
      funcs = Seq.empty,
      evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
      argOffsets = Array.empty,
      pythonInSchema = new StructType(),
      timeZoneId = "UTC",
      conf = Map.empty,
      maxBatchSize = 1024,
      pythonOutSchema = new StructType(),
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")
    assert(arrowRunner.udfLogMaxEntries === 37)
    assert(arrowRunner.udfLogLevel === "INFO")

    val coGroupedRunner = new GpuCoGroupedArrowPythonRunner(
      funcs = Seq.empty,
      evalType = PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      argOffsets = Array.empty,
      leftSchema = new StructType(),
      rightSchema = new StructType(),
      timeZoneId = "UTC",
      conf = Map.empty,
      batchSize = 1024,
      pythonOutSchema = new StructType(),
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")
    assert(coGroupedRunner.udfLogMaxEntries === 37)
    assert(coGroupedRunner.udfLogLevel === "INFO")
  }

  test("Python UDF logging configuration is serialized to the worker") {
    val bytes = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(bytes)
    WritePythonUDFUtils.writeUDFs(
      dataOut,
      funcs = Seq.empty,
      argOffsets = Array.empty,
      udfLogMaxEntries = 37,
      udfLogLevel = "INFO")

    val dataIn = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
    assert(dataIn.readInt() === 37)
    assert(PythonWorkerUtils.readUTF(dataIn) === "INFO")
    assert(dataIn.readInt() === 0)
    assert(dataIn.available() === 0)
  }
}
