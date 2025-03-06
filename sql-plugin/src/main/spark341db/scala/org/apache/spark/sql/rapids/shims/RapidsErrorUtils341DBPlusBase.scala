/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids.{DatabricksShimVersion, ShimLoader}

import org.apache.spark.sql.errors.QueryExecutionErrors

trait RapidsErrorUtils341DBPlusBase extends RapidsErrorUtilsBase
  with RapidsQueryErrorUtils {
  def sqlArrayIndexNotStartAtOneError(): RuntimeException = {
    QueryExecutionErrors.invalidIndexOfZeroError(context = null)
  }

  def unexpectedValueForStartInFunctionError(prettyName: String): RuntimeException = {
    QueryExecutionErrors.unexpectedValueForStartInFunctionError(prettyName)
  }

  // TODO: Create an independent shim for spark-350DBPlus to handle the interface divergence:
  // https://github.com/NVIDIA/spark-rapids/issues/12276
  def unexpectedValueForLengthInFunctionError(
      prettyName: String,
      length: Int): RuntimeException = {
    // A temporary version dispatcher to workaround interface conflict on Databricks runtime
    if (isAfter350) {
      unexpectedLengthErrorAfter350(prettyName, length)
    } else {
      unexpectedLengthErrorBefore350(prettyName)
    }
  }

  private val isAfter350 = ShimLoader.getShimVersion match {
    case DatabricksShimVersion(major, minor, _, _) if minor > 4 || major > 3 => true
    case _ => false
  }

  // unexpectedValueForLengthInFunctionError(name: String): RuntimeException
  @transient
  private lazy val unexpectedLengthErrorBefore350: String => RuntimeException = {
    val qeErrorsClz = Class.forName("org.apache.spark.sql.errors.QueryExecutionErrors$")
    val qeErrorsInst = qeErrorsClz.getField("MODULE$").get(null)
    val method = qeErrorsClz.getMethod(
      "unexpectedValueForLengthInFunctionError", classOf[String])

    (name: String) => {
      method.invoke(qeErrorsInst, name).asInstanceOf[RuntimeException]
    }
  }

  // unexpectedValueForLengthInFunctionError(name: String, length: Int): RuntimeException
  @transient
  private lazy val unexpectedLengthErrorAfter350: (String, Int) => RuntimeException = {
    val qeErrorsClz = Class.forName("org.apache.spark.sql.errors.QueryExecutionErrors$")
    val qeErrorsInst = qeErrorsClz.getField("MODULE$").get(null)
    val method = qeErrorsClz.getMethod(
      "unexpectedValueForLengthInFunctionError", classOf[String], Integer.TYPE)

    (name: String, len: Int) => {
      method.invoke(qeErrorsInst, name, Integer.valueOf(len)).asInstanceOf[RuntimeException]
    }
  }
}
