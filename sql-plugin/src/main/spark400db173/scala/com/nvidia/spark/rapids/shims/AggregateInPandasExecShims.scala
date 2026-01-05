/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.SparkPlan

/**
 * Databricks 17.3 version where AggregateInPandasExec was renamed to ArrowAggregatePythonExec.
 * See: https://github.com/apache/spark/commit/09dd5dd063b6fae4445b679b137407f3aefd4d0c
 * Provide empty map since the old class name doesn't exist.
 * Helper methods are in AggregateInPandasShims trait.
 */
object AggregateInPandasExecShims {
  val execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Map.empty
}

