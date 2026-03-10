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
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.adaptive.QueryStageExec

/**
 * Databricks 17.3 version where getRuntimeStatistics has compile-time access restrictions.
 * The method exists and is public at runtime, but compile-time metadata shows it as protected,
 * so we use reflection to access it.
 */
object QueryStageRowCountShims {
  private lazy val getRuntimeStatisticsMethod = {
    try {
      Some(classOf[QueryStageExec].getMethod("getRuntimeStatistics"))
    } catch {
      case _: NoSuchMethodException => None
    }
  }

  def getRowCount(qse: QueryStageExec): Option[BigInt] = {
    try {
      getRuntimeStatisticsMethod.flatMap { method =>
        method.invoke(qse).asInstanceOf[Statistics].rowCount
      }
    } catch {
      case _: Exception => None
    }
  }
}

