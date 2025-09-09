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
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExecChecks, ExecRule, GpuOverrides, TypeSig}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

/**
 * Utility object for handling InMemoryTableScan version differences.
 * For Spark 3.5.2+, InMemoryTableScanLike trait exists so we can safely enable GPU support.
 */
object InMemoryTableScanUtils {
  
  /**
   * Gets the InMemoryTableScan rule for Spark 3.5.2+.
   * Returns the original rule since InMemoryTableScanLike trait is available.
   */
  def getInMemoryTableScanExecRule: ExecRule[_ <: SparkPlan] = {
    val imtsKey = classOf[InMemoryTableScanExec].asSubclass(classOf[SparkPlan])
    GpuOverrides.commonExecs.getOrElse(imtsKey,
      throw new IllegalStateException("InMemoryTableScan should be overridden by default"))
  }

  def canTableCacheWrapGpuInMemoryTableScan: Boolean = true

  /**
   * Gets the TableCacheQueryStageExec rule for this Spark version.
   */
  def getTableCacheQueryStageExecRule: ExecRule[_ <: SparkPlan] = {
    GpuOverrides.exec[TableCacheQueryStageExec](
      "Table cache query stage that wraps InMemoryTableScan for AQE",
      ExecChecks(TypeSig.all, TypeSig.all),
      (tcqs, conf, p, r) => new TableCacheQueryStageExecMeta(tcqs, conf, p, r))
  }
}
