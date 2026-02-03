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
package org.apache.spark.sql.rapids

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.execution.BaseSubqueryExec

/**
 * Databricks 17.3 version with resultUpdated() method.
 */
case class GpuScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId) extends GpuScalarSubqueryBase(plan, exprId) {
  
  override def withNewPlan(query: BaseSubqueryExec): GpuScalarSubquery = copy(plan = query)

  // New method in Databricks 17.3
  override def resultUpdated(): Boolean = updated
}

