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

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}

/**
 * Databricks 17.3 version where DynamicPruningExpression added dynamicPruningInfo parameter.
 */
object DynamicPruningShims {
  // Extractor for pattern matching - works for all versions since .child exists in all
  def unapply(expr: Expression): Option[Expression] = expr match {
    case dpe: DynamicPruningExpression => Some(dpe.child)
    case _ => None
  }

  // Constructor for creating new instances
  def apply(child: Expression): DynamicPruningExpression = {
    // Databricks 17.3: Pass None for dynamicPruningInfo (second parameter)
    DynamicPruningExpression(child, None)
  }
}

