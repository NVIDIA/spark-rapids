/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.{QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.internal.SQLConf

/** Utility methods for manipulating Catalyst classes involved in Adaptive Query Execution */
object AQEUtils {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]

  /** Return a new QueryStageExec reuse instance with updated output attributes */
  def newReuseInstance(sqse: ShuffleQueryStageExec, newOutput: Seq[Attribute]): QueryStageExec = {
    sqse.newReuseInstance(sqse.id, newOutput)
  }

  def isAdaptiveExecutionSupportedInSparkVersion(conf: SQLConf): Boolean = true

  def injectRuntimeOptimizerRule(rule: RuleBuilder): Unit = {
    throw new UnsupportedOperationException(
      "This version of Spark does not support injecting runtime optimizations")
  }

}
