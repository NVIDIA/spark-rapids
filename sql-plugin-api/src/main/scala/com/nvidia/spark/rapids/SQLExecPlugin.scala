/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan, SparkStrategy}

/**
 * Extension point to enable GPU SQL processing.
 */
class SQLExecPlugin extends (SparkSessionExtensions => Unit) {
  private val strategyRules: SparkStrategy = ShimLoader.newStrategyRules()

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(columnarOverrides)
    extensions.injectQueryStagePrepRule(queryStagePrepOverrides)
    extensions.injectPlannerStrategy(_ => strategyRules)
    extensions.injectPostHocResolutionRule(postHocResolutionOverrides)
  }

  private def columnarOverrides(sparkSession: SparkSession): ColumnarRule = {
    ShimLoader.newColumnarOverrideRules()
  }

  private def queryStagePrepOverrides(sparkSession: SparkSession): Rule[SparkPlan] = {
    ShimLoader.newGpuQueryStagePrepOverrides()
  }

  private def postHocResolutionOverrides(sparkSession: SparkSession): Rule[LogicalPlan] = {
    ShimLoader.newGpuPostHocResolutionOverrides(sparkSession)
  }
}
