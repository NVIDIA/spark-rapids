/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * Extension point to enable GPU SQL processing.
 */
class SQLExecPlugin extends (SparkSessionExtensions => Unit) with Logging {
  private val strategyRules: Strategy = ShimLoader.newStrategyRules()

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(columnarOverrides)
    extensions.injectQueryStagePrepRule(queryStagePrepOverrides)
    extensions.injectPlannerStrategy(_ => strategyRules)
  }

  private def columnarOverrides(sparkSession: SparkSession): ColumnarRule = {
    ShimLoader.newColumnarOverrideRules()
  }

  private def queryStagePrepOverrides(sparkSession: SparkSession): Rule[SparkPlan] = {
    ShimLoader.newGpuQueryStagePrepOverrides()
  }
}
