/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims

object TestTrampolineUtil {
  def toLogicalPlan(df: DataFrame): LogicalPlan = {
    // Use the shim method and handle type casting internally
    val shimDf = df.asInstanceOf[TrampolineConnectShims.DataFrame]
    TrampolineConnectShims.getLogicalPlan(shimDf)
  }

  def toDataFrame(spark: Any, plan: LogicalPlan): DataFrame = {
    // Use the shim method and handle type casting internally
    TrampolineConnectShims.createDataFrame(
      spark.asInstanceOf[TrampolineConnectShims.SparkSession],
      plan).asInstanceOf[DataFrame]
  }
}
