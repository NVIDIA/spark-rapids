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

package org.apache.spark.sql.delta.rapids

/**
 * Trait to abstract GpuOptimisticTransaction version-specific API differences.
 */
trait GpuOptimisticTransactionShims {
  
  /**
   * Type alias for the version-specific SparkSession type used in writeFiles.
   * Delta 3.3.x uses SparkSession, Delta 4.0.x uses ClassicSparkSession.
   */
  type TxnSparkSession <: org.apache.spark.sql.SparkSession
  
  /**
   * Get the active SparkSession for stats collection.
   * Delta 3.3.x uses the instance field, Delta 4.0.x uses TrampolineConnectShims.
   */
  protected def getActiveSparkSession: TxnSparkSession

  /**
   * Create a DataFrame from a LogicalPlan for stats collection.
   * Delta 3.3.x uses Dataset.ofRows, Delta 4.0.x uses TrampolineConnectShims.
   */
  protected def createDataFrameForStats(
      spark: TxnSparkSession,
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): org.apache.spark.sql.DataFrame

  /**
   * Post-process the analyzed expression from stats collection.
   * Delta 3.3.x returns as-is, Delta 4.0.x unwraps RuntimeReplaceable.
   */
  protected def postProcessStatsExpr(
      expr: org.apache.spark.sql.catalyst.expressions.Expression
  ): org.apache.spark.sql.catalyst.expressions.Expression
}

