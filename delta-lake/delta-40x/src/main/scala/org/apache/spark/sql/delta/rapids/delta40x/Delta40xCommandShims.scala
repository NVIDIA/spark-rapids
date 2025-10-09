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

package org.apache.spark.sql.delta.rapids.delta40x

import org.apache.spark.sql.{Column, DataFrame, SparkSession => SqlSparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, RuntimeReplaceable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.delta.rapids.DeltaCommandShims
import org.apache.spark.sql.nvidia.DFUDFShims
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims

/**
 * Delta 4.0.x implementation of command shims.
 * Uses Spark 4.0's ClassicSparkSession and new Column APIs.
 */
trait Delta40xCommandShims extends DeltaCommandShims {
  override type RunSparkSession = SqlSparkSession
  override type OperationSparkSession = ClassicSparkSession

  override def toOperationSparkSession(spark: RunSparkSession): OperationSparkSession = {
    spark.asInstanceOf[ClassicSparkSession]
  }

  override def getActiveOperationSparkSession: OperationSparkSession = {
    ClassicSparkSession.active
  }

  override def createDataFrame(
      spark: OperationSparkSession,
      logicalPlan: LogicalPlan): DataFrame = {
    TrampolineConnectShims.createDataFrame(spark, logicalPlan)
  }

  override def exprToColumn(expr: Expression): Column = DFUDFShims.exprToColumn(expr)

  override def recacheByPlan(spark: RunSparkSession, plan: LogicalPlan): Unit = {
    val classic = ClassicSparkSession.active
    classic.sharedState.cacheManager.recacheByPlan(classic, plan)
  }

  // GpuOptimisticTransaction helper methods for Spark 4.0.x
  protected def getActiveSparkSession: ClassicSparkSession = {
    TrampolineConnectShims.getActiveSession
  }

  protected def createDataFrameForStats(
      spark: ClassicSparkSession,
      plan: LogicalPlan): DataFrame = {
    TrampolineConnectShims.createDataFrame(TrampolineConnectShims.getActiveSession, plan)
  }

  protected def postProcessStatsExpr(expr: Expression): Expression = {
    // Spark 4.0: JSON functions (e.g. StructsToJson) are RuntimeReplaceable and unevaluable.
    // Replace them with their concrete runtime expressions so they can be evaluated in tasks.
    expr.transform { case rr: RuntimeReplaceable => rr.replacement }
  }
}


