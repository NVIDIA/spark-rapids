/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
{"spark": "330db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.exchange.{EXECUTOR_BROADCAST, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.rapids.GpuElementAtMeta
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExec, GpuBroadcastNestedLoopJoinExec}

object SparkShimImpl extends Spark321PlusDBShims {
  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val elementAtExpr: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuElementAtMeta.elementAtRule(true)
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ DayTimeIntervalShims.exprs ++ RoundingShims.exprs ++ elementAtExpr
  }

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ PythonMapInArrowExecShims.execs

  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Seq(GpuOverrides.dataWriteCmd[CreateDataSourceTableAsSelectCommand](
    "Create table with select command",
    (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[DataWritingCommand]), r)).toMap
  }

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
      Map.empty
  }

  override def reproduceEmptyStringBug: Boolean = false

  override def isExecutorBroadcastShuffle(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
  }

  override def shuffleParentReadsShuffleData(shuffle: ShuffleExchangeLike,
      parent: SparkPlan): Boolean = {
    parent match {
      case _: GpuBroadcastHashJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _: GpuBroadcastNestedLoopJoinExec =>
        shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST)
      case _ => false
    }
  }

  override def addRowShuffleToQueryStageTransitionIfNeeded(c2r: ColumnarToRowTransition,
      sqse: ShuffleQueryStageExec): SparkPlan = {
    val plan = GpuTransitionOverrides.getNonQueryStagePlan(sqse)
    plan match {
      case shuffle: ShuffleExchangeLike if shuffle.shuffleOrigin.equals(EXECUTOR_BROADCAST) =>
        ShuffleExchangeExec(SinglePartition, c2r, EXECUTOR_BROADCAST)
      case _ =>
        c2r
    }
  }
}