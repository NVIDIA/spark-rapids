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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.Empty2Null
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{GpuWriteFilesMeta, WriteFilesExec}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null

trait Spark340PlusShims extends Spark331PlusShims {

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GlobalLimitExec](
      "Limiting of results across partitions",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (globalLimit, conf, p, r) =>
        new SparkPlanMeta[GlobalLimitExec](globalLimit, conf, p, r) {
          override def convertToGpu(): GpuExec =
            GpuGlobalLimitExec(
              globalLimit.limit, childPlans.head.convertIfNeeded(), globalLimit.offset)
        }),
    GpuOverrides.exec[CollectLimitExec](
      "Reduce to single partition and apply limit",
      ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.NULL +
          TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP).nested(),
        TypeSig.all),
      (collectLimit, conf, p, r) => new GpuCollectLimitMeta(collectLimit, conf, p, r) {
        override def convertToGpu(): GpuExec =
          GpuGlobalLimitExec(collectLimit.limit,
            GpuShuffleExchangeExec(
              GpuSinglePartitioning,
              GpuLocalLimitExec(collectLimit.limit, childPlans.head.convertIfNeeded()),
              ENSURE_REQUIREMENTS
            )(SinglePartition), collectLimit.offset)
      }
    ).disabledByDefault("Collect Limit replacement can be slower on the GPU, if huge number " +
        "of rows in a batch it could help by limiting the number of rows transferred from " +
        "GPU to CPU"),
    GpuOverrides.exec[WriteFilesExec](
      "v1 write files",
      // WriteFilesExec always has patterns:
      //   InsertIntoHadoopFsRelationCommand(WriteFilesExec) or InsertIntoHiveTable(WriteFilesExec)
      // The parent node of `WriteFilesExec` will check the types, here just let type check pass
      ExecChecks(TypeSig.all, TypeSig.all),
      (write, conf, p, r) => new GpuWriteFilesMeta(write, conf, p, r)
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

  // AnsiCast is removed from Spark3.4.0
  override def ansiCastRule: ExprRule[_ <: Expression] = null

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      // Empty2Null is pulled out of FileFormatWriter by default since Spark 3.4.0,
      // so it is visible in the overriding stage.
      GpuOverrides.expr[Empty2Null](
        "Converts the empty string to null for writing data",
        ExprChecks.unaryProjectInputMatchesOutput(
          TypeSig.STRING, TypeSig.STRING),
        (a, conf, p, r) => new UnaryExprMeta[Empty2Null](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuEmpty2Null(child)
        }
      )
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }

  override def getDataWriteCmds: Map[Class[_ <: DataWritingCommand],
      DataWritingCommandRule[_ <: DataWritingCommand]] = {
    Map.empty
  }

  override def getRunnableCmds: Map[Class[_ <: RunnableCommand],
      RunnableCommandRule[_ <: RunnableCommand]] = {
    Seq(
      GpuOverrides.runnableCmd[CreateDataSourceTableAsSelectCommand](
        "Write to a data source",
        (a, conf, p, r) => new CreateDataSourceTableAsSelectCommandMeta(a, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[RunnableCommand]), r)).toMap
  }
}
