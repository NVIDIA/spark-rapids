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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{BaseExprMeta, ExecChecks, ExecRule, GpuExec, GpuOverrides, GpuSinglePartitioning, GpuTopN, RowCountPlanVisitor, SparkPlanMeta, TypeSig}
import com.nvidia.spark.rapids.GpuOverrides.{exec, pluginSupportedOrderableSig}

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS

object LimitShims {
  object GlobalLimitShims {

    /**
     * Estimate the number of rows for a GlobalLimitExec.
     */
    def visit(plan: SparkPlanMeta[GlobalLimitExec]): Option[BigInt] = {
      val limit = plan.wrapped.limit
      RowCountPlanVisitor.visit(plan.childPlans.head).map(_.min(limit)).orElse(Some(limit))
    }
  }

  def execs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    Seq(
      exec[TakeOrderedAndProjectExec](
        "Take the first limit elements as defined by the sortOrder, and do projection if needed",
        // The SortOrder TypeSig will govern what types can actually be used as sorting key data
        // type. The types below are allowed as inputs and outputs.
        ExecChecks((pluginSupportedOrderableSig + TypeSig.DECIMAL_128 +
            TypeSig.ARRAY + TypeSig.STRUCT + TypeSig.MAP).nested(), TypeSig.all),
        (takeExec, conf, p, r) =>
          new SparkPlanMeta[TakeOrderedAndProjectExec](takeExec, conf, p, r) {
            val sortOrder: Seq[BaseExprMeta[SortOrder]] =
              takeExec.sortOrder.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            val projectList: Seq[BaseExprMeta[NamedExpression]] =
              takeExec.projectList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
            override val childExprs: Seq[BaseExprMeta[_]] = sortOrder ++ projectList

            override def convertToGpu(): GpuExec = {
              // To avoid metrics confusion we split a single stage up into multiple parts but only
              // if there are multiple partitions to make it worth doing.
              val so = sortOrder.map(_.convertToGpu().asInstanceOf[SortOrder])
              if (takeExec.child.outputPartitioning.numPartitions == 1) {
                GpuTopN(takeExec.limit, so,
                  projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
                  childPlans.head.convertIfNeeded())(takeExec.sortOrder)
              } else {
                GpuTopN(
                  takeExec.limit,
                  so,
                  projectList.map(_.convertToGpu().asInstanceOf[NamedExpression]),
                  GpuShuffleExchangeExec(
                    GpuSinglePartitioning,
                    GpuTopN(
                      takeExec.limit,
                      so,
                      takeExec.child.output,
                      childPlans.head.convertIfNeeded())(takeExec.sortOrder),
                    ENSURE_REQUIREMENTS
                  )(SinglePartition)
                )(takeExec.sortOrder)
              }
            }
          })
    ).collect { case r if r != null => (r.getClassFor.asSubclass(classOf[SparkPlan]), r) }.toMap
}