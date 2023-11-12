/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuOverrides.pluginSupportedOrderableSig

import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{CollectLimitExec, GlobalLimitExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null
import org.apache.spark.sql.types.StringType

trait Spark341PlusDBShims extends Spark332PlusDBShims {

  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[ToPrettyString]("An internal expressions which is used to " +
        "generate pretty string for all kinds of values",
        new ToPrettyStringChecks(),
        (toPrettyString, conf, p, r) => {
          new CastExprMetaBase[ToPrettyString](toPrettyString, conf, p, r) {

            override val toType: StringType.type = StringType

            override def convertToGpu(child: Expression): GpuExpression = {
              GpuToPrettyString(child)
            }
          }
        }),
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
    super.getExprs ++ shimExprs ++ DayTimeIntervalShims.exprs ++ RoundingShims.exprs
  }

  private val shimExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[TakeOrderedAndProjectExec](
        "Take the first limit elements after offset as defined by the sortOrder, and do " +
          "projection if needed",
        // The SortOrder TypeSig will govern what types can actually be used as sorting key data
        // type. The types below are allowed as inputs and outputs.
        ExecChecks((pluginSupportedOrderableSig +
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
                  childPlans.head.convertIfNeeded(), takeExec.offset)(takeExec.sortOrder)
              } else {
                // We are applying the offset only after the batch has been sorted into a single
                // partition. To further clarify we are doing the following
                // GpuTopN(0, end) -> Shuffle(single partition) -> GpuTopN(offset, end)
                // So the leaf GpuTopN (left most) doesn't take offset into account so all the
                // results can perculate above, where we shuffle into a single partition then
                // we drop the offset number of rows before projecting.
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
                  )(SinglePartition),
                  takeExec.offset)(takeExec.sortOrder)
              }
            }
          }),
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
        "GPU to CPU")
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap

  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] =
    super.getExecs ++ shimExecs

}
