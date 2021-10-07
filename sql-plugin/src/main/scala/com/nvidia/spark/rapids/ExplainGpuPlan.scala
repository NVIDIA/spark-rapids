/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{ColumnarToRowExec, ExecSubqueryExpression, InputAdapter, SparkPlan, SubqueryExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

object ExplainGPUPlan {
  def explainPotentialGPUPlanInit(df: DataFrame): String = {
    val gpuOverrideClass = ShimLoader.loadClass("com.nvidia.spark.rapids.GpuOverrides")
    /* val explainMethod = gpuOverrideClass
      .getDeclaredMethod("explainPotentialGPUPlan", classOf[DataFrame])
    explainMethod.invoke(null, df).asInstanceOf[String]

     */
    explainPotentialGPUPlan(df)
  }

  // only run the explain and don't actually convert or run on GPU
  def explainPotentialGPUPlan(df: DataFrame): String = {
    val plan = df.queryExecution.executedPlan
    val conf = new RapidsConf(plan.conf)
    val updatedPlan = prepareExplainOnly(plan)
    val subQueryExprs = getSubQueryPlans(plan)
    val preparedSubPlans = subQueryExprs.map(_.plan).map(prepareExplainOnly(_))
    val subPlanExplains = preparedSubPlans.map(explainSinglePlan(_, conf))
    val topPlanExplain = explainSinglePlan(updatedPlan, conf)
    (subPlanExplains :+ topPlanExplain).mkString("\n")
  }

  private def explainSinglePlan(updatedPlan: SparkPlan, conf: RapidsConf): String = {
    val wrap = GpuOverrides.wrapAndTagPlan(updatedPlan, conf)
    val reasonsToNotReplaceEntirePlan = wrap.getReasonsNotToReplaceEntirePlan
    if (conf.allowDisableEntirePlan && reasonsToNotReplaceEntirePlan.nonEmpty) {
      "Can't replace any part of this plan due to: " +
        s"${reasonsToNotReplaceEntirePlan.mkString(",")}"
    } else {
      wrap.runAfterTagRules()
      wrap.tagForExplain()
      wrap.explain(all = true)
    }
  }

  private def findSubqueryExpressions(e: Expression): Seq[ExecSubqueryExpression] = {
    val childExprs = e.children.flatMap(findSubqueryExpressions(_))
    val res = e match {
      case sq: ExecSubqueryExpression => Seq(sq)
      case _ => Seq.empty
    }
    childExprs ++ res
  }

  private def getSubQueryPlans(plan: SparkPlan): Seq[ExecSubqueryExpression] = {
    // strip out things that would have been added after our GPU plugin would have
    // processed the plan
    val childPlans = plan.children.flatMap(getSubQueryPlans(_))
    val pSubs = plan.expressions.flatMap {
      findSubqueryExpressions(_)
    }
    childPlans ++ pSubs
  }

  private def prepareExplainOnly(plan: SparkPlan): SparkPlan = {
    // Strip out things that would have been added after our GPU plugin would have
    // processed the plan.
    // AQE we look at the input plan so pretty much just like if AQE wasn't enabled.
    val planAfter = plan.transformUp {
      case ia: InputAdapter => prepareExplainOnly(ia.child)
      case ws: WholeStageCodegenExec => prepareExplainOnly(ws.child)
      case c2r: ColumnarToRowExec => prepareExplainOnly(c2r.child)
      case re: ReusedExchangeExec => prepareExplainOnly(re.child)
      case aqe: AdaptiveSparkPlanExec => prepareExplainOnly(aqe.inputPlan)
      case sub: SubqueryExec => prepareExplainOnly(sub.child)
    }
    planAfter
  }
}