/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY

object CombineAggregateForScalarSubquery extends Rule[LogicalPlan] {

  val ORDINAL_TAG = TreeNodeTag[Int]("combineAggregateOutputId")

  private case class CombineInfo(
      basePlan: LogicalPlan,
      aggregates: mutable.ListBuffer[NamedExpression] = mutable.ListBuffer.empty,
      scalars: mutable.ListBuffer[ExprId] = mutable.ListBuffer.empty)

  private def retargetAggregate(aggregate: NamedExpression,
      childPlan: LogicalPlan,
      targetPlan: LogicalPlan): NamedExpression = {
    if (childPlan.fastEquals(targetPlan)) {
      return aggregate
    }
    val exprIds = childPlan.output.map(_.exprId)
    val tgtOutput = targetPlan.output
    aggregate.transform {
      case ref: AttributeReference => tgtOutput(exprIds.indexOf(ref.exprId))
      case e => e
    }.asInstanceOf[NamedExpression]
  }

  private def extractBasePlan(plan: LogicalPlan): LogicalPlan = plan match {
    case prj: Project if prj.expressions.forall(_.isInstanceOf[NamedExpression]) =>
      prj.child
    case _ =>
      plan
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val builder = mutable.Map.empty[LogicalPlan, CombineInfo]

    plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case ssq @ ScalarSubquery(agg: Aggregate, _, _, _)
        if agg.groupingExpressions.isEmpty && agg.aggregateExpressions.length == 1 =>

        val info = builder.getOrElseUpdate(agg.child.canonicalized, CombineInfo(agg.child))
        info.aggregates += retargetAggregate(
          agg.aggregateExpressions.head, agg.child, info.basePlan)
        info.scalars += ssq.exprId
        ssq
    }

    val combinedAggregates = builder.flatMap { case (_, info) =>
      val combined = Aggregate(Nil, info.aggregates.toList, info.basePlan)
      info.scalars.zipWithIndex.map {
        case (id, index) => id -> Tuple2(index, combined)
      }
    }.toMap

    plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case ssq: ScalarSubquery if combinedAggregates.contains(ssq.exprId) =>
        val (outputID, agg) = combinedAggregates(ssq.exprId)
        val shared = ssq.copy(plan = agg)
        shared.setTagValue(ORDINAL_TAG, outputID)
        shared
    }
  }

  def build(ss: SparkSession): Rule[LogicalPlan] = {
    object BypassCombineAggregate extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan
    }

    if (ss.conf.get(RapidsConf.SQL_ENABLED.key).toBoolean) {
      CombineAggregateForScalarSubquery
    } else {
      BypassCombineAggregate
    }
  }
}
