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
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.SCALAR_SUBQUERY

object CombineAggregateForScalarSubquery extends Rule[LogicalPlan] {

  val ORDINAL_TAG = TreeNodeTag[Int]("combineAggregateOutputId")

  private case class CombineBlock(
      basePlan: LogicalPlan,
      scalar: ExprId,
      aggregate: NamedExpression,
      inputRefOrder: List[Map[ExprId, Int]],
      projects: List[NamedExpression])

  private case class FusedAggregate(aggregate: Aggregate, scalars: List[ExprId])

  private def createCombineBlock(scalar: ExprId, agg: Aggregate): CombineBlock = {
    agg.child match {
      case prj: Project =>
        val baseOutput = prj.child.output.map(_.exprId)
        val prjExpressions = mutable.ListBuffer.empty[NamedExpression]
        val inputRefs = mutable.ListBuffer.empty[Map[ExprId, Int]]
        prj.projectList.foreach { e =>
          prjExpressions += e
          inputRefs += e.collectLeaves().collect {
            case ar: AttributeReference =>
               ar.exprId -> baseOutput.indexOf(ar.exprId)
          }.toMap
        }
        CombineBlock(prj.child,
                     scalar,
                     agg.aggregateExpressions.head,
                     inputRefs.toList,
                     prjExpressions.toList)

      case plan =>
        val refOrderMaps = plan.output.zipWithIndex
            .map { case (attr, i) => Map(attr.exprId -> i) }
        val prjExpressions = plan.output.map(a => Alias(a, a.name)())
        CombineBlock(plan,
                     scalar,
                     agg.aggregateExpressions.head,
                     refOrderMaps.toList,
                     prjExpressions.toList)
    }
  }

  private def mergeCombineBlocks(blocks: Seq[CombineBlock]): FusedAggregate = {
    val basePlan = blocks.head.basePlan
    val baseOutput = basePlan.output

    val scalars = mutable.ListBuffer.empty[ExprId]
    val aggBuffer = mutable.ListBuffer.empty[NamedExpression]
    val prjBuffer = mutable.ListBuffer.empty[NamedExpression]
    val prjMap = mutable.HashMap.empty[LogicalPlan, ExprId]

    blocks.foreach { blk =>
      scalars += blk.scalar

      val boundProjects = blk.projects.zipWithIndex.map { case (p, i) =>
        val singlePrj = Project(p :: Nil, blk.basePlan).canonicalized
        prjMap.getOrElseUpdate(singlePrj, {
          val refMap = blk.inputRefOrder(i)
          val bound = p.transform {
            case ar: AttributeReference =>
              val newExprId = baseOutput(refMap(ar.exprId)).exprId
              ar.copy(ar.name, ar.dataType, ar.nullable, ar.metadata)(newExprId, ar.qualifier)
          }.asInstanceOf[NamedExpression]
          prjBuffer += bound
          bound.exprId
        })
      }

      val oldPrjExprIds = blk.projects.map(_.exprId)
      aggBuffer += blk.aggregate.transform {
        case ar: AttributeReference =>
          val newExprId = boundProjects(oldPrjExprIds.indexOf(ar.exprId))
          ar.copy(ar.name, ar.dataType, ar.nullable, ar.metadata)(newExprId, ar.qualifier)
      }.asInstanceOf[NamedExpression]
    }

    val fusedProject = Project(prjBuffer.toList, basePlan)
    val fusedAggregate = Aggregate(Nil, aggBuffer.toList, fusedProject)
    FusedAggregate(fusedAggregate, scalars.toList)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val builder = mutable.Map.empty[LogicalPlan, mutable.ListBuffer[CombineBlock]]

    plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case ssq @ ScalarSubquery(agg: Aggregate, _, _, _)
        if agg.groupingExpressions.isEmpty && agg.aggregateExpressions.length == 1 =>

        val combBlock = createCombineBlock(ssq.exprId, agg)
        val blockSeq = builder.getOrElseUpdate(
          combBlock.basePlan.canonicalized, mutable.ListBuffer.empty[CombineBlock])
        blockSeq += combBlock
        ssq
    }

    val fusedAggregates = builder.filter(_._2.length > 1)
        .flatMap { case (_, blocks) =>
          val fusedAgg = mergeCombineBlocks(blocks)
          fusedAgg.scalars.indices.map { i =>
            fusedAgg.scalars(i) -> Tuple2(i, fusedAgg.aggregate)
          }
        }.toMap

    val cb = plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case ssq: ScalarSubquery if fusedAggregates.contains(ssq.exprId) =>
        val (outputID, agg) = fusedAggregates(ssq.exprId)
        val shared = ssq.copy(plan = agg)
        shared.setTagValue(ORDINAL_TAG, outputID)
        shared
    }
    cb
  }

  def build(ss: SparkSession): Rule[LogicalPlan] = {
    object BypassCombineAggregate extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan
    }

    if (RapidsConf.SQL_ENABLED.get(ss.sqlContext.conf) &&
        RapidsConf.ENABLE_COMBINE_AGGREGATE.get(ss.sqlContext.conf)) {
      CombineAggregateForScalarSubquery
    } else {
      BypassCombineAggregate
    }
  }
}
