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
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, ExprId, Literal, NamedExpression, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALAR_SUBQUERY, TreePattern}
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, QueryExecution, SparkPlan, SubqueryExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * An optimizer rule which combines compatible scalar sub-queries with underlying aggregations.
 * The propose of this rule is to reuse the base plan of compatible scalar sub-queries so as to
 * prune unnecessary sub-queries jobs.
 *
 * This rule doesn't actually clean up sub-queries. Instead, it replaces underlying plans of
 * compatible sub-queries with an unified aggregation plan to make these sub-queries reusable.
 *
 * In overall, the rule can be divided into three steps:
 * <ol>
 *  <li>Extract aggregation, projections and base plans from aggregate-based scalar sub-queries.
 *     Then, put these sub-queries into groups according to their base plans. The base plan
 *     refers to top non-project descendant of Aggregate. Ideally, sub-queries with same kind
 *     of base plan can share a unified combined subquery plan. </li>
 *  <li>Combine aggregate and project expressions with corresponding base plans to build fused
 *      aggregates.</li>
 *   <li>Replace ScalarSubqueries with SharedScalarSubqueries based on fused aggregates.</li>
 * </ol>
 *
 * First example: sub-queries based on multiple fields of the same relation
 * {{{
 * SELECT SUM(i) FROM t
 * WHERE l > (SELECT MIN(l2) FROM t) AND l2 < (SELECT MAX(l) FROM t)
 * AND AND i2 <> (SELECT MAX(i2) FROM t) AND i2 <> (SELECT MIN(i2) FROM t)
 * }}}
 *
 * The optimized (pseudo) logical plan of above query:
 * {{{
 *  Aggregate [sum(i)]
 *  +- Project [i]
 *    +- Filter (((l > scalar-subquery#1) AND (l2 < scalar-subquery#2)) AND
 * (NOT (i2 = scalar-subquery#3) AND NOT (i2 = scalar-subquery#4)))
 *       :  :- Aggregate [min(l2)]
 *       :  :  +- Project [l2]
 *       :  :     +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [max(l)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [max(i2)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [min(i2)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 *
 * With current rule, underlying plans of sub-queries are replaced with a fused one:
 * {{{
 *  Aggregate [sum(i)]
 *  +- Project [i]
 *    +- Filter (((l > shared-scalar-subquery#1) AND (l2 < shared-scalar-subquery#2)) AND
 * (NOT (i2 = shared-scalar-subquery#3) AND NOT (i2 = shared-scalar-subquery#4)))
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :  :     +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 *
 * Second example: sub-queries of different filter conditions
 * {{{
 * SELECT SUM(i) FROM {0}
 * WHERE l > (SELECT MIN(l + l2 + i) FROM {0} WHERE l > 0)
 * AND l2 < (SELECT MAX(i) + MAX(i2) FROM {0} WHERE l > 0)
 * AND i2 > (SELECT COUNT(IF(i % 2 == 0, 1, NULL)) FROM {0} WHERE l < 0)
 * AND i > (SELECT COUNT(IF(i2 % 2 == 0, 1, NULL)) FROM {0} WHERE l < 0)
 * }}}
 *
 * With current rule, sub-queries sharing same base plan (Filter + Scan) will be combined:
 * {{{
 * Aggregate [sum(i)]
 * +- Project [i]
 *    +- Filter (((l > shared-scalar-subquery#1) AND (l2 < shared-scalar-subquery#2)) AND
 * ((i2 > shared-scalar-subquery#3) AND (i > shared-scalar-subquery#4)))
 *       :  :- Aggregate [min(l + l2 + i), (max(i) + max(i2))]
 *       :  :  +- Project [l, l2, i, i2]
 *       :  :     +- Filter (l#46L > 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l + l2 + i), (max(i) + max(i2))]
 *       :  :  +- Project [l, l2, i, i2]
 *       :  :     +- Filter (l#46L > 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [count(if (((i % 2) = 0)) 1 else null),
 *                        count(if (((i2 % 2) = 0)) 1 else null)]
 *       :  :  +- Project [i, i2]
 *       :  :     +- Filter (l < 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [count(if (((i % 2) = 0)) 1 else null),
 *                        count(if (((i2 % 2) = 0)) 1 else null)]
 *       :  :  +- Project [i, i2]
 *       :  :     +- Filter (l < 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 */
case class CombineScalarSubquery(ss: SparkSession) extends Rule[LogicalPlan] {

  // Data structure contains all information extracted from a ScalarSubquery, which is friendly
  // to the subsequent plan combination.
  // Among all fields, basePlan, scalar and aggregate are pretty straightforward.
  // The field `inputRefOrder` aligns with `projects`, which records the binding ordinals in the
  // child plan's output for corresponding project expression. As a Map, the key represents the
  // exprId of project's leaf (AttributeReference); the value represents the ordinal to fetch the
  // input data of the leaf expression from the output of the child plan.
  private case class CombineBlock(
      basePlan: LogicalPlan,
      scalar: ExprId,
      aggregate: NamedExpression,
      inputRefOrder: List[Map[ExprId, Int]],
      projects: List[NamedExpression])

  // Data structure holds the result of plan combination. The ordinal of scalar in the scalar list
  // represents the ordinal to fetch its value from the output of the fused plan.
  private case class FusedAggregate(aggregate: Aggregate, scalars: List[ExprId])

  private def createCombineBlock(scalar: ExprId, agg: Aggregate): CombineBlock = {
    agg.child match {
      case prj: Project =>
        val baseOutput = prj.child.output.map(_.exprId)
        val prjExpressions = mutable.ListBuffer.empty[NamedExpression]
        val inputRefs = mutable.ListBuffer.empty[Map[ExprId, Int]]
        prj.projectList.foreach { e =>
          prjExpressions += e
          // Collects all attributeReferences.
          // Then, finds and binds their ordinals in the base plan's output.
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
              ar.withExprId(newExprId)
          }.asInstanceOf[NamedExpression]
          prjBuffer += bound
          bound.exprId
        })
      }

      val oldPrjExprIds = blk.projects.map(_.exprId)
      aggBuffer += blk.aggregate.transform {
        case ar: AttributeReference =>
          val newExprId = boundProjects(oldPrjExprIds.indexOf(ar.exprId))
          ar.withExprId(newExprId)
      }.asInstanceOf[NamedExpression]
    }

    val fusedProject = Project(prjBuffer.toList, basePlan)
    val fusedAggregate = Aggregate(Nil, aggBuffer.toList, fusedProject)
    FusedAggregate(fusedAggregate, scalars.toList)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!RapidsConf.ENABLE_COMBINE_SUBQUERY.get(ss.sqlContext.conf)) {
      return plan
    }

    val builder = mutable.Map.empty[LogicalPlan, mutable.ListBuffer[CombineBlock]]

    plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case s @ expressions.ScalarSubquery(agg: Aggregate, _, _, _)
        if agg.groupingExpressions.isEmpty && agg.aggregateExpressions.length == 1 =>

        val combBlock = createCombineBlock(s.exprId, agg)
        val blockSeq = builder.getOrElseUpdate(
          combBlock.basePlan.canonicalized, mutable.ListBuffer.empty[CombineBlock])
        blockSeq += combBlock
        s
    }

    val fusedAggregates = builder.filter(_._2.length > 1)
        .flatMap { case (_, blocks) =>
          val fusedAgg = mergeCombineBlocks(blocks.toList)
          fusedAgg.scalars.indices.map { i =>
            fusedAgg.scalars(i) -> Tuple2(i, fusedAgg.aggregate)
          }
        }.toMap

    if (fusedAggregates.isEmpty) {
      plan
    } else {
      plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
        case s: expressions.ScalarSubquery if fusedAggregates.contains(s.exprId) =>
          val (ordinal, agg) = fusedAggregates(s.exprId)
          SharedScalarSubquery(agg, ordinal, s.outerAttrs, s.exprId, s.joinCond)
      }
    }
  }
}

object PlanSharedScalarSubqueries extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(SCALAR_SUBQUERY)) {
      case shared: SharedScalarSubquery =>
        val executedPlan = QueryExecution.prepareExecutedPlan(plan.session, shared.plan)
        SharedScalarSubqueryExec(
          SubqueryExec.createForScalarSubquery(
            s"shared-scalar-subquery#${shared.exprId.id}", executedPlan),
          shared.ordinal,
          shared.exprId)
    }
  }
}

case class SharedScalarSubquery(
    plan: LogicalPlan,
    ordinal: Int,
    outerAttrs: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    joinCond: Seq[Expression] = Seq.empty)
    extends SubqueryExpression(plan, outerAttrs, exprId, joinCond) with Unevaluable {
  override def dataType: DataType = {
    assert(plan.schema.fields.length > ordinal,
           s"Shared Scalar subquery should have only ${ordinal + 1} column")
    plan.schema.fields(ordinal).dataType
  }
  override def nullable: Boolean = true
  override def withNewPlan(plan: LogicalPlan): SharedScalarSubquery = copy(plan = plan)
  override def toString: String = s"shared-scalar-subquery#${exprId.id} $conditionString"
  override lazy val preCanonicalized: Expression = {
    SharedScalarSubquery(
      plan.canonicalized,
      ordinal,
      outerAttrs.map(_.preCanonicalized),
      ExprId(0),
      joinCond.map(_.preCanonicalized))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): SharedScalarSubquery =
    copy(
      outerAttrs = newChildren.take(outerAttrs.size),
      joinCond = newChildren.drop(outerAttrs.size))

  final override def nodePatternsInternal: Seq[TreePattern] = Seq(SCALAR_SUBQUERY)
}

case class SharedScalarSubqueryExec(
    plan: BaseSubqueryExec,
    ordinal: Int,
    exprId: ExprId)
    extends ExecSubqueryExpression with LeafLike[Expression] {

  override def dataType: DataType = plan.schema.fields(ordinal).dataType
  override def nullable: Boolean = true
  override def toString: String = plan.simpleString(SQLConf.get.maxToStringFields)
  override def withNewPlan(query: BaseSubqueryExec): SharedScalarSubqueryExec = copy(plan = query)

  override lazy val preCanonicalized: Expression = {
    SharedScalarSubqueryExec(
      plan.canonicalized.asInstanceOf[BaseSubqueryExec], ordinal, ExprId(0))
  }

  // the first column in first row from `query`.
  @volatile private var result: Any = _
  @volatile private var updated: Boolean = false

  def updateResult(): Unit = {
    val rows = plan.executeCollect()
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n$plan")
    }
    if (rows.length == 1) {
      assert(rows(0).numFields == plan.schema.fields.length,
        s"Expects ${plan.schema.fields.length} field, " +
            s"but got ${rows(0).numFields}; something went wrong in analysis")
      result = rows(0).get(ordinal, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      result = null
    }
    updated = true
  }

  override def eval(input: InternalRow): Any = {
    require(updated, s"$this has not finished")
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    require(updated, s"$this has not finished")
    Literal.create(result, dataType).doGenCode(ctx, ev)
  }
}
