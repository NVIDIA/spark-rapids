/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

/**
 * Utility object for analyzing join output to determine which columns are actually needed
 * by parent nodes. This enables an optimization where we skip gathering columns that will
 * be immediately dropped after the join.
 *
 * The analysis is conservative - if we cannot determine the needed columns, we return None
 * which means "keep all columns" (fail open for correctness).
 */
object JoinOutputAnalysis extends Logging {

  /**
   * Analyze the parent node to determine which columns from the join output are actually needed.
   *
   * @param joinMeta the SparkPlanMeta for the join being analyzed
   * @param joinOutput the full output attributes of the join
   * @param postJoinFilterCondition optional post-join filter condition (for non-AST conditions
   *                                that we add ourselves). Columns referenced by this must be kept.
   * @return None if we cannot determine (keep all columns),
   *         Some(Set.empty) if no columns are needed (e.g., COUNT(*)),
   *         Some(set) with the ExprIds of needed columns.
   */
  def getRequiredOutputColumns(
      joinMeta: SparkPlanMeta[_],
      joinOutput: Seq[Attribute],
      postJoinFilterCondition: Option[Expression]
  ): Option[Set[ExprId]] = {
    // Start with columns needed by our post-join filter (if any)
    val filterRefs = postJoinFilterCondition
      .map(_.references.map(_.exprId).toSet)
      .getOrElse(Set.empty[ExprId])

    // Analyze parent to find additional needed columns
    val parentRefs = joinMeta.parent match {
      case Some(parentMeta) =>
        analyzeParent(parentMeta, joinOutput)
      case None =>
        // No parent - we're at the top of the plan, keep all columns
        logDebug("No parent found for join, keeping all columns")
        None
    }

    // Combine the results
    parentRefs.map { refs =>
      refs ++ filterRefs
    }
  }

  /**
   * Analyze a parent node to determine which columns it needs from its child.
   *
   * @param parentMeta the parent SparkPlanMeta
   * @param childOutput the output attributes of the child (join) that we're analyzing
   * @return None if we cannot determine (keep all columns),
   *         Some(set) with ExprIds of needed columns
   */
  private def analyzeParent(
      parentMeta: RapidsMeta[_, _, _],
      childOutput: Seq[Attribute]
  ): Option[Set[ExprId]] = {
    // Note: During the tagging phase when this is called, parents are always CPU plans
    // (ProjectExec, FilterExec, etc.) not GPU plans. The GPU translation happens after tagging.
    parentMeta.wrapped match {
      case project: ProjectExec =>
        analyzeProject(project.projectList, childOutput)

      case filter: FilterExec =>
        // Filter doesn't reduce columns, look at its parent
        // But we need to include the filter condition references
        val filterRefs = filter.condition.references.map(_.exprId).toSet
        parentMeta.parent match {
          case Some(grandparent) =>
            analyzeParent(grandparent, childOutput).map(_ ++ filterRefs)
          case None =>
            // Filter at top with no parent - keep all columns
            None
        }

      case agg: HashAggregateExec =>
        analyzeAggregate(agg.resultExpressions, agg.groupingExpressions, childOutput)

      case takeOrdered: TakeOrderedAndProjectExec =>
        analyzeTakeOrderedAndProject(takeOrdered.projectList, takeOrdered.sortOrder, childOutput)

      case _ =>
        // Unknown parent type - fail open and keep all columns
        logDebug(s"Unknown parent type ${parentMeta.wrapped.getClass.getSimpleName}, " +
          "keeping all columns")
        None
    }
  }

  /**
   * Analyze a project to find which child output columns are referenced.
   */
  private def analyzeProject(
      projectList: Seq[Expression],
      childOutput: Seq[Attribute]
  ): Option[Set[ExprId]] = {
    val childExprIds = childOutput.map(_.exprId).toSet
    val referencedExprIds = projectList.flatMap(_.references.map(_.exprId)).toSet
    // Only keep references that are actually in the child output
    Some(referencedExprIds.intersect(childExprIds))
  }

  /**
   * Analyze an aggregate to find which child output columns are referenced.
   * This includes both grouping expressions and aggregate function inputs.
   */
  private def analyzeAggregate(
      resultExpressions: Seq[NamedExpression],
      groupingExpressions: Seq[Expression],
      childOutput: Seq[Attribute]
  ): Option[Set[ExprId]] = {
    val childExprIds = childOutput.map(_.exprId).toSet
    // Aggregate needs columns from both result expressions and grouping expressions
    val resultRefs = resultExpressions.flatMap(_.references.map(_.exprId)).toSet
    val groupingRefs = groupingExpressions.flatMap(_.references.map(_.exprId)).toSet
    val allRefs = resultRefs ++ groupingRefs
    Some(allRefs.intersect(childExprIds))
  }

  /**
   * Analyze TakeOrderedAndProject to find which child output columns are referenced.
   * This includes both the project list and sort order columns.
   */
  private def analyzeTakeOrderedAndProject(
      projectList: Seq[NamedExpression],
      sortOrder: Seq[Expression],
      childOutput: Seq[Attribute]
  ): Option[Set[ExprId]] = {
    val childExprIds = childOutput.map(_.exprId).toSet
    val projectRefs = projectList.flatMap(_.references.map(_.exprId)).toSet
    val sortRefs = sortOrder.flatMap(_.references.map(_.exprId)).toSet
    val allRefs = projectRefs ++ sortRefs
    Some(allRefs.intersect(childExprIds))
  }

  /**
   * Convert a set of required ExprIds to column indices for left and right sides of a join.
   *
   * @param requiredExprIds the set of ExprIds that are needed
   * @param leftOutput the output attributes of the left side
   * @param rightOutput the output attributes of the right side
   * @return a tuple of (leftIndices, rightIndices) where each is an array of column indices
   *         to gather from that side, or None if all columns should be kept
   */
  def computeGatherIndices(
      requiredExprIds: Option[Set[ExprId]],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]
  ): (Option[Array[Int]], Option[Array[Int]]) = {
    requiredExprIds match {
      case None =>
        // Keep all columns
        (None, None)
      case Some(exprIds) =>
        val leftIndices = leftOutput.zipWithIndex.collect {
          case (attr, idx) if exprIds.contains(attr.exprId) => idx
        }.toArray
        val rightIndices = rightOutput.zipWithIndex.collect {
          case (attr, idx) if exprIds.contains(attr.exprId) => idx
        }.toArray
        (Some(leftIndices), Some(rightIndices))
    }
  }

  /**
   * Compute the output attributes for a join given the column filtering.
   * This is used to update the join's output method to reflect actual columns produced.
   *
   * @param fullOutput the full join output (all columns from both sides)
   * @param leftOutput the left side's output attributes
   * @param rightOutput the right side's output attributes
   * @param leftIndices the indices of left columns to keep (None = all)
   * @param rightIndices the indices of right columns to keep (None = all)
   * @return the filtered output attributes
   */
  def computeFilteredOutput(
      fullOutput: Seq[Attribute],
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute],
      leftIndices: Option[Array[Int]],
      rightIndices: Option[Array[Int]]
  ): Seq[Attribute] = {
    val leftFiltered: Seq[Attribute] = leftIndices match {
      case None => leftOutput
      case Some(indices) => indices.map(leftOutput(_)).toSeq
    }
    val rightFiltered: Seq[Attribute] = rightIndices match {
      case None => rightOutput
      case Some(indices) => indices.map(rightOutput(_)).toSeq
    }
    leftFiltered ++ rightFiltered
  }
}
