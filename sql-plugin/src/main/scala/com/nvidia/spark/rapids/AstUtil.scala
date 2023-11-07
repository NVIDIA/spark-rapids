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

package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.rapids.catalyst.expressions.{GpuEquivalentExpressions, GpuExpressionEquals}


object AstUtil {

  /**
   * Check whether it can be split into non-ast sub-expression if needed
   *
   * @return true when: 1) If all ast-able in expr; 2) all non-ast-able tree nodes don't contain
   *         attributes from both join sides. In such case, it's not able
   *         to push down into single child.
   */
  def canExtractNonAstConditionIfNeed(expr: BaseExprMeta[_], left: Seq[Attribute],
      right: Seq[Attribute]): Boolean = {
    if (!expr.canSelfBeAst) {
      // It needs to be split since not ast-able. Check itself and childerns to ensure
      // pushing-down can be made, which doesn't need attributions from both sides.
      val exprRef = expr.wrapped.asInstanceOf[Expression]
      val leftTree = exprRef.references.exists(left.contains(_))
      val rightTree = exprRef.references.exists(right.contains(_))
      // Can't extract a condition involving columns from both sides
      !(rightTree && leftTree)
    } else {
      // Check whether any child contains the case not able to split
      expr.childExprs.isEmpty || expr.childExprs.forall(
        canExtractNonAstConditionIfNeed(_, left, right))
    }
  }

  /**
   *
   * @param condition to be split if needed
   * @param left attributions from left child
   * @param right attributions from right child
   * @param skipCheck whether skip split-able check
   * @return a tuple of [[Expression]] for remained expressions, List of [[NamedExpression]] for
   *         left child if any, List of [[NamedExpression]] for right child if any
   */
  def extractNonAstFromJoinCond(condition: Option[BaseExprMeta[_]],
      left: AttributeSeq, right: AttributeSeq, skipCheck: Boolean):
  (Option[Expression], List[NamedExpression], List[NamedExpression]) = {
    // Choose side with smaller key size. Use expr ID to check the side which project expr
    // belonging to.
    val (exprIds, isLeft) = if (left.attrs.size < right.attrs.size) {
      (left.attrs.map(_.exprId), true)
    } else {
      (right.attrs.map(_.exprId), false)
    }
    // List of expression pushing down to left side child
    val leftExprs: ListBuffer[NamedExpression] = ListBuffer.empty
    // List of expression pushing down to right side child
    val rightExprs: ListBuffer[NamedExpression] = ListBuffer.empty
    // Substitution map used to replace targeted expressions based on semantic equality
    val substitutionMap = mutable.HashMap.empty[GpuExpressionEquals, Expression]

    // 1st step to construct 1) left expr list; 2) right expr list; 3) substitutionMap
    // No need to consider common sub-expressions here since project node will use tiered execution
    condition.foreach(c =>
      if (skipCheck || canExtractNonAstConditionIfNeed(c, left.attrs, right.attrs)) {
        splitNonAstInternal(c, exprIds, leftExprs, rightExprs, substitutionMap, isLeft)
      })

    // 2nd step to replace expression pushing down to child plans in depth first fashion
    (condition.map(
      _.convertToGpu().mapChildren(
        GpuEquivalentExpressions.replaceWithSemanticCommonRef(_,
          substitutionMap))), leftExprs.toList, rightExprs.toList)
  }

  private[this] def splitNonAstInternal(condition: BaseExprMeta[_], childAtt: Seq[ExprId],
      left: ListBuffer[NamedExpression], right: ListBuffer[NamedExpression],
      substitutionMap: mutable.HashMap[GpuExpressionEquals, Expression], isLeft: Boolean): Unit = {
    for (child <- condition.childExprs) {
      if (!child.canSelfBeAst) {
        val exprRef = child.wrapped.asInstanceOf[Expression]
        val gpuProj = child.convertToGpu()
        val alias = substitutionMap.get(GpuExpressionEquals(gpuProj)) match {
          case Some(_) => None
          case None =>
            if (exprRef.references.exists(r => childAtt.contains(r.exprId)) ^ isLeft) {
              val alias = GpuAlias(gpuProj, s"_agpu_non_ast_r_${left.size}")()
              right += alias
              Some(alias)
            } else {
              val alias = GpuAlias(gpuProj, s"_agpu_non_ast_l_${left.size}")()
              left += alias
              Some(alias)
            }
        }
        alias.foreach(a => substitutionMap.put(GpuExpressionEquals(gpuProj), a.toAttribute))
      } else {
        splitNonAstInternal(child, childAtt, left, right, substitutionMap, isLeft)
      }
    }
  }
}
