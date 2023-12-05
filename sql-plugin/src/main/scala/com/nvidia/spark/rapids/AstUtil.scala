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

import com.nvidia.spark.rapids.Arm.withResource
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.rapids.catalyst.expressions.{GpuEquivalentExpressions, GpuExpressionEquals}
import org.apache.spark.sql.vectorized.ColumnarBatch


object AstUtil {

  /**
   * Check whether it can be split into non-ast sub-expression if needed
   *
   * @return true when: 1) If all ast-able in expr; 2) all non-ast-able tree nodes don't contain
   *         attributes from both join sides. In such case, it's not able
   *         to push down into single child.
   */
  def canExtractNonAstConditionIfNeed(expr: BaseExprMeta[_], left: scala.collection.Seq[ExprId],
      right: scala.collection.Seq[ExprId]): Boolean = {
    if (!expr.canSelfBeAst) {
      // It needs to be split since not ast-able. Check itself and childerns to ensure
      // pushing-down can be made, which doesn't need attributions from both sides.
      val exprRef = expr.wrapped.asInstanceOf[Expression]
      val leftTree = exprRef.references.exists(r => left.contains(r.exprId))
      val rightTree = exprRef.references.exists(r => right.contains(r.exprId))
      // Can't extract a condition involving columns from both sides
      !(rightTree && leftTree)
    } else {
      // Check whether any child contains the case not able to split
      expr.childExprs.isEmpty || expr.childExprs.forall(
        canExtractNonAstConditionIfNeed(_, left, right))
    }
  }

  /**
   * Extract non-AST functions from join conditions and update the original join condition. Based
   * on the attributes, it decides which side the split condition belongs to. The replaced
   * condition is wrapped with GpuAlias with new intermediate attributes.
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
      if (skipCheck || canExtractNonAstConditionIfNeed(c, left.attrs.map(_.exprId), right.attrs
          .map(_.exprId))) {
        splitNonAstInternal(c, exprIds, leftExprs, rightExprs, substitutionMap, isLeft)
      })

    // 2nd step to replace expression pushing down to child plans in depth first fashion
    (condition.map(
      _.convertToGpu().mapChildren(
        GpuEquivalentExpressions.replaceWithSemanticCommonRef(_,
          substitutionMap))), leftExprs.toList, rightExprs.toList)
  }

  private[this] def splitNonAstInternal(condition: BaseExprMeta[_],
      childAtt: scala.collection.Seq[ExprId],
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

  abstract class NonAstJoinCondSplitStrategy() {
    def remainedCond(): Option[Expression]

    def processBuildSide(input: ColumnarBatch): ColumnarBatch = { input }

    def processStreamSide(input: ColumnarBatch): ColumnarBatch = { input }

    def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = { iter }
  }

  case class NoopJoinCondSplit(condition: Option[Expression]) extends
      NonAstJoinCondSplitStrategy {
    override def remainedCond(): Option[Expression] = condition
  }

  // For inner joins we can apply a post -join condition for any conditions that cannot be
  // evaluated directly in a mixed join that leverages a cudf AST expression.
  case class JoinCondSplitAsPostFilter(expr: Option[Expression],
      attributeSeq: scala.collection.Seq[Attribute])
      extends NonAstJoinCondSplitStrategy {
    lazy val postFilter = if (expr.isDefined) {
      Some(GpuBindReferences.bindGpuReferencesTiered(
        scala.collection.Seq(expr.get), attributeSeq, false))
    } else None

    override def remainedCond(): Option[Expression] = None

    override def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      postFilter.map( filter =>
        iter.flatMap(cb =>
          GpuFilter.filterAndClose(cb, filter, NoopMetric, NoopMetric, NoopMetric)))
          .getOrElse(iter)
    }
  }

  case class JoinCondSplitAsProject(
    remains: Option[Expression],
    left: scala.collection.Seq[NamedExpression], leftAttrSeq: scala.collection.Seq[Attribute],
    right: scala.collection.Seq[NamedExpression], rightAttrSeq: scala.collection.Seq[Attribute],
    post: scala.collection.Seq[NamedExpression], postAttrSeq: scala.collection.Seq[Attribute],
    buildSide: GpuBuildSide
  ) extends NonAstJoinCondSplitStrategy {
    // Used to build build/stream side project
    lazy val (build, stream, buildAttr, streamAttr) = buildSide match {
      case GpuBuildLeft => (left, right, leftAttrSeq, rightAttrSeq)
      case GpuBuildRight => (right, left, rightAttrSeq, leftAttrSeq)
    }

    lazy val buildProj = if (!build.isEmpty) {
      Some(GpuBindReferences.bindGpuReferencesTiered(build, buildAttr, false))
    } else None

    lazy val streamProj = if (!stream.isEmpty) {
      Some(GpuBindReferences.bindGpuReferencesTiered(stream, streamAttr, false))
    } else None

    // Remove the intermediate attributes from left and right side project nodes. Output
    // attributes need to be updated based on types
    lazy val postProj = if (!post.isEmpty) {
      Some(GpuBindReferences.bindGpuReferencesTiered(post, postAttrSeq, false))
    } else None

    // Actual output of build/stream side project due to join condition split
    lazy val (buildOutputAttr, streamOutputAttr) = buildSide match {
      case GpuBuildLeft => (joinLeftOutput, joinRightOutput)
      case GpuBuildRight => (joinRightOutput, joinLeftOutput)
    }
    override def remainedCond(): Option[Expression] = remains

    // This is the left side child of join. In `split as project` strategy, it may be different
    // from original left child in case of extracting join condition as a project on top of original
    // left child.
    def joinLeftOutput(): Seq[Attribute] = left.map(expr => expr.toAttribute)

    // This is the right side child of join. In `split as project` strategy, it may be different
    // from original right child in case of extracting join condition as a project on top of
    // original right child.
    def joinRightOutput(): Seq[Attribute] = right.map(expr => expr.toAttribute)

    // Overriden build attribute list after join condition split as project node
    def buildOutput(): scala.collection.Seq[Attribute] = buildOutputAttr

    // Overriden stream attribute list after join condition split as project node
    def streamOutput(): scala.collection.Seq[Attribute] = streamOutputAttr

    override def processBuildSide(input: ColumnarBatch): ColumnarBatch = {
      buildProj.map( pj =>
        withResource(input) { cb =>
          pj.project(cb)
        }).getOrElse(input)
    }

    override def processStreamSide(input: ColumnarBatch): ColumnarBatch = {
      streamProj.map( pj =>
        withResource(input) { cb =>
          pj.project(cb)
        }).getOrElse(input)
    }

    override def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      postProj.map( proj =>
        iter.map( cb =>
          withResource(cb) { b =>
            proj.project(b)
          })).getOrElse(iter)
    }
  }
}
