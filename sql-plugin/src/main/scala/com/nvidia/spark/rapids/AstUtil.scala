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

import java.io.Serializable

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
  def canExtractNonAstConditionIfNeed(expr: BaseExprMeta[_], left: Seq[ExprId],
      right: Seq[ExprId]): Boolean = {
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

  /**
   * Transforms the original join condition into extra filter/project when necessary.
   * It's targeted for some cases join condition is not fully evaluated by ast.
   * Based on join condition, it can be transformed into three major strategies:
   * (1) [NoopJoinCondSplit]: noop when join condition can be fully evaluated with ast.
   * (2) [JoinCondSplitAsPostFilter]: entire join condition is pulled out as a post filter
   * after join condition.
   * (3) [JoinCondSplitAsProject]: extract not supported join condition into pre-project nodes
   * on each join child. One extra project node is introduced to remove intermediate attributes.
   */
  abstract class JoinCondSplitStrategy(left: Seq[NamedExpression],
      right: Seq[NamedExpression], buildSide: GpuBuildSide) extends Serializable {

    // Actual output of build/stream side project due to join condition split
    private[this] val (buildOutputAttr, streamOutputAttr) = buildSide match {
      case GpuBuildLeft => (joinLeftOutput, joinRightOutput)
      case GpuBuildRight => (joinRightOutput, joinLeftOutput)
    }

    // This is the left side child of join. In `split as project` strategy, it may be different
    // from original left child with extracted join condition attribute.
    def leftOutput(): Seq[NamedExpression] = left

    // This is the right side child of join. In `split as project` strategy, it may be different
    // from original right child with extracted join condition attribute.
    def rightOutput(): Seq[NamedExpression] = right

    def astCondition(): Option[Expression]

    def processBuildSideAndClose(input: ColumnarBatch): ColumnarBatch = input

    def processStreamSideAndClose(input: ColumnarBatch): ColumnarBatch = input

    def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = iter

    // This is the left side child of join. In `split as project` strategy, it may be different
    // from original left child with extracted join condition attribute.
    def joinLeftOutput(): Seq[Attribute] = leftOutput.map(expr => expr.toAttribute)

    // This is the right side child of join. In `split as project` strategy, it may be different
    // from original right child with extracted join condition attribute.
    def joinRightOutput(): Seq[Attribute] = rightOutput.map(expr => expr.toAttribute)

    // Updated build attribute list after join condition split as project node.
    // It may include extra attributes from split join condition.
    def buildSideOutput(): Seq[Attribute] = buildOutputAttr

    // Updated stream attribute list after join condition split as project node.
    // It may include extra attributes from split join condition.
    def streamedSideOutput(): Seq[Attribute] = streamOutputAttr
  }

  // For the case entire join condition can be evaluated as ast.
  case class NoopJoinCondSplit(condition: Option[Expression], left: Seq[NamedExpression],
      right: Seq[NamedExpression], buildSide: GpuBuildSide)
    extends JoinCondSplitStrategy(left, right, buildSide) {
    override def astCondition(): Option[Expression] = condition
  }

  // For inner joins we can apply a post-join condition for any conditions that cannot be
  // evaluated directly in a mixed join that leverages a cudf AST expression.
  case class JoinCondSplitAsPostFilter(expr: Option[Expression],
      attributeSeq: Seq[Attribute], left: Seq[NamedExpression],
      right: Seq[NamedExpression], buildSide: GpuBuildSide)
    extends JoinCondSplitStrategy(left, right, buildSide) {
    private[this] val postFilter = expr.map { e =>
      GpuBindReferences.bindGpuReferencesTiered(
        Seq(e), attributeSeq, false)
    }

    override def astCondition(): Option[Expression] = None

    override def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      postFilter.map { filter =>
        iter.flatMap { cb =>
          GpuFilter.filterAndClose(cb, filter, NoopMetric, NoopMetric, NoopMetric)
        }
      }.getOrElse(iter)
    }
  }

  /**
   * This is the split strategy targeting on the case where ast not supported join condition can be
   * extracted and wrapped into extra project node(s).
   *
   * @param astCond   remained join condition after extracting ast not supported parts
   * @param left      original expressions from join's left child. It's left project input
   *                  attribute.
   * @param leftProj  extra expressions extracted from original join condition which is not
   *                  supported by ast. It will be evaluated as a project on left side batch.
   * @param right     original expressions from join's right child. It's left project input
   *                  attribute.
   * @param rightProj extra expressions extracted from original join condition which is not
   *                  supported by ast. It will be evaluated as a project on right side batch.
   * @param post      eliminate the extra columns introduced by join condition split
   * @param buildSide indicates which side is build
   */
  case class JoinCondSplitAsProject(
      astCond: Option[Expression],
      left: Seq[NamedExpression], leftProj: Seq[NamedExpression],
      right: Seq[NamedExpression], rightProj: Seq[NamedExpression],
      post: Seq[NamedExpression], buildSide: GpuBuildSide
  ) extends JoinCondSplitStrategy(left ++ leftProj, right ++ rightProj, buildSide) {
    private[this] val leftInput = left.map(_.toAttribute)
    private[this] val rightInput = right.map(_.toAttribute)

    // Used to build build/stream side project
    private[this] val (buildOutput, streamOutput, buildInput, streamInput) = buildSide match {
      case GpuBuildLeft =>
        (leftOutput, rightOutput, leftInput, rightInput)
      case GpuBuildRight =>
        (rightOutput, leftOutput, rightInput, leftInput)
    }

    private[this] val buildProj = if (!buildOutput.isEmpty) {
      Some(GpuBindReferences.bindGpuReferencesTiered(buildOutput, buildInput, false))
    } else None

    private[this] val streamProj = if (!streamOutput.isEmpty) {
      Some(GpuBindReferences.bindGpuReferencesTiered(streamOutput, streamInput, false))
    } else None

    // Remove the intermediate attributes from left and right side project nodes. Output attributes
    // need to be updated based on join type. And its attributes covers both original plan and
    // extra project node.
    private[this] val postProj = if (!post.isEmpty) {
      Some(
        GpuBindReferences.bindGpuReferencesTiered(
          post, (leftOutput ++ rightOutput).map(_.toAttribute), false))
    } else None

    override def astCondition(): Option[Expression] = astCond

    override def processBuildSideAndClose(input: ColumnarBatch): ColumnarBatch = {
      buildProj.map { pj =>
        withResource(input) { cb =>
          pj.project(cb)
        }
      }.getOrElse(input)
    }

    override def processStreamSideAndClose(input: ColumnarBatch): ColumnarBatch = {
      streamProj.map { pj =>
        withResource(input) { cb =>
          pj.project(cb)
        }
      }.getOrElse(input)
    }

    override def processPostJoin(iter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      postProj.map { proj =>
        iter.map { cb =>
          withResource(cb) { b =>
            proj.project(b)
          }
        }
      }.getOrElse(iter)
    }
  }
}
