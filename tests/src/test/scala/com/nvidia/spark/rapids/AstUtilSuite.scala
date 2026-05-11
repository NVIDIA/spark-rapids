/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.rapids.{GpuAnd, GpuGreaterThan, GpuLength, GpuLessThan, GpuStringTrim}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType, StringType}


class AstUtilSuite extends GpuUnitTests {

  private[this] def testSingleNode(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    val expr = mock(classOf[Expression])
    val attributeSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    when(expr.references).thenReturn(attributeSet)

    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(Seq.empty)
    when(exprMeta.canSelfBeAst).thenReturn(!containsNonAstAble)
    when(exprMeta.wrapped).thenReturn(expr)

    AstUtil.canExtractNonAstConditionIfNeed(exprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2).map
    (_.exprId))
  }

  private[this] def testMultiNodes(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    val attributeSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val leftExprMeta = buildLeaf(attributeSet, containsNonAstAble)

    val rightExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rightExprMeta.childExprs).thenReturn(Seq.empty)
    when(rightExprMeta.canSelfBeAst).thenReturn(true)

    val rootExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootExprMeta.childExprs).thenReturn(Seq(leftExprMeta, rightExprMeta))

    when(rootExprMeta.canSelfBeAst).thenReturn(true)

    AstUtil.canExtractNonAstConditionIfNeed(rootExprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2)
        .map(_.exprId))
  }

  private[this] def buildLeaf(attributeSet: AttributeSet, containsNonAstAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = mock(classOf[Expression])
    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(Seq.empty)
    when(exprMeta.canSelfBeAst).thenReturn(!containsNonAstAble)

    when(expr.references).thenReturn(attributeSet)
    when(exprMeta.wrapped).thenReturn(expr)
    exprMeta
  }

  private[this] def testMultiNodes2(containsNonAstAble: Boolean, crossMultiChildPlan: Boolean)
  : Boolean = {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val r1 = AttributeReference("r1", StringType)()
    val r2 = AttributeReference("r2", StringType)()

    // Build left
    val leftAttrSet = if (crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val leftExprMeta = buildLeaf(leftAttrSet, containsNonAstAble)

    // Build right
    val rightAttrSet = if (!crossMultiChildPlan) {
      AttributeSet(Seq(l1, r1))
    } else {
      AttributeSet(Seq(l1, l2))
    }
    val rightExprMeta = buildLeaf(rightAttrSet, containsNonAstAble)

    // Build root
    val rootExprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootExprMeta.childExprs).thenReturn(Seq(leftExprMeta, rightExprMeta))
    when(rootExprMeta.canSelfBeAst).thenReturn(true)

    AstUtil.canExtractNonAstConditionIfNeed(rootExprMeta, Seq(l1, l2).map(_.exprId), Seq(r1, r2)
        .map(_.exprId))
  }

  test("Single node tree for ast split if needed") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (true, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testSingleNode(containsNonAstAble, crossMultiChildPlan))
    }
  }

  test("Multi-nodes tree for ast split if needed") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (true, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testMultiNodes(containsNonAstAble, crossMultiChildPlan))
    }
  }

  test("Multi-nodes tree for ast split if needed complex case") {
    for ((canAstSplitIfNeeded, containsNonAstAble, crossMultiChildPlan) <- Seq(
      (false, true, true), (false, true, false), (true, false, true), (true, false, false))) {
      assertResult(
        canAstSplitIfNeeded)(testMultiNodes2(containsNonAstAble, crossMultiChildPlan))
    }
  }

  // ======== test cases for AST split ========
  // Build a simple tree: string_trim(a:string). string_trim's AST-ability is controlled by
  // astAble for different test purposes
  private[this] def buildTree1(attSet: AttributeReference, astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuStringTrim(attSet)
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    when(rootMeta.childExprs).thenReturn(Seq.empty)
    when(rootMeta.canSelfBeAst).thenReturn(astAble)
    when(rootMeta.convertToGpu).thenReturn(expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  // Build a simple tree: length(string_trim(a:string)). string_length's AST-ability is
  // controlled by astAble for different test purposes
  private[this] def buildTree2(attSet: AttributeReference, astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuLength(GpuStringTrim(attSet))
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    val childExprs = Seq(buildTree1(attSet, astAble))
    when(rootMeta.childExprs).thenReturn(childExprs)
    when(rootMeta.canSelfBeAst).thenReturn(astAble)
    when(rootMeta.convertToGpu).thenReturn(expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  // Build a complex tree:
  //   length(trim(a1:string)) > length(trim(a2:string))
  private[this] def buildTree3(attSet1: AttributeReference, attSet2: AttributeReference,
      astAble: Boolean)
  : BaseExprMeta[Expression] = {
    val expr = GpuGreaterThan(GpuLength(GpuStringTrim(attSet1)), GpuLength(GpuStringTrim(attSet2)))
    val rootMeta = mock(classOf[BaseExprMeta[Expression]])
    val childExprs = Seq(buildTree2(attSet1, astAble), buildTree2(attSet2, astAble))
    when(rootMeta.childExprs).thenReturn(childExprs)
    when(rootMeta.canSelfBeAst).thenReturn(true)
    when(rootMeta.convertToGpu).thenReturn(expr)
    when(rootMeta.wrapped).thenReturn(expr)
    rootMeta
  }

  test("Tree of single ast-able node should not split") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree1(l1, true)), Seq(l1), Seq(l2))
    assertResult(true)(l.isEmpty)
    assertResult(true)(r.isEmpty)
    assertResult(true)(e.get.isInstanceOf[GpuStringTrim])
  }

  test("Project pushing down to same child") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l1, false)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 1)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(r.isEmpty)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(checkEquals(e.get, GpuGreaterThan(l(0).toAttribute, l(0).toAttribute)))
  }

  private def realExpr(expr: Expression): Expression = expr match {
    case e: GpuAlias => e.child
    case _ => expr
  }

  private def checkEquals(expr: Expression, other: Expression): Boolean = {
    realExpr(expr).semanticEquals(realExpr(other))
  }

  test("Project pushing down to different childern") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l2, false)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 1)
    assertResult(true)(l.exists(checkEquals(_, GpuLength(GpuStringTrim(l1)))))
    assertResult(true)(r.size == 1)
    assertResult(true)(r.exists(checkEquals(_, GpuLength(GpuStringTrim(l2)))))
    assertResult(true)(
      checkEquals(e.get, GpuGreaterThan(l(0).toAttribute, r(0).toAttribute)))
  }

  test("A tree with multiple ast-able childern should not split") {
    val l1 = AttributeReference("l1", StringType)()
    val l2 = AttributeReference("l2", StringType)()
    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, l2, true)), Seq(l1), Seq(l2))
    assertResult(true)(l.size == 0)
    assertResult(true)(r.size == 0)
    assertResult(true)(checkEquals(e.get,
      GpuGreaterThan(GpuLength(GpuStringTrim(l1)), GpuLength(GpuStringTrim(l2)))))
  }

  // Build a simple tree representing: cast(a:int as boolean)
  // This simulates a condition like: SELECT * FROM t1 JOIN t2 ON t1.intCol
  // where intCol is cast to boolean for the join condition
  private[this] def buildTreeWithCast(attr: AttributeReference): BaseExprMeta[Expression] = {
    // Create the mock cast expression (int -> boolean)
    val castExpr = mock(classOf[Expression])
    when(castExpr.references).thenReturn(AttributeSet(Seq(attr)))
    when(castExpr.dataType).thenReturn(BooleanType)
    
    val castMeta = mock(classOf[BaseExprMeta[Expression]])
    when(castMeta.childExprs).thenReturn(Seq.empty)
    when(castMeta.canSelfBeAst).thenReturn(false) // Cast cannot be AST
    when(castMeta.convertToGpu).thenReturn(castExpr)
    when(castMeta.wrapped).thenReturn(castExpr)
    castMeta
  }

  test("Top-level non-AST expression on single side should be extracted") {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()

    // Test with left side only
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTreeWithCast(l1)), Seq(l1), Seq(r1))

    // Should extract the cast to the left side
    assertResult(1)(leftExprs.size)
    assertResult(0)(rightExprs.size)
    assertResult(true)(expr.isDefined)

     // The returned expression should be an attribute reference (the replacement)
     assertResult(true)(expr.get.isInstanceOf[AttributeReference])
 
     // The attribute should match the one from the left expression alias
     val leftAlias = leftExprs.head.asInstanceOf[GpuAlias]
     val expectedAttr = AttributeReference(leftAlias.name, leftAlias.child.dataType,
       leftAlias.child.nullable, leftAlias.metadata)(leftAlias.exprId, leftAlias.qualifier)
     assertResult(expectedAttr)(expr.get)
  }

  test("Top-level non-AST expression on right side should be extracted to right") {
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()
    
    // Test with right side only
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTreeWithCast(r1)), Seq(l1), Seq(r1))
    
    // Should extract the cast to the right side
    assertResult(0)(leftExprs.size)
    assertResult(1)(rightExprs.size)
    
     // The returned expression should be an attribute reference (the replacement)
     assertResult(true)(expr.get.isInstanceOf[AttributeReference])
     
     // The attribute should match the one from the right expression alias
     val rightAlias = rightExprs.head.asInstanceOf[GpuAlias]
     val expectedAttr = AttributeReference(rightAlias.name, rightAlias.child.dataType,
       rightAlias.child.nullable, rightAlias.metadata)(rightAlias.exprId, rightAlias.qualifier)
     assertResult(expectedAttr)(expr.get)
  }

  // ============================================================================
  // Tests for equi-join integration path (issue #14283)
  // These simulate the exact pattern: a.range_start < CAST(b.b_end AS BIGINT)
  // AND a.range_end > CAST(b.b_start AS BIGINT)
  // ============================================================================

  private[this] def issue14283ExpressionWithReferences(
      dataType: DataType,
      refs: AttributeReference*): Expression = {
    val expr = mock(classOf[Expression])
    when(expr.references).thenReturn(AttributeSet(refs))
    when(expr.dataType).thenReturn(dataType)
    expr
  }

  private[this] def issue14283ExpressionMeta(
      wrapped: Expression,
      canSelfBeAst: Boolean,
      convertToGpu: Option[Expression] = None,
      childExprs: Seq[BaseExprMeta[_]] = Seq.empty): BaseExprMeta[Expression] = {
    val exprMeta = mock(classOf[BaseExprMeta[Expression]])
    when(exprMeta.childExprs).thenReturn(childExprs)
    when(exprMeta.canSelfBeAst).thenReturn(canSelfBeAst)
    when(exprMeta.convertToGpu).thenReturn(convertToGpu.getOrElse(wrapped))
    when(exprMeta.wrapped).thenReturn(wrapped)
    exprMeta
  }

  private[this] def issue14283AttrMeta(attr: AttributeReference): BaseExprMeta[Expression] = {
    issue14283ExpressionMeta(
      issue14283ExpressionWithReferences(attr.dataType, attr),
      canSelfBeAst = true,
      convertToGpu = Some(attr))
  }

  private[this] def issue14283CastMeta(
      dataType: DataType,
      refs: AttributeReference*): BaseExprMeta[Expression] = {
    issue14283ExpressionMeta(
      issue14283ExpressionWithReferences(dataType, refs: _*),
      canSelfBeAst = false)
  }

  /**
   * Build a tree representing: left_attr < cast(right_attr as bigint)
   * This simulates the equi-join non-equality condition from issue #14283.
   * The comparison (LessThan) is AST-able, but the Cast child is not.
   */
  private[this] def buildComparisonWithCast(
      leftAttr: AttributeReference,
      rightAttr: AttributeReference,
      comparison: (Expression, Expression) => Expression = GpuLessThan
  ): BaseExprMeta[Expression] = {
    val castMeta = issue14283CastMeta(LongType, rightAttr)
    val comparisonExpr = comparison(leftAttr, castMeta.convertToGpu)
    issue14283ExpressionMeta(
      comparisonExpr,
      canSelfBeAst = true,
      childExprs = Seq(issue14283AttrMeta(leftAttr), castMeta))
  }

  /**
   * Build a compound condition: (left1 < cast(right1)) AND (left2 > cast(right2))
   * This simulates the full join condition from issue #14283.
   */
  private[this] def buildFullRangeJoinCondition(
      leftStart: AttributeReference,
      leftEnd: AttributeReference,
      rightStart: AttributeReference,
      rightEnd: AttributeReference): BaseExprMeta[Expression] = {
    val ltMeta = buildComparisonWithCast(leftStart, rightEnd)
    val gtMeta = buildComparisonWithCast(leftEnd, rightStart, GpuGreaterThan)
    issue14283ExpressionMeta(
      GpuAnd(ltMeta.convertToGpu, gtMeta.convertToGpu),
      canSelfBeAst = true,
      childExprs = Seq(ltMeta, gtMeta))
  }

  test("Equi-join pattern: canExtractNonAstConditionIfNeed with cast on single side") {
    // Simulates: a.range_start < CAST(b.b_end AS BIGINT)
    // Cast references only the right side → extractable
    val lStart = AttributeReference("range_start", LongType)()
    val rEnd = AttributeReference("b_end", IntegerType)()

    val condMeta = buildComparisonWithCast(lStart, rEnd)
    val canExtract = AstUtil.canExtractNonAstConditionIfNeed(
      condMeta, Seq(lStart).map(_.exprId), Seq(rEnd).map(_.exprId))

    assertResult(true)(canExtract)
  }

  test("Equi-join pattern: extractNonAstFromJoinCond extracts cast to right side") {
    // Simulates: a.range_start < CAST(b.b_end AS BIGINT)
    val lStart = AttributeReference("range_start", LongType)()
    val rEnd = AttributeReference("b_end", IntegerType)()

    val condMeta = buildComparisonWithCast(lStart, rEnd)
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(Some(condMeta), Seq(lStart), Seq(rEnd))

    // Cast on right attr should be extracted to right side
    assertResult(0)(leftExprs.size)
    assertResult(1)(rightExprs.size)
    assertResult(true)(expr.isDefined)
    // Rewritten condition should be a LessThan with left unchanged, right replaced
    assertResult(true)(expr.get.isInstanceOf[GpuLessThan])
  }

  test("Equi-join pattern: full range condition with casts on right side") {
    // Simulates: a.range_start < CAST(b.b_end AS BIGINT)
    //        AND a.range_end > CAST(b.b_start AS BIGINT)
    val lStart = AttributeReference("range_start", LongType)()
    val lEnd = AttributeReference("range_end", LongType)()
    val rStart = AttributeReference("b_start", IntegerType)()
    val rEnd = AttributeReference("b_end", IntegerType)()

    val condMeta = buildFullRangeJoinCondition(lStart, lEnd, rStart, rEnd)
    val canExtract = AstUtil.canExtractNonAstConditionIfNeed(
      condMeta,
      Seq(lStart, lEnd).map(_.exprId),
      Seq(rStart, rEnd).map(_.exprId))

    // Both casts reference only right side → extractable
    assertResult(true)(canExtract)
  }

  test("Equi-join pattern: full range condition extraction produces right-side projections") {
    val lStart = AttributeReference("range_start", LongType)()
    val lEnd = AttributeReference("range_end", LongType)()
    val rStart = AttributeReference("b_start", IntegerType)()
    val rEnd = AttributeReference("b_end", IntegerType)()

    val condMeta = buildFullRangeJoinCondition(lStart, lEnd, rStart, rEnd)
    val (expr, leftExprs, rightExprs) =
      AstUtil.extractNonAstFromJoinCond(
        Some(condMeta), Seq(lStart, lEnd), Seq(rStart, rEnd))

    // Both CASTs are on right-side attributes → should be extracted to right
    assertResult(0)(leftExprs.size)
    assertResult(2)(rightExprs.size)
    assertResult(true)(expr.isDefined)
    // Rewritten condition should be an AND expression
    assertResult(true)(expr.get.isInstanceOf[GpuAnd])
  }

  test("Equi-join pattern: cast referencing both sides is NOT extractable") {
    // Simulates: CAST(a.col + b.col AS BIGINT) — references both sides
    val l1 = AttributeReference("l1", IntegerType)()
    val r1 = AttributeReference("r1", IntegerType)()

    val canExtract = AstUtil.canExtractNonAstConditionIfNeed(
      issue14283CastMeta(LongType, l1, r1), Seq(l1).map(_.exprId), Seq(r1).map(_.exprId))

    // Cannot extract — references both sides
    assertResult(false)(canExtract)
  }

  test("Equi-join pattern: fully AST-able condition needs no extraction") {
    // When all expressions are AST-able, canExtractNonAstConditionIfNeed returns true
    // but extractNonAstFromJoinCond should produce empty left/right lists
    val l1 = AttributeReference("l1", StringType)()
    val r1 = AttributeReference("r1", StringType)()

    val (e, l, r) =
      AstUtil.extractNonAstFromJoinCond(Some(buildTree3(l1, r1, true)), Seq(l1), Seq(r1))
    // No extraction needed — all AST-able
    assertResult(0)(l.size)
    assertResult(0)(r.size)
    assertResult(true)(e.isDefined)
  }
}
