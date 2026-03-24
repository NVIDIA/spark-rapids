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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.rapids.{GpuGreaterThan, GpuLength, GpuStringTrim}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}


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
}
