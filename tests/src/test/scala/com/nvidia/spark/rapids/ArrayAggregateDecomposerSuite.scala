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

import org.apache.spark.sql.catalyst.expressions.{Add, And, Cast, Divide, Expression,
  Greatest, LambdaFunction, Least, Literal, Multiply, NamedExpression, NamedLambdaVariable,
  Or, Subtract}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType}

// Extends GpuUnitTests so SQLConf.get is available for the default evalMode / failOnError
// parameter on Add/Subtract/Multiply/Divide (the field name differs across Spark versions;
// letting Spark apply its own default keeps this test shim-agnostic).
class ArrayAggregateDecomposerSuite extends GpuUnitTests {
  import ArrayAggregateDecomposer.decompose

  // --- helpers -----------------------------------------------------------

  private def lv(name: String, dt: DataType = IntegerType): NamedLambdaVariable =
    NamedLambdaVariable(name, dt, nullable = true, exprId = NamedExpression.newExprId)

  private def merge(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(body, Seq(acc, x))

  private def identityFinish(acc: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(acc, Seq(acc))

  private def plus(l: Expression, r: Expression): Add = Add(l, r)
  private def minus(l: Expression, r: Expression): Subtract = Subtract(l, r)
  private def times(l: Expression, r: Expression): Multiply = Multiply(l, r)
  private def div(l: Expression, r: Expression): Divide = Divide(l, r)
  private def greatest(l: Expression, r: Expression): Greatest = Greatest(Seq(l, r))
  private def least(l: Expression, r: Expression): Least = Least(Seq(l, r))

  /** Assert decomposition succeeds; returns the ArrayAggregateDecomposition for further checks. */
  private def assertDecomposes(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable,
      expectedOp: AggOp,
      expectedGChildIndex: Int): ArrayAggregateDecomposition = {
    val d = decompose(merge(body, acc, x), identityFinish(acc))
    assert(d.isDefined, s"expected decomposition for body=$body")
    assert(d.get.op == expectedOp)
    assert(d.get.gChildIndex == expectedGChildIndex)
    assert(d.get.accVarExprId == acc.exprId)
    assert(d.get.elemVar.exprId == x.exprId)
    d.get
  }

  private def assertRejects(
      mergeBody: LambdaFunction,
      finish: Expression,
      reason: String): Unit = {
    assert(decompose(mergeBody, finish).isEmpty, reason)
  }

  // --- positive: one per op ---------------------------------------------

  test("Add(acc, x) -> SUM, gChildIndex=1") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(plus(acc, x), acc, x, SumOp, 1)
  }

  test("Add(x, acc) (commuted) -> SUM, gChildIndex=0") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(plus(x, acc), acc, x, SumOp, 0)
  }

  test("Multiply(acc, x) -> PRODUCT") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(times(acc, x), acc, x, ProductOp, 1)
  }

  test("Greatest(acc, x) -> MAX") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(greatest(acc, x), acc, x, MaxOp, 1)
  }

  test("Least(acc, x) -> MIN") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(least(acc, x), acc, x, MinOp, 1)
  }

  test("And(acc, x) -> ALL") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType)
    assertDecomposes(And(acc, x), acc, x, AllOp, 1)
  }

  test("Or(acc, x) -> ANY") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType)
    assertDecomposes(Or(acc, x), acc, x, AnyOp, 1)
  }

  // --- positive: structural variations ----------------------------------

  test("Complex g(x) with no acc ref still decomposes") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    // g = Cast(x * 2 + 1, Long)
    val g = Cast(plus(times(x, Literal(2)), Literal(1)), LongType)
    assertDecomposes(plus(acc, g), acc, x, SumOp, 1)
  }

  test("Cast wrapping the acc side is unwrapped (single layer)") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    assertDecomposes(plus(Cast(acc, IntegerType), x), acc, x, SumOp, 1)
  }

  test("Cast wrapping the acc side is unwrapped (chained)") {
    val acc = lv("acc"); val x = lv("x")
    val doubleCastAcc = Cast(Cast(acc, LongType), IntegerType)
    assertDecomposes(plus(doubleCastAcc, x), acc, x, SumOp, 1)
  }

  // --- negative: wrong shape --------------------------------------------

  test("Subtract is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(minus(acc, x), acc, x), identityFinish(acc),
      "Subtract is not in the registered AggOps")
  }

  test("Divide is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(div(acc, x), acc, x), identityFinish(acc),
      "Divide is not in the registered AggOps")
  }

  test("Greatest with arity != 2 is not decomposed") {
    val acc = lv("acc"); val x = lv("x")
    val body = Greatest(Seq(acc, x, Literal(1)))
    assertRejects(merge(body, acc, x), identityFinish(acc),
      "Greatest with 3 children is not a 2-operand op")
  }

  test("g that references acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    // g = acc * x, references acc
    assertRejects(merge(plus(acc, times(acc, x)), acc, x), identityFinish(acc),
      "g must not reference acc")
  }

  test("both sides reference acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(plus(acc, acc), acc, x), identityFinish(acc),
      "neither side is a 'pure non-acc'")
  }

  test("neither side is a pure acc ref is rejected") {
    val acc = lv("acc"); val x = lv("x")
    // body = (acc + 1) + x
    assertRejects(merge(plus(plus(acc, Literal(1)), x), acc, x), identityFinish(acc),
      "left side isn't a naked acc ref")
  }

  // --- negative: finish lambda ------------------------------------------

  test("non-identity finish is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val badFinish = LambdaFunction(plus(finishAcc, Literal(1)), Seq(finishAcc))
    assertRejects(merge(plus(acc, x), acc, x), badFinish,
      "finish that multiplies the accumulator isn't identity")
  }

  test("finish referencing a different variable id is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val otherVar = lv("other")
    val badFinish = LambdaFunction(otherVar, Seq(finishAcc))
    assertRejects(merge(plus(acc, x), acc, x), badFinish,
      "finish body refers to a variable that isn't its own arg")
  }

  // --- negative: shape sanity --------------------------------------------

  test("merge with wrong arg count is rejected") {
    val acc = lv("acc"); val x = lv("x"); val extra = lv("extra")
    val body = LambdaFunction(plus(acc, x), Seq(acc, x, extra))
    assertRejects(body, identityFinish(acc), "merge must take 2 lambda args")
  }

  test("merge that isn't a LambdaFunction at all is rejected") {
    val acc = lv("acc")
    assert(decompose(plus(Literal(1), Literal(2)), identityFinish(acc)).isEmpty)
  }

  test("finish that isn't a LambdaFunction is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assert(decompose(merge(plus(acc, x), acc, x), Literal(0)).isEmpty)
  }
}
