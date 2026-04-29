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
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType,
  LongType}

// Extends GpuUnitTests so SQLConf.get is available for the default evalMode / failOnError
// parameter on Add/Subtract/Multiply/Divide (the field name differs across Spark versions;
// letting Spark apply its own default keeps this test shim-agnostic).
class ArrayAggregateDecomposerSuite extends GpuUnitTests {
  import ArrayAggregateDecomposer.decompose

  private def lv(name: String, dt: DataType = IntegerType): NamedLambdaVariable =
    NamedLambdaVariable(name, dt, nullable = true, exprId = NamedExpression.newExprId)

  private def merge(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(body, Seq(acc, x))

  private def identityFinish(acc: NamedLambdaVariable): LambdaFunction =
    LambdaFunction(acc, Seq(acc))

  /** Wrap zeroType in an ArrayType(_, containsNull = false) for the typical happy path. */
  private def arrTy(zeroType: DataType): ArrayType = ArrayType(zeroType, containsNull = false)

  private def assertDecomposes(
      body: Expression,
      acc: NamedLambdaVariable,
      x: NamedLambdaVariable,
      expectedOp: AggOp,
      expectedGIsLeft: Boolean,
      zeroType: DataType = IntegerType,
      argType: Option[DataType] = None): ArrayAggregateDecomposition = {
    val d = decompose(merge(body, acc, x), identityFinish(acc),
      argType.getOrElse(arrTy(zeroType)), zeroType)
    val r = d.getOrElse(fail(s"expected decomposition for body=$body, got Left: $d"))
    assert(r.op == expectedOp)
    assert(r.gIsLeftOfMergeBody == expectedGIsLeft,
      s"expected gIsLeftOfMergeBody=$expectedGIsLeft, got ${r.gIsLeftOfMergeBody}")
    assert(r.accVarExprId == acc.exprId)
    assert(r.elemVar.exprId == x.exprId)
    r
  }

  private def assertRejects(
      mergeBody: LambdaFunction,
      finish: Expression,
      reason: String,
      zeroType: DataType = IntegerType,
      argType: Option[DataType] = None): String = {
    val d = decompose(mergeBody, finish, argType.getOrElse(arrTy(zeroType)), zeroType)
    assert(d.isLeft, s"$reason — expected Left but got: $d")
    d.swap.getOrElse(fail("unreachable"))
  }

  test("Add(acc, x) -> SUM, g on the right") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Add(acc, x), acc, x, SumOp, expectedGIsLeft = false)
  }

  test("Add(x, acc) (commuted) -> SUM, g on the left") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Add(x, acc), acc, x, SumOp, expectedGIsLeft = true)
  }

  test("Multiply(acc, x) -> PRODUCT") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Multiply(acc, x), acc, x, ProductOp, expectedGIsLeft = false)
  }

  test("Greatest(acc, x) -> MAX") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Greatest(Seq(acc, x)), acc, x, MaxOp, expectedGIsLeft = false)
  }

  test("Least(acc, x) -> MIN") {
    val acc = lv("acc"); val x = lv("x")
    assertDecomposes(Least(Seq(acc, x)), acc, x, MinOp, expectedGIsLeft = false)
  }

  test("And(acc, x) -> ALL") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType)
    assertDecomposes(And(acc, x), acc, x, AllOp, expectedGIsLeft = false,
      zeroType = BooleanType)
  }

  test("Or(acc, x) -> ANY") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType)
    assertDecomposes(Or(acc, x), acc, x, AnyOp, expectedGIsLeft = false,
      zeroType = BooleanType)
  }

  test("Complex g(x) with no acc ref decomposes (g on the right)") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    val g = Cast(Add(Multiply(x, Literal(2)), Literal(1)), LongType)
    assertDecomposes(Add(acc, g), acc, x, SumOp, expectedGIsLeft = false, zeroType = LongType)
  }

  test("Cast wrapping the acc side is unwrapped (single layer)") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    assertDecomposes(Add(Cast(acc, IntegerType), x), acc, x, SumOp, expectedGIsLeft = false)
  }

  test("Cast wrapping the acc side is unwrapped (chained)") {
    val acc = lv("acc"); val x = lv("x")
    val doubleCastAcc = Cast(Cast(acc, LongType), IntegerType)
    assertDecomposes(Add(doubleCastAcc, x), acc, x, SumOp, expectedGIsLeft = false)
  }

  test("Subtract is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Subtract(acc, x), acc, x), identityFinish(acc),
      "Subtract is not in the registered AggOps")
  }

  test("Divide is not an associative op we recognize") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Divide(acc, x), acc, x), identityFinish(acc),
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
    assertRejects(merge(Add(acc, Multiply(acc, x)), acc, x), identityFinish(acc),
      "g must not reference acc")
  }

  test("both sides reference acc is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Add(acc, acc), acc, x), identityFinish(acc),
      "neither side is a 'pure non-acc'")
  }

  test("neither side is a pure acc ref is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assertRejects(merge(Add(Add(acc, Literal(1)), x), acc, x), identityFinish(acc),
      "left side isn't a naked acc ref")
  }

  test("non-identity finish is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val badFinish = LambdaFunction(Add(finishAcc, Literal(1)), Seq(finishAcc))
    assertRejects(merge(Add(acc, x), acc, x), badFinish,
      "finish that multiplies the accumulator isn't identity")
  }

  test("finish referencing a different variable id is rejected") {
    val acc = lv("acc"); val x = lv("x")
    val finishAcc = lv("finishAcc")
    val otherVar = lv("other")
    val badFinish = LambdaFunction(otherVar, Seq(finishAcc))
    assertRejects(merge(Add(acc, x), acc, x), badFinish,
      "finish body refers to a variable that isn't its own arg")
  }

  test("merge with wrong arg count is rejected") {
    val acc = lv("acc"); val x = lv("x"); val extra = lv("extra")
    val body = LambdaFunction(Add(acc, x), Seq(acc, x, extra))
    assertRejects(body, identityFinish(acc), "merge must take 2 lambda args")
  }

  test("merge that isn't a LambdaFunction at all is rejected") {
    val acc = lv("acc")
    assert(decompose(Add(Literal(1), Literal(2)), identityFinish(acc),
      arrTy(IntegerType), IntegerType).isLeft)
  }

  test("finish that isn't a LambdaFunction is rejected") {
    val acc = lv("acc"); val x = lv("x")
    assert(decompose(merge(Add(acc, x), acc, x), Literal(0),
      arrTy(IntegerType), IntegerType).isLeft)
  }

  // The decomposer now owns the "is this shape ever GPU-able" decision, so it must also
  // reject unsupported types and AllOp/AnyOp on null-bearing arrays.

  test("MaxOp on Double is rejected (NaN propagation differs from cuDF)") {
    val acc = lv("acc", DoubleType); val x = lv("x", DoubleType)
    val msg = assertRejects(merge(Greatest(Seq(acc, x)), acc, x), identityFinish(acc),
      "MAX should fall back on Double",
      zeroType = DoubleType)
    assert(msg.contains("MAX"), s"expected MAX-related error, got: $msg")
  }

  test("ALL on array<bool> with containsNull rejects") {
    val acc = lv("acc", BooleanType); val x = lv("x", BooleanType)
    val msg = assertRejects(merge(And(acc, x), acc, x), identityFinish(acc),
      "ALL on null-bearing array should fall back",
      zeroType = BooleanType,
      argType = Some(ArrayType(BooleanType, containsNull = true)))
    assert(msg.contains("ALL"), s"expected ALL-related error, got: $msg")
  }

  test("g type mismatch with zero type rejects") {
    val acc = lv("acc", LongType); val x = lv("x", IntegerType)
    // body sums a non-cast Int element into a Long acc — g.dataType=Int doesn't match
    // zeroType=Long, so this must fall back even though the shape is otherwise OK.
    val msg = assertRejects(merge(Add(acc, x), acc, x), identityFinish(acc),
      "g type mismatch should fall back",
      zeroType = LongType)
    assert(msg.contains("does not match"), s"expected type-mismatch error, got: $msg")
  }
}
