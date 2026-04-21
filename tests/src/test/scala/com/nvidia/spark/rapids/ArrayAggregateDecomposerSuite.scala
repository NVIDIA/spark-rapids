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

import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Expression, LambdaFunction,
  Literal, Multiply, NamedExpression, NamedLambdaVariable, Subtract}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}

// Extends GpuUnitTests so SQLConf.get is available for the default evalMode/failOnError
// parameter on Add/Subtract/Multiply (the field name differs across Spark versions; letting
// Spark apply its own default keeps this test shim-agnostic).
class ArrayAggregateDecomposerSuite extends GpuUnitTests {
  import ArrayAggregateDecomposer._

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

  // --- positive cases ----------------------------------------------------

  test("Add(acc, x) decomposes to SUM with gChildIndex=1") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(acc, x), acc, x)
    val d = decompose(m, identityFinish(acc))
    assert(d.isDefined)
    assert(d.get.op == SumOp)
    assert(d.get.gChildIndex == 1)
    assert(d.get.accVarExprId == acc.exprId)
    assert(d.get.elemVar.exprId == x.exprId)
  }

  test("Add(x, acc) (commuted) decomposes with gChildIndex=0") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(x, acc), acc, x)
    val d = decompose(m, identityFinish(acc))
    assert(d.isDefined)
    assert(d.get.gChildIndex == 0)
  }

  test("Add(acc, complex g(x)) where g contains no acc ref decomposes") {
    val acc = lv("acc", LongType)
    val x = lv("x", IntegerType)
    // g = x * 2 + 1 (as Long after Cast)
    val g = plus(times(x, Literal(2)), Literal(1))
    val m = merge(plus(acc, Cast(g, LongType)), acc, x)
    val d = decompose(m, identityFinish(acc))
    assert(d.isDefined)
    assert(d.get.gChildIndex == 1)
  }

  test("acc wrapped in a Cast on the acc-side is still accepted") {
    val acc = lv("acc", LongType)
    val x = lv("x", IntegerType)
    // body = Cast(acc, Int) + x
    val m = merge(plus(Cast(acc, IntegerType), x), acc, x)
    val d = decompose(m, identityFinish(acc))
    assert(d.isDefined)
    assert(d.get.gChildIndex == 1)
  }

  test("chained Cast on acc side is unwrapped") {
    val acc = lv("acc")
    val x = lv("x")
    // body = Cast(Cast(acc, Long), Int) + x
    val accDoubleCast = Cast(Cast(acc, LongType), IntegerType)
    val m = merge(plus(accDoubleCast, x), acc, x)
    val d = decompose(m, identityFinish(acc))
    assert(d.isDefined)
  }

  // --- negative cases ----------------------------------------------------

  test("Subtract rejected (only Add is SUM)") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(minus(acc, x), acc, x)
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("Multiply rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(times(acc, x), acc, x)
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("g that references acc is rejected") {
    val acc = lv("acc")
    val x = lv("x")
    // g = acc * x — references acc
    val m = merge(plus(acc, times(acc, x)), acc, x)
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("both sides reference acc is rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(acc, acc), acc, x)
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("neither side is a pure acc ref is rejected") {
    val acc = lv("acc")
    val x = lv("x")
    // body = (acc + 1) + x — left has acc but isn't a naked acc ref
    val leftWithPlusOne = plus(acc, Literal(1))
    val m = merge(plus(leftWithPlusOne, x), acc, x)
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("non-identity finish rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(acc, x), acc, x)
    val finishAcc = lv("finishAcc")
    val nonIdentityFinish =
      LambdaFunction(plus(finishAcc, Literal(1)), Seq(finishAcc))
    assert(decompose(m, nonIdentityFinish).isEmpty)
  }

  test("finish referencing a different variable id is rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(acc, x), acc, x)
    // finish's body references a NamedLambdaVariable with a *different* exprId,
    // so it is not the identity over the finish's arg.
    val finishAcc = lv("finishAcc")
    val someOther = lv("other")  // different exprId
    val badFinish = LambdaFunction(someOther, Seq(finishAcc))
    assert(decompose(m, badFinish).isEmpty)
  }

  test("merge with wrong arg count rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val extra = lv("extra")
    val m = LambdaFunction(plus(acc, x), Seq(acc, x, extra))
    assert(decompose(m, identityFinish(acc)).isEmpty)
  }

  test("merge that isn't a LambdaFunction at all is rejected") {
    val acc = lv("acc")
    // Pass something that isn't a lambda.
    assert(decompose(plus(Literal(1), Literal(2)), identityFinish(acc)).isEmpty)
  }

  test("finish that isn't a LambdaFunction rejected") {
    val acc = lv("acc")
    val x = lv("x")
    val m = merge(plus(acc, x), acc, x)
    assert(decompose(m, Literal(0)).isEmpty)
  }
}
