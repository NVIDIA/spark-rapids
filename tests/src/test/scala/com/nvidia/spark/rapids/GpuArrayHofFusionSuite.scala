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

import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, IntegerType, LongType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuArrayHofFusionSuite extends GpuUnitTests {
  private val arrayArg =
    GpuBoundReference(0, ArrayType(IntegerType, containsNull = true),
      nullable = true)(ExprId(0), "a")
  private val otherArrayArg =
    GpuBoundReference(1, ArrayType(IntegerType, containsNull = true),
      nullable = true)(ExprId(1), "other")
  private val outerB =
    GpuBoundReference(2, IntegerType, nullable = true)(ExprId(2), "b")
  private val outerC =
    GpuBoundReference(3, IntegerType, nullable = true)(ExprId(3), "c")

  private case class NonDeterministicExpr() extends GpuLeafExpression {
    override lazy val deterministic: Boolean = false
    override def dataType: DataType = IntegerType
    override def nullable: Boolean = false
    override def columnarEval(batch: ColumnarBatch): GpuColumnVector =
      throw new UnsupportedOperationException("test-only expression")
  }

  private def lambda(resultType: DataType, argCount: Int = 1): GpuLambdaFunction = {
    val args = (0 until argCount).map { index =>
      GpuNamedLambdaVariable(s"x$index", IntegerType, nullable = true, ExprId(100 + index))
    }
    val value = resultType match {
      case BooleanType => true
      case LongType => 1L
      case _ => 1
    }
    GpuLambdaFunction(GpuLiteral(value, resultType), args)
  }

  private def transform(
      argument: Expression = arrayArg,
      argCount: Int = 1,
      boundIntermediate: Seq[GpuExpression] = Seq.empty): GpuArrayTransform =
    GpuArrayTransform(argument, lambda(IntegerType, argCount), isBound = true, boundIntermediate)

  private def filter(
      argument: Expression = arrayArg,
      argCount: Int = 1,
      boundIntermediate: Seq[GpuExpression] = Seq.empty): GpuArrayFilter =
    GpuArrayFilter(argument, lambda(BooleanType, argCount), isBound = true, boundIntermediate)

  private def aggregate(
      argument: Expression = arrayArg,
      boundIntermediate: Seq[GpuExpression]): GpuArrayAggregate =
    GpuArrayAggregate(argument, GpuLiteral(0L, LongType), lambda(LongType), SumOp,
      isBound = true, boundIntermediate)

  private def alias(expr: GpuExpression, name: String): GpuAlias =
    GpuAlias(expr, name)()

  private def literalProject(name: String): GpuAlias =
    alias(GpuLiteral(1, IntegerType), name)

  private def nonDeterministicProject(name: String): GpuAlias =
    alias(NonDeterministicExpr(), name)

  test("finds heterogeneous array HOFs over the same argument") {
    val exprs = Seq(
      alias(transform(boundIntermediate = Seq(outerB)), "t"),
      literalProject("safe"),
      alias(filter(boundIntermediate = Seq(outerC)), "f"),
      alias(aggregate(boundIntermediate = Seq(outerB, outerC)), "a"))

    assertResult(Seq(Seq(0, 2, 3))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("does not fuse different arguments or lambda arities") {
    val exprs = Seq(
      alias(transform(), "one_arg"),
      alias(filter(argCount = 2), "two_arg"),
      alias(transform(otherArrayArg), "other_arg"))

    assertResult(Seq.empty[Seq[Int]]) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }

  test("splits fused groups around a non-deterministic barrier") {
    val exprs = Seq(
      alias(transform(), "before_left"),
      alias(filter(), "before_right"),
      nonDeterministicProject("barrier"),
      alias(transform(boundIntermediate = Seq(outerB)), "after_left"),
      literalProject("safe"),
      alias(filter(boundIntermediate = Seq(outerC)), "after_right"))

    assertResult(Seq(Seq(0, 1), Seq(3, 5))) {
      GpuArrayHofFusion.findFusedGroupIndexes(exprs)
    }
  }
}
