/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.rapids.GpuAnd

trait ShimExpression extends Expression

trait ShimUnaryExpression extends UnaryExpression

trait ShimBinaryExpression extends BinaryExpression

trait ShimTernaryExpression extends TernaryExpression {
  def first: Expression
  def second: Expression
  def third: Expression
  final def children: Seq[Expression] = IndexedSeq(first, second, third)
}

trait ShimSparkPlan extends SparkPlan

trait ShimUnaryExecNode extends UnaryExecNode

trait ShimBinaryExecNode extends BinaryExecNode

trait ShimUnaryCommand extends Command

trait ShimPredicateHelper extends PredicateHelper {
  // SPARK-30027 from 3.2.0
  // If one expression and its children are null intolerant, it is null intolerant.
  protected def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override protected def splitConjunctivePredicates(
    condition: Expression
  ): Seq[Expression] = {
    condition match {
      case GpuAnd(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => super.splitConjunctivePredicates(condition)
    }
  }
}