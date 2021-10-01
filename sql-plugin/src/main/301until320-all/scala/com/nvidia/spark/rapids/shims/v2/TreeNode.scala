/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, TernaryExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}

trait ShimExpression extends Expression {
  override final def withNewChildren(newChildren: Seq[Expression]): Expression =
    shimWithNewChildren(newChildren)

  def shimWithNewChildren(newChildren: Seq[Expression]): Expression =
    super.withNewChildren(newChildren)
}

trait ShimUnaryExpression extends UnaryExpression with ShimExpression

trait ShimBinaryExpression extends BinaryExpression with ShimExpression

trait ShimTernaryExpression extends TernaryExpression with ShimExpression {
  def first: Expression
  def second: Expression
  def third: Expression
  final def children: Seq[Expression] = IndexedSeq(first, second, third)
}

trait ShimSparkPlan extends SparkPlan {
  override final def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    shimWithNewChildren(newChildren)

  def shimWithNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    super.withNewChildren(newChildren)
}

trait ShimUnaryExecNode extends UnaryExecNode with ShimSparkPlan

trait ShimBinaryExecNode extends BinaryExecNode with ShimSparkPlan

trait ShimUnaryCommand extends Command
