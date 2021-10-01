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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryCommand}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan, UnaryExecNode}

trait ShimExpression extends Expression {
  override final def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    shimWithNewChildren(newChildren)
  }

  def shimWithNewChildren(newChildren: Seq[Expression]): Expression =
    legacyWithNewChildren(newChildren)
}

trait ShimUnaryExpression extends UnaryExpression {
  override final def withNewChildInternal(newChild: Expression): Expression =
    shimWithNewChildren(Seq(newChild))

  def shimWithNewChildren(newChildren: Seq[Expression]): Expression =
    legacyWithNewChildren(newChildren)
}

trait ShimBinaryExpression extends BinaryExpression {
  override final def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression = shimWithNewChildren(Seq(newLeft, newRight))

  def shimWithNewChildren(newChildren: Seq[Expression]): Expression =
    legacyWithNewChildren(newChildren)
}

trait ShimTernaryExpression extends TernaryExpression {
  override final def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = shimWithNewChildren(Seq(newFirst, newSecond, newThird))

  def shimWithNewChildren(newChildren: Seq[Expression]): Expression =
    legacyWithNewChildren(newChildren)
}

trait ShimSparkPlan extends SparkPlan {
  override final def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    shimWithNewChildren(newChildren)

  def shimWithNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    legacyWithNewChildren(newChildren)
}

trait ShimUnaryExecNode extends UnaryExecNode {
  override final def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    shimWithNewChildren(Seq(newChild))

  def shimWithNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    legacyWithNewChildren(newChildren)
}

trait ShimBinaryExecNode extends BinaryExecNode {
  override final def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): SparkPlan =
    shimWithNewChildren(Seq(newLeft, newRight))

  def shimWithNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    legacyWithNewChildren(newChildren)
}

trait ShimUnaryCommand extends UnaryCommand {
  override final def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    legacyWithNewChildren(Seq(newChild))
  }
}
