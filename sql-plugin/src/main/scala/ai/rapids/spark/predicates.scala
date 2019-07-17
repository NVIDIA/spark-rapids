/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.{BinaryOp, DType, UnaryOp}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Not, Or}

class GpuNot(child: Expression) extends Not(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.NOT
}

class GpuAnd(left: Expression, right: Expression) extends And(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_AND
}

class GpuOr(left: Expression, right: Expression) extends Or(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_OR
}

class GpuEqualTo(left: Expression, right: Expression) extends EqualTo(left, right)
  with CudfBinaryExpression {
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.EQUAL
}

class GpuGreaterThan(left: Expression, right: Expression) extends GreaterThan(left, right)
  with CudfBinaryExpression {
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.GREATER
}

class GpuGreaterThanOrEqual(left: Expression, right: Expression) extends GreaterThanOrEqual(left, right)
  with CudfBinaryExpression {
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.GREATER_EQUAL
}

class GpuLessThan(left: Expression, right: Expression) extends LessThan(left, right)
  with CudfBinaryExpression {
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.LESS
}

class GpuLessThanOrEqual(left: Expression, right: Expression) extends LessThanOrEqual(left, right)
  with CudfBinaryExpression {
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.LESS_EQUAL
}
