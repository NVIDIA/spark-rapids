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

import ai.rapids.cudf.{BinaryOp, Scalar, UnaryOp}

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Divide, Expression, IntegralDivide, Multiply, Remainder, Subtract, UnaryMinus, UnaryPositive}


class GpuUnaryMinus(child: Expression) extends UnaryMinus(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = {
    GpuColumnVector.from(Scalar.fromByte(0)
      .sub(input.getBase))
  }
}

class GpuUnaryPositive(child: Expression) extends UnaryPositive(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = input
}

class GpuAbs(child: Expression) extends Abs(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.ABS
}

class GpuAdd(left: Expression, right: Expression) extends Add(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

class GpuSubtract(left: Expression, right: Expression) extends Subtract(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

class GpuMultiply(left: Expression, right: Expression) extends Multiply(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.MUL
}

// This is for doubles and floats...
class GpuDivide(left: Expression, right: Expression) extends Divide(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV
}

class GpuIntegralDivide(left: Expression, right: Expression) extends IntegralDivide(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.DIV
}

class GpuRemainder(left: Expression, right: Expression) extends Remainder(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.MOD
}
