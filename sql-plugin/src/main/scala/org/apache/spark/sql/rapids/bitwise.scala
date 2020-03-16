/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{BinaryOp, UnaryOp}
import ai.rapids.spark.{CudfBinaryExpression, GpuColumnVector, GpuUnaryExpression}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types._

case class GpuShiftLeft(left: Expression, right: Expression) extends CudfBinaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def binaryOp: BinaryOp = BinaryOp.SHIFT_LEFT

  override def dataType: DataType = left.dataType
}

case class GpuShiftRight(left: Expression, right: Expression) extends CudfBinaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def binaryOp: BinaryOp = BinaryOp.SHIFT_RIGHT

  override def dataType: DataType = left.dataType

}

case class GpuShiftRightUnsigned(left: Expression, right: Expression) extends CudfBinaryExpression
  with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType), IntegerType)

  override def binaryOp: BinaryOp = BinaryOp.SHIFT_RIGHT_UNSIGNED

  override def dataType: DataType = left.dataType
}

case class GpuBitwiseAnd(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "&"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_AND
}

case class GpuBitwiseOr(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "|"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_OR
}

case class GpuBitwiseXor(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = IntegralType

  override def symbol: String = "^"

  override def binaryOp: BinaryOp = BinaryOp.BITWISE_XOR
}

case class GpuBitwiseNot(child: Expression) extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegralType)

  override def dataType: DataType = child.dataType

  override def toString: String = s"~${child.sql}"

  override protected def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    GpuColumnVector.from(input.getBase.unaryOp(UnaryOp.BIT_INVERT))
  }
}