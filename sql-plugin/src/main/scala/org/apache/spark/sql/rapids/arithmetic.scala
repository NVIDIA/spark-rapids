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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{BinaryOp, Scalar, UnaryOp}
import ai.rapids.spark.{CudfBinaryOperator, CudfUnaryExpression, GpuColumnVector, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, DataType, DecimalType, DoubleType, IntegralType, LongType, NumericType, TypeCollection}


case class GpuUnaryMinus(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  override def sql: String = s"(- ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = {
    GpuColumnVector.from(Scalar.fromByte(0)
      .sub(input.getBase))
  }
}

case class GpuUnaryPositive(child: Expression) extends GpuUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = input
}

case class GpuAbs(child: Expression) extends CudfUnaryExpression
    with ExpectsInputTypes with NullIntolerant {
  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  override def unaryOp: UnaryOp = UnaryOp.ABS
}

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {
  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess
}

case class GpuAdd(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

case class GpuSubtract(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

case class GpuMultiply(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  override def binaryOp: BinaryOp = BinaryOp.MUL
}

// This is for doubles and floats...
case class GpuDivide(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def nullable: Boolean = true

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV
}

case class GpuIntegralDivide(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def nullable: Boolean = true

  override def inputType: AbstractDataType = IntegralType

  override def dataType: DataType = if (SQLConf.get.integralDivideReturnLong) {
    LongType
  } else {
    left.dataType
  }

  override def symbol: String = "/"

  override def binaryOp: BinaryOp = BinaryOp.DIV
}

case class GpuRemainder(left: Expression, right: Expression) extends CudfBinaryArithmetic {
  override def nullable: Boolean = true

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"

  override def binaryOp: BinaryOp = BinaryOp.MOD
}
