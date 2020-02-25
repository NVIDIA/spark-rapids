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

import ai.rapids.cudf.Scalar
import ai.rapids.spark.{GpuBinaryExpression, GpuColumnVector, GpuExpression, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}

abstract class GpuUnaryString2StringExpression extends GpuUnaryExpression with ExpectsInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = StringType
}

case class GpuUpper(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"upper($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.upper())
}

case class GpuLower(child: Expression) extends GpuUnaryString2StringExpression {

  override def toString: String = s"lower($child)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.lower())
}

case class GpuStartsWith(left: GpuExpression, right: GpuExpression) extends GpuBinaryExpression {

  override def toString: String = s"gpustartswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      createAllTrueColumnVector(lhs)
    } else {
      GpuColumnVector.from(lhs.getBase.startsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in StartsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here," +
    "Cannot have a scalar as left side operand in StartsWith")

  override def dataType: DataType = left.dataType
}

case class GpuEndsWith(left: GpuExpression, right: GpuExpression) extends GpuBinaryExpression {

  override def toString: String = s"gpuendswith($left, $right)"

  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    if (rhs.getJavaString.isEmpty) {
      createAllTrueColumnVector(lhs)
    } else {
      GpuColumnVector.from(lhs.getBase.endsWith(rhs))
    }
  }

  override def doColumnar(lhs: GpuColumnVector,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have two column vectors as input in EndsWith")

  override def doColumnar(lhs: Scalar,
    rhs: GpuColumnVector): GpuColumnVector = throw new IllegalStateException("Really should not be here, " +
    "Cannot have a scalar as left side operand in EndsWith")

  override def dataType: DataType = left.dataType
}