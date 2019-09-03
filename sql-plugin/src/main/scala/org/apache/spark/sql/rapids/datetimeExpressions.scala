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

import ai.rapids.cudf.DType
import ai.rapids.spark.{GpuColumnVector, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.types.{AbstractDataType, DataType, DateType, IntegerType}

trait GpuDateTimeUnaryExpression extends GpuUnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override def outputTypeOverride = DType.INT32
}

case class GpuYear(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.year())
}

case class GpuMonth(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.month())
}

case class GpuDayOfMonth(child: Expression) extends GpuDateTimeUnaryExpression {
  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.day())
}