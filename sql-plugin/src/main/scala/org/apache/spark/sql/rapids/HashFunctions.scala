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

import ai.rapids.cudf.{BinaryOp, ColumnVector, DType}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuUnaryExpression}

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types._

case class GpuMd5(child: Expression)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def toString: String = s"md5($child)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val fullResult = ColumnVector.md5Hash(input.getBase)
    try {
      GpuColumnVector.from(fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, input.getBase))
    } finally {
      fullResult.close()
    }
  }
}
