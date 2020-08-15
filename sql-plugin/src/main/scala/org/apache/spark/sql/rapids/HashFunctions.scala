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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, PadSide, Scalar, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, NullIntolerant, Predicate, SubstringIndex}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

// case class GpuMd5(child: expression) extends GpuUnaryExpression with ImplicitCastInputTypes { //implict cast to binary, but we can't do that on any front right now
case class GpuMd5(child: Expression) extends GpuUnaryExpression {
  override def toString: String = s"md5($child)"
  override def dataType: DataType = StringType

  // override def inputTypes: Seq[DataType] = Seq(ByteTypes)

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(ColumnVector.md5Hash(input.getBase))
}
