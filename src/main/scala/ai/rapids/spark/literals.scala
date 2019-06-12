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

import ai.rapids.cudf.Scalar

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuScalar {
  def from(v: Any): Scalar = v match {
    case _ if (v == null) => Scalar.NULL
    case l: Long => Scalar.fromLong(l)
    case d: Double => Scalar.fromDouble(d)
    case i: Int => Scalar.fromInt(i)
    case f: Float => Scalar.fromFloat(f)
    case s: Short => Scalar.fromShort(s)
    case b: Byte => Scalar.fromByte(b)
    case b: Boolean => Scalar.fromBool(b)
    case _ => throw new IllegalStateException(s"${v} is not supported as a scalar yet")
  }
}

class GpuLiteral (value: Any, dataType: DataType) extends Literal(value, dataType)
  with GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = value
}
