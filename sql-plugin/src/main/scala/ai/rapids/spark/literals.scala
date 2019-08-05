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
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

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
    case s: String => Scalar.fromString(s)
    case s: UTF8String => Scalar.fromString(s.toString)
    case _ => throw new IllegalStateException(s"${v.getClass} '${v}' is not supported as a scalar yet")
  }

  def from(v: Any, t: DataType): Scalar = v match {
    case _ if v == null => Scalar.fromNull(GpuColumnVector.getRapidsType(t))
    case l: Long => t match {
      case LongType => Scalar.fromLong(l)
      case TimestampType => Scalar.timestampFromLong(l)
      case _ => throw new IllegalArgumentException(s"$t not supported for long values")
    }
    case d: Double => Scalar.fromDouble(d)
    case i: Int => t match {
      case IntegerType => Scalar.fromInt(i)
      case DateType => Scalar.dateFromInt(i)
      case _ => throw new IllegalArgumentException(s"$t not supported for int values")
    }
    case f: Float => Scalar.fromFloat(f)
    case s: Short => Scalar.fromShort(s)
    case b: Byte => Scalar.fromByte(b)
    case b: Boolean => Scalar.fromBool(b)
    case s: String => Scalar.fromString(s)
    case s: UTF8String => Scalar.fromString(s.toString)
    case _ => throw new IllegalStateException(s"${v.getClass} '${v}' is not supported as a scalar yet")
  }
}

class GpuLiteral (value: Any, dataType: DataType) extends Literal(value, dataType)
  with GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = value
}
