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

import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuScalar {
  def from(v: Any): Scalar = {
    if (v == null) {
      Scalar.NULL
    } else if (v.isInstanceOf[Long]) {
      Scalar.fromLong(v.asInstanceOf[Long])
    } else {
      throw new IllegalStateException(s"${v} is not supported as a scalar yet")
    }
  }
}

class GpuAdd(left: Expression, right: Expression)
    extends Add(left, right) {
  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      if (lhs == null || rhs == null) {
        ret = null
      } else if (lhs.isInstanceOf[GpuColumnVector] && rhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.getBase.add(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.add(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.add(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[GpuColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[GpuColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuAdd]
  }
}
