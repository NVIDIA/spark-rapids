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

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Divide, Expression, IntegralDivide, Multiply, Remainder, Subtract, UnaryMinus, UnaryPositive}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuScalar {
  def from(v: Any): Scalar = {
    if (v == null) {
      Scalar.NULL
    } else if (v.isInstanceOf[Long]) {
      Scalar.fromLong(v.asInstanceOf[Long])
    } else if (v.isInstanceOf[Double]) {
      Scalar.fromDouble(v.asInstanceOf[Double])
    } else if (v.isInstanceOf[Int]) {
      Scalar.fromInt(v.asInstanceOf[Int])
    } else if (v.isInstanceOf[Float]) {
      Scalar.fromFloat(v.asInstanceOf[Float])
    } else if (v.isInstanceOf[Short]) {
      Scalar.fromShort(v.asInstanceOf[Short])
    } else if (v.isInstanceOf[Byte]) {
      Scalar.fromByte(v.asInstanceOf[Byte])
    } else if (v.isInstanceOf[Boolean]) {
      Scalar.fromBool(v.asInstanceOf[Boolean])
    } else {
      throw new IllegalStateException(s"${v} is not supported as a scalar yet")
    }
  }
}

class GpuUnaryMinus(child: Expression) extends UnaryMinus(child) {
  override def supportsColumnar(): Boolean = child.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    val input = child.columnarEval(batch)
    try {
      if (input == null) {
        ret = null
      } else if (input.isInstanceOf[GpuColumnVector]) {
        ret = GpuColumnVector.from(Scalar.fromByte(0)
          .sub(input.asInstanceOf[GpuColumnVector].getBase))
      } else {
        ret = nullSafeEval(input)
      }
      ret
    } finally {
      if (input != null && input.isInstanceOf[ColumnVector]) {
        input.asInstanceOf[ColumnVector].close()
      }
    }
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuUnaryMinus]
  }
}

class GpuUnaryPositive(child: Expression) extends UnaryPositive(child) {
  override def supportsColumnar(): Boolean = child.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    child.columnarEval(batch)
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuUnaryPositive]
  }
}

class GpuAbs(child: Expression) extends Abs(child) {
  override def supportsColumnar(): Boolean = child.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.abs())
      } else {
        ret = nullSafeEval(input)
      }
    } finally {
      if (input != null && input.isInstanceOf[ColumnVector]) {
        input.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuAbs]
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
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
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

class GpuSubtract(left: Expression, right: Expression)
  extends Subtract(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.sub(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.sub(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.sub(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
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

class GpuMultiply(left: Expression, right: Expression)
  extends Multiply(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.mul(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.mul(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.mul(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuMultiply]
  }
}

// This is for doubles and floats...
class GpuDivide(left: Expression, right: Expression)
  extends Divide(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.trueDiv(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.trueDiv(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.trueDiv(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuDivide]
  }
}

class GpuIntegralDivide(left: Expression, right: Expression)
  extends IntegralDivide(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.div(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.div(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.div(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuIntegralDivide]
  }
}

class GpuRemainder(left: Expression, right: Expression)
  extends Remainder(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.mod(r.getBase))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.mod(r.getBase))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.mod(r))
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuRemainder]
  }
}
