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

import ai.rapids.cudf.DType

import org.apache.spark.sql.catalyst.expressions.{Acos, Asin, Atan, Ceil, Cos, Exp, Expression, Floor, Log, Pow, Sin, Sqrt, Tan}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuAcos(child: Expression) extends Acos(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.arccos(DType.FLOAT64))
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
    return other.isInstanceOf[GpuAcos]
  }
}

class GpuAsin(child: Expression) extends Asin(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.arcsin(DType.FLOAT64))
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
    return other.isInstanceOf[GpuAsin]
  }
}

// TODO it would be great if we could use the ENUM to some how make most of the copy/paste go away
class GpuAtan(child: Expression) extends Atan(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.arctan(DType.FLOAT64))
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
    return other.isInstanceOf[GpuAtan]
  }
}

class GpuCeil(child: Expression) extends Ceil(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a long, except for DecimalType that we don't support yet
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.ceil(DType.INT64))
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
    return other.isInstanceOf[GpuCeil]
  }
}

class GpuCos(child: Expression) extends Cos(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.cos(DType.FLOAT64))
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
    return other.isInstanceOf[GpuCos]
  }
}

class GpuExp(child: Expression) extends Exp(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.exp(DType.FLOAT64))
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
    return other.isInstanceOf[GpuExp]
  }
}

class GpuFloor(child: Expression) extends Floor(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a long, except for DecimalType that we don't support yet
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.floor(DType.INT64))
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
    return other.isInstanceOf[GpuFloor]
  }
}

class GpuLog(child: Expression) extends Log(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.log(DType.FLOAT64))
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
    return other.isInstanceOf[GpuLog]
  }
}

class GpuSin(child: Expression) extends Sin(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.sin(DType.FLOAT64))
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
    return other.isInstanceOf[GpuSin]
  }
}

class GpuSqrt(child: Expression) extends Sqrt(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.sqrt(DType.FLOAT64))
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
    return other.isInstanceOf[GpuSqrt]
  }
}

class GpuTan(child: Expression) extends Tan(child) {
  override def columnarEval(batch: ColumnarBatch): Any = {
    var ret: Any = null
    var input: Any = null
    try {
      input = child.columnarEval(batch)
      if (input.isInstanceOf[GpuColumnVector]) {
        // Output is always a double
        ret = GpuColumnVector.from(input.asInstanceOf[GpuColumnVector].getBase.tan(DType.FLOAT64))
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
    return other.isInstanceOf[GpuTan]
  }
}

class GpuPow(left: Expression, right: Expression)
  extends Pow(left, right) {
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
        ret = GpuColumnVector.from(l.getBase.pow(r.getBase, DType.FLOAT64))
      } else if (rhs.isInstanceOf[GpuColumnVector]) {
        val l = GpuScalar.from(lhs)
        val r = rhs.asInstanceOf[GpuColumnVector]
        ret = GpuColumnVector.from(l.pow(r.getBase, DType.FLOAT64))
      } else if (lhs.isInstanceOf[GpuColumnVector]) {
        val l = lhs.asInstanceOf[GpuColumnVector]
        val r = GpuScalar.from(rhs)
        ret = GpuColumnVector.from(l.getBase.pow(r, DType.FLOAT64))
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
    return other.isInstanceOf[GpuPow]
  }
}
