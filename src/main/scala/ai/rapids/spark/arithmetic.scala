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

import ai.rapids.cudf.{BinaryOp, BinaryOperable, DType, Scalar, UnaryOp}

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, BinaryExpression, Divide, Expression, IntegralDivide, Multiply, Remainder, Subtract, UnaryExpression, UnaryMinus, UnaryPositive}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

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

trait GpuUnaryExpression extends UnaryExpression {
  override def supportsColumnar(): Boolean = child.supportsColumnar

  def doColumnar(input: GpuColumnVector): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    val input = child.columnarEval(batch)
    try {
      input match {
        case vec: GpuColumnVector => doColumnar(vec)
        case v if (v != null) => nullSafeEval(v)
        case _ => null
      }
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
    return other.isInstanceOf[GpuUnaryExpression]
  }

  override def hashCode(): Int = super.hashCode()
}


trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp
  def outputTypeOverride: DType = null

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val base = input.getBase
    val outType = if (outputTypeOverride != null) {
      outputTypeOverride
    } else {
      base.getType
    }
    GpuColumnVector.from(base.unaryOp(unaryOp, outType))
  }
}


trait GpuBinaryExpression extends BinaryExpression {
  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      (lhs, rhs) match {
        case (l: GpuColumnVector, r: GpuColumnVector) => doColumnar(l, r)
        case (l, r: GpuColumnVector) => doColumnar(GpuScalar.from(l), r)
        case (l: GpuColumnVector, r) => doColumnar(l, GpuScalar.from(r))
        case (l, r) if (l != null && r != null) => nullSafeEval(l, r)
        case _ => null
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuBinaryExpression]
  }

  override def hashCode(): Int = super.hashCode()
}


trait CudfBinaryExpression extends GpuBinaryExpression {
  def binaryOp: BinaryOp
  def outputTypeOverride: DType = null

  def outputType(l: BinaryOperable, r: BinaryOperable) : DType = {
    val over = outputTypeOverride
    if (over == null) {
      BinaryOperable.implicitConversion(l, r)
    } else {
      over
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector = {
    val lBase = lhs.getBase
    val rBase = rhs.getBase
    val outType = outputType(lBase, rBase)
    GpuColumnVector.from(lBase.binaryOp(binaryOp, rBase, outType))
  }

  override def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector = {
    val rBase = rhs.getBase
    val outType = outputType(lhs, rBase)
    GpuColumnVector.from(lhs.binaryOp(binaryOp, rBase, outType))
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector = {
    val lBase = lhs.getBase
    val outType = outputType(lBase, rhs)
    GpuColumnVector.from(lBase.binaryOp(binaryOp, rhs, outType))
  }
}


class GpuUnaryMinus(child: Expression) extends UnaryMinus(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = {
    GpuColumnVector.from(Scalar.fromByte(0)
      .sub(input.getBase))
  }
}

class GpuUnaryPositive(child: Expression) extends UnaryPositive(child) with GpuUnaryExpression {
  override def doColumnar(input: GpuColumnVector) : GpuColumnVector = input
}

class GpuAbs(child: Expression) extends Abs(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.ABS
}

class GpuAdd(left: Expression, right: Expression) extends Add(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.ADD
}

class GpuSubtract(left: Expression, right: Expression) extends Subtract(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.SUB
}

class GpuMultiply(left: Expression, right: Expression) extends Multiply(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.MUL
}

// This is for doubles and floats...
class GpuDivide(left: Expression, right: Expression) extends Divide(left, right)
  with CudfBinaryExpression {

  override def binaryOp: BinaryOp = BinaryOp.TRUE_DIV
}

class GpuIntegralDivide(left: Expression, right: Expression) extends IntegralDivide(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.DIV
}

class GpuRemainder(left: Expression, right: Expression) extends Remainder(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.MOD
}
