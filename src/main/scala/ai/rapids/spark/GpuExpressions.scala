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

import ai.rapids.cudf.{BinaryOp, BinaryOperable, DType, Scalar, TimeUnit, UnaryOp}

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

trait GpuExpression extends Expression {
  /**
   * Returns the result of evaluating this expression on the entire
   * [[ColumnarBatch]]. The result of calling this may be a single [[GpuColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a literal that Add would have to be able to handle.
   *
   * By convention any [[GpuColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a [[ColumnarBatch]] and closing the batch or by closing the vector directly if it is a
   * temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuBinaryExpression]
  }

  override def hashCode(): Int = super.hashCode()
}

trait GpuUnaryExpression extends UnaryExpression with GpuExpression {
  def doColumnar(input: GpuColumnVector): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    val input = child.asInstanceOf[GpuExpression].columnarEval(batch)
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
}


trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp
  def outputTypeOverride: DType = null

  override def doColumnar(input: GpuColumnVector): GpuColumnVector = {
    val base = input.getBase
    // For unary opts the input and output type must match
    var tmp = base.unaryOp(unaryOp, base.getType)
    try {
      val ret = if (outputTypeOverride != null && outputTypeOverride != tmp.getType) {
        tmp.castTo(outputTypeOverride, TimeUnit.NONE)
      } else {
        val r = tmp
        tmp = null
        r
      }
      GpuColumnVector.from(ret)
    } finally {
      if (tmp != null) {
        tmp.close()
      }
    }
  }
}

trait GpuBinaryExpression extends BinaryExpression with GpuExpression {

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: Scalar, rhs: GpuColumnVector): GpuColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: Scalar): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    try {
      lhs = left.asInstanceOf[GpuExpression].columnarEval(batch)
      rhs = right.asInstanceOf[GpuExpression].columnarEval(batch)

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
