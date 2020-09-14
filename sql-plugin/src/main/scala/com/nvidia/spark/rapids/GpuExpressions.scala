/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, BinaryOperable, DType, Scalar, UnaryOp}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BinaryOperator, ComplexTypeMergingExpression, Expression, String2TrimExpression, TernaryExpression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuExpressionsUtils extends Arm {
  def evaluateBoundExpressions[A <: GpuExpression](cb: ColumnarBatch,
      boundExprs: Seq[A]): Seq[GpuColumnVector] = {
    val numCols = boundExprs.length
    closeOnExcept(new ArrayBuffer[GpuColumnVector](numCols)) { resultCvs =>
      for (i <- 0 until numCols) {
        val ref = boundExprs(i)
        resultCvs += ref.columnarEval(cb).asInstanceOf[GpuColumnVector]
      }
      resultCvs
    }
  }

  def getTrimString(trimStr: Option[Expression]): String = trimStr match {
    case Some(GpuLiteral(data, StringType)) =>
      if (data == null) {
        null
      } else {
        data.asInstanceOf[UTF8String].toString
      }

    case Some(GpuAlias(GpuLiteral(data, StringType), _)) =>
      if (data == null) {
        null
      } else {
        data.asInstanceOf[UTF8String].toString
      }

    case None => " "

    case _ =>
      throw new IllegalStateException("Internal Error GPU support for this data type is not " +
        "implemented and should have been disabled")
  }
}

/**
 * An Expression that cannot be evaluated in the traditional row-by-row sense (hence Unevaluable)
 * but instead can be evaluated on an entire column batch at once.
 */
trait GpuExpression extends Expression with Unevaluable with Arm {
  /**
   * Override this if your expression cannot allow combining of data from multiple files
   * into a single batch before it operates on them. These are for things like getting
   * the input file name. Which for spark is stored in a thread local variable which means
   * we have to jump through some hoops to make this work.
   */
  def disableCoalesceUntilInput(): Boolean =
    children.exists {
      case c: GpuExpression => c.disableCoalesceUntilInput()
      case _ => false // This path should never really happen
    }

  /**
   * Returns the result of evaluating this expression on the entire
   * `ColumnarBatch`. The result of calling this may be a single `GpuColumnVector` or a scalar
   * value. Scalar values typically happen if they are a part of the expression i.e. col("a") + 100.
   * In this case the 100 is a literal that Add would have to be able to handle.
   *
   * By convention any `GpuColumnVector` returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a `ColumnarBatch` and closing the batch or by closing the vector directly if it is a
   * temporary value.
   */
  def columnarEval(batch: ColumnarBatch): Any

  override lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    GpuCanonicalize.execute(withNewChildren(canonicalizedChildren))
  }
}

abstract class GpuLeafExpression extends GpuExpression {
  override final def children: Seq[Expression] = Nil
}

trait GpuUnevaluable extends GpuExpression {
  final override def columnarEval(batch: ColumnarBatch): Any =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

abstract class GpuUnevaluableUnaryExpression extends GpuUnaryExpression with GpuUnevaluable {
  final override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

abstract class GpuUnaryExpression extends UnaryExpression with GpuExpression {
  protected def doColumnar(input: GpuColumnVector): GpuColumnVector

  def outputTypeOverride: DType = null

  override def columnarEval(batch: ColumnarBatch): Any = {
    val input = child.columnarEval(batch)
    try {
      input match {
        case vec: GpuColumnVector =>
          var tmp = doColumnar(vec)
          try {
            val base = tmp.getBase
            if (outputTypeOverride != null && outputTypeOverride != base.getType) {
              GpuColumnVector.from(base.castTo(outputTypeOverride))
            } else {
              val r = tmp
              tmp = null
              r
            }
          } finally {
            if (tmp != null) {
              tmp.close()
            }
          }
        case _ => throw new IllegalStateException(
          s"Unary expression $this should only see a column result from child eval")
      }
    } finally {
      if (input.isInstanceOf[AutoCloseable]) {
        input.asInstanceOf[AutoCloseable].close()
      }
    }
  }
}

trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.unaryOp(unaryOp))
}

trait GpuBinaryExpression extends BinaryExpression with GpuExpression {

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
        case (l, r: GpuColumnVector) =>
          val scalar = GpuScalar.from(l, left.dataType)
          try {
            doColumnar(scalar, r)
          } finally {
            scalar.close()
          }
        case (l: GpuColumnVector, r) =>
          val scalar = GpuScalar.from(r, right.dataType)
          try {
            doColumnar(l, scalar)
          } finally {
            scalar.close()
          }
        case (l, r) if (l != null && r != null) => nullSafeEval(l, r)
        case _ => null
      }
    } finally {
      if (lhs.isInstanceOf[AutoCloseable]) {
        lhs.asInstanceOf[AutoCloseable].close()
      }
      if (rhs.isInstanceOf[AutoCloseable]) {
        rhs.asInstanceOf[AutoCloseable].close()
      }
    }
  }
}

trait GpuBinaryOperator extends BinaryOperator with GpuBinaryExpression

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

abstract class CudfBinaryOperator extends GpuBinaryOperator with CudfBinaryExpression

trait GpuString2TrimExpression extends String2TrimExpression with GpuExpression {

  override def srcStr: Expression

  override def trimStr: Option[Expression]

  override def children: Seq[Expression] = srcStr +: trimStr.toSeq

  def strippedColumnVector(value: GpuColumnVector, sclarValue: Scalar): GpuColumnVector

  override def sql: String = if (trimStr.isDefined) {
    s"TRIM($direction ${trimStr.get.sql} FROM ${srcStr.sql})"
  } else {
    super.sql

  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val trim = GpuExpressionsUtils.getTrimString(trimStr)
    val shouldBeColumn = srcStr.columnarEval(batch)
    try {
      // We know the first parameter is not a Literal, because trim(Literal, Literal) would already
      // have been optimized out
      val column = shouldBeColumn.asInstanceOf[GpuColumnVector]
      if (trim == null) {
        withResource(GpuScalar.from(null, StringType)) { nullScalar =>
          GpuColumnVector.from(nullScalar, column.getRowCount().toInt)
        }
      } else if (trim.isEmpty) {
        column.incRefCount() // This is a noop
      } else {
        withResource(GpuScalar.from(trim, StringType)) { t =>
          strippedColumnVector(column, t)
        }
      }
    } finally {
      if (shouldBeColumn.isInstanceOf[AutoCloseable]) {
        shouldBeColumn.asInstanceOf[AutoCloseable].close()
      }
    }
  }
}

trait GpuTernaryExpression extends TernaryExpression with GpuExpression {

  def doColumnar(
      val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector
  def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: GpuColumnVector): GpuColumnVector
  def doColumnar(val0: Scalar, val1: Scalar, val2: GpuColumnVector): GpuColumnVector
  def doColumnar(val0: Scalar, val1: GpuColumnVector, val2: Scalar): GpuColumnVector
  def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: GpuColumnVector): GpuColumnVector
  def doColumnar(val0: GpuColumnVector, val1: Scalar, val2: Scalar): GpuColumnVector
  def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: Scalar): GpuColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    var val0: Any = null
    var val1: Any = null
    var val2: Any = null
    try {
      val0 = children(0).columnarEval(batch)
      val1 = children(1).columnarEval(batch)
      val2 = children(2).columnarEval(batch)

      (val0, val1, val2) match {
        case (v0: GpuColumnVector, v1: GpuColumnVector, v2: GpuColumnVector) =>
          doColumnar(v0, v1, v2)
        case (v0, v1: GpuColumnVector, v2: GpuColumnVector) =>
          val scalar0 = GpuScalar.from(v0, children(0).dataType)
          try {
            doColumnar(scalar0, v1, v2)
          } finally {
            scalar0.close()
          }
        case (v0: GpuColumnVector, v1, v2: GpuColumnVector) =>
          val scalar1 = GpuScalar.from(v1, children(1).dataType)
          try {
            doColumnar(v0, scalar1, v2)
          } finally {
            scalar1.close()
          }
        case (v0: GpuColumnVector, v1: GpuColumnVector, v2) =>
          val scalar2 = GpuScalar.from(v2, children(2).dataType)
          try {
            doColumnar(v0, v1, scalar2)
          } finally {
            scalar2.close()
          }
        case (v0, v1, v2: GpuColumnVector) =>
          val scalar0 = GpuScalar.from(v0, children(0).dataType)
          val scalar1 = GpuScalar.from(v1, children(1).dataType)
          try {
            doColumnar(scalar0, scalar1, v2)
          } finally {
            scalar0.close()
            scalar1.close()
          }
        case (v0, v1: GpuColumnVector, v2) =>
          val scalar0 = GpuScalar.from(v0, children(0).dataType)
          val scalar2 = GpuScalar.from(v2, children(2).dataType)
          try {
            doColumnar(scalar0, v1, scalar2)
          } finally {
            scalar0.close()
            scalar2.close()
          }
        case (v0: GpuColumnVector, v1, v2) =>
          val scalar1 = GpuScalar.from(v1, children(1).dataType)
          val scalar2 = GpuScalar.from(v2, children(2).dataType)
          try {
            doColumnar(v0, scalar1, scalar2)
          } finally {
            scalar1.close()
            scalar2.close()
          }
        case (v0, v1, v2) if (v0 != null && v1 != null && v2 != null) =>
          nullSafeEval(v0, v1, v2)
        case _ => null
      }
    } finally {
      if (val0.isInstanceOf[AutoCloseable]) {
        val0.asInstanceOf[AutoCloseable].close()
      }
      if (val1.isInstanceOf[AutoCloseable]) {
        val1.asInstanceOf[AutoCloseable].close()
      }
      if (val2.isInstanceOf[AutoCloseable]) {
        val2.asInstanceOf[AutoCloseable].close()
      }
    }
  }
}

trait GpuComplexTypeMergingExpression extends ComplexTypeMergingExpression with GpuExpression {
  def columnarEval(batch: ColumnarBatch): Any
}
