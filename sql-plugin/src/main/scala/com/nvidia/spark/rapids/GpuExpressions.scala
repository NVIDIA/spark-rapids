/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BinaryOp, BinaryOperable, ColumnVector, DType, Scalar, UnaryOp}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuExpressionsUtils extends Arm {
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

  /**
   * Tries to resolve a `GpuColumnVector` from a Scala `Any`.
   *
   * This is a common handling of the result from the `columnarEval`, allowing one of
   *   - A GpuColumnVector
   *   - A GpuScalar
   * For other types, it will blow up.
   *
   * It is recommended to return only a `GpuScalar` or a `GpuColumnVector` from a GPU
   * expression's `columnarEval`, to keep the result handling simple. Besides, `GpuScalar` can
   * be created from a cudf Scalar or a Scala value, So the 'GpuScalar' and 'GpuColumnVector'
   * should cover all the cases for GPU pipelines.
   *
   * @param any the input value. It will be closed if it is a closeable after the call done.
   * @param numRows the expected row number of the output column, used when 'any' is a Scalar.
   * @param dType the data type of the output column, used when 'any' is a Scalar.
   * @return a `GpuColumnVector` if it succeeds. Users should close the column vector to avoid
   *         memory leak.
   */
  def resolveColumnVector(any: Any, numRows: Int, dType: DataType): GpuColumnVector = {
    withResourceIfAllowed(any) {
      case c: GpuColumnVector => c.incRefCount()
      case s: GpuScalar => GpuColumnVector.from(s, numRows, dType)
      case other =>
        throw new IllegalArgumentException(s"Cannot resolve a ColumnVector from the value:" +
          s" $other. Please convert it to a GpuScalar or a GpuColumnVector before returning.")
    }
  }

  /**
   * Tries to resolve a `GpuColumnVector` by evaluating an expression over a batch.
   *
   * This is a common handling of a GpuExpression for the nodes asking for a single
   * `GpuColumnVector`.
   *
   * @param expr the input expression to be evaluated.
   * @param batch the input batch.
   * @return a `GpuColumnVector` if it succeeds. Users should close the column vector to avoid
   *         memory leak.
   */
  def columnarEvalToColumn(expr: Expression, batch: ColumnarBatch): GpuColumnVector =
    resolveColumnVector(expr.columnarEval(batch), batch.numRows, expr.dataType)

  /**
   * Extract the GpuLiteral
   * @param exp the input expression to be extracted
   * @return an optional GpuLiteral
   */
  @scala.annotation.tailrec
  def extractGpuLit(exp: Expression): Option[GpuLiteral] = exp match {
    case gl: GpuLiteral => Some(gl)
    case ga: GpuAlias => extractGpuLit(ga.child)
    case _ => None
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
  final override def doColumnar(input: GpuColumnVector): ColumnVector =
    throw new UnsupportedOperationException(s"Cannot columnar evaluate expression: $this")
}

abstract class GpuUnaryExpression extends UnaryExpression with GpuExpression {
  protected def doColumnar(input: GpuColumnVector): ColumnVector

  def outputTypeOverride: DType = null

  private[this] def doItColumnar(input: GpuColumnVector): GpuColumnVector = {
    withResource(doColumnar(input)) { vec =>
      if (outputTypeOverride != null && !outputTypeOverride.equals(vec.getType)) {
        GpuColumnVector.from(vec.castTo(outputTypeOverride), dataType)
      } else {
        GpuColumnVector.from(vec.incRefCount(), dataType)
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(child, batch)) { col =>
      doItColumnar(col)
    }
  }
}

trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp

  override def doColumnar(input: GpuColumnVector): ColumnVector = input.getBase.unaryOp(unaryOp)
}

trait GpuBinaryExpression extends BinaryExpression with GpuExpression {

  def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector
  def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector
  def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEval(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuColumnVector) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuColumnVector, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(l, r), dataType)
          case (l: GpuScalar, r: GpuScalar) =>
            GpuColumnVector.from(doColumnar(batch.numRows(), l, r), dataType)
          case (l, r) =>
            throw new UnsupportedOperationException(s"Unsupported data '($l: " +
              s"${l.getClass}, $r: ${r.getClass})' for GPU binary expression.")
        }
      }
    }
  }
}

trait GpuBinaryOperator extends BinaryOperator with GpuBinaryExpression

trait CudfBinaryExpression extends GpuBinaryExpression {
  def binaryOp: BinaryOp
  def outputTypeOverride: DType = null
  def castOutputAtEnd: Boolean = false

  def outputType(l: BinaryOperable, r: BinaryOperable): DType = {
    val over = outputTypeOverride
    if (over == null) {
      BinaryOperable.implicitConversion(binaryOp, l, r)
    } else {
      over
    }
  }

  def doColumnar(lhs: BinaryOperable, rhs: BinaryOperable): ColumnVector = {
    val outType = if (castOutputAtEnd) {
      BinaryOperable.implicitConversion(binaryOp, lhs, rhs)
    } else {
      outputType(lhs, rhs)
    }
    val tmp = lhs.binaryOp(binaryOp, rhs, outType)
    // In some cases the output type is ignored
    val castType = outputType(lhs, rhs)
    if (!castType.equals(tmp.getType) && castOutputAtEnd) {
      withResource(tmp) { tmp =>
        tmp.castTo(castType)
      }
    } else {
      tmp
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    doColumnar(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    doColumnar(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    doColumnar(lhs.getBase, rhs.getBase)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }
}

abstract class CudfBinaryOperator extends GpuBinaryOperator with CudfBinaryExpression

trait GpuString2TrimExpression extends String2TrimExpression with GpuExpression {

  override def srcStr: Expression

  override def trimStr: Option[Expression]

  override def children: Seq[Expression] = srcStr +: trimStr.toSeq

  def strippedColumnVector(value: GpuColumnVector, scalarValue: Scalar): GpuColumnVector

  override def sql: String = if (trimStr.isDefined) {
    s"TRIM($direction ${trimStr.get.sql} FROM ${srcStr.sql})"
  } else {
    super.sql

  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val trim = GpuExpressionsUtils.getTrimString(trimStr)
    withResourceIfAllowed(srcStr.columnarEval(batch)) { shouldBeColumn =>
      // We know the first parameter is not a Literal, because trim(Literal, Literal) would already
      // have been optimized out
      val column = shouldBeColumn.asInstanceOf[GpuColumnVector]
      if (trim == null) {
        GpuColumnVector.fromNull(column.getRowCount.toInt, StringType)
      } else if (trim.isEmpty) {
        column.incRefCount() // This is a noop
      } else {
        withResource(Scalar.fromString(trim)) { t =>
          strippedColumnVector(column, t)
        }
      }
    }
  }
}

trait GpuTernaryExpression extends TernaryExpression with GpuExpression {

  def doColumnar(
      val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuColumnVector): ColumnVector
  def doColumnar(val0: GpuScalar, val1: GpuColumnVector, val2: GpuColumnVector): ColumnVector
  def doColumnar(val0: GpuScalar, val1: GpuScalar, val2: GpuColumnVector): ColumnVector
  def doColumnar(val0: GpuScalar, val1: GpuColumnVector, val2: GpuScalar): ColumnVector
  def doColumnar(val0: GpuColumnVector, val1: GpuScalar, val2: GpuColumnVector): ColumnVector
  def doColumnar(val0: GpuColumnVector, val1: GpuScalar, val2: GpuScalar): ColumnVector
  def doColumnar(val0: GpuColumnVector, val1: GpuColumnVector, val2: GpuScalar): ColumnVector
  def doColumnar(numRows: Int, val0: GpuScalar, val1: GpuScalar, val2: GpuScalar): ColumnVector

  override def columnarEval(batch: ColumnarBatch): Any = {
    val Seq(child0, child1, child2) = children
    withResourceIfAllowed(child0.columnarEval(batch)) { val0 =>
      withResourceIfAllowed(child1.columnarEval(batch)) { val1 =>
        withResourceIfAllowed(child2.columnarEval(batch)) { val2 =>
          (val0, val1, val2) match {
            case (v0: GpuColumnVector, v1: GpuColumnVector, v2: GpuColumnVector) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuScalar, v1: GpuColumnVector, v2: GpuColumnVector) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuColumnVector, v1: GpuScalar, v2: GpuColumnVector) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuColumnVector, v1: GpuColumnVector, v2: GpuScalar) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuScalar, v1: GpuScalar, v2: GpuColumnVector) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuScalar, v1: GpuColumnVector, v2: GpuScalar) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuColumnVector, v1: GpuScalar, v2: GpuScalar) =>
              GpuColumnVector.from(doColumnar(v0, v1, v2), dataType)
            case (v0: GpuScalar, v1: GpuScalar, v2: GpuScalar) =>
              GpuColumnVector.from(doColumnar(batch.numRows(), v0, v1, v2), dataType)
            case (v0, v1, v2) =>
              throw new UnsupportedOperationException(s"Unsupported data '($v0: ${v0.getClass}," +
                s" $v1: ${v1.getClass}, $v2: ${v2.getClass})' for GPU ternary expression.")
          }
        }
      }
    }
  }
}

trait GpuComplexTypeMergingExpression extends ComplexTypeMergingExpression with GpuExpression {
  def columnarEval(batch: ColumnarBatch): Any
}
