/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ast, BinaryOp, BinaryOperable, ColumnVector, DType, Scalar, UnaryOp}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{ShimBinaryExpression, ShimExpression, ShimTernaryExpression, ShimUnaryExpression}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

object GpuExpressionsUtils {
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
   * @param numRows the expected row number of the output column, used when 'any' is a GpuScalar.
   * @return a `GpuColumnVector` if it succeeds. Users should close the column vector to avoid
   *         memory leak.
   */
  def resolveColumnVector(any: Any, numRows: Int): GpuColumnVector = {
    withResourceIfAllowed(any) {
      case c: GpuColumnVector => c.incRefCount()
      case s: GpuScalar => GpuColumnVector.from(s, numRows, s.dataType)
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
    resolveColumnVector(expr.columnarEval(batch), batch.numRows)

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
trait GpuExpression extends Expression {

  // copied from Unevaluable to avoid inheriting  final foldable
  //
  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot generate code for expression: $this")

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

  /**
   * Build an equivalent representation of this expression in a cudf AST.
   * @param numFirstTableColumns number of columns in the leftmost input table. Spark places the
   *                             columns of all inputs in a single sequence, while cudf AST uses an
   *                             explicit table reference to make column indices unique. This
   *                             parameter helps translate input column references from Spark's
   *                             single sequence into cudf's separate sequences.
   * @return top node of the equivalent AST
   */
  def convertToAst(numFirstTableColumns: Int): ast.AstExpression =
    throw new IllegalStateException(s"Cannot convert ${this.getClass.getSimpleName} to AST")

  /** Could evaluating this expression cause side-effects, such as throwing an exception? */
  def hasSideEffects: Boolean =
    children.exists {
      case c: GpuExpression => c.hasSideEffects
      case _ => false // This path should never really happen
    }
}

abstract class GpuLeafExpression extends GpuExpression with ShimExpression {
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

abstract class GpuUnaryExpression extends ShimUnaryExpression with GpuExpression {
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

object CudfUnaryExpression {
  lazy val opToAstMap: Map[UnaryOp, ast.UnaryOperator] = Map(
    UnaryOp.ABS -> ast.UnaryOperator.ABS,
    UnaryOp.ARCSIN -> ast.UnaryOperator.ARCSIN,
    UnaryOp.ARCSINH -> ast.UnaryOperator.ARCSINH,
    UnaryOp.ARCCOS -> ast.UnaryOperator.ARCCOS,
    UnaryOp.ARCCOSH -> ast.UnaryOperator.ARCCOSH,
    UnaryOp.ARCTAN -> ast.UnaryOperator.ARCTAN,
    UnaryOp.ARCTANH -> ast.UnaryOperator.ARCTANH,
    UnaryOp.BIT_INVERT -> ast.UnaryOperator.BIT_INVERT,
    UnaryOp.CBRT -> ast.UnaryOperator.CBRT,
    UnaryOp.COS -> ast.UnaryOperator.COS,
    UnaryOp.COSH -> ast.UnaryOperator.COSH,
    UnaryOp.EXP -> ast.UnaryOperator.EXP,
    UnaryOp.NOT -> ast.UnaryOperator.NOT,
    UnaryOp.RINT -> ast.UnaryOperator.RINT,
    UnaryOp.SIN -> ast.UnaryOperator.SIN,
    UnaryOp.SINH -> ast.UnaryOperator.SINH,
    UnaryOp.SQRT -> ast.UnaryOperator.SQRT,
    UnaryOp.TAN -> ast.UnaryOperator.TAN,
    UnaryOp.TANH -> ast.UnaryOperator.TANH)
}

trait CudfUnaryExpression extends GpuUnaryExpression {
  def unaryOp: UnaryOp

  override def doColumnar(input: GpuColumnVector): ColumnVector = input.getBase.unaryOp(unaryOp)

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    val astOp = CudfUnaryExpression.opToAstMap.getOrElse(unaryOp,
      throw new IllegalStateException(s"${this.getClass.getSimpleName} is not supported by AST"))
    new ast.UnaryOperation(astOp,
      child.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns))
  }
}

trait GpuBinaryExpression extends ShimBinaryExpression with GpuExpression {

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
  def astOperator: Option[ast.BinaryOperator] = None

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

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    val astOp = astOperator.getOrElse(
      throw new IllegalStateException(s"$this is not supported by AST"))
    assert(left.dataType == right.dataType)
    new ast.BinaryOperation(astOp,
      left.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns),
      right.asInstanceOf[GpuExpression].convertToAst(numFirstTableColumns))
  }
}

abstract class CudfBinaryOperator extends GpuBinaryOperator with CudfBinaryExpression

trait GpuString2TrimExpression extends String2TrimExpression with GpuExpression
    with ShimExpression {

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
    withResourceIfAllowed(columnarEvalToColumn(srcStr, batch)) { column =>
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

  protected def doEval(
      srcString: org.apache.spark.unsafe.types.UTF8String,
      trimString: org.apache.spark.unsafe.types.UTF8String
  ): org.apache.spark.unsafe.types.UTF8String = {
    throw new UnsupportedOperationException("TODO: Columnar only message!")
  }

  protected def doEval(
      srcString: org.apache.spark.unsafe.types.UTF8String
  ): org.apache.spark.unsafe.types.UTF8String = {
    throw new UnsupportedOperationException("TODO: Columnar only message!")
  }
}

trait GpuTernaryExpression extends ShimTernaryExpression with GpuExpression {

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

trait GpuComplexTypeMergingExpression extends ComplexTypeMergingExpression
    with GpuExpression with ShimExpression {
  def columnarEval(batch: ColumnarBatch): Any
}
