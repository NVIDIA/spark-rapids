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

import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression, Predicate}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuNvl extends Arm {
  def apply(lhs: ColumnVector, rhs: ColumnVector): ColumnVector = {
    withResource(lhs.isNotNull) { isLhsNotNull =>
      isLhsNotNull.ifElse(lhs, rhs)
    }
  }

  def apply(lhs: ColumnVector, rhs: Scalar): ColumnVector = {
    withResource(lhs.isNotNull) { isLhsNotNull =>
      isLhsNotNull.ifElse(lhs, rhs)
    }
  }
}

case class GpuCoalesce(children: Seq[Expression]) extends GpuExpression with
  ComplexTypeMergingExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    // runningResult has precedence over runningScalar
    var runningResult: ColumnVector = null
    var runningScalar: GpuScalar = null
    try {
      children.reverse.foreach(expr => {
        expr.columnarEval(batch) match {
          case data: GpuColumnVector =>
            try {
              if (runningResult != null) {
                val tmp = GpuNvl(data.getBase, runningResult)
                runningResult.close()
                runningResult = tmp
              } else if (runningScalar != null) {
                runningResult = GpuNvl(data.getBase, runningScalar.getBase)
              } else {
                // They are both null
                runningResult = data.getBase.incRefCount()
              }
            } finally {
              data.close()
            }
          case s: GpuScalar =>
            // scalar now takes over
            if (runningResult != null) {
              runningResult.close()
              runningResult = null
            }

            if (runningScalar != null) {
              runningScalar.close()
              runningScalar = null
            }
            runningScalar = s
          case null => // NOOP does not matter the current state is unchanged
          case u => throw new IllegalStateException(s"Unexpected data type ${u.getClass}")
        }
      })

      if (runningResult != null) {
        GpuColumnVector.from(runningResult.incRefCount(), dataType)
      } else if (runningScalar != null) {
        // Wrap it as a GpuScalar instead of pulling data out of GPU.
        runningScalar.incRefCount
      } else {
        // null is not welcome, so use a null scalar instead
        GpuScalar(null, dataType)
      }
    } finally {
      if (runningResult != null) {
        runningResult.close()
      }
      if (runningScalar != null) {
        runningScalar.close()
      }
    }
  }

  // Coalesce is nullable if all of its children are nullable, or there are no children
  override def nullable: Boolean = children.forall(_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable)
}

/*
 * IsNull and IsNotNull should eventually become CpuUnaryExpressions, with corresponding
 * UnaryOp
 */

case class GpuIsNull(child: Expression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NULL)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.isNull
}

case class GpuIsNotNull(child: Expression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NOT NULL)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.isNotNull
}

case class GpuIsNan(child: Expression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NAN)"

  override def doColumnar(input: GpuColumnVector): ColumnVector =
    input.getBase.isNan
}

/**
 * A GPU accelerated predicate that is evaluated to be true if there are at least `n` non-null
 * and non-NaN values.
 */
case class GpuAtLeastNNonNulls(
    n: Int,
    exprs: Seq[Expression])
  extends GpuExpression
  with Predicate {
  override def nullable: Boolean = false
  override def foldable: Boolean = exprs.forall(_.foldable)
  override def toString: String = s"GpuAtLeastNNulls(n, ${children.mkString(",")})"
  override def children: Seq[Expression] = exprs
  /**
   * Returns the result of evaluating this expression on the entire
   * `ColumnarBatch`. The result of calling this may be a single [[GpuColumnVector]] or a scalar
   * value. Scalar values typically happen if they are a part of the expression
   * i.e. col("a") + 100.
   * In this case the 100 is a literal that Add would have to be able to handle.
   *
   * By convention any [[GpuColumnVector]] returned by [[columnarEval]]
   * is owned by the caller and will need to be closed by them. This can happen by putting it into
   * a `ColumnarBatch` and closing the batch or by closing the vector directly if it is a
   * temporary value.
   */
  override def columnarEval(batch: ColumnarBatch): Any = {
    val nonNullNanCounts : mutable.Queue[ColumnVector] = new mutable.Queue[ColumnVector]()
    try {
      exprs.foreach { expr =>
        var cv: ColumnVector = null
        var notNullVector: ColumnVector = null
        var notNanVector: ColumnVector = null
        var nanAndNullVector: ColumnVector = null
        try {
          cv = expr.columnarEval(batch).asInstanceOf[GpuColumnVector].getBase
          notNullVector = cv.isNotNull
          if (cv.getType == DType.FLOAT32 || cv.getType == DType.FLOAT64) {
            notNanVector = cv.isNotNan
            nanAndNullVector = notNanVector.and(notNullVector)
            nonNullNanCounts.enqueue(nanAndNullVector.castTo(DType.INT32))
          } else {
            nonNullNanCounts.enqueue(notNullVector.castTo(DType.INT32))
          }
        } finally {
          if (cv != null) {
            cv.close()
          }
          if (notNullVector != null) {
            notNullVector.close()
          }
          if (notNanVector != null) {
            notNanVector.close()
          }
          if (nanAndNullVector != null) {
            nanAndNullVector.close()
          }
        }
      }
      getFilterVector(nonNullNanCounts)
    } finally {
      nonNullNanCounts.safeClose()
    }
  }

  private def getFilterVector(nonNullNanCounts: mutable.Queue[ColumnVector]): GpuColumnVector = {
    var ret : GpuColumnVector = null
    var scalar : Scalar = null
    try {
      addColumnVectorsQ(nonNullNanCounts)
      scalar = Scalar.fromInt(n)
      if (nonNullNanCounts.nonEmpty) {
        ret = GpuColumnVector.from(nonNullNanCounts.head.greaterOrEqualTo(scalar), dataType)
      }
    } finally {
      if (scalar != null) {
        scalar.close()
      }
    }
    ret
  }

  private def addColumnVectorsQ(columnVectors : mutable.Queue[ColumnVector]) : Unit = {
    var firstCol: ColumnVector = null
    var secondCol: ColumnVector = null
    while (columnVectors.size > 1) {
      try {
        firstCol = columnVectors.dequeue()
        secondCol = columnVectors.dequeue()
        columnVectors.enqueue(firstCol.add(secondCol))
      } finally {
        if (firstCol != null) {
          firstCol.close()
        }
        if (secondCol != null) {
          secondCol.close()
        }
      }
    }
  }
}

case class GpuNaNvl(left: Expression, right: Expression) extends GpuBinaryExpression {
  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val lhsBase = lhs.getBase
    // By definition null is not nan
    withResource(lhsBase.isNotNan) { islhsNotNan =>
      islhsNotNan.ifElse(lhsBase, rhs.getBase)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val isNull = !lhs.isValid
    val isNan = lhs.isNan
    if (isNull || !isNan) {
      ColumnVector.fromScalar(lhs.getBase, rhs.getRowCount.toInt)
    } else {
      rhs.getBase.incRefCount()
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val lhsBase = lhs.getBase
    // By definition null is not nan
    withResource(lhsBase.isNotNan) { islhsNotNan =>
      islhsNotNan.ifElse(lhsBase, rhs.getBase)
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
    withResource(GpuColumnVector.from(lhs, numRows, left.dataType)) { expandedLhs =>
      doColumnar(expandedLhs, rhs)
    }
  }

  override def dataType: DataType = left.dataType

  // Access to AbstractDataType is not allowed, and not really needed here
//  override def inputTypes: Seq[AbstractDataType] =
//    Seq(TypeCollection(DoubleType, FloatType), TypeCollection(DoubleType, FloatType))
}
