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

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable

/*
 * IsNull and IsNotNull should eventually become CpuUnaryExpressions, with corresponding
 * UnaryOp
 */

case class GpuIsNull(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NULL)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNull)
}

case class GpuIsNotNull(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NOT NULL)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNotNull)
}

case class GpuIsNan(child: GpuExpression) extends GpuUnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def sql: String = s"(${child.sql} IS NAN)"

  override def doColumnar(input: GpuColumnVector): GpuColumnVector =
    GpuColumnVector.from(input.getBase.isNan)
}

/**
  * A GPU accelerated predicate that is evaluated to be true if there are at least `n` non-null and non-NaN values.
  */
case class GpuAtLeastNNonNulls(n: Int, exprs: Seq[GpuExpression]) extends GpuExpression with Predicate {
  override def nullable: Boolean = false
  override def foldable: Boolean = exprs.forall(_.foldable)
  override def toString: String = s"GpuAtLeastNNulls(n, ${children.mkString(",")})"
  override def children: Seq[Expression] = exprs
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
      scalar = GpuScalar.from(n, IntegerType)
      if (nonNullNanCounts.nonEmpty) {
        ret = GpuColumnVector.from(nonNullNanCounts.head.greaterOrEqualTo(scalar))
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
