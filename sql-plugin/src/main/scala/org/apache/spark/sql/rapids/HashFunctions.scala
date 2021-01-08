/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar, GpuUnaryExpression}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

case class GpuMd5(child: Expression)
  extends GpuUnaryExpression with ImplicitCastInputTypes with NullIntolerant {
  override def toString: String = s"md5($child)"
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def dataType: DataType = StringType

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    withResource(ColumnVector.md5Hash(input.getBase)) { fullResult =>
      fullResult.mergeAndSetValidity(BinaryOp.BITWISE_AND, input.getBase)
    }
  }
}

case class GpuMurmur3Hash(child: Seq[Expression]) extends GpuExpression {
  override def dataType: DataType = IntegerType

  override def toString: String = s"hash($child)"
  def nullable: Boolean = children.exists(_.nullable)
  def children: Seq[Expression] = child

  def columnarEval(batch: ColumnarBatch): Any = {
    val rows = batch.numRows()
    val columns: ArrayBuffer[ColumnVector] = new ArrayBuffer[ColumnVector]()
    try {
      children.foreach { child => child.columnarEval(batch) match {
          case vector: GpuColumnVector =>
            columns += vector.getBase
          case col => if (col != null) {
            withResource(GpuScalar.from(col)) { scalarValue =>
              columns += ai.rapids.cudf.ColumnVector.fromScalar(scalarValue, rows)
            }
          }
        }
      }
      GpuColumnVector.from(
        ColumnVector.spark32BitMurmurHash3(42, columns.toArray[ColumnView]), dataType)
    } finally {
      columns.safeClose()
    }
  }
}
