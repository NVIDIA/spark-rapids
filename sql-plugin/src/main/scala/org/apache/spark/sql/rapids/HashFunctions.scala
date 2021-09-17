/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{BinaryOp, ColumnVector, ColumnView, DType}
import com.nvidia.spark.rapids.{Arm, GpuCast, GpuColumnVector, GpuExpression, GpuIf, GpuIsNan, GpuLiteral, GpuProjectExec, GpuUnaryExpression, GpuUnscaledValue}
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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

object GpuMurmur3Hash extends Arm {
  def compute(batch: ColumnarBatch, boundExpr: Seq[Expression], seed: Int = 42): ColumnVector = {
    withResource(GpuProjectExec.project(batch, boundExpr)) { args =>
      val bases = GpuColumnVector.extractBases(args)
      ColumnVector.spark32BitMurmurHash3(seed, bases.toArray[ColumnView])
    }
  }
}

case class GpuMurmur3Hash(children: Seq[Expression], seed: Int) extends GpuExpression
  with ShimExpression {
  override def dataType: DataType = IntegerType

  override def toString: String = s"hash($children)"
  def nullable: Boolean = children.exists(_.nullable)

  def columnarEval(batch: ColumnarBatch): Any =
    GpuColumnVector.from(GpuMurmur3Hash.compute(batch, children, seed), dataType)
}
