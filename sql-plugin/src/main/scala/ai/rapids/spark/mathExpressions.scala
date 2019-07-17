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

import ai.rapids.cudf.{BinaryOp, DType, UnaryOp}

import org.apache.spark.sql.catalyst.expressions.{Acos, Asin, Atan, Ceil, Cos, Exp, Expression, Floor, Log, Pow, Sin, Sqrt, Tan}

class GpuAcos(child: Expression) extends Acos(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.ARCCOS
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuAsin(child: Expression) extends Asin(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.ARCSIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuAtan(child: Expression) extends Atan(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.ARCTAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuCeil(child: Expression) extends Ceil(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.CEIL
  override def outputTypeOverride: DType = DType.INT64
}

class GpuCos(child: Expression) extends Cos(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.COS
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuExp(child: Expression) extends Exp(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.EXP
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuFloor(child: Expression) extends Floor(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.FLOOR
  override def outputTypeOverride: DType = DType.INT64
}

class GpuLog(child: Expression) extends Log(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.LOG
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuSin(child: Expression) extends Sin(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.SIN
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuSqrt(child: Expression) extends Sqrt(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.SQRT
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuTan(child: Expression) extends Tan(child) with CudfUnaryExpression {
  override def unaryOp: UnaryOp = UnaryOp.TAN
  override def outputTypeOverride: DType = DType.FLOAT64
}

class GpuPow(left: Expression, right: Expression) extends Pow(left, right)
  with CudfBinaryExpression {
  override def binaryOp: BinaryOp = BinaryOp.POW
  override def outputTypeOverride: DType = DType.FLOAT64
}
