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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{BinaryOp, DType, UnaryOp}
import ai.rapids.spark.{CudfBinaryOperator, CudfUnaryExpression, GpuColumnVector}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant, Predicate}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BooleanType, DataType}

case class GpuNot(child: Expression) extends CudfUnaryExpression
    with Predicate with ImplicitCastInputTypes with NullIntolerant {
  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def sql: String = s"(NOT ${child.sql})"

  override def unaryOp: UnaryOp = UnaryOp.NOT
}

case class GpuAnd(left: Expression, right: Expression) extends CudfBinaryOperator with Predicate {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_AND
}

case class GpuOr(left: Expression, right: Expression) extends CudfBinaryOperator with Predicate {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_OR
}

abstract class CudfBinaryComparison extends CudfBinaryOperator with Predicate {
  // Note that we need to give a superset of allowable input types since orderable types are not
  // finitely enumerable. The allowable types are checked below by checkInputDataTypes.
  override def inputType: AbstractDataType = AnyDataType

  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess =>
      TypeUtils.checkForOrderingExpr(left.dataType, this.getClass.getSimpleName)
    case failure => failure
  }
}

case class GpuEqualTo(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.EQUAL
}

case class GpuGreaterThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = ">"
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.GREATER
}

case class GpuGreaterThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = ">="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.GREATER_EQUAL
}

case class GpuLessThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "<"
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.LESS
}

case class GpuLessThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "<="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.LESS_EQUAL
}
