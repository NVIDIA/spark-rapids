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

package com.nvidia.spark.rapids

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuConditionalExpression extends ComplexTypeMergingExpression with GpuExpression {
  private def computePredicate(
      batch: ColumnarBatch,
      predicateExpr: Expression): GpuColumnVector = {
    val predicate: Any = predicateExpr.columnarEval(batch)
    try {
      if (!predicate.isInstanceOf[GpuColumnVector]) {
        throw new IllegalStateException("Predicate result is not a column")
      }
      val p = predicate.asInstanceOf[GpuColumnVector]

      // TODO: This null replacement is no longer necessary when
      // https://github.com/rapidsai/cudf/issues/3856 is fixed.
      withResource(Scalar.fromBool(false)) { falseScalar =>
        GpuColumnVector.from(p.getBase.replaceNulls(falseScalar), BooleanType)
      }
    } finally {
      predicate match {
        case c: AutoCloseable => c.close()
        case _ =>
      }
    }
  }

  protected def computeIfElse(
      batch: ColumnarBatch,
      predicateExpr: Expression,
      trueExpr: Expression,
      falseValues: GpuColumnVector): GpuColumnVector = {
    withResource(computePredicate(batch, predicateExpr)) { predicate =>
      val trueResult: Any = trueExpr.columnarEval(batch)
      try {
        val result = trueResult match {
          case t: GpuColumnVector => predicate.getBase.ifElse(t.getBase, falseValues.getBase)
          case t: GpuScalar => predicate.getBase.ifElse(t.getBase, falseValues.getBase)
          case u =>
            throw new IllegalStateException(s"Unexpected inputs $u")
        }
        GpuColumnVector.from(result, dataType)
      } finally {
        trueResult match {
          case a: AutoCloseable => a.close()
          case _ =>
        }
      }
    }
  }

  protected def computeIfElse(
      batch: ColumnarBatch,
      predicateExpr: Expression,
      trueExpr: Expression,
      falseValue: Scalar): GpuColumnVector = {
    withResource(computePredicate(batch, predicateExpr)) { predicate =>
      val trueResult: Any = trueExpr.columnarEval(batch)
      try {
        val result = trueResult match {
          case t: GpuColumnVector => predicate.getBase.ifElse(t.getBase, falseValue)
          case t: GpuScalar => predicate.getBase.ifElse(t.getBase, falseValue)
          case u =>
            throw new IllegalStateException(s"Unexpected inputs $u")
        }
        GpuColumnVector.from(result, dataType)
      } finally {
        trueResult match {
          case a: AutoCloseable => a.close()
          case _ =>
        }
      }
    }
  }

  protected def computeIfElse(
      batch: ColumnarBatch,
      predicateExpr: Expression,
      trueExpr: Expression,
      falseExpr: Expression): GpuColumnVector = {
    val falseResult: Any = falseExpr.columnarEval(batch)
    try {
      falseResult match {
        case f: GpuColumnVector => computeIfElse(batch, predicateExpr, trueExpr, f)
        case f: GpuScalar => computeIfElse(batch, predicateExpr, trueExpr, f.getBase)
        case u =>
          throw new IllegalStateException(s"Unexpected inputs $u")
      }
    } finally {
      falseResult match {
        case a: AutoCloseable => a.close()
        case _ =>
      }
    }
  }
}

case class GpuIf(
    predicateExpr: Expression,
    trueExpr: Expression,
    falseExpr: Expression) extends GpuConditionalExpression {

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueExpr.dataType, falseExpr.dataType)
  }

  override def children: Seq[Expression] = predicateExpr :: trueExpr :: falseExpr :: Nil
  override def nullable: Boolean = trueExpr.nullable || falseExpr.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicateExpr.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        "type of predicate expression in If should be boolean, " +
          s"not ${predicateExpr.dataType.catalogString}")
    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueExpr.dataType.catalogString} and ${falseExpr.dataType.catalogString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = computeIfElse(batch, predicateExpr,
    trueExpr, falseExpr)

  override def toString: String = s"if ($predicateExpr) $trueExpr else $falseExpr"

  override def sql: String = s"(IF(${predicateExpr.sql}, ${trueExpr.sql}, ${falseExpr.sql}))"
}


case class GpuCaseWhen(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression] = None) extends GpuConditionalExpression with Serializable {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    branches.map(_._2.dataType) ++ elseValue.map(_.dataType)
  }

  override def nullable: Boolean = {
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.forall(_.nullable)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (TypeCoercion.haveSameType(inputTypesForMerging)) {
      // Make sure all branch conditions are boolean types.
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      val branchesStr = branches.map(_._2.dataType).map(dt => s"WHEN ... THEN ${dt.catalogString}")
        .mkString(" ")
      val elseStr = elseValue.map(expr => s" ELSE ${expr.dataType.catalogString}").getOrElse("")
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type," +
          s" got CASE $branchesStr$elseStr END")
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val elseExpr = elseValue.getOrElse(GpuLiteral(null, branches.last._2.dataType))
    try {
      branches.foldRight[Any](elseExpr) { case ((predicateExpr, trueExpr), falseObj) =>
        falseObj match {
          case v: GpuColumnVector =>
            try {
              computeIfElse(batch, predicateExpr, trueExpr, v)
            } finally {
              v.close()
            }
          case e: GpuExpression => computeIfElse(batch, predicateExpr, trueExpr, e)
        }
      }
    } finally {
      elseExpr match {
        case a: AutoCloseable => a.close()
        case _ =>
      }
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }
}
