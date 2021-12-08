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

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuConditionalExpression extends ComplexTypeMergingExpression with GpuExpression
    with ShimExpression {

  protected def computeIfElse(
      batch: ColumnarBatch,
      pred: GpuColumnVector,
      trueExpr: Expression,
      falseValue: Any): GpuColumnVector = {
    withResourceIfAllowed(falseValue) { falseRet =>
      withResourceIfAllowed(trueExpr.columnarEval(batch)) { trueRet =>
        val finalRet = (trueRet, falseRet) match {
          case (t: GpuColumnVector, f: GpuColumnVector) =>
            pred.getBase.ifElse(t.getBase, f.getBase)
          case (t: GpuScalar, f: GpuColumnVector) =>
            pred.getBase.ifElse(t.getBase, f.getBase)
          case (t: GpuColumnVector, f: GpuScalar) =>
            pred.getBase.ifElse(t.getBase, f.getBase)
          case (t: GpuScalar, f: GpuScalar) =>
            pred.getBase.ifElse(t.getBase, f.getBase)
          case (t, f) =>
            throw new IllegalStateException(s"Unexpected inputs" +
              s" ($t: ${t.getClass}, $f: ${f.getClass})")
        }
        GpuColumnVector.from(finalRet, dataType)
      }
    }
  }

  protected def isAllTrue(col: GpuColumnVector): Boolean = {
    assert(BooleanType == col.dataType())
    withResource(col.getBase.all()) { allTrue =>
      // null is treated as false in Spark, but skipped by 'all()' method.
      allTrue.isValid && allTrue.getBoolean && !col.hasNull
    }
  }

  protected def isAllFalse(col: GpuColumnVector): Boolean = {
    assert(BooleanType == col.dataType())
    withResource(col.getBase.any()) { anyTrue =>
      anyTrue.isValid && !anyTrue.getBoolean
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

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(predicateExpr, batch)) { pred =>
      if (isAllTrue(pred)) {
        // All are true
        trueExpr.columnarEval(batch)
      } else if (isAllFalse(pred)) {
        // All are false
        falseExpr.columnarEval(batch)
      } else {
        computeIfElse(batch, pred, trueExpr, falseExpr.columnarEval(batch))
      }
    }
  }

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

  @transient
  private[this] lazy val trueExpressions = branches.map(_._2)

  override def columnarEval(batch: ColumnarBatch): Any = {
    val size = branches.size
    val predications = new Array[GpuColumnVector](size)
    var isAllPredsFalse = true
    var i = 0

    withResource(predications) { preds =>
      while (i < size) {
        // If any predication is the first all-true, then evaluate its true expression
        // and return the result.
        preds(i) = GpuExpressionsUtils.columnarEvalToColumn(branches(i)._1, batch)
        val p = preds(i)
        if (isAllPredsFalse && isAllTrue(p)) {
          return trueExpressions(i).columnarEval(batch)
        }
        isAllPredsFalse = isAllPredsFalse && isAllFalse(p)
        i += 1
      }

      val elseRet = elseValue
        .map(_.columnarEval(batch))
        .getOrElse(GpuScalar(null, branches.last._2.dataType))
      if (isAllPredsFalse) {
        // No predication has a true, so return the else value.
        elseRet
      } else {
        preds.zip(trueExpressions).foldRight[Any](elseRet) { case ((p, trueExpr), falseRet) =>
          computeIfElse(batch, p, trueExpr, falseRet)
        }
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
