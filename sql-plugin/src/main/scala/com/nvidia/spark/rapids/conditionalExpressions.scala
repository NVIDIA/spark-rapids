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

import ai.rapids.cudf.{ColumnVector, NullPolicy, Scalar, ScanAggregation, ScanType, Table, UnaryOp}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType, DataTypes}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuConditionalExpression extends ComplexTypeMergingExpression with GpuExpression
  with ShimExpression {

  protected def computeIfElse(
      batch: ColumnarBatch,
      predExpr: Expression,
      trueExpr: Expression,
      falseValue: Any): GpuColumnVector = {
    withResourceIfAllowed(falseValue) { falseRet =>
      withResource(GpuExpressionsUtils.columnarEvalToColumn(predExpr, batch)) { pred =>
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
  }

  protected def isAllTrue(col: GpuColumnVector): Boolean = {
    assert(BooleanType == col.dataType())
    if (col.getRowCount == 0) {
      return true
    }
    if (col.hasNull) {
      return false
    }
    withResource(col.getBase.all()) { allTrue =>
      // Guaranteed there is at least one row and no nulls so result must be valid
      allTrue.getBoolean
    }
  }

  protected def isAllFalse(col: GpuColumnVector): Boolean = {
    assert(BooleanType == col.dataType())
    if (col.getRowCount == col.numNulls()) {
      // all nulls, and null values are false values here
      return true
    }
    withResource(col.getBase.any()) { anyTrue =>
      // null values are considered false values in this context
      !anyTrue.getBoolean
    }
  }

  protected def filterBatch(
      tbl: Table,
      pred: ColumnVector,
      colTypes: Array[DataType]): ColumnarBatch = {
    withResource(tbl.filter(pred)) { filteredData =>
      GpuColumnVector.from(filteredData, colTypes)
    }
  }

  private def boolToInt(cv: ColumnVector): ColumnVector = {
    withResource(GpuScalar.from(1, DataTypes.IntegerType)) { one =>
      withResource(GpuScalar.from(0, DataTypes.IntegerType)) { zero =>
        cv.ifElse(one, zero)
      }
    }
  }

  protected def gather(predicate: ColumnVector, t: GpuColumnVector): ColumnVector = {
    // convert the predicate boolean column to numeric where 1 = true
    // amd 0 = false and then use `scan` with `sum` to convert to
    // indices.
    //
    // For example, if the predicate evaluates to [F, F, T, F, T] then this
    // gets translated first to [0, 0, 1, 0, 1] and then the scan operation
    // will perform an exclusive sum on these values and
    // produce [0, 0, 0, 1, 1]. Combining this with the original
    // predicate boolean array results in the two T values mapping to
    // indices 0 and 1, respectively.

    withResource(boolToInt(predicate)) { boolsAsInts =>
      withResource(boolsAsInts.scan(
        ScanAggregation.sum(),
        ScanType.EXCLUSIVE,
        NullPolicy.INCLUDE)) { prefixSumExclusive =>

        // for the entries in the gather map that do not represent valid
        // values to be gathered, we change the value to -MAX_INT which
        // will be treated as null values in the gather algorithm
        val gatherMap = withResource(Scalar.fromInt(Int.MinValue)) {
          outOfBoundsFlag => predicate.ifElse(prefixSumExclusive, outOfBoundsFlag)
        }

        withResource(new Table(t.getBase)) { tbl =>
          withResource(gatherMap) { _ =>
            withResource(tbl.gather(gatherMap)) { gatherTbl =>
              gatherTbl.getColumn(0).incRefCount()
            }
          }
        }
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

  override def columnarEval(batch: ColumnarBatch): Any = {

    val gpuTrueExpr = trueExpr.asInstanceOf[GpuExpression]
    val gpuFalseExpr = falseExpr.asInstanceOf[GpuExpression]

    withResource(GpuExpressionsUtils.columnarEvalToColumn(predicateExpr, batch)) { pred =>
      if (isAllTrue(pred)) {
        GpuExpressionsUtils.columnarEvalToColumn(trueExpr, batch)
      } else if (isAllFalse(pred)) {
        GpuExpressionsUtils.columnarEvalToColumn(falseExpr, batch)
      } else if (gpuTrueExpr.hasSideEffects || gpuFalseExpr.hasSideEffects) {
        conditionalWithSideEffects(batch, pred, gpuTrueExpr, gpuFalseExpr)
      } else {
        withResourceIfAllowed(trueExpr.columnarEval(batch)) { trueRet =>
          withResourceIfAllowed(falseExpr.columnarEval(batch)) { falseRet =>
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
    }
  }

  /**
   * When computing conditional expressions on the CPU, the true and false
   * expressions are evaluated lazily, meaning that the true expression is
   * only evaluated for rows where the predicate is true, and the false
   * expression is only evaluated for rows where the predicate is false.
   * This is important in the case where the expressions can have
   * side-effects, such as throwing exceptions for invalid inputs.
   *
   * This method performs lazy evaluation on the GPU by first filtering the
   * input batch into two batches - one for rows where the predicate is true
   * and one for rows where the predicate is false. The expressions are
   * evaluated against these batches and then the results are combined
   * back into a single batch using the gather algorithm.
   */
  private def conditionalWithSideEffects(
      batch: ColumnarBatch,
      pred: GpuColumnVector,
      gpuTrueExpr: GpuExpression,
      gpuFalseExpr: GpuExpression): GpuColumnVector = {

    val colTypes = GpuColumnVector.extractTypes(batch)

    withResource(GpuColumnVector.from(batch)) { tbl =>
      withResource(pred.getBase.unaryOp(UnaryOp.NOT)) { inverted =>
        // evaluate true expression against true batch
        val tt = withResource(filterBatch(tbl, pred.getBase, colTypes)) { trueBatch =>
          gpuTrueExpr.columnarEval(trueBatch)
        }
        withResourceIfAllowed(tt) { _ =>
          // evaluate false expression against false batch
          val ff = withResource(filterBatch(tbl, inverted, colTypes)) { falseBatch =>
            gpuFalseExpr.columnarEval(falseBatch)
          }
          withResourceIfAllowed(ff) { _ =>
            val finalRet = (tt, ff) match {
              case (t: GpuColumnVector, f: GpuColumnVector) =>
                withResource(gather(pred.getBase, t)) { trueValues =>
                  withResource(gather(inverted, f)) { falseValues =>
                    pred.getBase.ifElse(trueValues, falseValues)
                  }
                }
              case (t: GpuScalar, f: GpuColumnVector) =>
                withResource(gather(inverted, f)) { falseValues =>
                  pred.getBase.ifElse(t.getBase, falseValues)
                }
              case (t: GpuColumnVector, f: GpuScalar) =>
                withResource(gather(pred.getBase, t)) { trueValues =>
                  pred.getBase.ifElse(trueValues, f.getBase)
                }
              case (_: GpuScalar, _: GpuScalar) =>
                throw new IllegalStateException(
                  "scalar expressions can never have side effects")
            }
            GpuColumnVector.from(finalRet, dataType)
          }
        }
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

  private lazy val branchesWithSideEffects =
    branches.exists(_._2.asInstanceOf[GpuExpression].hasSideEffects)

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
    if (branchesWithSideEffects) {
      columnarEvalWithSideEffects(batch)
    } else {
      // `elseRet` will be closed in `computeIfElse`.
      val elseRet = elseValue
        .map(_.columnarEval(batch))
        .getOrElse(GpuScalar(null, branches.last._2.dataType))
      branches.foldRight[Any](elseRet) {
        case ((predicateExpr, trueExpr), falseRet) =>
          computeIfElse(batch, predicateExpr, trueExpr, falseRet)
        }
      }
    }

  /**
   * Perform lazy evaluation of each branch sa that we only evaluate the THEN expressions
   * against rows where the WHEN expression is true.
   */
  private def columnarEvalWithSideEffects(batch: ColumnarBatch): Any = {
    val colTypes = GpuColumnVector.extractTypes(batch)

    // track cumulative state of predicates so that never evaluate expressions
    // if an earlier expression has already been evaluated
    var cumulativePred: Option[GpuColumnVector] = None

    // this variable contains the currently evaluated value for each row and gets updated
    // as each branch is evaluated
    var currentValue: Option[GpuColumnVector] = None

    try {
      withResource(GpuColumnVector.from(batch)) { tbl =>

        // iterate over the WHEN THEN branches first
        branches.foreach {
          case (whenExpr, thenExpr) =>
            // evaluate the WHEN predicate
            withResource(GpuExpressionsUtils.columnarEvalToColumn(whenExpr, batch)) { whenBool =>
              // we only want to evaluate where this WHEN is true and no previous WHEN has been true
              val whenBoolAdjusted: GpuColumnVector = cumulativePred match {
                case Some(prev) =>
                  withResource(prev.getBase.not()) { notPreviouslyTrue =>
                    GpuColumnVector.from(
                      whenBool.getBase.and(notPreviouslyTrue), DataTypes.BooleanType)
                  }
                case None =>
                  whenBool.incRefCount()
              }

              withResource(whenBoolAdjusted) { _ =>
                if (isAllTrue(whenBoolAdjusted)) {
                  // if this WHEN predicate is true for all rows and no previous predicate has
                  // been true then we can return immediately
                  return GpuExpressionsUtils.columnarEvalToColumn(thenExpr, batch)
                }
                val thenValues = withResource(filterBatch(tbl,
                    whenBoolAdjusted.getBase, colTypes)) {
                  trueBatch => GpuExpressionsUtils.columnarEvalToColumn(thenExpr, trueBatch)
                }
                withResource(thenValues) { _ =>
                  withResource(gather(whenBoolAdjusted.getBase, thenValues)) { gather =>
                    currentValue = Some(currentValue match {
                      case Some(v) =>
                        withResource(v) { _ =>
                          GpuColumnVector.from(whenBoolAdjusted.getBase.ifElse(gather, v.getBase),
                            dataType)
                        }
                      case _ =>
                        GpuColumnVector.from(gather.incRefCount(), dataType)
                    })
                  }

                  cumulativePred = Some(cumulativePred match {
                    case Some(prev) =>
                      withResource(prev) { _ =>
                        GpuColumnVector.from(whenBool.getBase.or(prev.getBase),
                          DataTypes.BooleanType)
                      }
                    case _ =>
                      whenBoolAdjusted.incRefCount()
                  })
                }
              }

            }
        }

        withResource(cumulativePred.get.getBase.not()) { elsePredicate =>
          // replace null predicates with true (because this is an inverted predicate)
          val elsePredNoNulls = withResource(Scalar.fromBool(true)) { t =>
            GpuColumnVector.from(elsePredicate.replaceNulls(t), DataTypes.BooleanType)
          }
          withResource(elsePredNoNulls) { _ =>
            elseValue match {
              case Some(expr) =>
                if (isAllTrue(elsePredNoNulls)) {
                  GpuExpressionsUtils.columnarEvalToColumn(expr, batch)
                } else {
                  val elseValues = withResource(
                      filterBatch(tbl, elsePredNoNulls.getBase, colTypes)) {
                    elseBatch => GpuExpressionsUtils.columnarEvalToColumn(expr, elseBatch)
                  }
                  withResource(elseValues) { _ =>
                    withResource(gather(elsePredNoNulls.getBase, elseValues)) { gather =>
                      GpuColumnVector.from(elsePredNoNulls.getBase.ifElse(
                        gather, currentValue.get.getBase), dataType)
                    }
                  }
                }

              case None =>
                withResource(GpuScalar.from(null, dataType)) { nullScalar =>
                  if (isAllTrue(elsePredNoNulls) && elsePredNoNulls.getRowCount <= Int.MaxValue) {
                    GpuColumnVector.from(nullScalar, elsePredNoNulls.getRowCount.toInt, dataType)
                  } else {
                    GpuColumnVector.from(
                      elsePredNoNulls.getBase.ifElse(nullScalar, currentValue.get.getBase),
                        dataType)
                  }
                }
            }
          }
        }
      }
    } finally {
      currentValue.foreach(_.safeClose())
      cumulativePred.foreach(_.safeClose())
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
