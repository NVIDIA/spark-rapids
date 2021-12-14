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

import ai.rapids.cudf.{ColumnVector, NullPolicy, ScanAggregation, ScanType, Table, UnaryOp}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, Expression}
import org.apache.spark.sql.types.{BooleanType, DataType, DataTypes}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait GpuConditionalExpression extends ComplexTypeMergingExpression with GpuExpression
  with ShimExpression {

  def computeIfElse(
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
      !anyTrue.isValid || !anyTrue.getBoolean
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
    val colTypes = GpuColumnVector.extractTypes(batch)

    withResource(GpuExpressionsUtils.columnarEvalToColumn(predicateExpr, batch)) { pred =>
      if (isAllTrue(pred)) {
        trueExpr.columnarEval(batch)
      } else if (isAllFalse(pred)) {
        falseExpr.columnarEval(batch)
      } else if (gpuTrueExpr.hasSideEffects || gpuFalseExpr.hasSideEffects) {
        withResource(GpuColumnVector.from(batch)) { tbl =>
          withResource(pred.getBase.unaryOp(UnaryOp.NOT)) { inverted =>
            withResource(filterBatch(tbl, pred.getBase, colTypes)) { trueBatch =>
              withResource(filterBatch(tbl, inverted, colTypes)) { falseBatch =>
                withResourceIfAllowed(gpuTrueExpr.columnarEval(trueBatch)) { tt =>
                  withResourceIfAllowed(gpuFalseExpr.columnarEval(falseBatch)) { ff =>
                    val finalRet = (tt, ff) match {
                      case (t: GpuColumnVector, f: GpuColumnVector) =>
                        withResource(GpuColumnVector.from(new Table(t.getBase),
                            Array(trueExpr.dataType))) { trueTable =>
                          withResource(GpuColumnVector.from(new Table(f.getBase),
                              Array(falseExpr.dataType))) { falseTable =>
                            withResource(gather(pred.getBase, trueTable)) { trueValues =>
                              withResource(gather(inverted, falseTable)) { falseValues =>
                                pred.getBase.ifElse(
                                  trueValues.getColumn(0),
                                  falseValues.getColumn(0))
                              }
                            }
                          }
                        }
                      case (t: GpuScalar, f: GpuColumnVector) =>
                        withResource(GpuColumnVector.from(new Table(f.getBase),
                            Array(falseExpr.dataType))) { falseTable =>
                          withResource(gather(inverted, falseTable)) { falseValues =>
                            pred.getBase.ifElse(
                              t.getBase,
                              falseValues.getColumn(0))
                          }
                        }
                      case (t: GpuColumnVector, f: GpuScalar) =>
                        withResource(GpuColumnVector.from(new Table(t.getBase),
                            Array(trueExpr.dataType))) { trueTable =>
                          withResource(gather(pred.getBase, trueTable)) { trueValues =>
                            pred.getBase.ifElse(
                              trueValues.getColumn(0),
                              f.getBase)
                          }
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
        }
      } else {
        // simple approach (original GpuIf code)
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

  private def filterBatch(
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

  private def gather(predicate: ColumnVector, batch: ColumnarBatch): Table = {
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
        val gatherMap = withResource(GpuScalar.from(Int.MinValue, DataTypes.IntegerType)) {
          outOfBoundsFlag => predicate.ifElse(prefixSumExclusive, outOfBoundsFlag)
        }

        withResource(gatherMap) { _ =>
          withResource(GpuColumnVector.from(batch)) { table =>
            table.gather(gatherMap)
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
    // `elseRet` will be closed in `computeIfElse`.
    val elseRet = elseValue
      .map(_.columnarEval(batch))
      .getOrElse(GpuScalar(null, branches.last._2.dataType))
    branches.foldRight[Any](elseRet) { case ((predicateExpr, trueExpr), falseRet) =>
      computeIfElse(batch, predicateExpr, trueExpr, falseRet)
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
