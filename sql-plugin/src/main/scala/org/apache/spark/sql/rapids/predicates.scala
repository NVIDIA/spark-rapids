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

package org.apache.spark.sql.rapids

import ai.rapids.cudf._
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NullIntolerant, Predicate}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, BooleanType, DataType, DoubleType, FloatType}

trait GpuPredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case GpuAnd(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }
}

case class GpuNot(child: Expression) extends CudfUnaryExpression
    with Predicate with ImplicitCastInputTypes with NullIntolerant {
  override def toString: String = s"NOT $child"

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def sql: String = s"(NOT ${child.sql})"

  override def unaryOp: UnaryOp = UnaryOp.NOT
}

object GpuLogicHelper {
  def eqNullAware(lhs: GpuScalar, rhs: Boolean): Boolean =
    lhs.isValid && (lhs.getValue.asInstanceOf[Boolean] == rhs)
}

case class GpuAnd(left: Expression, right: Expression) extends CudfBinaryOperator with Predicate {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_AND

  import GpuLogicHelper._

  // The CUDF implementation of `and` will return a null if either input is null
  // Spark does not.
  // |LHS/RHS| TRUE  | FALSE | NULL  |
  // |TRUE   | TRUE  | FALSE | NULL  |
  // |FALSE  | FALSE | FALSE | FALSE |
  // |NULL   | NULL  | FALSE | NULL  |
  // So we have to make some adjustments.
  //  IF (A <=> FALSE, FALSE,
  //    IF (B <=> FALSE, FALSE,
  //      A cudf_and B))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(false)) { falseVal =>
      val firstPass = withResource(l.and(r)) { lAndR =>
        withResource(l.equalToNullAware(falseVal)) { lIsFalse =>
          lIsFalse.ifElse(falseVal, lAndR)
        }
      }
      withResource(firstPass) { firstPass =>
        withResource(r.equalToNullAware(falseVal)) { rIsFalse =>
          rIsFalse.ifElse(falseVal, firstPass)
        }
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(false)) { falseVal =>
      if (eqNullAware(lhs, false)) {
        ColumnVector.fromScalar(falseVal, r.getRowCount.toInt)
      } else {
        withResource(l.and(r)) { lAndR =>
          withResource(r.equalToNullAware(falseVal)) { rIsFalse =>
            rIsFalse.ifElse(falseVal, lAndR)
          }
        }
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(false)) { falseVal =>
      if (eqNullAware(rhs, false)) {
        ColumnVector.fromScalar(falseVal, l.getRowCount.toInt)
      } else {
        withResource(l.and(r)) { lAndR =>
          withResource(l.equalToNullAware(falseVal)) { lIsFalse =>
            lIsFalse.ifElse(falseVal, lAndR)
          }
        }
      }
    }
  }
}

case class GpuOr(left: Expression, right: Expression) extends CudfBinaryOperator with Predicate {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

  override def binaryOp: BinaryOp = BinaryOp.LOGICAL_OR

  import GpuLogicHelper._

  // The CUDF implementation of `or` will return a null if either input is null
  // Spark does not.
  // |LHS/RHS| TRUE  | FALSE | NULL  |
  // |TRUE   | TRUE  | TRUE  | TRUE  |
  // |FALSE  | TRUE  | FALSE | NULL  |
  // |NULL   | TRUE  | NULL  | NULL  |
  // So we have to make some adjustments.
  //  IF (A <=> TRUE, TRUE,
  //    IF (B <=> TRUE, TRUE,
  //      A cudf_or B))

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(true)) { trueVal =>
      val firstPass = withResource(l.or(r)) { lOrR =>
        withResource(l.equalToNullAware(trueVal)) { lIsTrue =>
          lIsTrue.ifElse(trueVal, lOrR)
        }
      }
      withResource(firstPass) { firstPass =>
        withResource(r.equalToNullAware(trueVal)) { rIsTrue =>
          rIsTrue.ifElse(trueVal, firstPass)
        }
      }
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(true)) { trueVal =>
      if (eqNullAware(lhs, true)) {
        ColumnVector.fromScalar(trueVal, r.getRowCount.toInt)
      } else {
        withResource(l.or(r)) { lOrR =>
          withResource(r.equalToNullAware(trueVal)) { rIsTrue =>
            rIsTrue.ifElse(trueVal, lOrR)
          }
        }
      }
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val l = lhs.getBase
    val r = rhs.getBase
    withResource(Scalar.fromBool(true)) { trueVal =>
      if (eqNullAware(rhs, true)) {
        ColumnVector.fromScalar(trueVal, l.getRowCount.toInt)
      } else {
        withResource(l.or(r)) { lOrR =>
          withResource(l.equalToNullAware(trueVal)) { lIsFalse =>
            lIsFalse.ifElse(trueVal, lOrR)
          }
        }
      }
    }
  }
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

  def hasFloatingPointInputs = left.dataType == FloatType || left.dataType == DoubleType ||
    right.dataType == FloatType || right.dataType == DoubleType
}

/**
 * The table below shows how the result is calculated for Equal-to. To make calculation easier we
 * are leveraging the fact that the cudf-result(r) always returns false. So that result is used in
 * place of false when needed.
 *
 * Return (lhs.nan && rhs.nan) || result[i]
 *
 *  +-------------+------------+------------------+---------------+----+
 *  |  lhs.isNan()|  rhs.isNan |   cudf-result(r) |  final-result | eq |
 *  +-------------+------------+------------------+---------------+----+
 *  |    t        |     f      |       f          |      r        | f  |
 *  |    f        |     t      |       f          |      r        | f  |
 *  |    t        |     t      |       f          |      t        | t  |
 *  |    f        |     f      |       r          |      r        | na |
 *  +-------------+------------+------------------+---------------+----+
 */
case class GpuEqualTo(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "="
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.EQUAL

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan()) { lhsNan =>
          withResource(rhs.getBase.isNan()) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(Scalar.fromBool(lhs.isNan)) { lhsNan =>
          withResource(rhs.getBase.isNan()) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          withResource(Scalar.fromBool(rhs.isNan)) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }
}

case class GpuEqualNullSafe(left: Expression, right: Expression) extends CudfBinaryComparison
  with NullIntolerant {
  override def symbol: String = "<=>"
  override def nullable: Boolean = false
  override def outputTypeOverride: DType = DType.BOOL8
  override def binaryOp: BinaryOp = BinaryOp.NULL_EQUALS

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          withResource(rhs.getBase.isNan) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(Scalar.fromBool(lhs.isNan)) { lhsNan =>
          withResource(rhs.getBase.isNan) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan()) { lhsNan =>
          withResource(Scalar.fromBool(rhs.isNan)) { rhsNan =>
            withResource(lhsNan.and(rhsNan)) { lhsNanAndRhsNan =>
              lhsNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }
}

/**
 * The table below shows how the result is calculated for greater-than. To make calculation easier
 * we are leveraging the fact that the cudf-result(r) always returns false. So that result is used
 * in place of false when needed.
 *
 * In this case return (lhs.nan && !lhs.nan) || result[i]
 *
 *  +-------------+------------+-----------------+---------------+----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | gt |
 *  +-------------+------------+-----------------+---------------+----+
 *  |    t        |     f      |      f          |      t        | t  |
 *  |    f        |     t      |      f          |      r        | f  |
 *  |    t        |     t      |      f          |      r        | f  |
 *  |    f        |     f      |      r          |      r        | na |
 *  +-------------+------------+-----------------+---------------+----+
 */
case class GpuGreaterThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = ">"

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.GREATER

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          withResource(rhs.getBase.isNotNan) { rhsNotNan =>
            withResource(lhsNan.and(rhsNotNan)) { lhsNanAndRhsNotNan =>
              lhsNanAndRhsNotNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          withResource(Scalar.fromBool(rhs.isNotNan)) { rhsNotNan =>
            withResource(lhsNan.and(rhsNotNan)) { lhsNanAndRhsNotNan =>
              lhsNanAndRhsNotNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(Scalar.fromBool(lhs.isNan)) { lhsNan =>
          withResource(rhs.getBase.isNotNan) { rhsNotNan =>
            withResource(lhsNan.and(rhsNotNan)) { lhsNanAndRhsNotNan =>
              lhsNanAndRhsNotNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }
}

/**
 * The table below shows how the result is calculated for Greater-than-Eq. To make calculation
 * easier we are leveraging the fact that the cudf-result(r) always returns false. So that result
 * is used in place of false when needed.
 *
 * In this case return lhs.isNan || result[i]
 *
 *  +-------------+------------+-----------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | gte |
 *  +-------------+------------+-----------------+---------------+-----+
 *  |    t        |     f      |      f          |      t        |   t |
 *  |    f        |     t      |      f          |      r        |   f |
 *  |    t        |     t      |      f          |      t        |   t |
 *  |    f        |     f      |      r          |      r        |  NA |
 *  +-------------+------------+-----------------+---------------+-----+
 */
case class GpuGreaterThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = ">="

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.GREATER_EQUAL

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          lhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if(hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNan) { lhsNan =>
          lhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    if ((lhs.getBase.getType == DType.FLOAT32 ||
         lhs.getBase.getType == DType.FLOAT64) && lhs.isNan) {
      withResource(Scalar.fromBool(true)) { trueScalar =>
        if (rhs.hasNull) {
          withResource(rhs.getBase.isNotNull) { rhsIsNotNull =>
            trueScalar.and(rhsIsNotNull)
          }
        } else {
          ColumnVector.fromScalar(trueScalar, rhs.getRowCount.toInt)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }
}

/**
 * The table below shows how the result is calculated for Less-than. To make calculation easier we
 * are leveraging the fact that the cudf-result(r) always returns false. So that result is used in
 * place of false when needed.
 *
 * In this case return !lhs.nan && rhs.nan || result[i]
 *
 *  +-------------+------------+-----------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |  cudf-result(r) |  final-result | lt  |
 *  +-------------+------------+-----------------+---------------+-----+
 *  |    t        |     f      |      f          |      r        |   f |
 *  |    f        |     t      |      f          |      t        |   t |
 *  |    t        |     t      |      f          |      r        |   f |
 *  |    f        |     f      |      r          |      r        |  NA |
 *  +-------------+------------+-----------------+---------------+-----+
 */
case class GpuLessThan(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "<"

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.LESS

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNotNan) { lhsNotNan =>
          withResource(rhs.getBase.isNan) { rhsNan =>
            withResource(lhsNotNan.and(rhsNan)) { lhsNotNanAndRhsNan =>
              lhsNotNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(lhs.getBase.isNotNan) { lhsNotNan =>
          withResource(Scalar.fromBool(rhs.isNan)) { rhsNan =>
            withResource(lhsNotNan.and(rhsNan)) { lhsNotNanAndRhsNan =>
              lhsNotNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(Scalar.fromBool(lhs.isNotNan)) { lhsNotNan =>
          withResource(rhs.getBase.isNan) { rhsNan =>
            withResource(lhsNotNan.and(rhsNan)) { lhsNotNanAndRhsNan =>
              lhsNotNanAndRhsNan.or(result)
            }
          }
        }
      }
    } else {
      result
    }
  }
}

/**
 * The table below shows how the result is calculated for Less-than-Eq. To make calculation easier
 * we are leveraging the fact that the cudf-result(r) always returns false. So that result is used
 * in place of false when needed.
 *
 * In this case, return rhs.nan || result[i]
 *
 *  +-------------+------------+------------------+---------------+-----+
 *  |  lhs.isNan()|  rhs.isNan |   cudf-result(r) |  final-result | lte |
 *  +-------------+------------+------------------+---------------+-----+
 *  |    t        |     f      |       f          |      r        |   f |
 *  |    f        |     t      |       f          |      t        |   t |
 *  |    t        |     t      |       f          |      t        |   t |
 *  |    f        |     f      |       r          |      r        |  NA |
 *  +-------------+------------+------------------+---------------+-----+
 */
case class GpuLessThanOrEqual(left: Expression, right: Expression) extends CudfBinaryComparison
    with NullIntolerant {
  override def symbol: String = "<="

  override def outputTypeOverride: DType = DType.BOOL8

  override def binaryOp: BinaryOp = BinaryOp.LESS_EQUAL

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(rhs.getBase.isNan) { rhsNan =>
          rhsNan.or(result)
        }
      }
    } else {
      result
    }
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector = {
    if ((rhs.getBase.getType == DType.FLOAT32 ||
         rhs.getBase.getType == DType.FLOAT64) && rhs.isNan) {
      withResource(Scalar.fromBool(true)) { trueScalar =>
        if (lhs.hasNull) {
          withResource(lhs.getBase.isNotNull) { lhsIsNotNull =>
            trueScalar.and(lhsIsNotNull)
          }
        } else {
          ColumnVector.fromScalar(trueScalar, lhs.getRowCount.toInt)
        }
      }
    } else {
      super.doColumnar(lhs, rhs)
    }
  }

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {
    val result = super.doColumnar(lhs, rhs)
    if (hasFloatingPointInputs) {
      withResource(result) { result =>
        withResource(rhs.getBase.isNan) { rhsNan =>
          rhsNan.or(result)
        }
      }
    } else {
      result
    }
  }
}
