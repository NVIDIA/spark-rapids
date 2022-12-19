/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.math.{max, min}

import ai.rapids.cudf._
import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, NullIntolerant}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class CudfBinaryArithmetic extends CudfBinaryOperator with NullIntolerant {

  protected def allowPrecisionLoss = SQLConf.get.decimalOperationsAllowPrecisionLoss

  // arithmetic operations can overflow and throw exceptions in ANSI mode
  override def hasSideEffects: Boolean = super.hasSideEffects || SQLConf.get.ansiEnabled

  override def dataType: DataType = (left.dataType, right.dataType) match {
    case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
      resultDecimalType(p1, s1, p2, s2)
    case _ => left.dataType
  }

  protected def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    throw new IllegalStateException(
      s"${getClass.getSimpleName} must override `resultDecimalType`.")
  }

  override def checkInputDataTypes(): TypeCheckResult = (left.dataType, right.dataType) match {
    case (l: DecimalType, r: DecimalType) if inputType.acceptsType(l) && inputType.acceptsType(r) =>
      // We allow decimal type inputs with different precision and scale, and use special formulas
      // to calculate the result precision and scale.
      TypeCheckResult.TypeCheckSuccess
    case _ => super.checkInputDataTypes()
  }
}

trait GpuAddSub extends CudfBinaryArithmetic {

  override def outputTypeOverride: DType = GpuColumnVector.getNonNestedRapidsType(dataType)
  val failOnError: Boolean

  override def columnarEval(batch: ColumnarBatch): Any = {
    if (dataType.isInstanceOf[DecimalType]) {
      (left.dataType, right.dataType) match {
        case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
          val resultType = resultDecimalType(p1, s1, p2, s2)
          if (resultType.precision < 38) {
            super.columnarEval(batch)
          } else {
            // call the kernel to add 128-bit numbers
            prepareInputAndExecute(batch, resultType)
          }
        case _ => throw new IllegalStateException("Both operands should be Decimal")
      }
    } else {
      super.columnarEval(batch)
    }
  }

  protected def prepareInputAndExecute(
      batch: ColumnarBatch,
      resultType: DecimalType): Any = {
    val castLhs = withResource(GpuExpressionsUtils.columnarEvalToColumn(left, batch)) { lhs =>
      lhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, lhs.getBase.getType.getScale))
    }
    val retTab = withResource(castLhs) { castLhs =>
      val castRhs = withResource(GpuExpressionsUtils.columnarEvalToColumn(right, batch)) { rhs =>
        rhs.getBase.castTo(DType.create(DType.DTypeEnum.DECIMAL128, rhs.getBase.getType.getScale))
      }
      withResource(castRhs) { castRhs =>
        do128BitOperation(castLhs, castRhs, -resultType.scale)
      }
    }
    val retCol = withResource(retTab) { retTab =>
      val overflowed = retTab.getColumn(0)
      val sum = retTab.getColumn(1)
      if (failOnError) {
        withResource(overflowed.any()) { anyOverflow =>
          if (anyOverflow.isValid && anyOverflow.getBoolean) {
            throw new ArithmeticException(GpuCast.INVALID_INPUT_MESSAGE)
          }
        }
        sum.incRefCount()
      } else {
        withResource(GpuScalar.from(null, dataType)) { nullVal =>
          overflowed.ifElse(nullVal, sum)
        }
      }
    }
    GpuColumnVector.from(retCol, dataType)
  }

  protected def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table
}

case class GpuAdd(
    left: Expression,
    right: Expression,
    override val failOnError: Boolean)
    extends GpuAddBase(left, right, failOnError) with GpuAddSub {

  def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table = {
    com.nvidia.spark.rapids.jni.DecimalUtils.add128(castLhs, castRhs, outputScale)
  }

  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: max(s1, s2) + max(p1-s1, p2-s2) + 1
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

case class GpuSubtract(
    left: Expression,
    right: Expression,
    override val failOnError: Boolean)
    extends GpuSubtractBase(left, right, failOnError) with GpuAddSub {

  def do128BitOperation(
      castLhs: ColumnView,
      castRhs: ColumnView,
      outputScale: Int): Table = {
    com.nvidia.spark.rapids.jni.DecimalUtils.subtract128(castLhs, castRhs, outputScale)
  }

  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

case class GpuRemainder(left: Expression, right: Expression)
    extends GpuRemainderBase(left, right) {
  // scalastyle:off
  // The formula follows Hive which is based on the SQL standard and MS SQL:
  // https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
  // https://msdn.microsoft.com/en-us/library/ms190476.aspx
  // Result Precision: min(p1-s1, p2-s2) + max(s1, s2)
  // Result Scale:     max(s1, s2)
  // scalastyle:on
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}

case class GpuPmod(
    left: Expression,
    right: Expression) extends GpuPmodBase(left, right) {
  // This follows Remainder rule
  override def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
    val resultScale = max(s1, s2)
    val resultPrecision = min(p1 - s1, p2 - s2) + resultScale
    if (allowPrecisionLoss) {
      DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
    } else {
      DecimalType.bounded(resultPrecision, resultScale)
    }
  }
}