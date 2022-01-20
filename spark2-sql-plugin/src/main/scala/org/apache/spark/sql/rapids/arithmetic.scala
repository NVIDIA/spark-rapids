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

import java.math.BigInteger

import com.nvidia.spark.rapids._

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.{ComplexTypeMergingExpression, ExpectsInputTypes, Expression, NullIntolerant}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuDecimalMultiply {
  // For Spark the final desired output is
  // new_scale = lhs.scale + rhs.scale
  // new_precision = lhs.precision + rhs.precision + 1
  // But Spark will round the final result, so we need at least one more
  // decimal place on the scale to be able to do the rounding too.

  // In CUDF the output scale is the same lhs.scale + rhs.scale, but because we need one more
  // we will need to increase the scale for either the lhs or the rhs so it works. We will pick
  // the one with the smallest precision to do it, because it minimises the chance of requiring a
  // larger data type to do the multiply.

  /**
   * Get the scales that are needed for the lhs and rhs to produce the desired result.
   */
  def lhsRhsNeededScales(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): (Int, Int) = {
    val cudfIntermediateScale = lhs.scale + rhs.scale
    val requiredIntermediateScale = outputType.scale + 1
    if (requiredIntermediateScale > cudfIntermediateScale) {
      // In practice this should only ever be 1, but just to be cautious...
      val neededScaleDiff = requiredIntermediateScale - cudfIntermediateScale
      // So we need to add some to the LHS and some to the RHS.
      var addToLhs = 0
      var addToRhs = 0
      // We start by trying
      // to bring them both to the same precision.
      val precisionDiff = lhs.precision - rhs.precision
      if (precisionDiff > 0) {
        addToRhs = math.min(precisionDiff, neededScaleDiff)
      } else {
        addToLhs = math.min(math.abs(precisionDiff), neededScaleDiff)
      }
      val stillNeeded = neededScaleDiff - (addToLhs + addToRhs)
      if (stillNeeded > 0) {
        // We need to split it between the two
        val l = stillNeeded/2
        val r = stillNeeded - l
        addToLhs += l
        addToRhs += r
      }
      (lhs.scale + addToLhs, rhs.scale + addToRhs)
    } else {
      (lhs.scale, rhs.scale)
    }
  }

  def nonRoundedIntermediatePrecision(
      l: DecimalType,
      r: DecimalType,
      outputType: DecimalType): Int = {
    // CUDF ignores the precision, except for the underlying device type, so in general we
    // need to find the largest precision needed between the LHS, RHS, and intermediate output
    // In practice this should probably always be outputType.precision + 1, but just to be
    // cautions we calculate it all out.
    val (lhsScale, rhsScale) = lhsRhsNeededScales(l, r, outputType)
    val lhsPrecision = l.precision - l.scale + lhsScale
    val rhsPrecision = r.precision - r.scale + rhsScale
    // we add 1 to the output precision so we can round the final result to match Spark
    math.max(math.max(lhsPrecision, rhsPrecision), outputType.precision + 1)
  }

  def intermediatePrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int =
    math.min(
      nonRoundedIntermediatePrecision(lhs, rhs, outputType),
      GpuOverrides.DECIMAL128_MAX_PRECISION)

  def intermediateLhsRhsTypes(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): (DecimalType, DecimalType) = {
    val precision = intermediatePrecision(lhs, rhs, outputType)
    val (lhsScale, rhsScale) = lhsRhsNeededScales(lhs, rhs, outputType)
    (DecimalType(precision, lhsScale), DecimalType(precision, rhsScale))
  }

  def intermediateResultType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediatePrecision(lhs, rhs, outputType)
    DecimalType(precision,
      math.min(outputType.scale + 1, GpuOverrides.DECIMAL128_MAX_PRECISION))
  }

  private[this] lazy val max128Int = new BigInteger(Array(2.toByte)).pow(127)
      .subtract(BigInteger.ONE)
  private[this] lazy val min128Int = new BigInteger(Array(2.toByte)).pow(127)
      .negate()

}

object GpuDecimalDivide {
  // For Spark the final desired output is
  // new_scale = max(6, lhs.scale + rhs.precision + 1)
  // new_precision = lhs.precision - lhs.scale + rhs.scale + new_scale
  // But Spark will round the final result, so we need at least one more
  // decimal place on the scale to be able to do the rounding too.

  def lhsNeededScale(rhs: DecimalType, outputType: DecimalType): Int =
    outputType.scale + rhs.scale + 1

  def lhsNeededPrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int = {
    val neededLhsScale = lhsNeededScale(rhs, outputType)
    (lhs.precision - lhs.scale) + neededLhsScale
  }

  def nonRoundedIntermediateArgPrecision(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): Int = {
    val neededLhsPrecision = lhsNeededPrecision(lhs, rhs, outputType)
    math.max(neededLhsPrecision, rhs.precision)
  }

  def intermediateArgPrecision(lhs: DecimalType, rhs: DecimalType, outputType: DecimalType): Int =
    math.min(
      nonRoundedIntermediateArgPrecision(lhs, rhs, outputType),
      GpuOverrides.DECIMAL128_MAX_PRECISION)

  def intermediateLhsType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediateArgPrecision(lhs, rhs, outputType)
    val scale = math.min(lhsNeededScale(rhs, outputType), precision)
    DecimalType(precision, scale)
  }

  def intermediateRhsType(
      lhs: DecimalType,
      rhs: DecimalType,
      outputType: DecimalType): DecimalType = {
    val precision = intermediateArgPrecision(lhs, rhs, outputType)
    DecimalType(precision, rhs.scale)
  }

  def intermediateResultType(outputType: DecimalType): DecimalType = {
    // If the user says that this will not overflow we will still
    // try to do rounding for a correct answer, unless we cannot
    // because it is already a scale of 38
    DecimalType(
      math.min(outputType.precision + 1, GpuOverrides.DECIMAL128_MAX_PRECISION),
      math.min(outputType.scale + 1, GpuOverrides.DECIMAL128_MAX_PRECISION))
  }
}
