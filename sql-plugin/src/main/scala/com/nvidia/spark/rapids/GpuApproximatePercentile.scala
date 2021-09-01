/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.GpuCast.{recursiveDoColumnar}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{CudfTDigest, GpuAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The ApproximatePercentile function returns the approximate percentile(s) of a column at the given
 * percentage(s). A percentile is a watermark value below which a given percentage of the column
 * values fall. For example, the percentile of column `col` at percentage 50% is the median of
 * column `col`.
 *
 * This function supports partial aggregation.
 *
 * The GPU implementation uses t-digest to perform the initial aggregation (see
 * `updateExpressions` / `mergeExpressions`) and then applies the ApproxPercentileFromTDigestExpr`
 * expression to compute percentiles from the final t-digest (see `evaluateExpression`).
 *
 * There are two different data types involved here. The t-digests are a map of centroids
 * (`Map[mean: Double -> weight: Double]`) represented as `List[Struct[Double, Double]]` and
 * the final output is either a single double or an array of doubles, depending on whether
 * the `percentageExpression` parameter is a single value or an array.
 *
 * @param child child expression that can produce column value with `child.eval()`
 * @param percentageExpression Expression that represents a single percentage value or
 *                             an array of percentage values. Each percentage value must be between
 *                             0.0 and 1.0.
 * @param accuracyExpression Integer literal expression of approximation accuracy. Higher value
 *                           yields better accuracy, the default value is
 *                           DEFAULT_PERCENTILE_ACCURACY.
 */
case class GpuApproximatePercentile (
    child: Expression,
    percentageExpression: GpuLiteral,
    accuracyExpression: GpuLiteral)
  extends GpuAggregateFunction {

  override val inputProjection: Seq[Expression] = Seq(child)

  // Attributes of fields in the aggregation buffer.
  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  // Mark as lazy to avoid being initialized when creating a GpuApproximatePercentile.
  override lazy val initialValues: Seq[GpuExpression] = throw new UnsupportedOperationException

  // the update expression will create a t-digest (List[Struct[Double, Double])
  override lazy val updateExpressions: Seq[Expression] =
    new CudfTDigest(inputBuf,
      percentageExpression,
      accuracyExpression) :: Nil

  // the merge expression will merge t-digests
  override lazy val mergeExpressions: Seq[GpuExpression] =
    new CudfTDigest(outputBuf,
      percentageExpression,
      accuracyExpression) :: Nil

  // the evaluate expression will compute percentiles based on a t-digest
  override lazy val evaluateExpression: Expression = {
    ApproxPercentileFromTDigestExpr(outputBuf, percentiles, child.dataType)
  }

  // inputBuf represents the initial aggregation buffer
  protected final lazy val inputBuf: AttributeReference =
    AttributeReference("inputBuf", CudfTDigest.dataType)()

  // outputBuf represents the merged aggregation buffer
  protected final lazy val outputBuf: AttributeReference =
    inputBuf.copy("outputBuf", CudfTDigest.dataType)(inputBuf.exprId, inputBuf.qualifier)

  override def nullable: Boolean = false

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val (returnPercentileArray, percentiles) =
    percentageExpression match {
      case GpuLiteral(v, _) => makeArray(v)
      case _ => makeArray(percentageExpression.eval())
    }

  private def makeArray(v: Any)  = v match {
    // Rule ImplicitTypeCasts can cast other numeric types to double
    case null => (false, null)
    case num: Double => (false, Array(num))
    case arrayData: ArrayData => (true, arrayData.toDoubleArray())
  }

  // The result type is the same as the input type.
  private lazy val internalDataType: DataType = {
    if (returnPercentileArray) ArrayType(child.dataType,
      containsNull = false) else child.dataType
  }

  override def dataType: DataType = internalDataType

  override def children: Seq[Expression] = Seq(child, percentageExpression, accuracyExpression)
}

object GpuApproximatePercentile {
  def apply(child: Expression, percentageExpression: GpuLiteral): GpuApproximatePercentile = {
    GpuApproximatePercentile(child, percentageExpression,
      GpuLiteral(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  }
}

case class ApproxPercentileFromTDigestExpr(
    child: Expression,
    percentiles: Array[Double],
    finalDataType: DataType)
  extends GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = {
    val expr = child.asInstanceOf[GpuExpression]
    withResource(expr.columnarEval(batch).asInstanceOf[GpuColumnVector]) { cv =>
      withResource(cv.getBase.approxPercentile(percentiles)) { percentiles =>
        if (finalDataType == DataTypes.DoubleType) {
          GpuColumnVector.from(percentiles.incRefCount(), dataType)
        } else {
          // cast cuDF Array[Double] to Array[finalDataType]
          withResource(percentiles.getChildColumnView(0)) { childView =>
            withResource(recursiveDoColumnar(childView, DataTypes.DoubleType, finalDataType,
                ansiMode = SQLConf.get.ansiEnabled, legacyCastToString = false)) { childCv =>
              withResource(percentiles.replaceListChild(childCv)) { x =>
                GpuColumnVector.from(x.copyToColumnVector(), dataType)
              }
            }
          }
        }
      }
    }
  }
  override def nullable: Boolean = false
  override def dataType: DataType = new ArrayType(finalDataType, false)
  override def children: Seq[Expression] = Seq(child)
}