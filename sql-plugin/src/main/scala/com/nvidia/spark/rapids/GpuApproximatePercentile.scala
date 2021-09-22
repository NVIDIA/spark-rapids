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

import ai.rapids.cudf
import ai.rapids.cudf.{GroupByAggregation, GroupByAggregationOnColumn}
import com.nvidia.spark.rapids.GpuCast.recursiveDoColumnar

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{CudfAggregate, GpuAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructField, StructType}
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
    accuracyExpression: GpuLiteral = GpuLiteral(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  extends GpuAggregateFunction {

  override val inputProjection: Seq[Expression] = Seq(child)

  // Attributes of fields in the aggregation buffer.
  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  // initialValues is only used in reduction and this is not currently supported
  override lazy val initialValues: Seq[GpuExpression] = throw new UnsupportedOperationException(
    "approx_percentile does not support reduction")

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

  // inputBuf represents the initial t-digest aggregation buffer
  protected final lazy val inputBuf: AttributeReference =
    AttributeReference("inputBuf", CudfTDigest.dataType)()

  // outputBuf represents the merged t-digest aggregation buffer
  protected final lazy val outputBuf: AttributeReference =
    inputBuf.copy("outputBuf", CudfTDigest.dataType)(inputBuf.exprId, inputBuf.qualifier)

  override def nullable: Boolean = false

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val (returnPercentileArray, percentiles) = makeArray(percentageExpression.value)

  private def makeArray(v: Any): (Boolean, Either[Double, Array[Double]])  = v match {
    // Rule ImplicitTypeCasts can cast other numeric types to double
    case null => (false, Right(Array()))
    case num: Double => (false, Left(num))
    case arrayData: ArrayData => (true, Right(arrayData.toDoubleArray()))
    case other => throw new IllegalStateException(s"Invalid percentile expression $other")
  }

  // The result type is the same as the input type.
  private lazy val internalDataType: DataType = {
    if (returnPercentileArray) ArrayType(child.dataType,
      containsNull = false) else child.dataType
  }

  override def dataType: DataType = internalDataType

  override def children: Seq[Expression] = Seq(child, percentageExpression, accuracyExpression)
}

case class ApproxPercentileFromTDigestExpr(
    child: Expression,
    percentiles: Either[Double, Array[Double]],
    finalDataType: DataType)
  extends GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = {
    val expr = child.asInstanceOf[GpuExpression]
    withResource(expr.columnarEval(batch).asInstanceOf[GpuColumnVector]) { cv =>

      percentiles match {
        case Left(p) =>
          // For the scalar case, we still pass cuDF an array of percentiles
          // (containing a single item) and then extract the child column from the resulting
          // array and return that (after converting from Double to finalDataType
          withResource(cv.getBase.approxPercentile(Array(p))) { percentiles =>
            withResource(percentiles.getChildColumnView(0)) { childView =>
              withResource(recursiveDoColumnar(childView, DataTypes.DoubleType, finalDataType,
                  ansiMode = SQLConf.get.ansiEnabled, legacyCastToString = false,
                  stringToDateAnsiModeEnabled = SQLConf.get.ansiEnabled)) { childCv =>
                GpuColumnVector.from(childCv.copyToColumnVector(), dataType)
              }
            }
          }

        case Right(p) =>
          // array case - cast cuDF Array[Double] to Array[finalDataType]
          withResource(cv.getBase.approxPercentile(p)) { percentiles =>
            if (finalDataType == DataTypes.DoubleType) {
              GpuColumnVector.from(percentiles.incRefCount(), dataType)
            } else {
              withResource(percentiles.getChildColumnView(0)) { childView =>
                withResource(recursiveDoColumnar(childView, DataTypes.DoubleType, finalDataType,
                    ansiMode = SQLConf.get.ansiEnabled, legacyCastToString = false,
                    stringToDateAnsiModeEnabled = SQLConf.get.ansiEnabled)) { childCv =>
                  withResource(percentiles.replaceListChild(childCv)) { x =>
                    GpuColumnVector.from(x.copyToColumnVector(), dataType)
                  }
                }
              }
            }
          }

      }
    }
  }
  override def nullable: Boolean = false
  override def dataType: DataType = percentiles match {
    case Left(_) => finalDataType
    case Right(_) => ArrayType(finalDataType, containsNull = false)
  }
  override def children: Seq[Expression] = Seq(child)
}

class CudfTDigest(
    ref: Expression,
    percentileExpr: GpuLiteral,
    accuracyExpression: GpuLiteral)
  extends CudfAggregate(ref) {

  // Map Spark delta to cuDF delta
  private lazy val accuracy = accuracyExpression.value match {
    case delta: Int => delta.max(1000)
    case _ => 1000
  }

  override lazy val updateReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("TDigest is not yet supported in reduction")
  override lazy val mergeReductionAggregateInternal: cudf.ColumnVector => cudf.Scalar =
    throw new UnsupportedOperationException("TDigest is not yet supported in reduction")
  override lazy val updateAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.createTDigest(accuracy)
      .onColumn(getOrdinal(ref))
  override lazy val mergeAggregate: GroupByAggregationOnColumn =
    GroupByAggregation.mergeTDigest(accuracy)
      .onColumn(getOrdinal(ref))
  override def toString(): String = "CudfTDigest"
  override def dataType: DataType = CudfTDigest.dataType
  override def nullable: Boolean = false
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(percentileExpr, accuracyExpression)
}

object CudfTDigest {
  val dataType: DataType = StructType(Array(
    StructField("centroids", ArrayType(StructType(Array(
      StructField("mean", DataTypes.DoubleType, nullable = false),
      StructField("weight", DataTypes.DoubleType, nullable = false)
    )), containsNull = false)),
    StructField("min", DataTypes.DoubleType, nullable = false),
    StructField("max", DataTypes.DoubleType, nullable = false)
  ))
}
