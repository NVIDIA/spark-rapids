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

import ai.rapids.cudf.{ColumnVector, RollingAggregationOnColumn}
import com.nvidia.spark.rapids.GpuCast.{recursiveDoColumnar}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{CudfTDigest, GpuAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuApproximatePercentile (
    child: Expression,
    percentageExpression: GpuLiteral,
    accuracyExpression: GpuLiteral)
  extends GpuAggregateFunction with GpuAggregateWindowFunction {

  override val inputProjection: Seq[Expression] = Seq(child)

  // Attributes of fields in the aggregation buffer.
  override def aggBufferAttributes: Seq[AttributeReference] = outputBuf :: Nil

  // Mark as lazy to avoid being initialized when creating a GpuApproximatePercentile.
  override lazy val initialValues: Seq[GpuExpression] = throw new UnsupportedOperationException

  // the update expression will create a t-digest (List[Struct[[Double, Double]])
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
    AttributeReference("inputBuf", child.dataType)()

  // outputBuf represents the final output of the approx_percentile with type List[Double]
  protected final lazy val outputBuf: AttributeReference =
    inputBuf.copy("outputBuf", dataType)(inputBuf.exprId, inputBuf.qualifier)

  /**
   * Using child references, define the shape of the vectors sent to the window operations
   */
  override val windowInputProjection: Seq[Expression] = Seq.empty

  /**
   * Create the aggregation operation to perform for Windowing. The input to this method
   * is a sequence of (index, ColumnVector) that corresponds one to one with what was
   * returned by [[windowInputProjection]].  The index is the index into the Table for the
   * corresponding ColumnVector. Some aggregations need extra values.
   */
  override def windowAggregation(inputs: Seq[(ColumnVector, Int)]): RollingAggregationOnColumn = {
    throw new UnsupportedOperationException()
  }

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