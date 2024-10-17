/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.aggregate

import scala.collection.immutable.Seq

import ai.rapids.cudf
import ai.rapids.cudf.{DType, GroupByAggregation, ReductionAggregation}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResourceIfAllowed
import com.nvidia.spark.rapids.RapidsPluginImplicits.ReallyAGpuExpression
import com.nvidia.spark.rapids.jni.HLLPP
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.rapids.{GpuCreateNamedStruct, GpuGetStructField}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CudfHLLPP(override val dataType: DataType,
    precision: Int) extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) => input.reduce(
      ReductionAggregation.HLLPP(precision), DType.STRUCT)
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.HLLPP(precision)
  override val name: String = "CudfHyperLogLogPlusPlus"
}

case class CudfMergeHLLPP(override val dataType: DataType,
    precision: Int)
    extends CudfAggregate {
  override lazy val reductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (input: cudf.ColumnVector) =>
      input.reduce(ReductionAggregation.mergeHLL(precision), DType.STRUCT)
  override lazy val groupByAggregate: GroupByAggregation =
    GroupByAggregation.mergeHLL(precision)
  override val name: String = "CudfMergeHyperLogLogPlusPlus"
}

/**
 * Perform the final evaluation step to compute approximate count distinct from sketches.
 * Input is long columns, first construct struct of long then feed to cuDF
 */
case class GpuHyperLogLogPlusPlusEvaluation(childExpr: Expression,
    precision: Int)
    extends GpuExpression with ShimExpression {
  override def dataType: DataType = LongType

  override def nullable: Boolean = false

  override def prettyName: String = "HyperLogLogPlusPlus_evaluation"

  override def children: scala.Seq[Expression] = Seq(childExpr)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(childExpr.columnarEval(batch)) { sketches =>
      val distinctValues = HLLPP.estimateDistinctValueFromSketches(
        sketches.getBase, precision)
      GpuColumnVector.from(distinctValues, LongType)
    }
  }
}

/**
 * Gpu version of HyperLogLogPlusPlus
 * Spark APPROX_COUNT_DISTINCT on NULLs returns zero
 */
case class GpuHyperLogLogPlusPlus(childExpr: Expression, relativeSD: Double)
    extends GpuAggregateFunction with Serializable {

  // Consistent with Spark
  private lazy val precision: Int =
    Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt;

  private lazy val numRegistersPerSketch: Int = 1 << precision;

  // Each long contains 10 register(max 6 bits)
  private lazy val numWords = numRegistersPerSketch / 10 + 1

  // Spark agg buffer type: long array
  private lazy val sparkAggBufferAttributes: Seq[AttributeReference] = {
    Seq.tabulate(numWords) { i =>
      AttributeReference(s"MS[$i]", LongType)()
    }
  }

  /**
   * Spark uses long columns to save agg buffer, e.g.: long[52]
   * Each long compacts multiple registers to save memory
   */
  override val aggBufferAttributes: Seq[AttributeReference] = sparkAggBufferAttributes

  /**
   * init long array with all zero
   */
  override lazy val initialValues: Seq[Expression] = Seq.tabulate(numWords) { _ =>
    GpuLiteral(0L, LongType)
  }

  override lazy val inputProjection: Seq[Expression] = Seq(childExpr)

  /**
   * cuDF HLLPP sketch type: struct<long, ..., long>
   */
  private lazy val cuDFBufferType: DataType = StructType.fromAttributes(aggBufferAttributes)

  /**
   * cuDF uses Struct<long, ..., long> column to do aggregate
   */
  override lazy val updateAggregates: Seq[CudfAggregate] =
    Seq(CudfHLLPP(cuDFBufferType, precision))

  /**
   * Convert long columns to Struct<long, ..., long> column
   */
  private def genStruct: Seq[Expression] = {
    val names = Seq.tabulate(numWords) { i => GpuLiteral(s"MS[$i]", StringType) }
    Seq(GpuCreateNamedStruct(names.zip(aggBufferAttributes).flatten { case (a, b) => List(a, b) }))
  }

  /**
   * Convert Struct<long, ..., long> column to long columns
   */
  override lazy val postUpdate: Seq[Expression] = Seq.tabulate(numWords) {
    i => GpuGetStructField(postUpdateAttr.head, i)
  }

  /**
   * convert to Struct<long, ..., long>
   */
  override lazy val preMerge: Seq[Expression] = genStruct

  override lazy val mergeAggregates: Seq[CudfAggregate] =
    Seq(CudfMergeHLLPP(cuDFBufferType, precision))

  /**
   * Convert Struct<long, ..., long> column to long columns
   */
  override lazy val postMerge: Seq[Expression] = Seq.tabulate(numWords) {
    i => GpuGetStructField(postMergeAttr.head, i)
  }

  override lazy val evaluateExpression: Expression =
    GpuHyperLogLogPlusPlusEvaluation(genStruct.head, precision)

  override def dataType: DataType = LongType

  // Spark APPROX_COUNT_DISTINCT on NULLs returns zero
  override def nullable: Boolean = false

  override def prettyName: String = "approx_count_distinct"

  override def children: Seq[Expression] = Seq(childExpr)
}
