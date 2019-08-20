/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, AttributeSet, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.{TypeUtils, truncatedString}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer

// AggregateFunction is not a case class
trait GpuAggregateFunction extends AggregateFunction with GpuUnevaluable {
  // using the child reference, define the shape of the vectors sent to
  // the update/merge expressions
  val inputProjection: Seq[GpuExpression]

  // returns the attribute references associated with this function
  // given a mode of aggregation
  override val aggBufferAttributes: Seq[AttributeReference]
}

case class GpuAggregateExpression(aggregateFunction: GpuAggregateFunction,
                             mode: AggregateMode,
                             isDistinct: Boolean,
                             resultId: ExprId)
  extends GpuExpression with GpuUnevaluable {
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuAggregateExpression]
  }

  //
  // Overrides form AggregateExpression
  //

  // We compute the same thing regardless of our final result.
  override lazy val canonicalized: Expression = {
    val normalizedAggFunc = mode match {
      // For PartialMerge or Final mode, the input to the `aggregateFunction` is aggregate buffers,
      // and the actual children of `aggregateFunction` is not used, here we normalize the expr id.
      case PartialMerge | Final => aggregateFunction.transform {
        case a: AttributeReference => a.withExprId(ExprId(0))
      }
      case Partial | Complete => aggregateFunction
    }

    GpuAggregateExpression(
      normalizedAggFunc.canonicalized.asInstanceOf[GpuAggregateFunction],
      mode,
      isDistinct,
      ExprId(0))
  }

  override def nullable: Boolean = aggregateFunction.nullable
  override def dataType: DataType = aggregateFunction.dataType
  override def children: Seq[Expression] = aggregateFunction.children

  @transient
  override lazy val references: AttributeSet = {
    mode match {
      case Partial | Complete => aggregateFunction.references
      case PartialMerge | Final => AttributeSet(aggregateFunction.aggBufferAttributes)
    }
  }

  override def toString: String = {
    val prefix = mode match {
      case Partial => "partial_"
      case PartialMerge => "merge_"
      case Final | Complete => ""
    }
    prefix + aggregateFunction.toAggString(isDistinct)
  }

  override def sql: String = aggregateFunction.sql(isDistinct)
}

abstract case class CudfAggregate(ref: GpuExpression) extends GpuUnevaluable {
  // we use this to get the ordinal of the bound reference, s.t. we can ask cudf to perform
  // the aggregate on that column
  def getOrdinal(ref: GpuExpression): Int = ref.asInstanceOf[GpuBoundReference].ordinal
  val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar
  val updateAggregate: cudf.Aggregate
  val mergeAggregate: cudf.Aggregate

  def dataType: DataType = ref.dataType
  def nullable: Boolean = ref.nullable
  def children: Seq[Expression] = ref :: Nil
}

class CudfCount(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => cudf.Scalar.fromLong(col.getRowCount)
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.count(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
}

class CudfSum(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.sum
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.sum(getOrdinal(ref))
}

class CudfMax(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.max
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.max(getOrdinal(ref))
}

class CudfMin(ref: GpuExpression) extends CudfAggregate(ref) {
  override val updateReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override val mergeReductionAggregate: cudf.ColumnVector => cudf.Scalar =
    (col: cudf.ColumnVector) => col.min
  override lazy val updateAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
  override lazy val mergeAggregate: cudf.Aggregate = cudf.Table.min(getOrdinal(ref))
}

abstract class GpuDeclarativeAggregate extends GpuAggregateFunction with GpuUnevaluable {
  // these are values that spark calls initial because it uses
  // them to initialize the aggregation buffer, and returns them in case
  // of an empty aggregate when there are no expressions,
  // here we copy them but with the gpu equivalent
  val initialValues: Seq[GpuExpression]

  // update: first half of the aggregation (count = count)
  val updateExpressions: Seq[GpuExpression]

  // merge: second half of the aggregation (count = sum). Also use to merge multiple batches.
  val mergeExpressions: Seq[GpuExpression]

  // mostly likely a pass through (count => sum we merged above).
  // average has a more interesting expression to compute the division of sum/count
  val evaluateExpression: GpuExpression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType = null //not used in GPU version

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())
}


// TODO: for all constructors below s/Expression/GpuExpression, once we get the rest
// of the expressions fixed
// TODO2: remove asInstanceOf[Seq[GpuExpression]] from the inputProjections

case class GpuMin(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMin = new GpuAttributeReference("cudf_min", child.dataType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMin(cudfMin))
  override lazy val evaluateExpression: GpuExpression = cudfMin

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMin :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, child.dataType))

  // Copied from Min
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function gpu min")
}

case class GpuMax(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, child.dataType))

  // Copied from Max
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function gpu max")
}

case class GpuSum(child: Expression) extends GpuDeclarativeAggregate {
  private lazy val resultType = child.dataType match {
    case _: DoubleType => DoubleType
    case _ => LongType
  }

  private lazy val cudfSum = new GpuAttributeReference("cudf_sum", resultType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum))
  override lazy val evaluateExpression: GpuExpression = cudfSum

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfSum :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(null, resultType))

  // Copied from Sum
  override def nullable: Boolean = true
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = child :: Nil
  //protected SQL override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function gpu sum")
}

case class GpuCount(children: Seq[Expression]) extends GpuDeclarativeAggregate {
  // counts are Long
  private lazy val cudfCount = new GpuAttributeReference("cudf_count", LongType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(children.head.asInstanceOf[GpuExpression])
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfCount(cudfCount))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfCount))
  override lazy val evaluateExpression: GpuExpression = cudfCount

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfCount :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(new GpuLiteral(0L, LongType))

  // Copied from Count
  override def nullable: Boolean = false
  override def dataType: DataType = LongType
}

case class GpuAverage(child: Expression) extends GpuDeclarativeAggregate {
  // averages are either Decimal or Double. We don't support decimal yet, so making this double.
  private lazy val cudfSum = new GpuAttributeReference("cudf_sum", DoubleType)()
  private lazy val cudfCount = new GpuAttributeReference("cudf_count", LongType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child,
    if (child.isInstanceOf[GpuLiteral]) {
      child
    } else {
      // takes any column and turns it into 1 for non null, and 0 for null
      // a sum of this == the count
      new GpuCast(new GpuIsNotNull(child), LongType)
    }).asInstanceOf[Seq[GpuExpression]]
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum), new CudfSum(cudfCount))
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfSum(cudfSum), new CudfSum(cudfCount))
  override lazy val evaluateExpression: GpuExpression = new GpuDivide(
    new GpuCast(cudfSum, DoubleType),
    new GpuCast(cudfCount, DoubleType))

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, DoubleType),
    new GpuLiteral(null, LongType))

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfSum :: cudfCount :: Nil

  // Copied from Average
  override def prettyName: String = "gpuavg"
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType // we don't support Decimal
  override def children: Seq[Expression] = child :: Nil
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function gpu average")
}

/*
 * First/Last are "good enough" for the hash aggregate, and should only be used when we
 * want to collapse to a grouped key. The hash aggregate doesn't make guarantees on the
 * ordering of how batches are processed, so this is as good as picking any old function
 * (we picked max)
 *
 * These functions have an extra field they expect to be around in the aggregation buffer.
 * So this adds a "max" of that, and currently sends it to the GPU. The CPU version uses it
 * to check if the value was set (if we don't ignore nulls, valueSet is true, that's what we do here).
 */
case class GpuFirst(child: Expression, ignoreNullsExpr: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()
  private lazy val valueSet = new GpuAttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child, new GpuNot(ignoreNullsExpr)).asInstanceOf[Seq[GpuExpression]]
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, child.dataType),
    new GpuLiteral(false, BooleanType))

  // Copied from First
  // Expected input data type.
  // protected: SQL override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: ignoreNullsExpr :: Nil
  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false
  private def ignoreNulls: Boolean = ignoreNullsExpr.eval().asInstanceOf[Boolean]
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ignoreNullsExpr.foldable) {
      TypeCheckFailure(
        s"The second argument of GpuFirst must be a boolean literal, but got: ${ignoreNullsExpr.sql}")
    } else {
      TypeCheckSuccess
    }
  }
  override def toString: String = s"gpufirst($child)${if (ignoreNulls) " ignore nulls"}"
}

case class GpuLast(child: Expression, ignoreNullsExpr: Expression) extends GpuDeclarativeAggregate {
  private lazy val cudfMax = new GpuAttributeReference("cudf_max", child.dataType)()
  private lazy val valueSet = new GpuAttributeReference("valueSet", BooleanType)()

  override lazy val inputProjection: Seq[GpuExpression] = Seq(child, new GpuNot(ignoreNullsExpr)).asInstanceOf[Seq[GpuExpression]]
  override lazy val updateExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val mergeExpressions: Seq[GpuExpression] = Seq(new CudfMax(cudfMax), new CudfMax(valueSet))
  override lazy val evaluateExpression: GpuExpression = cudfMax

  override lazy val aggBufferAttributes: Seq[GpuAttributeReference] = cudfMax :: valueSet :: Nil

  override lazy val initialValues: Seq[GpuLiteral] = Seq(
    new GpuLiteral(null, child.dataType),
    new GpuLiteral(false, BooleanType))

  // Copied from Last
  // protected: SQL override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)
  override def nullable: Boolean = true
  override def dataType: DataType = child.dataType
  override def children: Seq[Expression] = child :: ignoreNullsExpr :: Nil
  // Last is not a deterministic function.
  override lazy val deterministic: Boolean = false
  private def ignoreNulls: Boolean = ignoreNullsExpr.eval().asInstanceOf[Boolean]
  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ignoreNullsExpr.foldable) {
      TypeCheckFailure(
        s"The second argument of GpuLast must be a boolean literal, but got: ${ignoreNullsExpr.sql}")
    } else {
      TypeCheckSuccess
    }
  }
  override def toString: String = s"gpulast($child)${if (ignoreNulls) " ignore nulls"}"
}

/**
  * GpuHashAggregateExec - is the GPU version of HashAggregateExec, with some major differences:
  * - it doesn't support spilling to disk
  * - it doesn't support strings in the grouping key
  * - it doesn't support count(col1, col2, ..., colN)
  * - it doesn't support distinct
  * @param requiredChildDistributionExpressions - this is unchanged by the GPU. It is used in EnsureRequirements
  *                                               to be able to add shuffle nodes
  * @param groupingExpressions - The expressions that, when applied to the input batch, return the grouping key
  * @param aggregateExpressions - The GpuAggregateExpression instances for this node
  * @param aggregateAttributes - References to each GpuAggregateExpression (attribute references)
  * @param initialInputBufferOffset - this is not used in the GPU version, but it's used to offset the slot in the
  *                                   aggregation buffer that aggregates should start referencing
  * @param resultExpressions - the expected output expression of this hash aggregate (which this node should project)
  * @param child - incoming plan (where we get input columns from)
  */
case class GpuHashAggregateExec(requiredChildDistributionExpressions: Option[Seq[GpuExpression]],
                           groupingExpressions: Seq[GpuExpression],
                           aggregateExpressions: Seq[GpuAggregateExpression],
                           aggregateAttributes: Seq[GpuAttributeReference],
                           initialInputBufferOffset: Int,
                           resultExpressions: Seq[NamedExpression], //TODO: make this a GpuNamedExpression
                           child: SparkPlan) extends UnaryExecNode with GpuExec {

  // This handles GPU hash aggregation without spilling.
  //
  // The CPU version of this is (assume no fallback to sort-based aggregation)
  //  Re: TungstenAggregationIterator.scala, and AggregationIterator.scala
  //
  // 1) Obtaining an input row and finding a buffer (by hash of the grouping key) where
  //    to aggregate on.
  // 2) Once it has a buffer, it calls processRow on it with the incoming row.
  // 3) This will in turn update the buffer, for each row received, agg function by agg function
  // 4) In the happy case, we never spill, and an iterator (from the HashMap) is produced s.t.
  //    downstream nodes can access the aggregated and projected results.
  // 5) Spill case (not handled in gpu case)
  //    a) we create an external sorter [[UnsafeKVExternalSorter]] from the hash map (spilling)
  //       this leaves room for more aggregations to happen. The external sorter first sorts, and then
  //       stores the results to disk, and last it clears the in-memory hash map.
  //    b) we merge external sorters as we spill the map further.
  //    c) after the hash agg is done with its incoming rows, it will switch to sort-based aggregation,
  //       because it detected a spill
  //    d) sort based aggregation is then the mode forward, and not covered in this.
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    // These metrics are supported by the cpu hash aggregate
    //
    // We should eventually have gpu versions of:
    //
    //   This is the peak memory used max of what the hash map has used
    //   and what the external sorter has used
    //    val peakMemory = longMetric("peakMemory")
    //
    //   Byte amount spilled.
    //    val spillSize = longMetric("spillSize")
    //
    // These don't make a lot of sense for the gpu case:
    //
    //   This the time that has passed while setting up the iterators for tungsten
    //    val aggTime = longMetric("aggTime")
    //
    //   Avg number of bucket list iterations per lookup in the underlying map
    //    val avgHashProbe = longMetric("avgHashProbe")
    //
    val rdd = child.executeColumnar()
    rdd.mapPartitions { cbIter => {
      var batch: ColumnarBatch = null // incoming bach
      //
      // aggregated[Input]Cb
      // This is the equivalent of the aggregation buffer for the cpu case with the grouping key.
      // Its columns are: [key1, key2, ..., keyN, cudfAgg1, cudfAgg2, ..., cudfAggN]
      //
      // For aggregate expressions that are multiple cudf aggregates (like average),
      // aggregated[Input]Cb can have one or more cudf aggregate columns. This isn't different than
      // the cpu version, other than in the cpu version everything is a catalyst expression.
      var aggregatedInputCb: ColumnarBatch = null // aggregated raw incoming batch
      var aggregatedCb: ColumnarBatch = null // aggregated concatenated batches

      var finalCb: ColumnarBatch = null // batch after the final projection for each aggregator
      var resultCb: ColumnarBatch = null // after the result projection given in resultExpressions
      var success = false

      var childCvs: Seq[GpuColumnVector] = null
      var concatCvs: Seq[GpuColumnVector] = null
      var resultCvs: Seq[GpuColumnVector] = null

      //
      // For aggregate exec there are four stages of operation:
      //   1) extract columns from input
      //   2) aggregation (update/merge)
      //   3) finalize aggregations (avg = sum/count)
      //   4) result projection (resolve any expressions in the output)
      //
      // In the CPU hash aggregate, Spark is using a buffer to aggregate the results.
      // This buffer has room for each aggregate it is computing (based on type).
      // Note that some AggregateExpressions are more than one slot in this buffer
      // (avg is a sum and a count slot).
      //
      // In the GPU, we don't have an aggregation buffer in Spark code (this happens behind the scenes
      // in cudf), but we still need to be able to pick out columns out of the input CB (for aggregation)
      // and the aggregated CB (for the result projection).
      //
      // Partial mode:
      //  1. boundInputReferences: picks column from raw input
      //  2. boundUpdateAgg: performs the partial half of the aggregates (GpuCount => CudfCount)
      //  3. boundMergeAgg: (if needed) perform a merge of partial aggregates (CudfCount => CudfSum)
      //  4. boundResultReferences: is a pass-through of the merged aggregate
      //
      // Final mode:
      //  1. boundInputReferences: is a pass-through of the merged aggregate
      //  2. boundMergeAgg: perform merges of incoming, and subsequent batches if required.
      //  3. boundFinalProjections: on merged batches, finalize aggregates (GpuAverage => CudfSum/CudfCount)
      //  4. boundResultReferences: project the result expressions Spark expects in the output.
      val (boundInputReferences,
           boundUpdateAgg,
           boundMergeAgg,
           boundFinalProjections,
           boundResultReferences) =
      setupReferences(
        finalMode, child.output, groupingExpressions, aggregateExpressions)
      try {
        while (cbIter.hasNext) {
          // 1) Consume the raw incoming batch, evaluating nested expressions (e.g. avg(col1 + col2)),
          //    obtaining ColumnVectors that can be aggregated
          batch = cbIter.next()

          childCvs = processIncomingBatch(batch, boundInputReferences)

          // done with the batch, clean it as soon as possible
          batch.close()
          batch = null

          // 2) When a partition gets multiple batches, we need to do two things:
          //     a) if this is the first batch, run aggregation and store the aggregated result
          //     b) if this is a subsequent batch, we need to merge the previously aggregated results
          //        with the incoming batch
          aggregatedInputCb = computeAggregate(childCvs, finalMode, groupingExpressions,
            if (finalMode) boundMergeAgg else boundUpdateAgg)

          childCvs.foreach(_.close)
          childCvs = null

          if (aggregatedCb == null) {
            // this is the first batch, regardless of mode.
            aggregatedCb = aggregatedInputCb
            aggregatedInputCb = null
          } else {
            // this is a subsequent batch, and we must:
            // 1) concatenate aggregatedInputCb with the prior result (aggregatedCb)
            // 2) perform a merge aggregate on the concatenated columns
            //
            // In the future, we could plugin in spilling here, where if the concatenated
            // batch sizes would go over a threshold, we'd spill the aggregatedCb,
            // and perform aggregation on the new batch (which would need to be merged, with the
            // spilled aggregates)
            concatCvs = concatenateBatches(aggregatedInputCb, aggregatedCb)
            aggregatedCb.close()
            aggregatedCb = null
            aggregatedInputCb.close()
            aggregatedInputCb = null

            // 3) Compute aggregate. In subsequent iterations we'll use this result
            //    to concatenate against incoming batches (step 2)
            aggregatedCb = computeAggregate(concatCvs, merge = true, groupingExpressions, boundMergeAgg)
            concatCvs.foreach(_.close)
            concatCvs = null
          }
        }

        // if - the input iterator was empty &&
        //      we weren't grouping (reduction op)
        // We need to return a single row, that contains the initial values of each
        // aggregator.
        // Note: for grouped aggregates, we will eventually return an empty iterator.
        if (aggregatedCb == null && groupingExpressions.isEmpty) {
          val aggregateFunctions = aggregateExpressions.map(_.aggregateFunction)
          val defaultValues = aggregateFunctions.asInstanceOf[Seq[GpuDeclarativeAggregate]].flatMap(_.initialValues)
          val vecs = defaultValues.map(ref =>
            GpuColumnVector.from(GpuScalar.from(ref.asInstanceOf[GpuLiteral].value, ref.dataType), 1))
          aggregatedCb = new ColumnarBatch(vecs.toArray, 1)
        }

        // 4) Finally, project the result to the expected layout that Spark expects
        //    i.e.: select avg(foo) from bar group by baz will produce:
        //       Partial mode: 3 columns => [bar, sum(foo) as sum_foo, count(foo) as count_foo]
        //       Final mode:   2 columns => [bar, sum(sum_foo) / sum(count_foo)]
        finalCb = if (boundFinalProjections.isDefined) { 
          if (aggregatedCb != null) {
            val finalCvs =
              boundFinalProjections.get.map { ref =>
                // aggregatedCb is made up of ColumnVectors
                // and the final projections from the aggregates won't change that,
                // so we can assume they will be vectors after we eval
                ref.columnarEval(aggregatedCb).asInstanceOf[GpuColumnVector]
              }
            aggregatedCb.close()
            aggregatedCb = null
            new ColumnarBatch(finalCvs.toArray, finalCvs.head.getRowCount.toInt)
          } else {
            null // this was a grouped aggregate, with an empty input
          }
        } else {
          aggregatedCb
        }

        aggregatedCb = null

        if (finalCb != null) {
          // Perform the last project to get the correct shape that Spark expects. Note this will add things like
          // literals, that were not part of the aggregate into the batch.
          resultCvs = boundResultReferences.map { ref =>
            val result = ref.columnarEval(finalCb)
            // Result references can be virtually anything, we need to coerce
            // them to be vectors since this is going into a ColumnarBatch
            result match {
              case cv: ColumnVector => cv.asInstanceOf[GpuColumnVector]
              case _ => GpuColumnVector.from(GpuScalar.from(result), finalCb.numRows)
            }
          }

          finalCb.close()
          finalCb = null

          resultCb = if (resultCvs.isEmpty) {
            new ColumnarBatch(Seq().toArray, 0)
          } else {
            numOutputRows += resultCvs.head.getBase.getRowCount
            new ColumnarBatch(resultCvs.toArray, resultCvs.head.getBase.getRowCount.toInt)
          }
          success = true
          new Iterator[ColumnarBatch] {
            TaskContext.get().addTaskCompletionListener[Unit] { _ =>
              if (resultCb != null) {
                resultCb.close()
                resultCb = null
              }
            }

            override def hasNext: Boolean = resultCb != null

            override def next(): ColumnarBatch = {
              val out = resultCb
              resultCb = null
              out
            }
          }
        } else {
          // we had a grouped aggregate, without input
          Iterator.empty
        }
      } finally {
        if (!success) {
          if (resultCvs != null) {
            resultCvs.foreach(_.close)
          }
        }
        if (batch != null) {
          batch.close()
        }
        if (childCvs != null) {
          childCvs.foreach(_.close)
        }
        if (aggregatedInputCb != null) {
          aggregatedInputCb.close()
        }
        if (aggregatedCb != null) {
          aggregatedCb.close()
        }
        if (concatCvs != null) {
          concatCvs.foreach(_.close)
        }
        if (finalCb != null) {
          finalCb.close()
        }
      }
    }}
  }

  private def processIncomingBatch(batch: ColumnarBatch,
                                   boundInputReferences: Seq[GpuExpression]): Seq[GpuColumnVector] = {
    boundInputReferences.map { ref => {
      val childCv: GpuColumnVector = null
      var success = false
      try {
        val in = ref.columnarEval(batch)
        val childCv = in match {
          case cv: ColumnVector => cv.asInstanceOf[GpuColumnVector]
          case _ => GpuColumnVector.from(GpuScalar.from(in), batch.numRows)
        }
        val childCvCasted = if (childCv.dataType != ref.dataType) {
          val newCv = GpuColumnVector.from(
            childCv.asInstanceOf[GpuColumnVector].getBase.castTo(
              GpuColumnVector.getRapidsType(childCv.dataType),
              GpuColumnVector.getTimeUnits(ref.dataType)))
          // out with the old, in with the new
          childCv.close()
          newCv
        } else {
          childCv
        }
        success = true
        childCvCasted
      } finally {
        if (!success && childCv != null) {
          childCv.close()
        }
      }
    }}
  }

  /**
    * concatenateBatches - given two ColumnarBatch instances, return a sequence of GpuColumnVector that is
    * the concatenated columns of the two input batches.
    * @param aggregatedInputCb - this is an incoming batch
    * @param aggregatedCb - this is a batch that was kept for concatenation
    * @return Seq[GpuColumnVector] with concatenated vectors
    */
  private def concatenateBatches(aggregatedInputCb: ColumnarBatch, aggregatedCb: ColumnarBatch): Seq[GpuColumnVector] = {
    // get tuples of columns to concatenate

    val zipped = (0 until aggregatedCb.numCols()).map { i =>
      (aggregatedInputCb.column(i), aggregatedCb.column(i))
    }

    val concatCvs = zipped.map {
      case (col1, col2) =>
        GpuColumnVector.from(
          cudf.ColumnVector.concatenate(
            col1.asInstanceOf[GpuColumnVector].getBase,
            col2.asInstanceOf[GpuColumnVector].getBase))
      }

    concatCvs
  }

  // this will need to change when we support distinct aggregates
  // because in this case, a hash aggregate exec could be both a "final" (merge)
  // and partial
  private lazy val finalMode = {
    val modes = aggregateExpressions.map(_.mode).distinct
    modes.contains(Final) || modes.contains(Complete)
  }

  /**
    * getCudfAggregates returns a sequence of [[cudf.Aggregate]], given the current mode
    * [[AggregateMode]], and a sequence of all expressions for this [[GpuHashAggregateExec]]
    * node, we get all the expressions as that's important for us to be able to resolve the current
    * ordinal for this cudf aggregate.
    *
    * Examples:
    * fn = sum, min, max will always be Seq(fn)
    * avg will be Seq(sum, count) for Partial mode, but Seq(sum, sum) for other modes
    * count will be Seq(count) for Partial mode, but Seq(sum) for other modes
    *
    * @return - Seq of [[cudf.Aggregate]], with one or more aggregates that correspond to each expression
    *         in allExpressions
    */
  def setupReferences(finalMode: Boolean,
                      childAttr: AttributeSeq,
                      groupingExpressions: Seq[GpuExpression],
                      aggregateExpressions: Seq[GpuAggregateExpression]):
    (Seq[GpuExpression], Seq[CudfAggregate], Seq[CudfAggregate], Option[Seq[GpuExpression]], Seq[GpuExpression]) = {

    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    //
    // update expressions are those performed on the raw input data
    // e.g. for count it's count, and for average it's sum and count.
    //
    val updateExpressions =
      aggregateExpressions.flatMap(_.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].updateExpressions)

    val updateAggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    val boundUpdateAgg = GpuBindReferences.bindReferences(updateExpressions, updateAggBufferAttributes)

    //
    // merge expressions are used while merging multiple batches, or while on final mode
    // e.g. for count it's sum, and for average it's sum and sum.
    //
    val mergeExpressions =
      aggregateExpressions.flatMap(_.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].mergeExpressions)

    val mergeAggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    val boundMergeAgg = GpuBindReferences.bindReferences(mergeExpressions, mergeAggBufferAttributes)

    //
    // expressions to pick input to the aggregate, and finalize the output to the result projection
    //
    val inputProjections =
      groupingExpressions ++ aggregateExpressions.flatMap(_.aggregateFunction.inputProjection)

    val finalProjections =
      groupingExpressions ++
        aggregateExpressions.map(_.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].evaluateExpression)

    // boundInputReferences is used to pick out of the input batch the appropriate columns for aggregation
    // - Partial mode: we use the aggregateExpressions to pick out the correct columns.
    // - Final mode: we pick the columns in the order as handed to us.
    val boundInputReferences = if (finalMode) {
      GpuBindReferences.bindReferences(childAttr, childAttr)
    } else {
      GpuBindReferences.bindReferences(inputProjections, childAttr)
    }

    val boundFinalProjections = if (finalMode) {
      Some(GpuBindReferences.bindReferences(finalProjections, mergeAggBufferAttributes))
    } else {
      None
    }

    // boundResultReferences is used to project the aggregated input batch(es) for the result.
    // - Partial mode: it's a pass through. We take whatever was aggregated and let it come
    //   out of the node as is.
    // - Final mode: we use resultExpressions to pick out the correct columns that finalReferences
    //   has pre-processed for us
    var boundResultReferences: Seq[GpuExpression] = null

    // allAttributes can be different things, depending on aggregation mode:
    // - Partial mode: grouping key + cudf aggregates (e.g. no avg, intead sum::count
    // - Final mode: grouping key + spark aggregates (e.g. avg)
    val finalAttributes = groupingAttributes ++ aggregateAttributes

    if (finalMode) {
      boundResultReferences =
        GpuBindReferences.bindReferences(
          resultExpressions.asInstanceOf[Seq[GpuExpression]],
          finalAttributes.asInstanceOf[Seq[GpuAttributeReference]])
    } else {
      boundResultReferences =
        GpuBindReferences.bindReferences(
          resultExpressions.asInstanceOf[Seq[GpuExpression]],
          resultExpressions.map(_.toAttribute))
    }

    (boundInputReferences,
      boundUpdateAgg.asInstanceOf[Seq[CudfAggregate]],
      boundMergeAgg.asInstanceOf[Seq[CudfAggregate]],
      boundFinalProjections,
      boundResultReferences)
  }

  def computeAggregate(toAggregateCvs: Seq[GpuColumnVector],
                       merge: Boolean,
                       groupingExpressions: Seq[GpuExpression],
                       aggregates: Seq[CudfAggregate]): ColumnarBatch  = {
    if (groupingExpressions.nonEmpty) {
      // Perform group by aggregation
      var tbl: cudf.Table = null
      var result: cudf.Table = null
      try {
        // Create a cudf Table, which we use as the base of aggregations.
        // At this point we are getting the cudf aggregate's merge or update version
        //
        // For example: GpuAverage has an update version of: (CudfSum, CudfCount)
        // and CudfCount has an update version of AggregateOp.COUNT and a
        // merge version of AggregateOp.COUNT.
        var cudfAggregates = aggregates.map { agg => {
          if (merge) {
            agg.mergeAggregate
          } else {
            agg.updateAggregate
          }
        }}

        tbl = new cudf.Table(toAggregateCvs.map(_.getBase): _*)

        if (cudfAggregates.isEmpty) {
          // we can't have empty aggregates, so pick a dummy max
          // could be caused by something like: select 1 from table group by awesome_key
          cudfAggregates = Seq(cudf.Table.max(0))
        }

        result = tbl.groupBy(groupingExpressions.indices: _*).aggregate(cudfAggregates: _*)

        // Turn aggregation into a ColumnarBatch for the result evaluation
        // Note that the resulting ColumnarBatch has the following shape:
        //
        //   [key1, key2, ..., keyN, cudfAgg1, cudfAgg2, ..., cudfAggN]
        //
        // where cudfAgg_i can be multiple columns foreach Spark aggregate
        // (i.e. partial_gpuavg => cudf sum and cudf count)
        //
        // The type of the columns returned by aggregate depends on cudf. A count of a long column
        // may return a 32bit column, which is bad if you are trying to concatenate batches
        // later. Cast here to the type that the aggregate expects (e.g. Long in case of count)
        if (aggregates.nonEmpty) {
          val dataTypes =
            groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)

          val resCols = new ArrayBuffer[ColumnVector](result.getNumberOfColumns)
          for (i <- 0 until result.getNumberOfColumns) {
            val castedCol = {
              val resCol = result.getColumn(i)
              val rapidsType = GpuColumnVector.getRapidsType(dataTypes(i))
              val timeUnit = GpuColumnVector.getTimeUnits(dataTypes(i))
              resCol.castTo(rapidsType, timeUnit) // just does refCount++ for same type
            }
            var success = false
            try {
              resCols += GpuColumnVector.from(castedCol)
              success = true
            } finally {
              if (!success) {
                castedCol.close()
              }
            }
          }
          new ColumnarBatch(resCols.toArray, result.getRowCount.toInt)
        } else {
          // the types of the aggregate columns didn't change
          // because there are no aggregates, so just return the original result
          GpuColumnVector.from(result)
        }
      } finally {
        if (tbl != null) {
          tbl.close()
        }
        if (result != null) {
          result.close()
        }
      }
    } else {
      // Reduction aggregate
      // we ask the appropriate merge or update CudfAggregates, what their
      // reduction merge or update aggregates functions are
      val cvs = aggregates.map { agg => {
        val aggFn = if (merge) {
          agg.mergeReductionAggregate
        } else {
          agg.updateReductionAggregate
        }

        val res = aggFn(toAggregateCvs(agg.getOrdinal(agg.ref)).getBase)
        GpuColumnVector.from(cudf.ColumnVector.fromScalar(res, 1))
      }}
      new ColumnarBatch(cvs.toArray, cvs.head.getBase.getRowCount.toInt)
    }
  }

  //TODO: add metrics specific for the gpu hash aggregate
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),

    // not supported in GPU
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation build"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probe bucket list iters"))

  //
  // This section is derived (copied in most cases) from HashAggregateExec
  //
  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // Used in de-duping and optimizer rules
  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  // AllTuples = distribution with a single partition and all tuples of the dataset are co-located.
  // Clustered = dataset with tuples co-located in the same partition if they share a specific value
  // Unspecified = distribution with no promises about co-location
  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def output: Seq[Attribute] = {
    // TODO: once we get a GpuNamedExpression
    // this map should go away, and should be replaced with
    // resultExpressions.map(_.toAttribute), where resultExpressions is a Seq[GpuNamedExpression]

    // Without this hack, as the code currently stands, resultExpressions could become cpu-only, making
    // calls to GpuBindReferences.bindReferences fail, this is a temporary hack to get around that,
    // until we finalize the expression tree cleanup
    resultExpressions.map(exp => {
      new GpuAttributeReference(exp.name, exp.dataType, exp.nullable,
        exp.metadata)(exp.exprId, exp.qualifier).toAttribute
    })
  }

  override def doExecute(): RDD[InternalRow] = throw new IllegalStateException(
    "Row-based execution should not occur for this class")

  /**
    * All the attributes that are used for this plan. NOT used for aggregation
    */
  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"GpuHashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"GpuHashAggregate(keys=$keyString, functions=$functionString)"
    }
  }
  //
  // End copies from HashAggregateExec
  //
}
