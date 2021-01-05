/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.GpuOverrides.isSupportedType
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, If, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ExplainUtils, SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.{CudfAggregate, GpuAggregateExpression, GpuDeclarativeAggregate}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, MapType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object AggregateUtils {

  private val aggs = List("min", "max", "avg", "sum", "count", "first", "last")

  /**
   * Return true if the Attribute passed is one of aggregates in the aggs list.
   * Use it with caution. We are comparing the name of a column looking for anything that matches
   * with the values in aggs.
   */
  def validateAggregate(attributes: AttributeSet): Boolean = {
    attributes.toSeq.exists(attr => aggs.exists(agg => attr.name.contains(agg)))
  }

  /**
   * Return true if there are multiple distinct functions along with non-distinct functions.
   */
  def shouldFallbackMultiDistinct(aggExprs: Seq[AggregateExpression]): Boolean = {
    // Check if there is an `If` within `First`. This is included in the plan for non-distinct
    // functions only when multiple distincts along with non-distinct functions are present in the
    // query. We fall back to CPU in this case when references of `If` are an aggregate. We cannot
    // call `isDistinct` here on aggregateExpressions to get the total number of distinct functions.
    // If there are multiple distincts, the plan is rewritten by `RewriteDistinctAggregates` where
    // regular aggregations and every distinct aggregation is calculated in a separate group.
    aggExprs.map(e => e.aggregateFunction).exists {
      func => {
        func match {
          case First(If(_, _, _), _) if validateAggregate(func.references) => {
            true
          }
          case _ => false
        }
      }
    }
  }
}

class GpuHashAggregateMeta(
    agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[HashAggregateExec](agg, conf, parent, rule) {
  private val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
    agg.requiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
  private val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    requiredChildDistributionExpressions.getOrElse(Seq.empty) ++
      groupingExpressions ++
      aggregateExpressions ++
      aggregateAttributes ++
      resultExpressions

  override def isSupportedType(t: DataType): Boolean =
    GpuOverrides.isSupportedType(t,
      allowNull = true,
      allowStringMaps = true)

  override def tagPlanForGpu(): Unit = {
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    val hashAggMode = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    if (!hashAggReplaceMode.equals("all")) {
      hashAggReplaceMode match {
        case "partial" => if (hashAggMode.contains(Final) || hashAggMode.contains(Complete)) {
          // replacing only Partial hash aggregates, so a Final or Complete one should not replace
          willNotWorkOnGpu("Replacing Final or Complete hash aggregates disabled")
        }
          // In partial mode, if there are non-distinct functions and multiple distinct functions,
          // non-distinct functions are computed using the First operator. The final result would be
          // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
          // the first batch computed and sent to CPU doesn't contain all the rows required to
          // compute non-distinct function(s), then Spark would consider that value as final result
          // (due to First). Fall back to CPU in this case.
          if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
            willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
              "functions are non-deterministic for non-distinct functions as it is " +
              "computed using First.")
          }
        case "final" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Complete)) {
          // replacing only Final hash aggregates, so a Partial or Complete one should not replace
          willNotWorkOnGpu("Replacing Partial or Complete hash aggregates disabled")
        }
        case "complete" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Final)) {
          // replacing only Complete hash aggregates, so a Partial or Final one should not replace
          willNotWorkOnGpu("Replacing Partial or Final hash aggregates disabled")
        }
        case _ =>
          throw new IllegalArgumentException(s"The hash aggregate replacement mode " +
            s"$hashAggReplaceMode is not valid. Valid options are: 'partial', " +
            s"'final', 'complete', or 'all'")
      }
    }
    if (!conf.partialMergeDistinctEnabled && hashAggMode.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
        s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
    if (agg.aggregateExpressions.exists(expr => expr.isDistinct)
      && agg.aggregateExpressions.exists(expr => expr.filter.isDefined)) {
      // Distinct with Filter is not supported on the CPU currently,
      // this makes sure that if we end up here, the plan falls back to th CPU,
      // which will do the right thing.
      willNotWorkOnGpu(
        "DISTINCT and FILTER cannot be used in aggregate functions at the same time")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuHashAggregateExec(
      requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
      groupingExpressions.map(_.convertToGpu()),
      aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
      aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      agg.initialInputBufferOffset,
      resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans(0).convertIfNeeded())
  }
}

class GpuSortAggregateMeta(
  agg: SortAggregateExec,
  conf: RapidsConf,
  parent: Option[RapidsMeta[_, _, _]],
  rule: ConfKeysAndIncompat) extends SparkPlanMeta[SortAggregateExec](agg, conf, parent, rule) {

  private val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
    agg.requiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
  private val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    requiredChildDistributionExpressions.getOrElse(Seq.empty) ++
      groupingExpressions ++
      aggregateExpressions ++
      aggregateAttributes ++
      resultExpressions

  override def tagPlanForGpu(): Unit = {
    if (GpuOverrides.isAnyStringLit(agg.groupingExpressions)) {
      willNotWorkOnGpu("string literal values are not supported in a hash aggregate")
    }
    val groupingExpressionTypes = agg.groupingExpressions.map(_.dataType)
    if (conf.hasNans &&
      (groupingExpressionTypes.contains(FloatType) ||
        groupingExpressionTypes.contains(DoubleType))) {
      willNotWorkOnGpu("grouping expressions over floating point columns " +
        "that may contain -0.0 and NaN are disabled. You can bypass this by setting " +
        s"${RapidsConf.HAS_NANS}=false")
    }
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    val hashAggMode = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    if (!hashAggReplaceMode.equals("all")) {
      hashAggReplaceMode match {
        case "partial" => if (hashAggMode.contains(Final) || hashAggMode.contains(Complete)) {
          // replacing only Partial hash aggregates, so a Final or Commplete one should not replace
          willNotWorkOnGpu("Replacing Final or Complete hash aggregates disabled")
        }
          // In partial mode, if there are non-distinct functions and multiple distinct functions,
          // non-distinct functions are computed using the First operator. The final result would be
          // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
          // the first batch computed and sent to CPU doesn't contain all the rows required to
          // compute non-distinct function(s), then Spark would consider that value as final result
          // (due to First). Fall back to CPU in this case.
          if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
            willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
              "functions are non-deterministic for non-distinct functions as it is " +
              "computed using First.")
          }
        case "final" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Complete)) {
          // replacing only Final hash aggregates, so a Partial or Complete one should not replace
          willNotWorkOnGpu("Replacing Partial or Complete hash aggregates disabled")
        }
        case "complete" => if (hashAggMode.contains(Partial) || hashAggMode.contains(Final)) {
          // replacing only Complete hash aggregates, so a Partial or Final one should not replace
          willNotWorkOnGpu("Replacing Partial or Final hash aggregates disabled")
        }
        case _ =>
          throw new IllegalArgumentException(s"The hash aggregate replacement mode " +
            s"${hashAggReplaceMode} is not valid. Valid options are: 'partial', 'final', " +
            s"'complete', or 'all'")
      }
    }

    // make sure this is the last check - if this is SortAggregate, the children can be Sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // HashAggregate.  We don't want to do this if there is a first or last aggregate,
    // because dropping the sort will make them no longer deterministic.
    // In the future we might be able to pull the sort functionality into the aggregate so
    // we can sort a single batch at a time and sort the combined result as well which would help
    // with data skew.
    val hasFirstOrLast = agg.aggregateExpressions.exists { agg =>
      agg.aggregateFunction match {
        case _: First => true
        case _: Last => true
        case _ => false
      }
    }
    if (canThisBeReplaced && !hasFirstOrLast) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu(s"can't replace sortAggregate because one of the SortExec's before " +
              s"can't be replaced.")
          } else {
            plan.shouldBeRemoved("removing SortExec as part replacing sortAggregate with " +
              s"hashAggregate")
          }
        }
      }
    }
  }

  override def isSupportedType(t: DataType): Boolean =
    GpuOverrides.isSupportedType(t,
      allowNull = true)

  override def convertToGpu(): GpuExec = {
    // we simply convert to a HashAggregateExec and let GpuOverrides take care of inserting a
    // GpuSortExec if one is needed
    GpuHashAggregateExec(
      requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
      groupingExpressions.map(_.convertToGpu()),
      aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
      aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      agg.initialInputBufferOffset,
      resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans(0).convertIfNeeded())
  }
}

/**
 * GpuHashAggregateExec - is the GPU version of HashAggregateExec, with some major differences:
 * - it doesn't support spilling to disk
 * - it doesn't support strings in the grouping key
 * - it doesn't support count(col1, col2, ..., colN)
 * - it doesn't support distinct
 * @param requiredChildDistributionExpressions this is unchanged by the GPU. It is used in
 *                                             EnsureRequirements to be able to add shuffle nodes
 * @param groupingExpressions The expressions that, when applied to the input batch, return the
 *                            grouping key
 * @param aggregateExpressions The GpuAggregateExpression instances for this node
 * @param aggregateAttributes References to each GpuAggregateExpression (attribute references)
 * @param initialInputBufferOffset this is not used in the GPU version, but it's used to offset
 *                                 the slot in the aggregation buffer that aggregates should
 *                                 start referencing
 * @param resultExpressions the expected output expression of this hash aggregate (which this
 *                          node should project)
 * @param child incoming plan (where we get input columns from)
 */
case class GpuHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode with GpuExec with Arm {

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("Keys", groupingExpressions)}
       |${ExplainUtils.generateFieldString("Functions", aggregateExpressions)}
       |${ExplainUtils.generateFieldString("Aggregate Attributes", aggregateAttributes)}
       |${ExplainUtils.generateFieldString("Results", resultExpressions)}
       |""".stripMargin
  }

  case class BoundExpressionsModeAggregates(boundInputReferences: Seq[GpuExpression] ,
    boundFinalProjections: Option[scala.Seq[GpuExpression]],
    boundResultReferences: scala.Seq[Expression] ,
    aggModeCudfAggregates: scala.Seq[(AggregateMode, scala.Seq[CudfAggregate])])
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
  //       this leaves room for more aggregations to happen. The external sorter first sorts, and
  //       then stores the results to disk, and last it clears the in-memory hash map.
  //    b) we merge external sorters as we spill the map further.
  //    c) after the hash agg is done with its incoming rows, it will switch to sort-based
  //       aggregation, because it detected a spill
  //    d) sort based aggregation is then the mode forward, and not covered in this.
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val computeAggTime = longMetric("computeAggTime")
    val concatTime = longMetric("concatTime")
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

    // cache in a local variable to avoid serializing the full child plan
    val childOutput = child.output

    rdd.mapPartitions { cbIter => {
      var batch: ColumnarBatch = null // incoming batch
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
      // In the GPU, we don't have an aggregation buffer in Spark code (this happens behind the
      // scenes in cudf), but we still need to be able to pick out columns out of the input CB (for
      // aggregation)
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
      //  3. boundFinalProjections: on merged batches, finalize aggregates
      //     (GpuAverage => CudfSum/CudfCount)
      //  4. boundResultReferences: project the result expressions Spark expects in the output.
      val boundExpression = setupReferences(childOutput, groupingExpressions, aggregateExpressions)
      try {
        while (cbIter.hasNext) {
          // 1) Consume the raw incoming batch, evaluating nested expressions
          // (e.g. avg(col1 + col2)), obtaining ColumnVectors that can be aggregated
          batch = cbIter.next()
          if (batch.numRows() == 0) {
            batch.close()
            batch = null
          } else {
            val nvtxRange =
              new NvtxWithMetrics("Hash Aggregate Batch", NvtxColor.YELLOW, totalTime)
            try {
              childCvs = processIncomingBatch(batch, boundExpression.boundInputReferences)

              // done with the batch, clean it as soon as possible
              batch.close()
              batch = null

              // 2) When a partition gets multiple batches, we need to do two things:
              //     a) if this is the first batch, run aggregation and store the aggregated result
              //     b) if this is a subsequent batch, we need to merge the previously aggregated
              //        results with the incoming batch
              //     c) also update total time and aggTime metrics
              aggregatedInputCb = computeAggregate(childCvs, groupingExpressions,
                boundExpression.aggModeCudfAggregates, false, computeAggTime)

              childCvs.safeClose()
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
                concatCvs = concatenateBatches(aggregatedInputCb, aggregatedCb, concatTime)
                aggregatedCb.close()
                aggregatedCb = null
                aggregatedInputCb.close()
                aggregatedInputCb = null

                // 3) Compute aggregate. In subsequent iterations we'll use this result
                //    to concatenate against incoming batches (step 2)
                aggregatedCb = computeAggregate(concatCvs, groupingExpressions,
                  boundExpression.aggModeCudfAggregates, true, computeAggTime)
                concatCvs.safeClose()
                concatCvs = null
              }
            } finally {
              nvtxRange.close()
            }
          }
        }

        // if - the input iterator was empty &&
        //      we weren't grouping (reduction op)
        // We need to return a single row, that contains the initial values of each
        // aggregator.
        // Note: for grouped aggregates, we will eventually return an empty iterator.
        val finalNvtxRange = new NvtxWithMetrics("Final column eval", NvtxColor.YELLOW,
          totalTime)
        try {
          if (aggregatedCb == null && groupingExpressions.isEmpty) {
            val aggregateFunctions = aggregateExpressions.map(_.aggregateFunction)
            val defaultValues =
              aggregateFunctions.asInstanceOf[Seq[GpuDeclarativeAggregate]].flatMap(_.initialValues)
            val vecs = defaultValues.map { ref =>
              val scalar = GpuScalar.from(ref.asInstanceOf[GpuLiteral].value, ref.dataType)
              try {
                GpuColumnVector.from(scalar, 1, ref.dataType)
              } finally {
                scalar.close()
              }
            }
            aggregatedCb = new ColumnarBatch(vecs.toArray, 1)
          }

          // 4) Finally, project the result to the expected layout that Spark expects
          //    i.e.: select avg(foo) from bar group by baz will produce:
          //       Partial mode: 3 columns => [bar, sum(foo) as sum_foo, count(foo) as count_foo]
          //       Final mode:   2 columns => [bar, sum(sum_foo) / sum(count_foo)]
          finalCb = if (boundExpression.boundFinalProjections.isDefined) {
            if (aggregatedCb != null) {
              val finalCvs =
                boundExpression.boundFinalProjections.get.map { ref =>
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
            // Perform the last project to get the correct shape that Spark expects. Note this will
            // add things like literals, that were not part of the aggregate into the batch.
            resultCvs = boundExpression.boundResultReferences.map { ref =>
              val result = ref.columnarEval(finalCb)
              // Result references can be virtually anything, we need to coerce
              // them to be vectors since this is going into a ColumnarBatch
              result match {
                case cv: ColumnVector => cv.asInstanceOf[GpuColumnVector]
                case _ =>
                  withResource(GpuScalar.from(result, ref.dataType)) { scalar =>
                    GpuColumnVector.from(scalar, finalCb.numRows, ref.dataType)
                  }
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
            numOutputBatches += 1
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
          finalNvtxRange.close()
        }
      } finally {
        if (!success) {
          if (resultCvs != null) {
            resultCvs.safeClose()
          }
        }
        childCvs.safeClose()
        concatCvs.safeClose()
        Seq(batch, aggregatedInputCb, aggregatedCb, finalCb)
          .safeClose()
      }
    }}
  }

  private def processIncomingBatch(batch: ColumnarBatch,
      boundInputReferences: Seq[Expression]): Seq[GpuColumnVector] = {
    boundInputReferences.safeMap { ref =>
      val in = ref.columnarEval(batch)
      val childCv = in match {
        case cv: ColumnVector => cv.asInstanceOf[GpuColumnVector]
        case _ =>
          withResource(GpuScalar.from(in, ref.dataType)) { scalar =>
            GpuColumnVector.from(scalar, batch.numRows, ref.dataType)
          }
      }
      if (childCv.dataType == ref.dataType) {
        childCv
      } else {
        withResource(childCv) { childCv =>
          val rapidsType = GpuColumnVector.getNonNestedRapidsType(ref.dataType)
          GpuColumnVector.from(childCv.getBase.castTo(rapidsType), ref.dataType)
        }
      }
    }
  }

  /**
   * concatenateBatches - given two ColumnarBatch instances, return a sequence of GpuColumnVector
   * that is the concatenated columns of the two input batches.
   * @param aggregatedInputCb this is an incoming batch
   * @param aggregatedCb this is a batch that was kept for concatenation
   * @return Seq[GpuColumnVector] with concatenated vectors
   */
  private def concatenateBatches(aggregatedInputCb: ColumnarBatch,
      aggregatedCb: ColumnarBatch,
      concatTime: SQLMetric): Seq[GpuColumnVector] = {
    withResource(new NvtxWithMetrics("concatenateBatches", NvtxColor.BLUE, concatTime)) { _ =>
      // get tuples of columns to concatenate

      val zipped = (0 until aggregatedCb.numCols()).map { i =>
        (aggregatedInputCb.column(i), aggregatedCb.column(i))
      }

      zipped.map {
        case (col1, col2) =>
          GpuColumnVector.from(
            cudf.ColumnVector.concatenate(
              col1.asInstanceOf[GpuColumnVector].getBase,
              col2.asInstanceOf[GpuColumnVector].getBase), col1.dataType())
      }
    }
  }

  private lazy val allModes: Seq[AggregateMode] = aggregateExpressions.map(_.mode)
  private lazy val uniqueModes: Seq[AggregateMode] = allModes.distinct
  private lazy val partialMode = uniqueModes.contains(Partial) || uniqueModes.contains(PartialMerge)
  private lazy val finalMode = uniqueModes.contains(Final)
  private lazy val completeMode = uniqueModes.contains(Complete)

  /**
   * getCudfAggregates returns a sequence of `cudf.Aggregate`, given the current mode
   * `AggregateMode`, and a sequence of all expressions for this [[GpuHashAggregateExec]]
   * node, we get all the expressions as that's important for us to be able to resolve the current
   * ordinal for this cudf aggregate.
   *
   * Examples:
   * fn = sum, min, max will always be Seq(fn)
   * avg will be Seq(sum, count) for Partial mode, but Seq(sum, sum) for other modes
   * count will be Seq(count) for Partial mode, but Seq(sum) for other modes
   *
   * @return Seq of `cudf.Aggregate`, with one or more aggregates that correspond to each
   *         expression in allExpressions
   */
  def setupReferences(childAttr: AttributeSeq,
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[GpuAggregateExpression]): BoundExpressionsModeAggregates = {

    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    val aggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    //
    // update expressions are those performed on the raw input data
    // e.g. for count it's count, and for average it's sum and count.
    //
    val updateExpressionsSeq =
      aggregateExpressions.map(
        _.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].updateExpressions)
    //
    // merge expressions are used while merging multiple batches, or while on final mode
    // e.g. for count it's sum, and for average it's sum and sum.
    //
    val mergeExpressionsSeq =
      aggregateExpressions.map(
        _.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].mergeExpressions)

    val aggModeCudfAggregates = aggregateExpressions.zipWithIndex.map { case (expr, modeIndex) =>
      val cudfAggregates = if (expr.mode == Partial || expr.mode == Complete) {
        GpuBindReferences.bindGpuReferences(updateExpressionsSeq(modeIndex), aggBufferAttributes)
          .asInstanceOf[Seq[CudfAggregate]]
      } else {
        GpuBindReferences.bindGpuReferences(mergeExpressionsSeq(modeIndex), aggBufferAttributes)
          .asInstanceOf[Seq[CudfAggregate]]
      }
      (expr.mode, cudfAggregates)
    }

    //
    // expressions to pick input to the aggregate, and finalize the output to the result projection.
    //
    // Pick update distinct attributes or input projections for Partial
    val (distinctAggExpressions, nonDistinctAggExpressions) = aggregateExpressions.partition(
      _.isDistinct)
    val updateExpressionsDistinct =
      distinctAggExpressions.flatMap(
        _.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].updateExpressions)
    val updateAttributesDistinct =
      distinctAggExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val inputProjectionsDistinct =
      distinctAggExpressions.flatMap(_.aggregateFunction.inputProjection)

    // Pick merge non-distinct for PartialMerge
    val mergeExpressionsNonDistinct =
      nonDistinctAggExpressions
        .flatMap(_.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate].mergeExpressions)
        .map(_.asInstanceOf[CudfAggregate].ref)
    val mergeAttributesNonDistinct =
      nonDistinctAggExpressions.flatMap(
        _.aggregateFunction.aggBufferAttributes)

    // Partial with no distinct or when modes are empty
    val inputProjections: Seq[Expression] = groupingExpressions ++ aggregateExpressions
      .flatMap(_.aggregateFunction.inputProjection)

    var distinctAttributes = Seq[Attribute]()
    var distinctExpressions = Seq[Expression]()
    var nonDistinctAttributes = Seq[Attribute]()
    var nonDistinctExpressions = Seq[Expression]()
    uniqueModes.foreach {
      case PartialMerge =>
        nonDistinctAttributes = mergeAttributesNonDistinct
        nonDistinctExpressions = mergeExpressionsNonDistinct
      case Partial =>
        // Partial with distinct case
        val updateExpressionsCudfAggsDistinct =
          updateExpressionsDistinct.filter(_.isInstanceOf[CudfAggregate])
            .map(_.asInstanceOf[CudfAggregate].ref)
        if (inputProjectionsDistinct.exists(p => !p.isInstanceOf[NamedExpression])) {
          // Case of distinct average we need to evaluate the "GpuCast and GpuIsNotNull" columns.
          // Refer to how input projections are setup for GpuAverage.
          // In the case where we have expressions to evaluate, pick the unique attributes
          // references from them as you only have one column for it before you start evaluating.
          distinctExpressions = inputProjectionsDistinct
          distinctAttributes = inputProjectionsDistinct.flatMap(ref =>
            ref.references.toSeq).distinct
        } else {
          distinctAttributes = updateAttributesDistinct
          distinctExpressions = updateExpressionsCudfAggsDistinct
        }
      case _ =>
    }
    val inputBindExpressions = groupingExpressions ++ nonDistinctExpressions ++ distinctExpressions
    val resultingBindAttributes = groupingAttributes ++ distinctAttributes ++ nonDistinctAttributes

    val finalProjections = groupingExpressions ++
        aggregateExpressions.map(_.aggregateFunction.asInstanceOf[GpuDeclarativeAggregate]
          .evaluateExpression)

    // boundInputReferences is used to pick out of the input batch the appropriate columns
    // for aggregation
    // - Partial Merge mode: we use the inputBindExpressions which can be only
    //   non distinct merge expressions.
    // - Partial or Complete mode: we use the inputProjections or distinct update expressions.
    // - Partial, PartialMerge mode: we use the inputProjections or distinct update expressions
    //   for Partial and non distinct merge expressions for PartialMerge.
    // - Final mode: we pick the columns in the order as handed to us.
    val boundInputReferences = if (uniqueModes.contains(PartialMerge)) {
      GpuBindReferences.bindGpuReferences(inputBindExpressions, resultingBindAttributes)
    } else if (finalMode) {
      GpuBindReferences.bindGpuReferences(childAttr.attrs.asInstanceOf[Seq[Expression]], childAttr)
    } else {
      GpuBindReferences.bindGpuReferences(inputProjections, childAttr)
    }

    val boundFinalProjections = if (finalMode || completeMode) {
      Some(GpuBindReferences.bindGpuReferences(finalProjections, aggBufferAttributes))
    } else {
      None
    }

    // allAttributes can be different things, depending on aggregation mode:
    // - Partial mode: grouping key + cudf aggregates (e.g. no avg, intead sum::count
    // - Final mode: grouping key + spark aggregates (e.g. avg)
    val finalAttributes = groupingAttributes ++ aggregateAttributes

    // boundResultReferences is used to project the aggregated input batch(es) for the result.
    // - Partial mode: it's a pass through. We take whatever was aggregated and let it come
    //   out of the node as is.
    // - Final or Complete mode: we use resultExpressions to pick out the correct columns that
    //   finalReferences has pre-processed for us
    val boundResultReferences = if (partialMode) {
      GpuBindReferences.bindGpuReferences(
        resultExpressions,
        resultExpressions.map(_.toAttribute))
    } else if (finalMode || completeMode) {
      GpuBindReferences.bindGpuReferences(
        resultExpressions,
        finalAttributes)
    } else {
      GpuBindReferences.bindGpuReferences(
        resultExpressions,
        groupingAttributes)
    }
    BoundExpressionsModeAggregates(boundInputReferences, boundFinalProjections,
      boundResultReferences, aggModeCudfAggregates)
  }

  def computeAggregate(toAggregateCvs: Seq[GpuColumnVector],
                       groupingExpressions: Seq[Expression],
                       aggModeCudfAggregates : Seq[(AggregateMode, Seq[CudfAggregate])],
                       merge : Boolean,
                       computeAggTime: SQLMetric): ColumnarBatch  = {
    val nvtxRange = new NvtxWithMetrics("computeAggregate", NvtxColor.CYAN, computeAggTime)
    try {
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
          val aggregates = aggModeCudfAggregates.flatMap(_._2)
          val cudfAggregates = aggModeCudfAggregates.flatMap { case (mode, aggregates) =>
            if ((mode == Partial || mode == Complete) && !merge) {
              aggregates.map(a => a.updateAggregate.onColumn(a.getOrdinal(a.ref)))
            } else {
              aggregates.map(a => a.mergeAggregate.onColumn(a.getOrdinal(a.ref)))
            }
          }
          tbl = new cudf.Table(toAggregateCvs.map(_.getBase): _*)

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
          val dataTypes =
          groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)

          val resCols = new ArrayBuffer[ColumnVector](result.getNumberOfColumns)
          for (i <- 0 until result.getNumberOfColumns) {
            val rapidsType = GpuColumnVector.getNonNestedRapidsType(dataTypes(i))
            // cast will be cheap if type matches, only does refCount++ in that case
            closeOnExcept(result.getColumn(i).castTo(rapidsType)) { castedCol =>
              resCols += GpuColumnVector.from(castedCol, dataTypes(i))
            }
          }
          new ColumnarBatch(resCols.toArray, result.getRowCount.toInt)
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
        val cvs = ArrayBuffer[GpuColumnVector]()
        aggModeCudfAggregates.foreach { case (mode, aggs) =>
          aggs.foreach {agg =>
            val aggFn = if ((mode == Partial || mode == Complete) && !merge) {
              agg.updateReductionAggregate
            } else {
              agg.mergeReductionAggregate
            }
            withResource(aggFn(toAggregateCvs(agg.getOrdinal(agg.ref)).getBase)) { res =>
              val rapidsType = GpuColumnVector.getNonNestedRapidsType(agg.dataType)
              withResource(cudf.ColumnVector.fromScalar(res, 1)) { cv =>
                cvs += GpuColumnVector.from(cv.castTo(rapidsType), agg.dataType)
              }
            }
          }
        }
        new ColumnarBatch(cvs.toArray, cvs.head.getBase.getRowCount.toInt)
      }
    } finally {
      nvtxRange.close()
    }
  }

  override lazy val additionalMetrics = Map(
    // not supported in GPU
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probe bucket list iters"),
    "computeAggTime"->
      SQLMetrics.createNanoTimingMetric(sparkContext, "time in compute agg"),
    "concatTime"-> SQLMetrics.createNanoTimingMetric(sparkContext, "time in batch concat")
  )

  protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  //
  // This section is derived (copied in most cases) from HashAggregateExec
  //
  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  final override def outputPartitioning: Partitioning = {
    if (hasAlias) {
      child.outputPartitioning match {
        case h: GpuHashPartitioning => h.copy(expressions = replaceAliases(h.expressions))
        case h: HashPartitioning => h.copy(expressions = replaceAliases(h.expressions))
        case other => other
      }
    } else {
      child.outputPartitioning
    }
  }

  protected def hasAlias: Boolean = outputExpressions.collectFirst { case _: Alias => }.isDefined

  protected def replaceAliases(exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map {
      case a: AttributeReference => replaceAlias(a).getOrElse(a)
      case other => other
    }
  }

  protected def replaceAlias(attr: AttributeReference): Option[Attribute] = {
    outputExpressions.collectFirst {
      case a @ Alias(child: AttributeReference, _) if child.semanticEquals(attr) =>
        a.toAttribute
    }
  }

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

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

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

    val keyString =
      truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString =
      truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"GpuHashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"GpuHashAggregate(keys=$keyString, functions=$functionString)," +
        s" filters=${aggregateExpressions.map(_.filter)})"
    }
  }
  //
  // End copies from HashAggregateExec
  //
}
