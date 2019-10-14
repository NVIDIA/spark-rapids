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
import ai.rapids.cudf.NvtxColor
import ai.rapids.spark.GpuMetricNames._
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.{CudfAggregate, GpuAggregateExpression, GpuDeclarativeAggregate}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ArrayBuffer
import scala.math.max

import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType}

class GpuHashAggregateMeta(
    agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[HashAggregateExec](agg, conf, parent, rule) { 
  private val requiredChildDistributionExpressions: Option[Seq[ExprMeta[_]]] =
    agg.requiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
  private val groupingExpressions: Seq[ExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateExpressions: Seq[ExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val aggregateAttributes: Seq[ExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  private val resultExpressions: Seq[ExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[ExprMeta[_]] =
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
    if (!conf.stringHashGroupByEnabled && groupingExpressionTypes.contains(StringType)) {
      willNotWorkOnGpu("strings are not enabled as grouping keys for hash aggregation.")
    }
    if (conf.hasNans &&
      (groupingExpressionTypes.contains(FloatType) ||
        groupingExpressionTypes.contains(DoubleType))) {
      willNotWorkOnGpu("grouping expressions over floating point columns " +
        "that may contain -0.0 and NaN are disabled. You can bypass this by setting " +
        "spark.rapids.sql.hasNans=false")
    }
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    val hashAggMode = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    if (!hashAggReplaceMode.equals("all")) {
      hashAggReplaceMode match {
        case "partial" => if (hashAggMode.contains(Final)) {
          // replacing only Partial hash aggregates, so a Final one should not replace
          willNotWorkOnGpu("Replacing Final hash aggregates disabled")
        }
        case "final" => if (hashAggMode.contains(Partial)) {
          // replacing only Final hash aggregates, so a Partial one should not replace
          willNotWorkOnGpu("Replacing Partial aggregates disabled")
        }
        case _ =>
          throw new IllegalArgumentException(s"The hash aggregate replacement mode ${hashAggReplaceMode} " +
            "is not valid. Valid options are: \"partial\", \"final\", or \"all\"")
      }
    }
  }

  override def convertToGpu(): GpuExec =
    GpuHashAggregateExec(
      requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
      groupingExpressions.map(_.convertToGpu()),
      aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
      aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[GpuAttributeReference]],
      agg.initialInputBufferOffset,
      resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans(0).convertIfNeeded())
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
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val computeAggTime = longMetric("computeAggTime")
    val concatTime = longMetric("concatTime")
    val peakDevMemory = longMetric("peakDevMemory")
    var maximum:Long = 0
    // These metrics are supported by the cpu hash aggregate
    // We have the corresponding gpu versions of:
    //    val peakDevMemory = longMetric("peakDevMemory")
    //    This is the gpu version of peakMemory.
    //    Max memory used by ColumnarBatches and GpuColumnVectors at a given time.
    //
    // We should eventually have gpu versions of:
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

          val nvtxRange = new NvtxWithMetrics("Hash Aggregate Batch", NvtxColor.YELLOW, totalTime)
          try {
            childCvs = processIncomingBatch(batch, boundInputReferences)
            maximum = max(GpuColumnVector.getTotalDeviceMemoryUsed(batch), maximum)
            // done with the batch, clean it as soon as possible
            batch.close()
            batch = null

            // 2) When a partition gets multiple batches, we need to do two things:
            //     a) if this is the first batch, run aggregation and store the aggregated result
            //     b) if this is a subsequent batch, we need to merge the previously aggregated results
            //        with the incoming batch
            //     c) also update total time and aggTime metrics
            aggregatedInputCb = computeAggregate(childCvs, finalMode, groupingExpressions,
              if (finalMode) boundMergeAgg else boundUpdateAgg, computeAggTime)

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
              maximum = max(GpuColumnVector.getTotalDeviceMemoryUsed(concatCvs.toArray), maximum)
              aggregatedCb.close()
              aggregatedCb = null
              aggregatedInputCb.close()
              aggregatedInputCb = null

              // 3) Compute aggregate. In subsequent iterations we'll use this result
              //    to concatenate against incoming batches (step 2)
              aggregatedCb = computeAggregate(concatCvs, merge = true, groupingExpressions,
                boundMergeAgg, computeAggTime)
              maximum = max(GpuColumnVector.getTotalDeviceMemoryUsed(aggregatedCb), maximum)
              concatCvs.safeClose()
              concatCvs = null
            }
          } finally {
            nvtxRange.close()
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
              maximum = max(max(GpuColumnVector.getTotalDeviceMemoryUsed(aggregatedCb),
                GpuColumnVector.getTotalDeviceMemoryUsed(finalCvs.toArray)), maximum)
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

            maximum = max(max(GpuColumnVector.getTotalDeviceMemoryUsed(finalCb),
              GpuColumnVector.getTotalDeviceMemoryUsed(resultCvs.toArray)), maximum)
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
        peakDevMemory.set(maximum)
        childCvs.safeClose()
        concatCvs.safeClose()
        (Seq(batch, aggregatedInputCb, aggregatedCb, finalCb))
          .safeClose()
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
          // note that for string categories, childCv.dataType == StringType
          // so we are not going to cast them to string unnecessarily here,
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
  private def concatenateBatches(aggregatedInputCb: ColumnarBatch, aggregatedCb: ColumnarBatch,
      concatTime: SQLMetric): Seq[GpuColumnVector] = {
    val nvtxRange = new NvtxWithMetrics("concatenateBatches", NvtxColor.BLUE, concatTime)
    try {
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
    } finally {
      nvtxRange.close()
    }
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
                       aggregates: Seq[CudfAggregate],
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
          var cudfAggregates = aggregates.map { agg =>
            if (merge) {
              agg.mergeAggregate
            } else {
              agg.updateAggregate
            }
          }

          var aggregateCvsWithCategories = Seq[GpuColumnVector]()
          try { 
            aggregateCvsWithCategories = toAggregateCvs.safeMap(_.convertToStringCategoriesIfNeeded())
            tbl = new cudf.Table(aggregateCvsWithCategories.map(_.getBase): _*)
          } finally {
            aggregateCvsWithCategories.safeClose()
          }

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
                if (rapidsType == cudf.DType.STRING && 
                  resCol.getType() == cudf.DType.STRING_CATEGORY) {
                  // don't cast to string unnecesarily here, keep string categories as such
                  resCol.incRefCount()
                } else {
                  resCol.castTo(rapidsType, timeUnit) // just does refCount++ for same type
                }
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
        val cvs = aggregates.map { agg =>
          val aggFn = if (merge) {
            agg.mergeReductionAggregate
          } else {
            agg.updateReductionAggregate
          }

          val res = aggFn(toAggregateCvs(agg.getOrdinal(agg.ref)).getBase)
          GpuColumnVector.from(cudf.ColumnVector.fromScalar(res, 1))
        }
        new ColumnarBatch(cvs.toArray, cvs.head.getBase.getRowCount.toInt)
      }
    } finally {
      nvtxRange.close()
    }
  }

  override lazy val additionalMetrics = Map(
    // not supported in GPU
    "peakDevMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak dev memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "avgHashProbe" ->
      SQLMetrics.createAverageMetric(sparkContext, "avg hash probe bucket list iters"),
    "computeAggTime"-> SQLMetrics.createNanoTimingMetric(sparkContext, "time in compute agg"),
    "concatTime"-> SQLMetrics.createNanoTimingMetric(sparkContext, "time in batch concat")
  )

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
