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

package com.nvidia.spark.rapids

import java.util

import scala.collection.mutable

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, Scalar}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, If, NamedExpression, NullsFirst}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ExplainUtils, SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.rapids.{CudfAggregate, GpuAggregateExpression}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType, StructType}
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
          case First(If(_, _, _), _) if validateAggregate(func.references) => true
          case _ => false
        }
      }
    }
  }

  /**
   * Computes a target input batch size based on the assumption that computation can consume up to
   * 4X the configured batch size.
   * @param confTargetSize user-configured maximum desired batch size
   * @param inputTypes input batch schema
   * @param outputTypes output batch schema
   * @param isReductionOnly true if this is a reduction-only aggregation without grouping
   * @return maximum target batch size to keep computation under the 4X configured batch limit
   */
  def computeTargetBatchSize(
      confTargetSize: Long,
      inputTypes: Seq[DataType],
      outputTypes: Seq[DataType],
      isReductionOnly: Boolean): Long = {
    def typesToSize(types: Seq[DataType]): Long =
      types.map(GpuBatchUtils.estimateGpuMemory(_, nullable = false, rowCount = 1)).sum
    val inputRowSize = typesToSize(inputTypes)
    val outputRowSize = typesToSize(outputTypes)
    // The cudf hash table implementation allocates four 32-bit integers per input row.
    val hashTableRowSize = 4 * 4

    // Using the memory management for joins as a reference, target 4X batch size as a budget.
    var totalBudget = 4 * confTargetSize

    // Compute the amount of memory being consumed per-row in the computation
    var computationBytesPerRow = inputRowSize + hashTableRowSize
    if (isReductionOnly) {
      // Remove the lone output row size from the budget rather than track per-row in computation
      totalBudget -= outputRowSize
    } else {
      // The worst-case memory consumption during a grouping aggregation is the case where the
      // grouping does not combine any input rows, so just as many rows appear in the output.
      computationBytesPerRow += outputRowSize
    }

    // Calculate the max rows that can be processed during computation within the budget
    val maxRows = totalBudget / computationBytesPerRow

    // Finally compute the input target batching size taking into account the cudf row limits
    Math.min(inputRowSize * maxRows, Int.MaxValue)
  }

  /**
   * Compute the aggregation modes and aggregate expressions for all aggregation expressions
   * @param aggExpressions the aggregate expressions
   * @param aggBufferAttributes attributes to be bound to the aggregate expressions
   */
  def computeAggModeCudfAggregates(
      aggExpressions: Seq[GpuAggregateExpression],
      aggBufferAttributes: Seq[Attribute]): Seq[(AggregateMode, Seq[CudfAggregate])] = {
    //
    // update expressions are those performed on the raw input data
    // e.g. for count it's count, and for average it's sum and count.
    //
    val updateExpressionsSeq = aggExpressions.map(_.aggregateFunction.updateExpressions)

    //
    // merge expressions are used while merging multiple batches, or while on final mode
    // e.g. for count it's sum, and for average it's sum and sum.
    //
    val mergeExpressionsSeq = aggExpressions.map(_.aggregateFunction.mergeExpressions)

    aggExpressions.zipWithIndex.map { case (expr, modeIndex) =>
      val cudfAggregates = if (expr.mode == Partial || expr.mode == Complete) {
        GpuBindReferences.bindGpuReferences(updateExpressionsSeq(modeIndex), aggBufferAttributes)
            .asInstanceOf[Seq[CudfAggregate]]
      } else {
        GpuBindReferences.bindGpuReferences(mergeExpressionsSeq(modeIndex), aggBufferAttributes)
            .asInstanceOf[Seq[CudfAggregate]]
      }
      (expr.mode, cudfAggregates)
    }
  }
}

/** Utility class to hold all of the metrics related to hash aggregation */
case class GpuHashAggregateMetrics(
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    totalTime: GpuMetric,
    computeAggTime: GpuMetric,
    concatTime: GpuMetric,
    sortTime: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback)

/** Utility class to convey information on the aggregation modes being used */
case class AggregateModeInfo(
    uniqueModes: Seq[AggregateMode],
    hasPartialMode: Boolean,
    hasFinalMode: Boolean,
    hasCompleteMode: Boolean)

class GpuHashAggregateIterator(
    cbIter: Iterator[ColumnarBatch],
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    childOutput: Seq[Attribute],
    modeInfo: AggregateModeInfo,
    metrics: GpuHashAggregateMetrics,
    configuredTargetBatchSize: Long)
    extends Iterator[ColumnarBatch] with Arm with AutoCloseable with Logging {
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
  private case class BoundExpressionsModeAggregates(
      boundInputReferences: Seq[GpuExpression],
      boundFinalProjections: Option[Seq[GpuExpression]],
      boundResultReferences: Seq[Expression],
      aggModeCudfAggregates: Seq[(AggregateMode, Seq[CudfAggregate])])

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private[this] val isReductionOnly = groupingExpressions.isEmpty
  private[this] val boundExpressions = setupReferences(childOutput)
  private[this] val targetMergeBatchSize = computeTargetMergeBatchSize(configuredTargetBatchSize)
  private[this] val aggregatedBatches = new util.ArrayDeque[LazySpillableColumnarBatch]
  private[this] var outOfCoreIter: Option[GpuOutOfCoreSortIterator] = None

  /** Iterator for fetching aggregated batches if a sort-based fallback has occurred */
  private[this] var sortFallbackIter: Option[Iterator[ColumnarBatch]] = None

  /** Whether a batch is pending for a reduction-only aggregation */
  private[this] var hasReductionOnlyBatch: Boolean = isReductionOnly

  override def hasNext: Boolean = {
    // FIXME: totalTime accounting if it stays
    sortFallbackIter.map(_.hasNext).getOrElse {
      // reductions produce a result even if the input is empty
      hasReductionOnlyBatch || !aggregatedBatches.isEmpty || cbIter.hasNext
    }
  }

  override def next(): ColumnarBatch = {
    val batch = sortFallbackIter.map(_.next()).getOrElse {
      // aggregate and merge all pending inputs
      if (cbIter.hasNext) {
        aggregateInputBatches()
        mergeAggregatedBatches()
      }

      if (aggregatedBatches.size() > 1) {
        // Unable to merge to a single output, so must fall back to a sort-based approach.
        sortFallbackIter = Some(buildSortFallbackIterator())
        sortFallbackIter.get.next()
      } else if (aggregatedBatches.isEmpty) {
        if (hasReductionOnlyBatch) {
          hasReductionOnlyBatch = false
          generateEmptyReductionBatch()
        } else {
          throw new NoSuchElementException("batches exhausted")
        }
      } else {
        // this will be the last batch
        hasReductionOnlyBatch = false
        withResource(aggregatedBatches.pop()) { lazyBatch =>
          GpuColumnVector.incRefCounts(lazyBatch.getBatch)
        }
      }
    }

    finalProjectBatch(batch)
  }

  override def close(): Unit = {
    aggregatedBatches.forEach(_.safeClose())
    aggregatedBatches.clear()
    outOfCoreIter.foreach(_.close())
    outOfCoreIter = None
    sortFallbackIter = None
    hasReductionOnlyBatch = false
  }

  private def computeTargetMergeBatchSize(confTargetSize: Long): Long = {
    val aggregates = boundExpressions.aggModeCudfAggregates.flatMap(_._2)
    val mergedTypes = groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)
    AggregateUtils.computeTargetBatchSize(confTargetSize, mergedTypes, mergedTypes,isReductionOnly)
  }

  /** Aggregate all input batches and place the results in the aggregatedBatches queue. */
  private def aggregateInputBatches(): Unit = {
    while (cbIter.hasNext) {
      val (childCvs, isLastInputBatch) = withResource(cbIter.next()) { inputBatch =>
        val isLast = GpuColumnVector.isTaggedAsFinalBatch(inputBatch)
        (processIncomingBatch(inputBatch), isLast)
      }
      withResource(childCvs) { _ =>
        withResource(computeAggregate(childCvs, merge = false)) { aggregatedBatch =>
          val batch = LazySpillableColumnarBatch(aggregatedBatch, metrics.spillCallback,
            "aggbatch")
          // Avoid making batch spillable for the common case of the last and only batch
          if (!(isLastInputBatch && aggregatedBatches.isEmpty)) {
            batch.allowSpilling()
          }
          aggregatedBatches.add(batch)
        }
      }
    }
  }

  /**
   * Attempt to merge adjacent batches in the aggregatedBatches queue until either there is only
   * one batch or merging adjacent batches would exceed the target batch size.
   */
  private def mergeAggregatedBatches(): Unit = {
    while (aggregatedBatches.size() > 1) {
      val concatTime = metrics.concatTime
      withResource(new NvtxWithMetrics("agg merge pass", NvtxColor.BLUE, concatTime)) { _ =>
        // continue merging as long as some batches are able to be combined
        if (!mergePass()) {
          if (aggregatedBatches.size() > 1 && isReductionOnly) {
            // We were unable to merge the aggregated batches within the target batch size limit,
            // which means normally we would fallback to a sort-based approach. However for
            // reduction-only aggregation there are no keys to use for a sort. The only way this
            // can work is if all batches are merged. This will exceed the target batch size limit,
            // but at this point it is either risk an OOM/cudf error and potentially work or
            // not work at all.
            logWarning(s"Unable to merge reduction-only aggregated batches within " +
                s"target batch limit of $targetMergeBatchSize, attempting to merge remaining " +
                s"${aggregatedBatches.size()} batches beyond limit")
            withResource(mutable.ArrayBuffer[LazySpillableColumnarBatch]()) { batchesToConcat =>
              aggregatedBatches.forEach(b => batchesToConcat += b)
              aggregatedBatches.clear()
              val batch = concatenateAndMerge(batchesToConcat)
              // batch does not need to be marked spillable since it is the last and only batch
              // and will be immediately retrieved on the next() call.
              aggregatedBatches.add(batch)
            }
          }
          return
        }
      }
    }
  }

  /**
   * Perform a single pass over the aggregated batches attempting to merge adjacent batches.
   * @return true if at least one merge operation occurred
   */
  private def mergePass(): Boolean = {
    val batchesToConcat: mutable.ArrayBuffer[LazySpillableColumnarBatch] = mutable.ArrayBuffer.empty
    var wasBatchMerged = false
    var concatSize: Long = 0L
    var batchesLeftInPass = aggregatedBatches.size()

    while (batchesLeftInPass > 0) {
      closeOnExcept(batchesToConcat) { _ =>
        var isConcatSearchFinished = false
        // Although tempting to allow the pass to "wrap around" and pick up batches previously
        // merged in this pass, it's avoided to prevent changing the order of aggregated batches.
        while (batchesLeftInPass > 0 && !isConcatSearchFinished) {
          val candidate = aggregatedBatches.getFirst
          val potentialSize = concatSize + candidate.deviceMemorySize
          isConcatSearchFinished = concatSize > 0 && potentialSize > targetMergeBatchSize
          if (!isConcatSearchFinished) {
            batchesLeftInPass -= 1
            batchesToConcat += aggregatedBatches.removeFirst()
            concatSize = potentialSize
          }
        }
      }

      val mergedBatch = if (batchesToConcat.length > 1) {
        wasBatchMerged = true
        val batch = concatenateAndMerge(batchesToConcat)
        batch.allowSpilling()
        batch
      } else {
        // Unable to find a neighboring buffer to produce a valid merge in this pass,
        // so simply put this buffer back on the queue for other passes.
        batchesToConcat.remove(0)
      }

      aggregatedBatches.addLast(mergedBatch)
      batchesToConcat.clear()
      concatSize = 0
    }

    wasBatchMerged
  }

  /**
   * Concatenate batches together and perform a merge aggregation on the result. The input batches
   * will be closed as part of this operation.
   * @param batches batches to concatenate and merge aggregate
   * @return lazy spillable batch which has NOT been marked spillable
   */
  private def concatenateAndMerge(
      batches: mutable.ArrayBuffer[LazySpillableColumnarBatch]): LazySpillableColumnarBatch = {
    withResource(batches) { _ =>
      withResource(concatenateBatches(batches)) { concatVectors =>
        withResource(computeAggregate(concatVectors, merge = true)) { mergedBatch =>
          LazySpillableColumnarBatch(mergedBatch, metrics.spillCallback, "agg merged batch")
        }
      }
    }
  }

  /** Build an iterator that uses a sort-based approach to merge aggregated batches together. */
  private def buildSortFallbackIterator(): Iterator[ColumnarBatch] = {
    logInfo("Falling back to sort-based aggregation with ${aggregatedBatches.size()} batches")
    val aggregatedBatchIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = !aggregatedBatches.isEmpty

      override def next(): ColumnarBatch = {
        withResource(aggregatedBatches.removeFirst()) { lazyBatch =>
          GpuColumnVector.incRefCounts(lazyBatch.getBatch)
        }
      }
    }

    if (isReductionOnly) {
      // TODO: Should reduction-only aggregation ignore the target batch size and try to merge
      //       batches up to the cudf limit instead?  In other words, risk a potential but not
      //       guaranteed OOM while trying to make it work instead of assuming it cannot work.
      throw new IllegalStateException("Unable to fallback to sort-based aggregation " +
          "without grouping keys")
    }

    val shims = ShimLoader.getSparkShims
    val ordering = groupingExpressions.map(shims.sortOrder(_, Ascending, NullsFirst))
    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val sorter = new GpuSorter(ordering, aggBufferAttributes)
    val aggregates = boundExpressions.aggModeCudfAggregates.flatMap(_._2)
    val aggBatchTypes = groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)

    // Use the out of core sort iterator to sort the batches by grouping key
    outOfCoreIter = Some(GpuOutOfCoreSortIterator(
      aggregatedBatchIter,
      sorter,
      LazilyGeneratedOrdering.forSchema(TrampolineUtil.fromAttributes(groupingAttributes)),
      configuredTargetBatchSize,
      totalTime = NoopMetric,
      sortTime = metrics.sortTime,
      outputBatches = NoopMetric,
      outputRows = NoopMetric,
      peakDevMemory = NoopMetric,
      spillCallback = metrics.spillCallback))

    // The out of core sort iterator does not guarantee that a batch contains all of the values
    // for a particular key, so add a key batching iterator to enforce this. That allows each batch
    // to be merge-aggregated safely since all values associated with a particular key are
    // guaranteed to be in the same batch.
    val keyBatchingIter = new GpuKeyBatchingIterator(
      outOfCoreIter.get,
      sorter,
      aggBatchTypes.toArray,
      configuredTargetBatchSize,
      numInputRows = NoopMetric,
      numInputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      numOutputBatches = NoopMetric,
      collectTime = NoopMetric,
      concatTime = metrics.concatTime,
      totalTime = NoopMetric,
      peakDevMemory = NoopMetric,
      spillCallback = metrics.spillCallback)

    // Finally wrap the key batching iterator with a merge aggregation on the output batches.
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = keyBatchingIter.hasNext

      override def next(): ColumnarBatch = {
        // batches coming out of the sort need to be merged
        withResource(keyBatchingIter.next()) { unmergedBatch =>
          computeAggregate(GpuColumnVector.extractColumns(unmergedBatch), merge = true)
        }
      }
    }
  }

  /**
   * Generates the result of a reduction-only aggregation on empty input by emitting the
   * initial value of each aggregator.
   */
  private def generateEmptyReductionBatch(): ColumnarBatch = {
    val aggregateFunctions = aggregateExpressions.map(_.aggregateFunction)
    val defaultValues =
      aggregateFunctions.flatMap(_.initialValues)
    val vecs = defaultValues.safeMap { ref =>
      withResource(GpuScalar.from(ref.asInstanceOf[GpuLiteral].value, ref.dataType)) {
        scalar => GpuColumnVector.from(scalar, 1, ref.dataType)
      }
    }
    new ColumnarBatch(vecs.toArray, 1)
  }

  /**
   * Project a merged aggregated batch result to the layout that Spark expects
   * i.e.: select avg(foo) from bar group by baz will produce:
   *  Partial mode: 3 columns => [bar, sum(foo) as sum_foo, count(foo) as count_foo]
   *  Final mode:   2 columns => [bar, sum(sum_foo) / sum(count_foo)]
   */
  private def finalProjectBatch(batch: ColumnarBatch): ColumnarBatch = {
    val finalBatch = if (boundExpressions.boundFinalProjections.isDefined) {
      withResource(batch) { _ =>
        val finalCvs = boundExpressions.boundFinalProjections.get.map { ref =>
            // aggregatedCb is made up of ColumnVectors
            // and the final projections from the aggregates won't change that,
            // so we can assume they will be vectors after we eval
            ref.columnarEval(batch).asInstanceOf[GpuColumnVector]
          }
        new ColumnarBatch(finalCvs.toArray, finalCvs.head.getRowCount.toInt)
      }
    } else {
      batch
    }

    // Perform the last project to get the correct shape that Spark expects. Note this may
    // add things like literals that were not part of the aggregate into the batch.
    val resultCvs = withResource(finalBatch) { _ =>
      boundExpressions.boundResultReferences.safeMap { ref =>
        // Result references can be virtually anything, we need to coerce
        // them to be vectors since this is going into a ColumnarBatch
        GpuExpressionsUtils.columnarEvalToColumn(ref, finalBatch)
      }
    }
    closeOnExcept(resultCvs) { _ =>
      val rowCount = if (resultCvs.isEmpty) 0 else resultCvs.head.getRowCount.toInt
      metrics.numOutputRows += resultCvs.head.getBase.getRowCount
      metrics.numOutputBatches += 1
      new ColumnarBatch(resultCvs.toArray, rowCount)
    }
  }

  /** Perform the initial projection on the input batch and extract the result columns */
  private def processIncomingBatch(batch: ColumnarBatch): Seq[GpuColumnVector] = {
    boundExpressions.boundInputReferences.safeMap { ref =>
      val childCv = GpuExpressionsUtils.columnarEvalToColumn(ref, batch)
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
   * Concatenates batches by concatenating the corresponding column vectors within the batches.
   * @note the input batches are not closed as part of this operation
   * @param batchesToConcat batches to concatenate
   * @return concatenated vectors that together represent the concatenated batch result
   */
  private def concatenateBatches(
      batchesToConcat: mutable.ArrayBuffer[LazySpillableColumnarBatch]): Seq[GpuColumnVector] = {
    val concatTime = metrics.concatTime
    withResource(new NvtxWithMetrics("concatenateBatches", NvtxColor.BLUE, concatTime)) { _ =>
      val numCols = batchesToConcat.head.numCols
      (0 until numCols).safeMap { i =>
        val columnType = batchesToConcat.head.getBatch.column(i).dataType()
        val columnsToConcat = batchesToConcat.map {
          _.getBatch.column(i).asInstanceOf[GpuColumnVector].getBase
        }
        GpuColumnVector.from(cudf.ColumnVector.concatenate(columnsToConcat: _*), columnType)
      }
    }
  }

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
  private def setupReferences(childAttr: AttributeSeq): BoundExpressionsModeAggregates = {
    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    val aggModeCudfAggregates =
      AggregateUtils.computeAggModeCudfAggregates(aggregateExpressions, aggBufferAttributes)

    //
    // expressions to pick input to the aggregate, and finalize the output to the result projection.
    //
    // Pick update distinct attributes or input projections for Partial
    val (distinctAggExpressions, nonDistinctAggExpressions) = aggregateExpressions.partition(
      _.isDistinct)
    val updateExpressionsDistinct =
      distinctAggExpressions.flatMap(
        _.aggregateFunction.updateExpressions)
    val updateAttributesDistinct =
      distinctAggExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val inputProjectionsDistinct =
      distinctAggExpressions.flatMap(_.aggregateFunction.inputProjection)

    // Pick merge non-distinct for PartialMerge
    val mergeExpressionsNonDistinct =
      nonDistinctAggExpressions
          .flatMap(_.aggregateFunction.mergeExpressions)
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
    modeInfo.uniqueModes.foreach {
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
        aggregateExpressions.map(_.aggregateFunction.evaluateExpression)

    // boundInputReferences is used to pick out of the input batch the appropriate columns
    // for aggregation
    // - Partial Merge mode: we use the inputBindExpressions which can be only
    //   non distinct merge expressions.
    // - Partial or Complete mode: we use the inputProjections or distinct update expressions.
    // - Partial, PartialMerge mode: we use the inputProjections or distinct update expressions
    //   for Partial and non distinct merge expressions for PartialMerge.
    // - Final mode: we pick the columns in the order as handed to us.
    val boundInputReferences = if (modeInfo.uniqueModes.contains(PartialMerge)) {
      GpuBindReferences.bindGpuReferences(inputBindExpressions, resultingBindAttributes)
    } else if (modeInfo.hasFinalMode) {
      GpuBindReferences.bindGpuReferences(childAttr.attrs.asInstanceOf[Seq[Expression]], childAttr)
    } else {
      GpuBindReferences.bindGpuReferences(inputProjections, childAttr)
    }

    val boundFinalProjections = if (modeInfo.hasFinalMode || modeInfo.hasCompleteMode) {
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
    val boundResultReferences = if (modeInfo.hasPartialMode) {
      GpuBindReferences.bindGpuReferences(
        resultExpressions,
        resultExpressions.map(_.toAttribute))
    } else if (modeInfo.hasFinalMode || modeInfo.hasCompleteMode) {
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

  /**
   * Compute the aggregations on the projected input columns.
   * @param toAggregateCvs column vectors representing the input batch to aggregate
   * @param merge true indicates a merge aggregation should be performed
   * @return aggregated batch
   */
  private def computeAggregate(
      toAggregateCvs: Seq[GpuColumnVector],
      merge: Boolean): ColumnarBatch  = {
    val aggModeCudfAggregates = boundExpressions.aggModeCudfAggregates
    val computeAggTime = metrics.computeAggTime
    withResource(new NvtxWithMetrics("computeAggregate", NvtxColor.CYAN, computeAggTime)) { _ =>
      if (groupingExpressions.nonEmpty) {
        // Perform group by aggregation
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
        val result = withResource(new cudf.Table(toAggregateCvs.map(_.getBase): _*)) { tbl =>
          tbl.groupBy(groupingExpressions.indices: _*).aggregate(cudfAggregates: _*)
        }
        withResource(result) { result =>
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
          val dataTypes = groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)

          val resCols = new mutable.ArrayBuffer[ColumnVector](result.getNumberOfColumns)
          for (i <- 0 until result.getNumberOfColumns) {
            val rapidsType = GpuColumnVector.getNonNestedRapidsType(dataTypes(i))
            // cast will be cheap if type matches, only does refCount++ in that case
            closeOnExcept(result.getColumn(i).castTo(rapidsType)) { castedCol =>
              resCols += GpuColumnVector.from(castedCol, dataTypes(i))
            }
          }
          new ColumnarBatch(resCols.toArray, result.getRowCount.toInt)
        }
      } else {
        // Reduction aggregate
        // we ask the appropriate merge or update CudfAggregates, what their
        // reduction merge or update aggregates functions are
        val cvs = mutable.ArrayBuffer[GpuColumnVector]()
        aggModeCudfAggregates.foreach { case (mode, aggs) =>
          aggs.foreach { agg =>
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
        // If cvs is empty, we add a single row with zero value. The value in the row is
        // meaningless as it doesn't matter what we put in it. The projection will add a zero
        // column to the result set in case of a parameter-less count.
        // This is to fix a bug in the plugin where a paramater-less count wasn't returning the
        // desired result compared to Spark-CPU.
        // For more details go to https://github.com/NVIDIA/spark-rapids/issues/1737
        if (cvs.isEmpty) {
          withResource(Scalar.fromLong(0L)) { ZERO =>
            cvs += GpuColumnVector.from(cudf.ColumnVector.fromScalar(ZERO, 1), LongType)
          }
        }
        new ColumnarBatch(cvs.toArray, cvs.head.getBase.getRowCount.toInt)
      }
    }
  }
}

abstract class GpuBaseAggregateMeta[INPUT <: SparkPlan](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  val agg: BaseAggregateExec

  private val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
    aggRequiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
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
    agg.groupingExpressions
      .find(_.dataType match {
        case _@(ArrayType(_, _) | MapType(_, _, _)) | _@StructType(_) => true
        case _ => false
      })
      .foreach(_ =>
        willNotWorkOnGpu("Nested types in grouping expressions are not supported"))
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }

    tagForReplaceMode()

    if (agg.aggregateExpressions.exists(expr => expr.isDistinct)
        && agg.aggregateExpressions.exists(expr => expr.filter.isDefined)) {
      // Distinct with Filter is not supported on the GPU currently,
      // This makes sure that if we end up here, the plan falls back to the CPU
      // which will do the right thing.
      willNotWorkOnGpu(
        "DISTINCT and FILTER cannot be used in aggregate functions at the same time")
    }
  }

  /** Tagging checks tied to configs that control the aggregation modes that are replaced */
  private def tagForReplaceMode(): Unit = {
    val hashAggModes = agg.aggregateExpressions.map(_.mode).distinct
    val hashAggReplaceMode = conf.hashAggReplaceMode.toLowerCase
    hashAggReplaceMode match {
      case "all" =>
      case "partial" =>
        if (hashAggModes.contains(Final) || hashAggModes.contains(Complete)) {
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
      case "final" =>
        if (hashAggModes.contains(Partial) || hashAggModes.contains(Complete)) {
          // replacing only Final hash aggregates, so a Partial or Complete one should not replace
          willNotWorkOnGpu("Replacing Partial or Complete hash aggregates disabled")
        }
      case "complete" =>
        if (hashAggModes.contains(Partial) || hashAggModes.contains(Final)) {
          // replacing only Complete hash aggregates, so a Partial or Final one should not replace
          willNotWorkOnGpu("Replacing Partial or Final hash aggregates disabled")
        }
      case _ =>
        throw new IllegalArgumentException(s"The hash aggregate replacement mode " +
            s"$hashAggReplaceMode is not valid. Valid options are: 'partial', " +
            s"'final', 'complete', or 'all'")
    }

    if (!conf.partialMergeDistinctEnabled && hashAggModes.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
          s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuHashAggregateExec(
      requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
      groupingExpressions.map(_.convertToGpu()),
      aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
      aggregateAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      resultExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
      childPlans.head.convertIfNeeded(),
      conf.gpuTargetBatchSizeBytes)
  }
}

class GpuHashAggregateMeta(
    override val agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseAggregateMeta(agg, agg.requiredChildDistributionExpressions,
      conf, parent, rule)

class GpuSortAggregateMeta(
    override val agg: SortAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseAggregateMeta(agg, agg.requiredChildDistributionExpressions,
      conf, parent, rule) {
  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // Make sure this is the last check - if this is SortAggregate, the children can be sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // HashAggregate.  We don't want to do this if there is a first or last aggregate,
    // because dropping the sort will make them no longer deterministic.
    // In the future we might be able to pull the sort functionality into the aggregate so
    // we can sort a single batch at a time and sort the combined result as well which would help
    // with data skew.
    val hasFirstOrLast = agg.aggregateExpressions.exists { agg =>
      agg.aggregateFunction match {
        case _: First | _: Last => true
        case _ => false
      }
    }
    if (canThisBeReplaced && !hasFirstOrLast) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu("one of the preceding SortExec's cannot be replaced")
          } else {
            plan.shouldBeRemoved("replacing sort aggregate with hash aggregate")
          }
        }
      }
    }
  }
}

/**
 * The GPU version of HashAggregateExec
 *
 * @param requiredChildDistributionExpressions this is unchanged by the GPU. It is used in
 *                                             EnsureRequirements to be able to add shuffle nodes
 * @param groupingExpressions The expressions that, when applied to the input batch, return the
 *                            grouping key
 * @param aggregateExpressions The GpuAggregateExpression instances for this node
 * @param aggregateAttributes References to each GpuAggregateExpression (attribute references)
 * @param resultExpressions the expected output expression of this hash aggregate (which this
 *                          node should project)
 * @param child incoming plan (where we get input columns from)
 * @param configuredTargetBatchSize user-configured maximum device memory size of a batch
 */
case class GpuHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan,
    configuredTargetBatchSize: Long) extends UnaryExecNode with GpuExec with Arm {
  private lazy val uniqueModes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct
  private lazy val partialMode = uniqueModes.contains(Partial) || uniqueModes.contains(PartialMerge)
  private lazy val finalMode = uniqueModes.contains(Final)
  private lazy val completeMode = uniqueModes.contains(Complete)

  private lazy val targetInputBatchSize = computeTargetInputBatchSize()

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    AGG_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_AGG_TIME),
    CONCAT_TIME-> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME),
    SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME)
  ) ++ spillMetrics

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(TargetSize(targetInputBatchSize))

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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val aggMetrics = GpuHashAggregateMetrics(
      numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS),
      numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES),
      totalTime = gpuLongMetric(TOTAL_TIME),
      computeAggTime = gpuLongMetric(AGG_TIME),
      concatTime = gpuLongMetric(CONCAT_TIME),
      sortTime = gpuLongMetric(SORT_TIME),
      makeSpillCallback(allMetrics))

    // cache in a local variable to avoid serializing the full child plan
    val childOutput = child.output
    val groupingExprs = groupingExpressions
    val aggregateExprs = aggregateExpressions
    val aggregateAttrs = aggregateAttributes
    val resultExprs = resultExpressions
    val modeInfo = AggregateModeInfo(
      uniqueModes,
      hasPartialMode = partialMode,
      hasFinalMode = finalMode,
      hasCompleteMode = completeMode)

    val rdd = child.executeColumnar()

    rdd.mapPartitions { cbIter =>
      new GpuHashAggregateIterator(
        cbIter,
        groupingExprs,
        aggregateExprs,
        aggregateAttrs,
        resultExprs,
        childOutput,
        modeInfo,
        aggMetrics,
        configuredTargetBatchSize)
    }
  }

  protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  private def computeTargetInputBatchSize(): Long = {
    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val aggModeCudfAggregates =
      AggregateUtils.computeAggModeCudfAggregates(aggregateExpressions, aggBufferAttributes)

    val inputTypes = child.output.map(_.dataType)
    val aggregates = aggModeCudfAggregates.flatMap(_._2)
    val mergedTypes = groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)
    AggregateUtils.computeTargetBatchSize(configuredTargetBatchSize, inputTypes, mergedTypes,
      groupingExpressions.isEmpty)
  }

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
