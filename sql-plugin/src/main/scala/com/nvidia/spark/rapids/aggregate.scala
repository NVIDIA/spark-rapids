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

import scala.annotation.tailrec
import scala.collection.mutable

import ai.rapids.cudf
import ai.rapids.cudf.{GroupByAggregationOnColumn, NvtxColor, Scalar}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.v2.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, AttributeSeq, AttributeSet, Expression, ExprId, If, NamedExpression, NullsFirst}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ExplainUtils, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.rapids.{CpuToGpuAggregateBufferConverter, CudfAggregate, GpuAggregateExpression, GpuToCpuAggregateBufferConverter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleMeta, TrampolineUtil}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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
      aggBufferAttributes: Seq[Attribute],
      mergeBufferAttributes: Seq[Attribute]): Seq[(
        GpuAggregateExpression, AggregateMode, Seq[CudfAggregate])] = {
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
        GpuBindReferences.bindGpuReferences(mergeExpressionsSeq(modeIndex), mergeBufferAttributes)
            .asInstanceOf[Seq[CudfAggregate]]
      }
      (expr, expr.mode, cudfAggregates)
    }
  }
}

/** Utility class to hold all of the metrics related to hash aggregation */
case class GpuHashAggregateMetrics(
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    numTasksFallBacked: GpuMetric,
    computeAggTime: GpuMetric,
    concatTime: GpuMetric,
    sortTime: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback)

/** Utility class to convey information on the aggregation modes being used */
case class AggregateModeInfo(
    uniqueModes: Seq[AggregateMode],
    hasPartialMode: Boolean,
    hasPartialMergeMode: Boolean,
    hasFinalMode: Boolean,
    hasCompleteMode: Boolean)

object AggregateModeInfo {
  def apply(uniqueModes: Seq[AggregateMode]): AggregateModeInfo = {
    AggregateModeInfo(
      uniqueModes = uniqueModes,
      hasPartialMode = uniqueModes.contains(Partial),
      hasPartialMergeMode = uniqueModes.contains(PartialMerge),
      hasFinalMode = uniqueModes.contains(Final),
      hasCompleteMode = uniqueModes.contains(Complete)
    )
  }
}

/**
 * Iterator that takes another columnar batch iterator as input and emits new columnar batches that
 * are aggregated based on the specified grouping and aggregation expressions. This iterator tries
 * to perform a hash-based aggregation but is capable of falling back to a sort-based aggregation
 * which can operate on data that is either larger than can be represented by a cudf column or
 * larger than can fit in GPU memory.
 *
 * The iterator starts by pulling all batches from the input iterator, performing an initial
 * projection and aggregation on each individual batch via `aggregateInputBatches()`. The resulting
 * aggregated batches are cached in memory as spillable batches. Once all input batches have been
 * aggregated, `tryMergeAggregatedBatches()` is called to attempt a merge of the aggregated batches
 * into a single batch. If this is successful then the resulting batch can be returned, otherwise
 * `buildSortFallbackIterator` is used to sort the aggregated batches by the grouping keys and
 * performs a final merge aggregation pass on the sorted batches.
 *
 * @param cbIter iterator providing the nput columnar batches
 * @param groupingExpressions expressions used for producing the grouping keys
 * @param aggregateExpressions GPU aggregate expressions used to produce the aggregations
 * @param aggregateAttributes attribute references to each aggregate expression
 * @param resultExpressions output expression for the aggregation
 * @param childOutput input attributes to identify the input columns from the input batches
 * @param modeInfo identifies which aggregation modes are being used
 * @param metrics metrics that will be updated during aggregation
 * @param configuredTargetBatchSize user-specified value for the targeted input batch size
 */
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
  //
  // Complete mode:
  //  1. boundInputReferences: picks column from raw input
  //  2. boundUpdateAgg: performs the partial half of the aggregates (GpuCount => CudfCount)
  //  3. boundMergeAgg: (if needed) perform a merge of partial aggregates (CudfCount => CudfSum)
  //  4. boundResultReferences: project the result expressions Spark expects in the output.
  private case class BoundExpressionsModeAggregates(
      boundInputReferences: Seq[GpuExpression],
      boundFinalProjections: Option[Seq[GpuExpression]],
      boundResultReferences: Seq[Expression],
      aggModeCudfAggregates: Seq[(GpuAggregateExpression, AggregateMode, Seq[CudfAggregate])])

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
        tryMergeAggregatedBatches()
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
    val aggregates = boundExpressions.aggModeCudfAggregates.flatMap(_._3)
    val mergedTypes = groupingExpressions.map(_.dataType) ++ aggregates.map(_.dataType)
    AggregateUtils.computeTargetBatchSize(confTargetSize, mergedTypes, mergedTypes,isReductionOnly)
  }

  /** Aggregate all input batches and place the results in the aggregatedBatches queue. */
  private def aggregateInputBatches(): Unit = {
    while (cbIter.hasNext) {
      val (childBatch, isLastInputBatch) = withResource(cbIter.next()) { inputBatch =>
        val isLast = GpuColumnVector.isTaggedAsFinalBatch(inputBatch)
        (processIncomingBatch(inputBatch), isLast)
      }
      withResource(childBatch) { _ =>
        withResource(computeAggregate(childBatch, merge = false)) { aggBatch =>
          val batch = LazySpillableColumnarBatch(aggBatch, metrics.spillCallback, "aggbatch")
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
  private def tryMergeAggregatedBatches(): Unit = {
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
    // Current size in bytes of the batches targeted for the next concatenation
    var concatSize: Long = 0L
    var batchesLeftInPass = aggregatedBatches.size()

    while (batchesLeftInPass > 0) {
      closeOnExcept(batchesToConcat) { _ =>
        var isConcatSearchFinished = false
        // Old batches are picked up at the front of the queue and freshly merged batches are
        // appended to the back of the queue. Although tempting to allow the pass to "wrap around"
        // and pick up batches freshly merged in this pass, it's avoided to prevent changing the
        // order of aggregated batches.
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

      // Add the merged batch to the end of the aggregated batch queue. Only a single pass over
      // the batches is being performed due to the batch count check above, so the single-pass
      // loop will terminate before picking up this new batch.
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
        val concatBatch = new ColumnarBatch(
          concatVectors.toArray,
          concatVectors.head.getRowCount.toInt)
        withResource(computeAggregate(concatBatch, merge = true)) { mergedBatch =>
          LazySpillableColumnarBatch(mergedBatch, metrics.spillCallback, "agg merged batch")
        }
      }
    }
  }

  /** Build an iterator that uses a sort-based approach to merge aggregated batches together. */
  private def buildSortFallbackIterator(): Iterator[ColumnarBatch] = {
    logInfo("Falling back to sort-based aggregation with ${aggregatedBatches.size()} batches")
    metrics.numTasksFallBacked += 1
    val aggregatedBatchIter = new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = !aggregatedBatches.isEmpty

      override def next(): ColumnarBatch = {
        withResource(aggregatedBatches.removeFirst()) { lazyBatch =>
          GpuColumnVector.incRefCounts(lazyBatch.getBatch)
        }
      }
    }

    if (isReductionOnly) {
      // Normally this should never happen because `tryMergeAggregatedBatches` should have done
      // a last-ditch effort to concatenate all batches together regardless of target limits.
      throw new IllegalStateException("Unable to fallback to sort-based aggregation " +
          "without grouping keys")
    }

    val shims = ShimLoader.getSparkShims
    val ordering = groupingExpressions.map(shims.sortOrder(_, Ascending, NullsFirst))
    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val sorter = new GpuSorter(ordering, aggBufferAttributes)
    val aggregates = boundExpressions.aggModeCudfAggregates.flatMap(_._3)
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
        withResource(keyBatchingIter.next()) { batch =>
          computeAggregate(batch, merge = true, isSorted = true)
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
    val aggTime = metrics.computeAggTime
    withResource(new NvtxWithMetrics("finalize agg", NvtxColor.DARK_GREEN, aggTime)) { _ =>
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
        metrics.numOutputRows += rowCount
        metrics.numOutputBatches += 1
        new ColumnarBatch(resultCvs.toArray, rowCount)
      }
    }
  }

  /** Perform the initial projection on the input batch and extract the result columns */
  private def processIncomingBatch(batch: ColumnarBatch): ColumnarBatch = {
    val aggTime = metrics.computeAggTime
    withResource(new NvtxWithMetrics("prep agg batch", NvtxColor.CYAN, aggTime)) { _ =>
      val cols = boundExpressions.boundInputReferences.safeMap { ref =>
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
      new ColumnarBatch(cols.toArray, cols.head.getRowCount.toInt)
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
    val mergeBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.mergeBufferAttributes)

    val aggModeCudfAggregates =
      AggregateUtils.computeAggModeCudfAggregates(
        aggregateExpressions, aggBufferAttributes, mergeBufferAttributes)

    // boundInputReferences is used to pick out of the input batch the appropriate columns
    // for aggregation.
    //
    // - DistinctAggExpressions with nonDistinctAggExpressions in other mode: we switch the
    //   position of distinctAttributes and nonDistinctAttributes in childAttr. And we use the
    //   inputProjections for nonDistinctAggExpressions.
    // - Final mode, PartialMerge-only mode or no AggExpressions: we pick the columns in the order
    //   as handed to us.
    // - Partial mode or Complete mode: we use the inputProjections.
    val boundInputReferences =
    if (modeInfo.uniqueModes.length > 1 && aggregateExpressions.exists(_.isDistinct)) {
      // This block takes care of AggregateExec which contains nonDistinctAggExpressions and
      // distinctAggExpressions with different AggregateModes. All nonDistinctAggExpressions share
      // one mode and all distinctAggExpressions are in another mode. The specific mode varies in
      // different Spark runtimes, so this block applies a general condition to adapt different
      // runtimes:
      //
      // 1. Apache Spark: The 3rd stage of AggWithOneDistinct
      // The 3rd stage of AggWithOneDistinct, which consists of for nonDistinctAggExpressions in
      // PartialMerge mode and distinctAggExpressions in Partial mode. For this stage, we need to
      // switch the position of distinctAttributes and nonDistinctAttributes if there exists at
      // least one nonDistinctAggExpression. Because the positions of distinctAttributes are ahead
      // of nonDistinctAttributes in the output of previous stage, since distinctAttributes are
      // included in groupExpressions.
      // To be specific, the schema of the 2nd stage's outputs is:
      // (groupingAttributes ++ distinctAttributes) ++ nonDistinctAggBufferAttributes
      // The schema of the 3rd stage's expressions is:
      // groupingAttributes ++ nonDistinctAggExpressions(PartialMerge) ++
      // distinctAggExpressions(Partial)
      //
      // 2. Databricks runtime: The final stage of AggWithOneDistinct
      // Databricks runtime squeezes the 4-stage AggWithOneDistinct into 2 stages. Basically, it
      // combines the 1st and 2nd stage into a "Partial" stage; and it combines the 3nd and 4th
      // stage into a "Merge" stage. Similarly, nonDistinctAggExpressions are ahead of distinct
      // ones in the layout of "Merge" stage's expressions:
      // groupingAttributes ++ nonDistinctAggExpressions(Final) ++ DistinctAggExpressions(Complete)
      // Meanwhile, as Apache Spark, distinctAttributes are ahead of nonDistinctAggBufferAttributes
      // in the output schema of the "Partial" stage.
      // Therefore, this block also works on the final stage of AggWithOneDistinct under Databricks
      // runtime.

      val (distinctAggExpressions, nonDistinctAggExpressions) = aggregateExpressions.partition(
        _.isDistinct)

      // The schema of childAttr: [groupAttr, distinctAttr, nonDistinctAttr].
      // With the size of nonDistinctAttr, we can easily extract distinctAttr and nonDistinctAttr
      // from childAttr.
      val sizeOfNonDistAttr = nonDistinctAggExpressions
          .map(_.aggregateFunction.aggBufferAttributes.length).sum
      val nonDistinctAttributes = childAttr.attrs.takeRight(sizeOfNonDistAttr)
      val distinctAttributes = childAttr.attrs.slice(
        groupingAttributes.length, childAttr.attrs.length - sizeOfNonDistAttr)

      // For nonDistinctExpressions, they are in either PartialMerge or Final modes. With either
      // mode, we just need to pass through childAttr.
      val nonDistinctExpressions = nonDistinctAttributes.asInstanceOf[Seq[Expression]]
      // For nonDistinctExpressions, they are in either Final or Complete modes. With either mode,
      // we need to apply the input projections on these AggExpressions.
      val distinctExpressions = distinctAggExpressions.flatMap(_.aggregateFunction.inputProjection)

      // Align the expressions of input projections and input attributes
      val inputProjections = groupingExpressions ++ nonDistinctExpressions ++ distinctExpressions
      val inputAttributes = groupingAttributes ++ distinctAttributes ++ nonDistinctAttributes
      GpuBindReferences.bindGpuReferences(inputProjections, inputAttributes)
    } else if (modeInfo.hasFinalMode ||
        (modeInfo.hasPartialMergeMode && modeInfo.uniqueModes.length == 1)) {
      // This block takes care of two possible conditions:
      // 1. The Final stage, including the 2nd stage of NoDistinctAgg and 4th stage of
      // AggWithOneDistinct, which needs no input projections. Because the child outputs are
      // internal aggregation buffers, which are aligned for the final stage.
      // 2. The 2nd stage (PartialMerge) of AggWithOneDistinct, which works like the final stage
      // taking the child outputs as inputs without any projections.
      GpuBindReferences.bindGpuReferences(childAttr.attrs.asInstanceOf[Seq[Expression]], childAttr)
    } else if (modeInfo.hasPartialMode || modeInfo.hasCompleteMode ||
        modeInfo.uniqueModes.isEmpty) {
      // The first aggregation stage which contains AggExpressions (in either Partial or Complete
      // mode). In this case, the input projections are essential.
      // To be specific, there are four conditions matching this case:
      // 1. The Partial (1st) stage of NoDistinctAgg
      // 2. The Partial (1st) stage of AggWithOneDistinct
      // 3. In Databricks runtime, the "Final" (2nd) stage of AggWithOneDistinct which only contains
      // DistinctAggExpressions (without any nonDistinctAggExpressions)
      //
      // In addition, this block also fits for aggregation stages without any AggExpressions.
      val inputProjections: Seq[Expression] = groupingExpressions ++ aggregateExpressions
          .flatMap(_.aggregateFunction.inputProjection)
      GpuBindReferences.bindGpuReferences(inputProjections, childAttr)
    } else {
      // This branch should NOT be reached.
      throw new IllegalStateException(
        s"Unable to handle aggregate with modes: ${modeInfo.uniqueModes}")
    }

    val boundFinalProjections = if (modeInfo.hasFinalMode || modeInfo.hasCompleteMode) {
      val finalProjections = groupingExpressions ++
          aggregateExpressions.map(_.aggregateFunction.evaluateExpression)
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
    val boundResultReferences = if (modeInfo.hasPartialMode || modeInfo.hasPartialMergeMode) {
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
   * @param toAggregateBatch input batch to aggregate
   * @param merge true indicates a merge aggregation should be performed
   * @param isSorted true indicates the data is already sorted by the grouping keys
   * @return aggregated batch
   */
  private def computeAggregate(
      toAggregateBatch: ColumnarBatch,
      merge: Boolean,
      isSorted: Boolean = false): ColumnarBatch  = {
    val groupingAttributes = groupingExpressions.map(_.asInstanceOf[NamedExpression].toAttribute)

    val aggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

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
        var dataTypes = new mutable.ArrayBuffer[DataType]()
        val cudfAggregates = new mutable.ArrayBuffer[GroupByAggregationOnColumn]()

        // `GpuAggregateFunction` can add a pre and post step for update
        // and merge aggregates.
        val preStep = new mutable.ArrayBuffer[Expression]()
        val postStep = new mutable.ArrayBuffer[Expression]()
        val postStepAttr = new mutable.ArrayBuffer[Attribute]()

        // we add the grouping expression first, which bind as pass-through
        preStep ++= GpuBindReferences.bindGpuReferences(
          groupingAttributes, groupingAttributes)
        postStep ++= GpuBindReferences.bindGpuReferences(
          groupingAttributes, groupingAttributes)
        postStepAttr ++= groupingAttributes
        dataTypes ++=
          groupingExpressions.map(_.dataType)

        for ((aggExp, mode, aggregates) <- aggModeCudfAggregates) {
          // bind pre-merge to the aggBufferAttributes (input)
          val aggFn = aggExp.aggregateFunction
          if ((mode == Partial || mode == Complete) && ! merge) {
            preStep ++= aggFn.preUpdate
            postStep ++= aggFn.postUpdate
            postStepAttr ++= aggFn.postUpdateAttr
          } else {
            preStep ++= aggFn.preMerge
            postStep ++= aggFn.postMerge
            postStepAttr ++= aggFn.postMergeAttr
          }

          aggregates.map { a =>
            if ((mode == Partial || mode == Complete) && !merge) {
              cudfAggregates += a.updateAggregate
              dataTypes += a.updateDataType
            } else {
              cudfAggregates += a.mergeAggregate
              dataTypes += a.dataType
            }
          }
        }

        // a pre-processing step required before we go into the cuDF aggregate, in some cases
        // casting and in others creating a struct (MERGE_M2 for instance, requires a struct)
        val preStepBound = GpuBindReferences.bindGpuReferences(preStep, aggBufferAttributes)
        withResource(GpuProjectExec.project(toAggregateBatch, preStepBound)) { preProcessed =>
          withResource(GpuColumnVector.from(preProcessed)) { preProcessedTbl =>
            val groupOptions = cudf.GroupByOptions.builder()
              .withIgnoreNullKeys(false)
              .withKeysSorted(isSorted)
              .build()

            // perform the aggregate
            withResource(preProcessedTbl
              .groupBy(groupOptions, groupingExpressions.indices: _*)
              .aggregate(cudfAggregates: _*)) { result =>
              withResource(GpuColumnVector.from(result, dataTypes.toArray)) { resultBatch =>
                // a post-processing step required in some scenarios, casting or picking
                // apart a struct
                val postStepBound = GpuBindReferences.bindGpuReferences(postStep, postStepAttr)
                GpuProjectExec.project(resultBatch, postStepBound)
              }
            }
          }
        }
      } else {
        // Reduction aggregate
        // we ask the appropriate merge or update CudfAggregates, what their
        // reduction merge or update aggregates functions are
        val cvs = mutable.ArrayBuffer[GpuColumnVector]()
        aggModeCudfAggregates.foreach { case (_, mode, aggs) =>
          aggs.foreach { agg =>
            val aggFn = if ((mode == Partial || mode == Complete) && !merge) {
              agg.updateReductionAggregate
            } else {
              agg.mergeReductionAggregate
            }
            withResource(aggFn(GpuColumnVector.extractColumns(toAggregateBatch))) { res =>
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

  val requiredChildDistributionExpressions: Option[Seq[BaseExprMeta[_]]] =
    aggRequiredChildDistributionExpressions.map(_.map(GpuOverrides.wrapExpr(_, conf, Some(this))))
  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    requiredChildDistributionExpressions.getOrElse(Seq.empty) ++
        groupingExpressions ++
        aggregateExpressions ++
        aggregateAttributes ++
        resultExpressions

  override def tagPlanForGpu(): Unit = {
    if (agg.resultExpressions.isEmpty) {
      willNotWorkOnGpu("result expressions is empty")
    }
    // We don't support Arrays and Maps as GroupBy keys yet, even they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val allTypesAreSupported = agg.groupingExpressions.forall(e =>
      !TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType]))
    if (!allTypesAreSupported) {
      willNotWorkOnGpu("ArrayTypes or MayTypes in grouping expressions are not supported")
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

/**
 * Base class for metadata around `SortAggregateExec` and `ObjectHashAggregateExec`, which may
 * contain TypedImperativeAggregate functions in aggregate expressions.
 */
abstract class GpuTypedImperativeSupportedAggregateExecMeta[INPUT <: BaseAggregateExec](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends GpuBaseAggregateMeta[INPUT](plan,
  aggRequiredChildDistributionExpressions, conf, parent, rule) {

  private val mayNeedAggBufferConversion: Boolean =
    agg.aggregateExpressions.exists { expr =>
      expr.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]] &&
          (expr.mode == Partial || expr.mode == PartialMerge)
    }

  // overriding data types of Aggregation Buffers if necessary
  if (mayNeedAggBufferConversion) overrideAggBufTypes()

  override protected lazy val outputTypeMetas: Option[Seq[DataTypeMeta]] =
    if (mayNeedAggBufferConversion) {
      Some(resultExpressions.map(_.typeMeta))
    } else {
      None
    }

  override val availableRuntimeDataTransition: Boolean =
    aggregateExpressions.map(_.childExprs.head).forall {
      case aggMeta: TypedImperativeAggExprMeta[_] => aggMeta.supportBufferConversion
      case _ => true
    }

  override def tagPlanForGpu(): Unit = {
    super.tagPlanForGpu()

    // If a typedImperativeAggregate function run across CPU and GPU (ex: Partial mode on CPU,
    // Merge mode on GPU), it will lead to a runtime crash. Because aggregation buffers produced
    // by the previous stage of function are NOT in the same format as the later stage consuming.
    // If buffer converters are available for all incompatible buffers, we will build these buffer
    // converters and bind them to certain plans, in order to integrate these converters into
    // R2C/C2R Transitions during GpuTransitionOverrides to fix the gap between GPU and CPU.
    // Otherwise, we have to fall back all Aggregate stages to CPU once any of them did fallback.
    //
    // The binding also works when AQE is on, since it leverages the TreeNodeTag to cache buffer
    // converters.
    GpuTypedImperativeSupportedAggregateExecMeta.handleAggregationBuffer(this)
  }

  override def convertToGpu(): GpuExec = {
    if (mayNeedAggBufferConversion) {
      // transforms the data types of aggregate attributes with typeMeta
      val aggAttributes = aggregateAttributes.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      // transforms the data types of result expressions with typeMeta
      val retExpressions = resultExpressions.map {
        case meta if meta.typeMeta.typeConverted =>
          val ref = meta.wrapped.asInstanceOf[AttributeReference]
          val converted = ref.copy(
            dataType = meta.typeMeta.dataType.get)(ref.exprId, ref.qualifier)
          GpuOverrides.wrapExpr(converted, conf, Some(this))
        case meta => meta
      }
      GpuHashAggregateExec(
        requiredChildDistributionExpressions.map(_.map(_.convertToGpu())),
        groupingExpressions.map(_.convertToGpu()),
        aggregateExpressions.map(_.convertToGpu()).asInstanceOf[Seq[GpuAggregateExpression]],
        aggAttributes.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
        retExpressions.map(_.convertToGpu()).asInstanceOf[Seq[NamedExpression]],
        childPlans.head.convertIfNeeded(),
        conf.gpuTargetBatchSizeBytes)
    } else {
      super.convertToGpu()
    }
  }

  /**
   * The method replaces data types of aggregation buffers created by TypedImperativeAggregate
   * functions with the actual data types used in the GPU runtime.
   *
   * Firstly, this method traverses aggregateFunctions, to search attributes referring to
   * aggregation buffers of TypedImperativeAggregate functions.
   * Then, we extract the desired (actual) data types on GPU runtime for these attributes,
   * and map them to expression IDs of attributes.
   * At last, we traverse aggregateAttributes and resultExpressions, overriding data type in
   * RapidsMeta if necessary, in order to ensure TypeChecks tagging exact data types in runtime.
   */
  private def overrideAggBufTypes(): Unit = {
    val desiredAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    val desiredInputAggBufTypes = mutable.HashMap.empty[ExprId, DataType]
    // Collects exprId from TypedImperativeAggBufferAttributes, and maps them to the data type
    // of `TypedImperativeAggExprMeta.aggBufferAttribute`.
    aggregateExpressions.map(_.childExprs.head).foreach {
      case aggMeta: TypedImperativeAggExprMeta[_] =>
        val aggFn = aggMeta.wrapped.asInstanceOf[TypedImperativeAggregate[_]]
        val desiredType = aggMeta.aggBufferAttribute.dataType

        desiredAggBufTypes(aggFn.aggBufferAttributes.head.exprId) = desiredType
        desiredInputAggBufTypes(aggFn.inputAggBufferAttributes.head.exprId) = desiredType

      case _ =>
    }

    // Overrides the data types of typed imperative aggregation buffers for type checking
    aggregateAttributes.foreach { attrMeta =>
      attrMeta.wrapped match {
        case ar: AttributeReference if desiredAggBufTypes.contains(ar.exprId) =>
          attrMeta.overrideDataType(desiredAggBufTypes(ar.exprId))
        case _ =>
      }
    }
    resultExpressions.foreach { retMeta =>
      retMeta.wrapped match {
        case ar: AttributeReference if desiredInputAggBufTypes.contains(ar.exprId) =>
          retMeta.overrideDataType(desiredInputAggBufTypes(ar.exprId))
        case _ =>
      }
    }
  }
}

object GpuTypedImperativeSupportedAggregateExecMeta {

  private val bufferConverterInjected = TreeNodeTag[Boolean](
    "rapids.gpu.bufferConverterInjected")

  /**
   * The method will bind buffer converters (CPU Expressions) to certain CPU Plans if necessary,
   * so the method returns nothing. The binding work will help us to insert these converters as
   * pre/post processing of GpuRowToColumnarExec/GpuColumnarToRowExec during the materialization
   * of these transition Plans.
   *
   * In the beginning, it collects all physical Aggregate Plans (stages) of current Aggregate.
   * Then, it goes through all stages in pair, to check whether buffer converters need to be
   * inserted between its child and itself.
   * If it is necessary, it goes on to check whether all these buffers are available to convert.
   * If not, it falls back all stages to CPU to keep consistency of intermediate data; Otherwise,
   * it creates buffer converters based on the createBufferConverter methods of
   * TypedImperativeAggExprMeta, and binds these newly-created converters to certain Plans.
   *
   * In terms of binding, it is critical and sophisticated to find out where to bind these
   * converters. Generally, we need to find out CPU plans right before/after the potential R2C/C2R
   * transitions, so as to integrate these converters during post columnar overriding. These plans
   * may be Aggregate stages themselves. They may also be plans inserted between Aggregate stages,
   * such as: ShuffleExchangeExec, SortExec.
   *
   * The binding carries out by storing buffer converters as tag values of certain CPU Plans. These
   * plans are either the child of GpuRowToColumnarExec or the parent of GpuColumnarToRowExec. The
   * converters are cached inside CPU Plans rather than GPU ones (like: the child of
   * GpuColumnarToRowExec), because we can't access the GPU Plans during binding since they are
   * yet created. And GPU Plans created by RapidsMeta don't keep the tags of their CPU
   * counterparts.
   */
  private def handleAggregationBuffer(
      meta: GpuTypedImperativeSupportedAggregateExecMeta[_]): Unit = {
    // We only run the check for final stages which contain TypedImperativeAggregate.
    val needToCheck = containTypedImperativeAggregate(meta, Some(Final))
    if (!needToCheck) return
    // Avoid duplicated check and fallback.
    val checked = meta.agg.getTagValue[Boolean](bufferConverterInjected).contains(true)
    if (checked) return
    meta.agg.setTagValue(bufferConverterInjected, true)

    // Fetch AggregateMetas of all stages which belong to current Aggregate
    val stages = getAggregateOfAllStages(meta, meta.agg.logicalLink.get)

    // Find out stages in which the buffer converters are essential.
    val needBufferConversion = stages.indices.map {
      case i if i == stages.length - 1 => false
      case i =>
        // Buffer converters are only needed to inject between two stages if [A] and [B].
        // [A]. there will be a R2C or C2R transition between them
        // [B]. there exists TypedImperativeAggregate functions in each of them
        (stages(i).canThisBeReplaced ^ stages(i + 1).canThisBeReplaced) &&
            containTypedImperativeAggregate(stages(i)) &&
            containTypedImperativeAggregate(stages(i + 1))
    }

    // Return if all internal aggregation buffers are compatible with GPU Overrides.
    if (needBufferConversion.forall(!_)) return

    // Fall back all GPU supported stages to CPU, if there exists TypedImperativeAggregate buffer
    // who doesn't support data format transition in runtime. Otherwise, build buffer converters
    // and bind them to certain SparkPlans.
    val entireFallback = !stages.zip(needBufferConversion).forall { case (stage, needConvert) =>
      !needConvert || stage.availableRuntimeDataTransition
    }
    if (entireFallback) {
      stages.foreach {
        case aggMeta if aggMeta.canThisBeReplaced =>
          aggMeta.willNotWorkOnGpu("Associated fallback for TypedImperativeAggregate")
        case _ =>
      }
    } else {
      bindBufferConverters(stages, needBufferConversion)
    }
  }

  /**
   * Bind converters as TreeNodeTags into the CPU plans who are right before/after the potential
   * R2C/C2R transitions (the transitions are yet inserted).
   *
   * These converters are CPU expressions, and they will be integrated into GpuRowToColumnarExec/
   * GpuColumnarToRowExec as pre/post transitions. What we do here, is to create these converters
   * according to the stage pairs.
   */
  private def bindBufferConverters(stages: Seq[GpuBaseAggregateMeta[_]],
      needBufferConversion: Seq[Boolean]): Unit = {

    needBufferConversion.zipWithIndex.foreach {
      case (needConvert, i) if needConvert =>
        // Find the next edge from the given stage which is a splitting point between CPU plans
        // and GPU plans. The method returns a pair of plans around the edge. For instance,
        //
        // stage(i): GpuObjectHashAggregateExec ->
        //    pair_head: GpuShuffleExec ->
        //        pair_tail and stage(i + 1): ObjectHashAggregateExec
        //
        // In example above, stage(i) is a GpuPlan while stage(i + 1) is not. And we assume
        // both of them including TypedImperativeAgg. So, in terms of stage(i) the buffer
        // conversion is necessary. Then, we call the method nextEdgeForConversion to find
        // the edge next to stage(i), which is between GpuShuffleExec and stage(i + 1).
        nextEdgeForConversion(stages(i)) match {
          // create preRowToColumnarTransition, and bind it to the child node (CPU plan) of
          // GpuRowToColumnarExec
          case List(parent, child) if parent.canThisBeReplaced =>
            val childPlan = child.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1), true)
            childPlan.setTagValue(GpuOverrides.preRowToColProjection, expressions)
          // create postColumnarToRowTransition, and bind it to the parent node (CPU plan) of
          // GpuColumnarToRowExec
          case List(parent, _) =>
            val parentPlan = parent.wrapped.asInstanceOf[SparkPlan]
            val expressions = createBufferConverter(stages(i), stages(i + 1), false)
            parentPlan.setTagValue(GpuOverrides.postColToRowProjection, expressions)
        }
      case _ =>
    }
  }

  private def containTypedImperativeAggregate(meta: GpuBaseAggregateMeta[_],
      desiredMode: Option[AggregateMode] = None): Boolean =
    meta.agg.aggregateExpressions.exists {
      case e: AggregateExpression if desiredMode.forall(_ == e.mode) =>
        e.aggregateFunction.isInstanceOf[TypedImperativeAggregate[_]]
      case _ => false
    }

  private def createBufferConverter(mergeAggMeta: GpuBaseAggregateMeta[_],
      partialAggMeta: GpuBaseAggregateMeta[_],
      fromCpuToGpu: Boolean): Seq[NamedExpression] = {

    val converters = mutable.Queue[Either[
        CpuToGpuAggregateBufferConverter, GpuToCpuAggregateBufferConverter]]()
    mergeAggMeta.childExprs.foreach {
      case e if e.childExprs.length == 1 &&
          e.childExprs.head.isInstanceOf[TypedImperativeAggExprMeta[_]] =>
        e.wrapped.asInstanceOf[AggregateExpression].mode match {
          case Final | PartialMerge =>
            val typImpAggMeta = e.childExprs.head.asInstanceOf[TypedImperativeAggExprMeta[_]]
            val converter = if (fromCpuToGpu) {
              Left(typImpAggMeta.createCpuToGpuBufferConverter())
            } else {
              Right(typImpAggMeta.createGpuToCpuBufferConverter())
            }
            converters.enqueue(converter)
          case _ =>
        }
      case _ =>
    }

    val expressions = partialAggMeta.resultExpressions.map {
      case retExpr if retExpr.typeMeta.typeConverted =>
        val resultExpr = retExpr.wrapped.asInstanceOf[AttributeReference]
        val ref = if (fromCpuToGpu) {
          resultExpr
        } else {
          resultExpr.copy(dataType = retExpr.typeMeta.dataType.get)(
            resultExpr.exprId, resultExpr.qualifier)
        }
        converters.dequeue() match {
          case Left(converter) =>
            ShimLoader.getSparkShims.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
          case Right(converter) =>
            ShimLoader.getSparkShims.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
        }
      case retExpr =>
        retExpr.wrapped.asInstanceOf[NamedExpression]
    }

    expressions
  }

  private def getAggregateOfAllStages(
      currentMeta: SparkPlanMeta[_], logical: LogicalPlan): List[GpuBaseAggregateMeta[_]] = {
    currentMeta match {
      case aggMeta: GpuBaseAggregateMeta[_] if aggMeta.agg.logicalLink.contains(logical) =>
        List[GpuBaseAggregateMeta[_]](aggMeta) ++
            getAggregateOfAllStages(aggMeta.childPlans.head, logical)
      case shuffleMeta: GpuShuffleMeta =>
        getAggregateOfAllStages(shuffleMeta.childPlans.head, logical)
      case sortMeta: GpuSortMeta =>
        getAggregateOfAllStages(sortMeta.childPlans.head, logical)
      case _ =>
        List[GpuBaseAggregateMeta[_]]()
    }
  }

  @tailrec
  private def nextEdgeForConversion(meta: SparkPlanMeta[_]): Seq[SparkPlanMeta[_]] = {
    val child = meta.childPlans.head
    if (meta.canThisBeReplaced ^ child.canThisBeReplaced) {
      List(meta, child)
    } else {
      nextEdgeForConversion(child)
    }
  }
}

class GpuHashAggregateMeta(
    override val agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBaseAggregateMeta(agg, agg.requiredChildDistributionExpressions,
      conf, parent, rule)

class GpuSortAggregateExecMeta(
    override val agg: SortAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
      agg.requiredChildDistributionExpressions, conf, parent, rule) {
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

class GpuObjectHashAggregateExecMeta(
    override val agg: ObjectHashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
      agg.requiredChildDistributionExpressions, conf, parent, rule)

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
    configuredTargetBatchSize: Long) extends ShimUnaryExecNode with GpuExec with Arm {
  private lazy val uniqueModes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_TASKS_FALL_BACKED -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_TASKS_FALL_BACKED),
    AGG_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_AGG_TIME),
    CONCAT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_CONCAT_TIME),
    SORT_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_SORT_TIME)
  ) ++ spillMetrics

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
      numTasksFallBacked = gpuLongMetric(NUM_TASKS_FALL_BACKED),
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
    val modeInfo = AggregateModeInfo(uniqueModes)

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
