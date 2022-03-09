/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{AggregationTagging, ShimUnaryExecNode, SparkShimImpl}

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
import org.apache.spark.sql.types.{ArrayType, DataType, MapType}
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
}

/** Utility class to hold all of the metrics related to hash aggregation */
case class GpuHashAggregateMetrics(
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    numTasksFallBacked: GpuMetric,
    opTime: GpuMetric,
    computeAggTime: GpuMetric,
    concatTime: GpuMetric,
    sortTime: GpuMetric,
    semWaitTime: GpuMetric,
    spillCallback: SpillCallback)

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
 * @param cbIter iterator providing the input columnar batches
 * @param inputAttributes input attributes to identify the input columns from the input batches
 * @param groupingExpressions expressions used for producing the grouping keys
 * @param aggregateExpressions GPU aggregate expressions used to produce the aggregations
 * @param aggregateAttributes attribute references to each aggregate expression
 * @param resultExpressions output expression for the aggregation
 * @param modeInfo identifies which aggregation modes are being used
 * @param metrics metrics that will be updated during aggregation
 * @param configuredTargetBatchSize user-specified value for the targeted input batch size
 */
class GpuHashAggregateIterator(
    cbIter: Iterator[ColumnarBatch],
    inputAttributes: Seq[Attribute],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    modeInfo: AggregateModeInfo,
    metrics: GpuHashAggregateMetrics,
    configuredTargetBatchSize: Long)
    extends Iterator[ColumnarBatch] with Arm with AutoCloseable with Logging {

  // Partial mode:
  //  1. boundInputReferences: picks column from raw input
  //  2. boundFinalProjections: is a pass-through of the agg buffer
  //  3. boundResultReferences: is a pass-through of the merged aggregate
  //
  // Final mode:
  //  1. boundInputReferences: is a pass-through of the merged aggregate
  //  2. boundFinalProjections: on merged batches, finalize aggregates
  //     (GpuAverage => CudfSum/CudfCount)
  //  3. boundResultReferences: project the result expressions Spark expects in the output.
  //
  // Complete mode:
  //  1. boundInputReferences: picks column from raw input
  //  2. boundFinalProjections: on merged batches, finalize aggregates
  //     (GpuAverage => CudfSum/CudfCount)
  //  3. boundResultReferences: project the result expressions Spark expects in the output.
  private case class BoundExpressionsModeAggregates(
      boundFinalProjections: Option[Seq[GpuExpression]],
      boundResultReferences: Seq[Expression])

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private[this] val isReductionOnly = groupingExpressions.isEmpty
  private[this] val boundExpressions = setupReferences()
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
    val mergedTypes = groupingExpressions.map(_.dataType) ++ aggregateExpressions.map(_.dataType)
    AggregateUtils.computeTargetBatchSize(confTargetSize, mergedTypes, mergedTypes,isReductionOnly)
  }

  /** Aggregate all input batches and place the results in the aggregatedBatches queue. */
  private def aggregateInputBatches(): Unit = {
    val aggHelper = new AggHelper(forceMerge = false)
    while (cbIter.hasNext) {
      withResource(cbIter.next()) { childBatch =>
        val isLastInputBatch = GpuColumnVector.isTaggedAsFinalBatch(childBatch)
        withResource(computeAggregate(childBatch, aggHelper)) { aggBatch =>
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
      val opTime = metrics.opTime
      withResource(new NvtxWithMetrics("agg merge pass", NvtxColor.BLUE, concatTime,
        opTime)) { _ =>
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

  private lazy val concatAndMergeHelper = new AggHelper(forceMerge = true)

  /**
   * Concatenate batches together and perform a merge aggregation on the result. The input batches
   * will be closed as part of this operation.
   * @param batches batches to concatenate and merge aggregate
   * @return lazy spillable batch which has NOT been marked spillable
   */
  private def concatenateAndMerge(
      batches: mutable.ArrayBuffer[LazySpillableColumnarBatch]): LazySpillableColumnarBatch = {
    withResource(batches) { _ =>
      withResource(concatenateBatches(batches)) { concatBatch =>
        withResource(computeAggregate(concatBatch, concatAndMergeHelper)) { mergedBatch =>
          LazySpillableColumnarBatch(mergedBatch, metrics.spillCallback, "agg merged batch")
        }
      }
    }
  }

  /** Build an iterator that uses a sort-based approach to merge aggregated batches together. */
  private def buildSortFallbackIterator(): Iterator[ColumnarBatch] = {
    logInfo(s"Falling back to sort-based aggregation with ${aggregatedBatches.size()} batches")
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

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val ordering = groupingAttributes.map(SparkShimImpl.sortOrder(_, Ascending, NullsFirst))
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
    val sorter = new GpuSorter(ordering, aggBufferAttributes)
    val aggBatchTypes = aggBufferAttributes.map(_.dataType)

    // Use the out of core sort iterator to sort the batches by grouping key
    outOfCoreIter = Some(GpuOutOfCoreSortIterator(
      aggregatedBatchIter,
      sorter,
      LazilyGeneratedOrdering.forSchema(TrampolineUtil.fromAttributes(groupingAttributes)),
      configuredTargetBatchSize,
      opTime = metrics.opTime,
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
      concatTime = metrics.concatTime,
      opTime = metrics.opTime,
      peakDevMemory = NoopMetric,
      spillCallback = metrics.spillCallback)

    // Finally wrap the key batching iterator with a merge aggregation on the output batches.
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = keyBatchingIter.hasNext

      private val mergeSortedHelper = new AggHelper(true, isSorted = true)

      override def next(): ColumnarBatch = {
        // batches coming out of the sort need to be merged
        withResource(keyBatchingIter.next()) { batch =>
          computeAggregate(batch, mergeSortedHelper)
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
    // We have to grab the semaphore in this scenario, since this is a reduction that produces
    // rows on the GPU out of empty input, meaning that if a batch has 0 rows, a new single
    // row is getting created with 0 as the count (if count is the operation), and other default
    // values.
    GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics.semWaitTime)
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
    val opTime = metrics.opTime
    withResource(new NvtxWithMetrics("finalize agg", NvtxColor.DARK_GREEN, aggTime,
      opTime)) { _ =>
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

      // If `resultCvs` empty, it means we don't have any `resultExpressions` for this
      // aggregate. In these cases, the row count is the only important piece of information
      // that this aggregate exec needs to report up, so it will return batches that have no columns
      // but that do have a row count. If `resultCvs` is non-empty, the row counts match
      // `finalBatch.numRows` since `columnarEvalToColumn` cannot change the number of rows.
      val finalNumRows = finalBatch.numRows()

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
        metrics.numOutputRows += finalNumRows
        metrics.numOutputBatches += 1
        new ColumnarBatch(resultCvs.toArray, finalNumRows)
      }
    }
  }

  /**
   * Concatenates batches after extracting them from `LazySpillableColumnarBatch`
   * @note the input batches are not closed as part of this operation
   * @param spillableBatchesToConcat lazy spillable batches to concatenate
   * @return concatenated batch result
   */
  private def concatenateBatches(
      spillableBatchesToConcat: mutable.ArrayBuffer[LazySpillableColumnarBatch]): ColumnarBatch = {
    val concatTime = metrics.concatTime
    val opTime = metrics.opTime
    withResource(new NvtxWithMetrics("concatenateBatches", NvtxColor.BLUE, concatTime,
      opTime)) { _ =>
      val batchesToConcat = spillableBatchesToConcat.map(_.getBatch)
      val numCols = batchesToConcat.head.numCols()
      val dataTypes = (0 until numCols).map {
        c => batchesToConcat.head.column(c).dataType
      }.toArray
      withResource(batchesToConcat.map(GpuColumnVector.from)) { tbl =>
        withResource(cudf.Table.concatenate(tbl: _*)) { concatenated =>
          GpuColumnVector.from(concatenated, dataTypes)
        }
      }
    }
  }

  /**
   * `setupReferences` binds input, final and result references for the aggregate.
   * - input: used to obtain columns coming into the aggregate from the child
   * - final: some aggregates like average use this to specify an expression to produce
   *          the final output of the aggregate. Average keeps sum and count throughout,
   *          and at the end it has to divide the two, to produce the single sum/count result.
   * - result: used at the end to output to our parent
   */
  private def setupReferences(): BoundExpressionsModeAggregates = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val aggBufferAttributes = groupingAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

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
    BoundExpressionsModeAggregates(
      boundFinalProjections,
      boundResultReferences)
  }

  /**
   * Internal class used in `computeAggregates` for the pre, agg, and post steps
   *
   * @param forceMerge - if true, we are merging two pre-aggregated batches, so we should use
   *                the merge steps for each aggregate function
   * @param isSorted - if the batch is sorted this is set to true and is passed to cuDF
   *                   as an optimization hint
   */
  class AggHelper(forceMerge: Boolean, isSorted: Boolean = false) {
    // `CudfAggregate` instances to apply, either update or merge aggregates
    private val cudfAggregates = new mutable.ArrayBuffer[CudfAggregate]()

    // integers for each column the aggregate is operating on
    private val aggOrdinals = new mutable.ArrayBuffer[Int]

    // the resulting data type from the cuDF aggregate (from
    // the update or merge aggregate, be it reduction or group by)
    private val postStepDataTypes = new mutable.ArrayBuffer[DataType]()

    private val groupingAttributes = groupingExpressions.map(_.toAttribute)
    private val aggBufferAttributes = groupingAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

    // `GpuAggregateFunction` can add a pre and post step for update
    // and merge aggregates.
    private val preStep = new mutable.ArrayBuffer[Expression]()
    private val postStep = new mutable.ArrayBuffer[Expression]()
    private val postStepAttr = new mutable.ArrayBuffer[Attribute]()

    // we add the grouping expression first, which should bind as pass-through
    if (forceMerge) {
      // a grouping expression can do actual computation, but we cannot do that computation again
      // on a merge, nor would we want to if we could. So use the attributes instead of the
      // original expression when we are forcing a merge.
      preStep ++= groupingAttributes
    } else {
      preStep ++= groupingExpressions
    }
    postStep ++= groupingAttributes
    postStepAttr ++= groupingAttributes
    postStepDataTypes ++=
      groupingExpressions.map(_.dataType)

    private var ix = groupingAttributes.length
    for (aggExp <- aggregateExpressions) {
      val aggFn = aggExp.aggregateFunction
      if ((aggExp.mode == Partial || aggExp.mode == Complete) && !forceMerge) {
        val ordinals = (ix until ix + aggFn.updateAggregates.length)
        aggOrdinals ++= ordinals
        ix += ordinals.length
        val updateAggs = aggFn.updateAggregates
        postStepDataTypes ++= updateAggs.map(_.dataType)
        cudfAggregates ++= updateAggs
        preStep ++= aggFn.inputProjection
        postStep ++= aggFn.postUpdate
        postStepAttr ++= aggFn.postUpdateAttr
      } else {
        val ordinals = (ix until ix + aggFn.mergeAggregates.length)
        aggOrdinals ++= ordinals
        ix += ordinals.length
        val mergeAggs = aggFn.mergeAggregates
        postStepDataTypes ++= mergeAggs.map(_.dataType)
        cudfAggregates ++= mergeAggs
        preStep ++= aggFn.preMerge
        postStep ++= aggFn.postMerge
        postStepAttr ++= aggFn.postMergeAttr
      }
    }

    // a bound expression that is applied before the cuDF aggregate
    private val preStepBound = if (forceMerge) {
      GpuBindReferences.bindGpuReferences(preStep.toList, aggBufferAttributes.toList)
    } else {
      GpuBindReferences.bindGpuReferences(preStep, inputAttributes)
    }

    // a bound expression that is applied after the cuDF aggregate
    private val postStepBound =
      GpuBindReferences.bindGpuReferences(postStep, postStepAttr)

    /**
     * Apply the "pre" step: preMerge for merge, or pass-through in the update case
     * @param toAggregateBatch - input (to the agg) batch from the child directly in the
     *                         merge case, or from the `inputProjection` in the update case.
     * @return a pre-processed batch that can be later cuDF aggregated
     */
    def preProcess(toAggregateBatch: ColumnarBatch): ColumnarBatch = {
      GpuProjectExec.project(toAggregateBatch, preStepBound)
    }

    /**
     * Invoke reduction functions as defined in each `CudfAggreagte`
     * @param preProcessed - a batch after the "pre" step
     * @return
     */
    def performReduction(preProcessed: ColumnarBatch): ColumnarBatch = {
      val cvs = mutable.ArrayBuffer[GpuColumnVector]()
      cudfAggregates.zipWithIndex.foreach { case (cudfAgg, ix) =>
        val aggFn = cudfAgg.reductionAggregate
        val cols = GpuColumnVector.extractColumns(preProcessed)
        val reductionCol = cols(aggOrdinals(ix))
        withResource(aggFn(reductionCol.getBase)) { res =>
          cvs += GpuColumnVector.from(
            cudf.ColumnVector.fromScalar(res, 1), cudfAgg.dataType)
        }
      }
      new ColumnarBatch(cvs.toArray, 1)
    }

    /**
     * Used to produce a group-by aggregate
     * @param preProcessed the batch after the "pre" step
     * @return a Table that has been cuDF aggregated
     */
    def performGroupByAggregation(preProcessed: ColumnarBatch): ColumnarBatch = {
      withResource(GpuColumnVector.from(preProcessed)) { preProcessedTbl =>
        val groupOptions = cudf.GroupByOptions.builder()
          .withIgnoreNullKeys(false)
          .withKeysSorted(isSorted)
          .build()

        val cudfAggsOnColumn = cudfAggregates.zip(aggOrdinals).map {
          case (cudfAgg, ord) => cudfAgg.groupByAggregate.onColumn(ord)
        }

        // perform the aggregate
        val aggTbl = preProcessedTbl
          .groupBy(groupOptions, groupingExpressions.indices: _*)
          .aggregate(cudfAggsOnColumn: _*)

        withResource(aggTbl) { _ =>
          GpuColumnVector.from(aggTbl, postStepDataTypes.toArray)
        }
      }
    }

    /**
     * Used to produce the outbound batch from the aggregate that could be
     * shuffled or could be passed through the evaluateExpression if we are in the final
     * stage.
     * It takes a cuDF aggregated batch and applies the "post" step:
     * postUpdate for update, or postMerge for merge
     * @param resultBatch - cuDF aggregated batch
     * @return output batch from the aggregate
     */
    def postProcess(resultBatch: ColumnarBatch): ColumnarBatch = {
      withResource(resultBatch) { _ =>
        GpuProjectExec.project(resultBatch, postStepBound)
      }
    }
  }

  /**
   * Compute the aggregations on the projected input columns.
   * @param toAggregateBatch input batch to aggregate
   * @param helper an internal object that carries state required to execute computeAggregate from
   *               different parts of the codebase.
   * @return aggregated batch
   */
  private def computeAggregate(
      toAggregateBatch: ColumnarBatch, helper: AggHelper): ColumnarBatch  = {
    val computeAggTime = metrics.computeAggTime
    val opTime = metrics.opTime
    withResource(new NvtxWithMetrics("computeAggregate", NvtxColor.CYAN, computeAggTime,
      opTime)) { _ =>
      // a pre-processing step required before we go into the cuDF aggregate, in some cases
      // casting and in others creating a struct (MERGE_M2 for instance, requires a struct)
      withResource(helper.preProcess(toAggregateBatch)) { preProcessed =>
        val resultBatch = if (groupingExpressions.nonEmpty) {
          helper.performGroupByAggregation(preProcessed)
        } else {
          helper.performReduction(preProcessed)
        }

        // a post-processing step required in some scenarios, casting or picking
        // apart a struct
        helper.postProcess(resultBatch)
      }
    }
  }
}

object GpuBaseAggregateMeta {
  private val aggPairReplaceChecked = TreeNodeTag[Boolean](
    "rapids.gpu.aggPairReplaceChecked")

  def getAggregateOfAllStages(
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
}

abstract class GpuBaseAggregateMeta[INPUT <: SparkPlan](
    plan: INPUT,
    aggRequiredChildDistributionExpressions: Option[Seq[Expression]],
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[INPUT](plan, conf, parent, rule) {

  val agg: BaseAggregateExec

  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def tagPlanForGpu(): Unit = {
    // We don't support Arrays and Maps as GroupBy keys yet, even they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val arrayOrMapGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType]))
    if (arrayOrMapGroupings) {
      willNotWorkOnGpu("ArrayTypes or MapTypes in grouping expressions are not supported")
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

    if (AggregationTagging.mustReplaceBoth) {
      tagForMixedReplacement()
    }
  }

  /**
   * Tagging checks tied to configs that control the aggregation modes that are replaced.
   *
   * The rule of replacement is determined by `spark.rapids.sql.hashAgg.replaceMode`, which
   * is a string configuration consisting of AggregateMode names in lower cases connected by
   * &(AND) and |(OR). The default value of this config is `all`, which indicates replacing all
   * aggregates if possible.
   *
   * The `|` serves as the outer connector, which represents patterns of both sides are able to be
   * replaced. For instance, `final|partialMerge` indicates that aggregate plans purely in either
   * Final mode or PartialMerge mode can be replaced. But aggregate plans also contain
   * AggExpressions of other mode will NOT be replaced, such as: stage 3 of single distinct
   * aggregate who contains both Partial and PartialMerge.
   *
   * On the contrary, the `&` serves as the inner connector, which intersects modes of both sides
   * to form a mode pattern. The replacement only takes place for aggregate plans who have the
   * exact same mode pattern as what defined the rule. For instance, `partial&partialMerge` means
   * that aggregate plans can be only replaced if they contain AggExpressions of Partial and
   * contain AggExpressions of PartialMerge and don't contain AggExpressions of other modes.
   *
   * In practice, we need to combine `|` and `&` to form some sophisticated patterns. For instance,
   * `final&complete|final|partialMerge` represents aggregate plans in three different patterns are
   * GPU-replaceable: plans contain both Final and Complete modes; plans only contain Final mode;
   * plans only contain PartialMerge mode.
   */
  private def tagForReplaceMode(): Unit = {
    val aggPattern = agg.aggregateExpressions.map(_.mode).toSet
    val strPatternToReplace = conf.hashAggReplaceMode.toLowerCase

    if (aggPattern.nonEmpty && strPatternToReplace != "all") {
      val aggPatternsCanReplace = strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }
      if (!aggPatternsCanReplace.contains(aggPattern)) {
        val message = aggPattern.map(_.toString).mkString(",")
        willNotWorkOnGpu(s"Replacing mode pattern `$message` hash aggregates disabled")
      } else if (aggPattern == Set(Partial)) {
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
      }
    }

    if (!conf.partialMergeDistinctEnabled && aggPattern.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
          s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }

  /** Prevent mixing of CPU and GPU aggregations */
  private def tagForMixedReplacement(): Unit = {
    // only run the check for final stages that have not already been checked
    val haveChecked =
      agg.getTagValue[Boolean](GpuBaseAggregateMeta.aggPairReplaceChecked).contains(true)
    val needCheck = !haveChecked && agg.aggregateExpressions.exists {
      case e: AggregateExpression if e.mode == Final => true
      case _ => false
    }

    if (needCheck) {
      agg.setTagValue(GpuBaseAggregateMeta.aggPairReplaceChecked, true)
      val stages = GpuBaseAggregateMeta.getAggregateOfAllStages(this, agg.logicalLink.get)
      // check if aggregations will mix CPU and GPU across stages
      val hasMixedAggs = stages.indices.exists {
        case i if i == stages.length - 1 => false
        case i => stages(i).canThisBeReplaced ^ stages(i + 1).canThisBeReplaced
      }
      if (hasMixedAggs) {
        stages.foreach {
          case aggMeta if aggMeta.canThisBeReplaced =>
            aggMeta.willNotWorkOnGpu("mixing CPU and GPU aggregations is not supported")
          case _ =>
        }
      }
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuHashAggregateExec(
      aggRequiredChildDistributionExpressions,
      groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
      aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression]),
      aggregateAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
      resultExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
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
        aggRequiredChildDistributionExpressions,
        groupingExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
        aggregateExpressions.map(_.convertToGpu().asInstanceOf[GpuAggregateExpression]),
        aggAttributes.map(_.convertToGpu().asInstanceOf[Attribute]),
        retExpressions.map(_.convertToGpu().asInstanceOf[NamedExpression]),
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
    val stages = GpuBaseAggregateMeta.getAggregateOfAllStages(meta, meta.agg.logicalLink.get)

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
            SparkShimImpl.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
          case Right(converter) =>
            SparkShimImpl.alias(converter.createExpression(ref),
              ref.name + "_converted")(NamedExpression.newExprId)
        }
      case retExpr =>
        retExpr.wrapped.asInstanceOf[NamedExpression]
    }

    expressions
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
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[GpuAggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan,
    configuredTargetBatchSize: Long) extends ShimUnaryExecNode with GpuExec with Arm {

  // lifted directly from `BaseAggregateExec.inputAttributes`, edited comment.
  def inputAttributes: Seq[Attribute] = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(PartialMerge)) {
      // SPARK-31620: when planning aggregates, the partial aggregate uses aggregate function's
      // `inputAggBufferAttributes` as its output. And Final and PartialMerge aggregate rely on the
      // output to bind references used by `mergeAggregates`. But if we copy the
      // aggregate function somehow after aggregate planning, the `DeclarativeAggregate` will
      // be replaced by a new instance with new `inputAggBufferAttributes`. Then Final and
      // PartialMerge aggregate can't bind the references used by `mergeAggregates` with the output
      // of the partial aggregate, as they use the `inputAggBufferAttributes` of the
      // original `DeclarativeAggregate` before copy. Instead, we shall use
      // `inputAggBufferAttributes` after copy to match the new `mergeExpressions`.
      val aggAttrs = inputAggBufferAttributes
      child.output.dropRight(aggAttrs.length) ++ aggAttrs
    } else {
      child.output
    }
  }

  private val inputAggBufferAttributes: Seq[Attribute] = {
    aggregateExpressions
        // there're exactly four cases needs `inputAggBufferAttributes` from child according to the
        // agg planning in `AggUtils`: Partial -> Final, PartialMerge -> Final,
        // Partial -> PartialMerge, PartialMerge -> PartialMerge.
        .filter(a => a.mode == Final || a.mode == PartialMerge)
        .flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  private lazy val uniqueModes: Seq[AggregateMode] = aggregateExpressions.map(_.mode).distinct

  protected override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  protected override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    NUM_TASKS_FALL_BACKED -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_TASKS_FALL_BACKED),
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    AGG_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_AGG_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    SORT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SORT_TIME)
  ) ++ spillMetrics

  // requiredChildDistributions are CPU expressions, so remove it from the GPU expressions list
  override def gpuExpressions: Seq[Expression] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

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
      opTime = gpuLongMetric(OP_TIME),
      computeAggTime = gpuLongMetric(AGG_TIME),
      concatTime = gpuLongMetric(CONCAT_TIME),
      sortTime = gpuLongMetric(SORT_TIME),
      semWaitTime = gpuLongMetric(SEMAPHORE_WAIT_TIME),
      makeSpillCallback(allMetrics))

    // cache in a local variable to avoid serializing the full child plan
    val groupingExprs = groupingExpressions
    val aggregateExprs = aggregateExpressions
    val aggregateAttrs = aggregateAttributes
    val resultExprs = resultExpressions
    val modeInfo = AggregateModeInfo(uniqueModes)

    val rdd = child.executeColumnar()

    rdd.mapPartitions { cbIter =>
      new GpuHashAggregateIterator(
        cbIter,
        inputAttributes,
        groupingExprs,
        aggregateExprs,
        aggregateAttrs,
        resultExprs,
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
        aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)

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
