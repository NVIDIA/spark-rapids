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

package com.nvidia.spark.rapids.window

import com.nvidia.spark.rapids.{GpuBindReferences, GpuExpression, GpuMetric, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch


// It is not really simple to do a single iterator that can do the splits and retries along with
// The data as needed. Instead we are going to decompose the problem into multiple iterators that
// feed into each other.
// The first pass iterator will take in a batch of data and produce one or more aggregated result
// pairs that include the original input data with them.

case class AggResult(inputData: SpillableColumnarBatch,
    aggResult: SpillableColumnarBatch) extends AutoCloseable {
  override def close(): Unit = {
    inputData.close()
    aggResult.close()
  }
}

// TODO not sure that these are the right args to pass into this,
//  Because we have to do a merge later on, we might want to preprocess the
//  window ops and pass in agg ops.
class GpuUnboundedToUnboundedAggWindowFirstPassIterator(
    input: Iterator[ColumnarBatch],
    boundWindowOps: Seq[GpuExpression],
    boundPartitionSpec: Seq[GpuExpression],
    boundOrderSpec: Seq[SortOrder],
    outputTypes: Array[DataType],
    opTime: GpuMetric) extends Iterator[AggResult] {
  private var subIterator: Option[Iterator[AggResult]] = None
  override def hasNext: Boolean = subIterator.exists(_.hasNext) || input.hasNext

  override def next(): AggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next()).get
    } else {
      val currIter = withRetry(
        SpillableColumnarBatch(input.next(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY),
        splitSpillableInHalfByRows) { scb =>
        withResource(scb.getColumnarBatch()) { cb =>
          // TODO actually do the agg
          throw new IllegalStateException("Do the agg!!!")
        }
      }
      val result = currIter.next()
      subIterator = Some(currIter)
      result
    }
  }
}

// The second pass through the data will take the input data, slice it based off of what is
// known to be complete and what is not known yet. Then combine the aggregations as needed
// This is similar to a merge stage. We are not going to try and combine small slices together
// yet.

// TODO again not sure that these are the right args to pass into this,
//  Because we have to do a merge later on, we might want to preprocess the
//  window ops and pass in agg ops.
class GpuUnboundedToUnboundedAggWindowSecondPassIterator(
    input: Iterator[AggResult],
    boundWindowOps: Seq[GpuExpression],
    boundPartitionSpec: Seq[GpuExpression],
    boundOrderSpec: Seq[SortOrder],
    outputTypes: Array[DataType],
    opTime: GpuMetric) extends Iterator[AggResult] {
  // input data where we don't know if the results are done yet
  private val inputDataPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()
  // Agg results where the input keys are not fully complete yet. They will need to be combined
  // together before being returned.
  // TODO private var aggResultsPendingCompletion = new util.LinkedList[SpillableColumnarBatch]()

  private val inputDataThatIsComplete = new util.LinkedList[SpillableColumnarBatch]()
  private var aggResultsThatAreComplete: Option[SpillableColumnarBatch] = None

  override def hasNext: Boolean = (!inputDataThatIsComplete.isEmpty) ||
      (!inputDataPendingCompletion.isEmpty) || input.hasNext

  override def next(): AggResult = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    while (inputDataThatIsComplete.isEmpty) {
      if (input.hasNext) {
        withResource(input.next()) { newData =>
          throw new IllegalStateException("Actually split the inputs")
          // TODO newData should be sliced based off of which rows are known to be completed and
          //  which are not. Then they should be placed in the appropriate state queues. Please note
          //  that this cannot be done with a split and retry, but should be done with regular retry
        }
      } else {
        throw new IllegalStateException("Merge aggResultsPendingCompletion")
        // TODO There is no more data, so we need to merge the aggResultsPendingCompletion
        //  into a single SpillableColumnarBatch, and put the result in aggResultsThatAreComplete
        //  then move all of the batches in inputDataPendingCompletion to inputDataThatIsComplete
        //  Please note that this cannot be done with a split and retry, but should be done with
        //  regular retry.
      }
    }
    val nextData = inputDataThatIsComplete.pop
    val aggResult = aggResultsThatAreComplete.get
    if (inputDataThatIsComplete.isEmpty) {
      // Nothing left to work on
      aggResultsThatAreComplete = None
      AggResult(nextData, aggResult)
    } else {
      // We are reusing this spillable columnar batch so inc the ref count to avoid it being
      // close too early
      AggResult(nextData, aggResult.incRefCount())
    }
  }
}

// The final step is to take the original input data along with the agg data, estimate how
// to split/combine the input batches to output batches that are close to the target batch size

// TODO again not sure that these are the right args to pass into this,
//  Because we have to do a merge later on, we might want to preprocess the
//  window ops and pass in agg ops.
class GpuUnboundedToUnboundedAggFinalIterator(
    input: Iterator[AggResult],
    boundWindowOps: Seq[GpuExpression],
    boundPartitionSpec: Seq[GpuExpression],
    boundOrderSpec: Seq[SortOrder],
    outputTypes: Array[DataType],
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = input.hasNext

  override def next(): ColumnarBatch = {
    throw new IllegalStateException("Do all of the work to split this and expand the results")
    // TODO also need to make sure that we update the output metrics
  }
}

/**
 * An iterator that can do unbounded to unbounded window aggregations as group by aggregations
 * followed by an expand/join.
 */
object GpuUnboundedToUnboundedAggWindowIterator {
  def apply(input: Iterator[ColumnarBatch],
      boundWindowOps: Seq[GpuExpression],
      boundPartitionSpec: Seq[GpuExpression],
      boundOrderSpec: Seq[SortOrder],
      outputTypes: Array[DataType],
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    val firstPass = new GpuUnboundedToUnboundedAggWindowFirstPassIterator(input, boundWindowOps,
      boundPartitionSpec, boundOrderSpec, outputTypes, opTime)
    val secondPass = new GpuUnboundedToUnboundedAggWindowSecondPassIterator(firstPass,
      boundWindowOps, boundPartitionSpec, boundOrderSpec, outputTypes, opTime)
    new GpuUnboundedToUnboundedAggFinalIterator(secondPass, boundWindowOps, boundPartitionSpec,
      boundOrderSpec, outputTypes, numOutputBatches, numOutputRows, opTime)
  }
}

/**
 * This allows for batches of data to be processed without needing them to correspond to
 * the partition by boundaries. This is specifically for unbounded to unbounded window
 * operations that can be replaced with an aggregation and then expanded out/joined with
 * the original input data.
 */
case class GpuUnboundedToUnboundedAggWindowExec(
    windowOps: Seq[NamedExpression],
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    child: SparkPlan)(
    override val cpuPartitionSpec: Seq[Expression],
    override val cpuOrderSpec: Seq[SortOrder]) extends GpuWindowBaseExec {

  override def otherCopyArgs: Seq[AnyRef] = cpuPartitionSpec :: cpuOrderSpec :: Nil

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundWindowOps = GpuBindReferences.bindGpuReferences(windowOps, child.output)
    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { iter =>
      GpuUnboundedToUnboundedAggWindowIterator(iter, boundWindowOps, boundPartitionSpec,
        boundOrderSpec, output.map(_.dataType).toArray, numOutputBatches, numOutputRows, opTime)
    }
  }
}