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

import com.nvidia.spark.rapids.{GpuAlias, GpuBindReferences, GpuColumnVector, GpuExpression, GpuLiteral, GpuMetric, GpuProjectExec, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry, withRetryNoSplit}
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.aggregate.{GpuAggregateExpression, GpuCount}
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

class GpuUnboundedToUnboundedAggWindowFirstPassIterator(
    input: Iterator[ColumnarBatch],
    boundStages: GpuUnboundedToUnboundedAggStages,
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
class GpuUnboundedToUnboundedAggWindowSecondPassIterator(
    input: Iterator[AggResult],
    boundStages: GpuUnboundedToUnboundedAggStages,
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

class GpuUnboundedToUnboundedAggFinalIterator(
    input: Iterator[AggResult],
    boundStages: GpuUnboundedToUnboundedAggStages,
    numOutputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    opTime: GpuMetric) extends Iterator[ColumnarBatch] {
  override def hasNext: Boolean = input.hasNext

  override def next(): ColumnarBatch = {
    // TODO we need to add in the retry code, and pre-splitting of the data if possible, but
    //  for now we are just going to try it.
    val (aggResult, rideAlong) = withResource(input.next()) { data =>
      (data.aggResult.incRefCount(), data.inputData.incRefCount())
    }

    // The first stage is to expand the aggregate based on the count column
    val repeatedCb = closeOnExcept(rideAlong) { _ =>
      withRetryNoSplit(aggResult) { scb =>
        opTime.ns {
          withResource(scb.getColumnarBatch()) { cb =>
            withResource(boundStages.boundCount.columnarEval(cb)) { counts =>
              withResource(GpuProjectExec.project(cb, boundStages.boundAggsToRepeat)) { toRepeat =>
                withResource(GpuColumnVector.from(toRepeat)) { table =>
                  GpuColumnVector.from(table.repeat(counts.getBase),
                    boundStages.boundAggsToRepeat.map(_.dataType).toArray)
                }
              }
            }
          }
        }
      }
    }
    val combined = withResource(rideAlong) { _ =>
      // Second step is to stitch the two together
      withResource(repeatedCb) { _ =>
        withResource(rideAlong.getColumnarBatch()) { rideAlong =>
          opTime.ns {
            GpuColumnVector.appendColumns(rideAlong, GpuColumnVector.extractColumns(repeatedCb): _*)
          }
        }
      }
    }
    withResource(combined) { _ =>
      opTime.ns {
        closeOnExcept(GpuProjectExec.project(combined, boundStages.boundFinalProject)) { ret =>
          numOutputBatches += 1
          numOutputRows += ret.numRows()
          ret
        }
      }
    }
  }
}

/**
 * Holds the bound references for various aggregation stages
 * @param boundRideAlong used for a project that pulls out columns that are passing through
 *                       unchanged.
 * @param boundAggregations aggregations to be done. NOTE THIS IS WIP
 * @param boundCount The column that contains the count in it for the number of aggregations
 * @param boundAggsToRepeat the columns to get that need to be repeated by the amount in count
 * @param boundFinalProject the final project to get the output in the right order
 */
case class GpuUnboundedToUnboundedAggStages(
    boundRideAlong: Seq[GpuExpression],
    boundAggregations: Seq[GpuExpression],
    boundCount: GpuExpression,
    boundAggsToRepeat: Seq[GpuExpression],
    boundFinalProject: Seq[GpuExpression]) extends Serializable

/**
 * An iterator that can do unbounded to unbounded window aggregations as group by aggregations
 * followed by an expand/join.
 */
object GpuUnboundedToUnboundedAggWindowIterator {

  private def rideAlongProjection(windowOps: Seq[NamedExpression],
      childOutput: Seq[Attribute]): (Seq[Attribute], Seq[GpuExpression]) = {
    val rideAlong = windowOps.filter {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => true
      case _ => false
    }
    val rideAlongOutput = rideAlong.map(_.toAttribute)
    val boundRideAlong = GpuBindReferences.bindGpuReferences(rideAlong, childOutput)
    (rideAlongOutput, boundRideAlong)
  }


  private def tmpAggregationOps(windowOps: Seq[NamedExpression],
      childOutput: Seq[Attribute]): (Seq[Attribute], Seq[GpuExpression]) = {
    //  TODO I don't know what this is really going to look like. I am just doing an approximation
    //    here so I can get the output of the aggregations after everything is done for the
    //    repeat. Please fill this in/split it apart, whatever to make it work for you
    val windowAggs = windowOps.flatMap {
      case GpuAlias(_: AttributeReference, _) | _: AttributeReference => None
      case ga@GpuAlias(GpuWindowExpression(agg: GpuUnboundedToUnboundedWindowAgg, _), _) =>
        // We don't care about the spec, they are all unbounded to unbounded so just get the func
        // We do care that we keep the expression id so we can line it up at the very end
        Some(GpuAlias(agg, ga.name)(ga.exprId))
      case ga@GpuAlias(GpuWindowExpression(GpuAggregateExpression(
      agg: GpuUnboundedToUnboundedWindowAgg, _, _, _, _), _), _) =>
        // TODO should I verify distinct, filter, etc
        // We don't care about the spec, they are all unbounded to unbounded so just get the func
        // We do care that we keep the expression id so we can line it up at the very end
        Some(GpuAlias(agg, ga.name)(ga.exprId))
      case other =>
        // This should only happen if we did something wrong with how this was created.
        throw new IllegalArgumentException(
          s"Found unexpected expression $other in window exec ${other.getClass}")
    } :+ GpuAlias(GpuCount(Seq(GpuLiteral(1L))), "_count")()
    // Later code by conventions "knows" that the last column is a count and that it can be
    // thrown away. If we ever dedupe this with a COUNT(1) that already exists, then
    // we need to update the output of this to have a clean way to say what is the count,
    // and if that count is needed see repeatOps

    val aggregationsOutput = windowAggs.map(_.toAttribute)
    val boundAggregations = GpuBindReferences.bindGpuReferences(windowAggs, childOutput)
    (aggregationsOutput, boundAggregations)
  }

  private def repeatOps(
      aggregationsOutput: Seq[Attribute]): (GpuExpression, Seq[Attribute], Seq[GpuExpression]) = {
    // It is assumed that the last aggregation column is a count that we will use for repeat
    // If that ever changes, this code needs to be updated.
    val aggOutputExpressions = aggregationsOutput.map { attr =>
      GpuAlias(
        AttributeReference(attr.name, attr.dataType, attr.nullable)(attr.exprId),
        attr.name)(attr.exprId)
    }
    val boundAggOutputExpressions =
      GpuBindReferences.bindGpuReferences(aggOutputExpressions, aggregationsOutput)

    val boundCount = boundAggOutputExpressions.last
    val aggsToRepeat = boundAggOutputExpressions.slice(0, boundAggOutputExpressions.length - 1)
    val aggsToRepeatOutput = aggregationsOutput.slice(0, aggregationsOutput.length - 1)
    (boundCount, aggsToRepeatOutput, aggsToRepeat)
  }

  def computeFinalProject(rideAlongOutput: Seq[Attribute],
      aggsToRepeatOutput: Seq[Attribute],
      windowOps: Seq[NamedExpression]): Seq[GpuExpression] = {
    val combinedOutput = rideAlongOutput ++ aggsToRepeatOutput
    val remapped = windowOps.map { expr =>
      GpuAlias(AttributeReference(expr.name, expr.dataType, expr.nullable)(expr.exprId),
        expr.name)(expr.exprId)
    }
    GpuBindReferences.bindGpuReferences(remapped, combinedOutput)
  }

  /**
   * Break up the window operations into the various needed stages and bind them.
   * @param gpuPartitionSpec the partition spec for the GPU
   * @param windowOps the window operations (along with the pass-through columns)
   * @param childOutput what the output of the operation feeding this looks like
   * @return
   */
  def breakUpAggregations(gpuPartitionSpec: Seq[Expression],
      windowOps: Seq[NamedExpression],
      childOutput: Seq[Attribute]): GpuUnboundedToUnboundedAggStages = {
    // STEP 1. project that will pull out the columns that are output unchanged.
    val (rideAlongOutput, boundRideAlong) = rideAlongProjection(windowOps, childOutput)

    // STEP 2. project that will pull out the columns needed for the aggregation.
    val (aggregationsOutput, boundAggregations) = tmpAggregationOps(windowOps, childOutput)

    // STEP N: Given the output of the aggregations get count column and the other
    //  columns so we can do the repeat call.
    val (boundCount, aggsToRepeatOutput, aggsToRepeat) = repeatOps(aggregationsOutput)

    // STEP N + 1: After the repeat is done the repeated columns are put at the end of the
    //  rideAlong columns and then we need to do a project that would put them all in the
    //  proper output order, according to the windowOps
    val finalProject = computeFinalProject(rideAlongOutput, aggsToRepeatOutput, windowOps)

    GpuUnboundedToUnboundedAggStages(boundRideAlong, boundAggregations,
      boundCount, aggsToRepeat, finalProject)
  }

  def apply(input: Iterator[ColumnarBatch],
      boundStages: GpuUnboundedToUnboundedAggStages,
      numOutputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      opTime: GpuMetric): Iterator[ColumnarBatch] = {
    val firstPass = new GpuUnboundedToUnboundedAggWindowFirstPassIterator(input, boundStages,
      opTime)
    val secondPass = new GpuUnboundedToUnboundedAggWindowSecondPassIterator(firstPass,
      boundStages, opTime)
    new GpuUnboundedToUnboundedAggFinalIterator(secondPass, boundStages,
      numOutputBatches, numOutputRows, opTime)
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

  // For this we only need the data to be sorted by the partition columns, but
  //  we don't change the input sort from the CPU yet. In some cases we might even
  //  be able to remove the sort entirely. https://github.com/NVIDIA/spark-rapids/issues/9989
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(cpuPartitionOrdering)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundStages = GpuUnboundedToUnboundedAggWindowIterator.breakUpAggregations(
      gpuPartitionSpec, windowOps, child.output)

    child.executeColumnar().mapPartitions { iter =>
      GpuUnboundedToUnboundedAggWindowIterator(iter, boundStages,
        numOutputBatches, numOutputRows, opTime)
    }
  }
}