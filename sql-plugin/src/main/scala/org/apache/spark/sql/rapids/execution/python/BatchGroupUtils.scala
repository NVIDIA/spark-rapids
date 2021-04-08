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

package org.apache.spark.sql.rapids.execution.python

import scala.collection.mutable

import ai.rapids.cudf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * Basic functionality to deal with groups in a batch.
 *
 * It plays the similar role to the Spark 'PandasGroupUtils', but dealing with 'ColumnarBatch',
 * instead of 'InternalRow'.
 */
private[python] object BatchGroupUtils extends Arm {

  /**
   * Returns the deduplicated attributes of the Spark plan, and the argument offsets of the
   * keys and values.
   *
   * The deduplicated attributes are needed because the Spark plan may contain an attribute
   * twice; once in the key and once in the value. For any such attribute we need to
   * deduplicate.
   *
   * The argument offsets are used to distinguish grouping attributes and data attributes
   * as following:
   *
   * argOffsets[0] is the length of the argOffsets array
   * argOffsets[1] is the length of grouping attribute
   * argOffsets[2 .. argOffsets[1]+2] is the argument offsets for grouping attributes
   * argOffsets[argOffsets[1]+2 .. ] is the argument offsets for data attributes
   */
  def resolveArgOffsets(
      child: SparkPlan,
      groupingAttrs: Seq[Attribute]): (Seq[Attribute], Array[Int], Seq[Int]) = {

    val dataAttrs = child.output.drop(groupingAttrs.length)
    val dedupGroupingAttrs = new mutable.ArrayBuffer[Attribute]

    val groupingArgOffsets = groupingAttrs.map { gpAttr =>
      val index = dataAttrs.indexWhere(gpAttr.semanticEquals)
      if (index == -1) {
        // Not a duplicated grouping attribute, just appends it to the tail.
        dedupGroupingAttrs += gpAttr
        dataAttrs.length + dedupGroupingAttrs.length - 1
      } else {
        // A duplicated grouping attribute, uses the one in data attributes, and
        // skips the one in grouping attributes.
        index
      }
    }

    // Attributes after deduplication, the layout is
    // (data1, data2, ..., key1, key2, ...), matching the argument offsets.
    val dedupAttrs = dataAttrs ++ dedupGroupingAttrs

    // Layout of argument offsets:
    //   Array(length of `argOffsets`,
    //         length of `groupingAttrs`,
    //         group 1 arg offset, group 2 arg offset, ... ,
    //         data 1 arg offset,  data 2 arg offset, ...)
    val argOffsetLen = groupingAttrs.length + dataAttrs.length + 1
    val argOffsets = Array(argOffsetLen, groupingAttrs.length) ++
      groupingArgOffsets ++ dataAttrs.indices

    (dedupAttrs, argOffsets, groupingArgOffsets)
  }

  /**
   * Projects each input batch into the deduplicated schema, and splits
   * into separate group batches.
   *
   * BatchGroupedIterator will probably return more batches than input, so projecting
   * first, then grouping. Doing this is likely to save time.
   */
  def projectAndGroup(
      inputIter: Iterator[ColumnarBatch],
      inputAttrs: Seq[Attribute],
      dedupAttrs: Seq[Attribute],
      groupingOffsetsInDedup: Seq[Int],
      inputRows: GpuMetric,
      inputBatches: GpuMetric,
      spillCallback: RapidsBuffer.SpillCallback): Iterator[ColumnarBatch] = {
    val dedupRefs = GpuBindReferences.bindReferences(dedupAttrs, inputAttrs)
    val dedupIter = inputIter.map { batch =>
      // Close the original input batches.
      withResource(batch) { b =>
        inputBatches += 1
        inputRows += b.numRows()
        GpuProjectExec.project(b, dedupRefs)
      }
    }
    // Groups rows on the batches being projected
    BatchGroupedIterator(dedupIter, dedupAttrs, groupingOffsetsInDedup, spillCallback)
  }

  /**
   * Passes the data to the python runner. After that extracts the children columns from
   * each resulting ColumnarBatch returned from Python, since the resulting columns are
   * put in a struct column, not top-level columns.
   */
  def executePython(
      pyInputIterator: Iterator[ColumnarBatch],
      output: Seq[Attribute],
      pyRunner: GpuArrowPythonRunner,
      outputRows: GpuMetric,
      outputBatches: GpuMetric): Iterator[ColumnarBatch] = {
    val context = TaskContext.get()
    val pythonOutputIter = pyRunner.compute(pyInputIterator, context.partitionId(), context)

    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = pythonOutputIter.hasNext

      override def next(): ColumnarBatch = {
        withResource(pythonOutputIter.next()) { cbFromPython =>
          outputBatches += 1
          outputRows += cbFromPython.numRows
          // UDF returns a StructType column in ColumnarBatch, select
          // the children here
          BatchGroupedIterator.extractChildren(cbFromPython, output)
        }
      } // end of next
    } // end of Iterator
  }

}

/**
 * Runs a `groupby` on the input batch, then split it into separate batches by the grouping
 * expressions.
 * Since the rows in the batches are already sorted by Spark, a better performance is probably
 * achieved in the cudf `groupby`.
 *
 * Example Input: (Grouping: `x`)
 *   A batch of 3 rows, two groups.
 *         x    y
 *        --------
 *         a    1
 *         b    2
 *         b    3
 *
 * Result:
 *   Two batches, one group one batch.
 * First call to next(), returning the batch of group `a`:
 *         x    y
 *        --------
 *         a    1
 *
 * Second call to next(), returning the batch of group `b`:
 *         x    y
 *        --------
 *         b    2
 *         b    3
 *
 * Note, the order of the groups returned is NOT always the same with it in the input batch.
 *
 * Besides the class does not handle the case of an empty input for simplicity of implementation.
 * Use the factory to construct a new instance.
 *
 * @param input An iterator of batches.
 * @param inputAttributes The schema of the batch in the `input` iterator.
 * @param groupingIndices The set of column indices used to do grouping.
 */
private[python] class BatchGroupedIterator private(
    input: Iterator[ColumnarBatch],
    inputAttributes: Seq[Attribute],
    groupingIndices: Seq[Int],
    spillCallback: RapidsBuffer.SpillCallback) extends Iterator[ColumnarBatch] with Arm {

  private val batchesQueue: mutable.Queue[SpillableColumnarBatch] = mutable.Queue.empty

  // Suppose runs inside a task context.
  TaskContext.get().addTaskCompletionListener[Unit]{ _ =>
    batchesQueue.foreach(_.close())
    batchesQueue.clear()
  }

  override def hasNext: Boolean = batchesQueue.nonEmpty || input.hasNext

  override def next(): ColumnarBatch = {
    if (groupingIndices.isEmpty) {
      // Empty grouping expression, so the entire batch is a single group.
      // Returns it directly.
      input.next()
    } else {
      // Grouping expression is not empty, so returns the first batch in the queue.
      // If the queue is empty, tries to read and split the next batch from input.
      if (batchesQueue.isEmpty) {
        val batch = input.next()

        // Splits the batch per grouping expressions
        val groupTables = withResource(batch) { b =>
          withResource(GpuColumnVector.from(b)) { table =>
            // In Spark, the rows in a batch are already sorted by grouping keys in
            // the order of `Ascending & NullsFirst` for Pandas UDF plans.
            // So passes the info to cudf for better performance on `groupBy` operation.
            val builder = cudf.GroupByOptions.builder()
            builder.withIgnoreNullKeys(false)
                   .withKeysSorted(true)
                   .withKeysDescending(groupingIndices.map(_ => false): _*)
                   .withKeysNullSmallest(groupingIndices.map(_ => true): _*)
            table.groupBy(builder.build(), groupingIndices: _*)
                 .contiguousSplitGroups()
          }
        }

        // Convert to `SpillableColumnarBatch` and puts them in the queue.
        val inputTypes = inputAttributes.map(_.dataType).toArray
        val groupBatches = withResource(groupTables) { tables =>
          // safe map to avoid memory leaks on failure
          tables.safeMap( t =>
            SpillableColumnarBatch(
              GpuColumnVectorFromBuffer.from(t, inputTypes),
              SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
              spillCallback)
          )
        }
        batchesQueue.enqueue(groupBatches: _*)
      }

      batchesQueue.dequeue().getColumnarBatch()
    }
  }

}

private[python] object BatchGroupedIterator extends Arm {

  /**
   * Create a `BatchGroupedIterator` instance
   */
  def apply(wrapped: Iterator[ColumnarBatch],
            inputAttributes: Seq[Attribute],
            groupingIndices: Seq[Int],
            spillCallback: RapidsBuffer.SpillCallback): Iterator[ColumnarBatch] = {
    if (wrapped.hasNext) {
      new BatchGroupedIterator(wrapped, inputAttributes, groupingIndices, spillCallback)
    } else {
      Iterator.empty
    }
  }

  /**
   * Extract the children columns from a batch consisting of only one struct column, and
   * wrap them up by a ColumnarBatch where they become the top-level children.
   *
   * @param batch         The input batch
   * @param childrenAttrs The attributes of the children columns to be pulled out
   * @return a columnar batch with the children pulled out.
   */
  def extractChildren(batch: ColumnarBatch, childrenAttrs: Seq[Attribute]): ColumnarBatch = {
    assert(batch.numCols() == 1, "Expect only one struct column")
    assert(batch.column(0).dataType().isInstanceOf[StructType],
      "Expect a struct column")
    val structColumn = batch.column(0).asInstanceOf[GpuColumnVector].getBase
    val outputColumns = childrenAttrs.zipWithIndex.safeMap {
      case (attr, i) =>
        withResource(structColumn.getChildColumnView(i)) { childView =>
          GpuColumnVector.from(childView.copyToColumnVector(), attr.dataType)
        }
    }
    new ColumnarBatch(outputColumns.toArray, batch.numRows())
  }

}
