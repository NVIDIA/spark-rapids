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

package org.apache.spark.sql.rapids.execution.python.shims.spark310db

import scala.collection.mutable

import ai.rapids.cudf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

// This file is a copy of the base file with package changed to be built against specific
// version of spark because GpuArrowPythonRunner is different

/**
 * A helper class to pack the group related items for the Python input.
 *
 * @param dedupAttrs the deduplicated attributes for the output of a Spark plan.
 * @param argOffsets the argument offsets which will be used to distinguish grouping columns
 *                   and data columns by the Python workers.
 * @param groupingOffsets the grouping offsets(aka column indices) in the deduplicated attributes.
 */
case class GroupArgs(
    dedupAttrs: Seq[Attribute],
    argOffsets: Array[Int],
    groupingOffsets: Seq[Int])

/**
 * Basic functionality to deal with groups in a batch.
 *
 * It plays the similar role to the Spark 'PandasGroupUtils', but dealing with 'ColumnarBatch',
 * instead of 'InternalRow'.
 */
private[python] object BatchGroupUtils extends Arm {

  /**
   * It mainly does 2 things:
   *
   * 1) Drops the duplicated attributes for the output of the Spark plan.
   *
   * Doing this is because the Spark plan may contain an attribute twice; once in the
   * key and once in the value. For any such attribute we need to deduplicate.
   *
   * Besides, this API supposes the grouping attributes are placed before the others in
   * the output attributes of the plan.
   *
   * For example, assuming there is a DataFrame 'df' with two columns 'a' and 'b'.
   *     +---+---+
   *     |  a|  b|
   *     +---+---+
   *     | s1|  1|
   *     +---+---+
   *
   * When executes a groupby operation, e.g. `df.groupBy('a').agg(count('b')).show()`, the
   * output attributes of the child plan will be
   *     ['a', 'a', 'b'],
   * and the grouping attributes will be
   *     ['a'].
   * After deduplication, the result will be
   *     ['a', 'b'].
   *
   * 2) Resolves the argument offsets which will be used to distinguish grouping columns and
   *    data columns. All the offsets are actually the column indices in the deduplicated
   *    attributes.
   * The following is the details of the array of the argument offsets.
   *
   *  `argOffsets[0]` is the length of the `argOffsets` array (excludes itself).
   *  `argOffsets[1]` is the length of grouping attribute.
   *  `argOffsets[2.. argOffsets[1]+2)` is the grouping column indices in deduplicated attributes.
   *  `argOffsets[argOffsets[1]+2 .. )` is the data column indices in deduplicated attributes.
   *
   *   This is the argument protocol which will be used by the Python workers to separate the key
   *   columns from value columns.
   *   (Python code: https://github.com/apache/spark/blob/master/python/pyspark/worker.py#L386)
   *
   * For the example above, the argument offsets will be
   *   Array(
   *     4,   // The length of the following offsets (1 for grouping length and 3 column indices)
   *     1,   // There is only one grouping column 'a'
   *     0,   // The single grouping column ('a') index in the deduplicated attributes['a', 'b']
   *     0,1  // The data column('a', 'b') indices in the deduplicated attributes.
   *   )
   *
   * @param plan The input plan whose output attributes will be deduplicated.
   * @param groupingAttrs The grouping attributes.
   * @return a GroupArgs consisting of the deduplicated attributes, the argument offsets
   *         and the grouping offsets in the deduplicated attributes.
   */
  def resolveArgOffsets(plan: SparkPlan, groupingAttrs: Seq[Attribute]): GroupArgs = {

    val dataAttrs = plan.output.drop(groupingAttrs.length)
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
    //   Array(length of `argOffsets` without itself,
    //         length of `groupingAttrs`,
    //         group 1 arg offset, group 2 arg offset, ... ,
    //         data 1 arg offset,  data 2 arg offset, ...)
    val argOffsetLen = groupingAttrs.length + dataAttrs.length + 1
    val argOffsets = Array(argOffsetLen, groupingAttrs.length) ++
      groupingArgOffsets ++ dataAttrs.indices

    GroupArgs(dedupAttrs, argOffsets, groupingArgOffsets)
  }

  /**
   *
   * Projects each input batch into the deduplicated schema, and splits
   * into separate group batches.
   *
   * Since this API will use a `BatchGroupedIterator` inside, so it also requires the rows
   * in the input batches are presorted in the order of `Ascending & NullsFirst`.
   *
   * BatchGroupedIterator will probably return more batches than input, so projecting
   * first, then grouping. Doing this is likely to save time.
   *
   * @param inputIter the input iterator.
   * @param inputAttrs the schema of the batches in the `inputIter`.
   * @param dedupAttrs the deduplicated attributes for the `inputAttrs`.
   * @param groupingOffsetsInDedup the grouping column indices in the 'dedupAttrs'
   * @param inputRows a metric to record the input rows.
   * @param inputBatches a metric to record the input batches.
   * @return an iterator of the group batches, meaning each batch contains only one group.
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
   *
   * Passes the data to the python runner. After that extracts the children columns from
   * each resulting ColumnarBatch returned from Python, since the resulting columns are
   * put in a struct column, not top-level columns.
   *
   * @param pyInputIterator the batch iterator for python input
   * @param output the output attributes of the plan
   * @param pyRunner the Python runner to execute the Python UDF.
   * @param outputRows a metric to record the output rows.
   * @param outputBatches a metric to record the output batches.
   * @return an iterator of the resulting batches from the Python runner.
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
 * An iterator that splits the groups in each input batch into separate batches by the grouping
 * expressions, then each batch returned from the call to 'next' contains only one group.
 *
 * This iterator supposes the rows in the input batches are presorted in the order of
 * `Ascending & NullsFirst`.
 *
 * Since the rows in the batches are already sorted by Spark for the Pandas UDF plans, a better
 * performance is probably achieved in the cudf `groupby`.
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
 * @param input An iterator of batches where the rows are presorted in the order
 *              of `Ascending & NullsFirst`.
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
            // the order of `Ascending & NullsFirst` for Pandas UDF plans. This is ensured by
            // overriding the 'requiredChildOrdering' in each plan.
            // So passes the info to cudf for a better performance on `groupBy` operation.
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

/**
 * An iterator combines the batches in a `inputBatchQueue` and the result batches
 * in `pythonOutputIter` one by one.
 *
 * Both the batches from  `inputBatchQueue` and `pythonOutputIter` should have the same row
 * number.
 *
 * In each batch returned by calling to the `next`, the columns of the result batch
 * are appended to the columns of the input batch.
 *
 * @param inputBatchQueue the queue caching the original input batches.
 * @param pythonOutputIter the iterator of the result batches from the python side.
 * @param pythonArrowReader the gpu arrow reader to read batches from the python side.
 * @param numOutputRows a metric for output rows.
 * @param numOutputBatches a metric for output batches
 */
private[python] class CombiningIterator(
    inputBatchQueue: BatchQueue,
    pythonOutputIter: Iterator[ColumnarBatch],
    pythonArrowReader: GpuPythonArrowOutput,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric) extends Iterator[ColumnarBatch] with Arm {

  // For `hasNext` we are waiting on the queue to have something inserted into it
  // instead of waiting for a result to be ready from Python. The reason for this
  // is to let us know the target number of rows in the batch that we want when reading.
  // It is a bit hacked up but it works. In the future when we support spilling we should
  // store the number of rows separate from the batch. That way we can get the target batch
  // size out without needing to grab the GpuSemaphore which we cannot do if we might block
  // on a read operation.
  override def hasNext: Boolean = inputBatchQueue.hasNext || pythonOutputIter.hasNext

  override def next(): ColumnarBatch = {
    val numRows = inputBatchQueue.peekBatchSize
    // Updates the expected batch size for next read
    pythonArrowReader.updateMinReadTargetBatchSize(numRows)
    // Reads next batch from Python and combines it with the input batch by the left side.
    withResource(pythonOutputIter.next()) { cbFromPython =>
      assert(cbFromPython.numRows() == numRows)
      withResource(inputBatchQueue.remove()) { origBatch =>
        numOutputBatches += 1
        numOutputRows += numRows
        combine(origBatch, cbFromPython)
      }
    }
  }

  private def combine(lBatch: ColumnarBatch, rBatch: ColumnarBatch): ColumnarBatch = {
    val lColumns = GpuColumnVector.extractColumns(lBatch).map(_.incRefCount())
    val rColumns = GpuColumnVector.extractColumns(rBatch).map(_.incRefCount())
    new ColumnarBatch(lColumns ++ rColumns, lBatch.numRows())
  }

}
