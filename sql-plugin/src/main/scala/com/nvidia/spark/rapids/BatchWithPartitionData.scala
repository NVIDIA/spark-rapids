/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Wrapper class that specifies how many rows to replicate
 * the partition value.
 */
case class PartitionRowData(rowValue: InternalRow, rowNum: Int)

/**
 * Class to wrap columnar batch and partition rows data and utility functions to merge them.
 *
 * @param inputBatch           Input ColumnarBatch.
 * @param partitionedRowsData  Array of (row counts, InternalRow) pairs. Each entry specifies how
 *                             many rows to replicate the partition value.
 * @param partitionSchema      Schema of the partitioned data.
 */
class BatchWithPartitionData(
    inputBatch: SpillableColumnarBatch,
    partitionedRowsData: Array[PartitionRowData],
    partitionSchema: StructType) extends AutoCloseable {

  /**
   * Merges the partitioned data with the input ColumnarBatch.
   *
   * @return Merged ColumnarBatch.
   */
  def mergeWithPartitionData(): ColumnarBatch = {
    withResource(inputBatch.getColumnarBatch()) { columnarBatch =>
      if (partitionSchema.isEmpty) {
        columnarBatch // No partition columns, return the original batch.
      } else {
        val partitionColumns = buildPartitionColumns()
        val numRows = partitionColumns.head.getRowCount.toInt
        withResource(new ColumnarBatch(partitionColumns.toArray, numRows)) { partBatch =>
          assert(columnarBatch.numRows == partBatch.numRows)
          GpuColumnVector.combineColumns(columnarBatch, partBatch)
        }
      }
    }
  }

  /**
   * Builds GPU-based partition columns by mapping partitioned data to a sequence of
   * GpuColumnVectors.
   *
   * @return A sequence of GpuColumnVectors, one for each partitioned column in the schema.
   */
  private def buildPartitionColumns(): Seq[GpuColumnVector] = {
    closeOnExcept(new Array[GpuColumnVector](partitionSchema.length)) { partitionColumns =>
      for ((field, colIndex) <- partitionSchema.zipWithIndex) {
        val dataType = field.dataType
        // Create an array to hold the individual columns for each partition.
        val singlePartCols = partitionedRowsData.safeMap {
          case PartitionRowData(valueRow, rowNum) =>
            val singleValue = valueRow.get(colIndex, dataType)
            withResource(GpuScalar.from(singleValue, dataType)) { singleScalar =>
              // Create a column vector from the GPU scalar, associated with the row number.
              ColumnVector.fromScalar(singleScalar, rowNum)
            }
        }

        // Concatenate the individual columns into a single column vector.
        val concatenatedColumn = withResource(singlePartCols) { _ =>
          if (singlePartCols.length > 1) {
            ColumnVector.concatenate(singlePartCols: _*)
          } else {
            singlePartCols.head.incRefCount()
          }
        }
        partitionColumns(colIndex) = GpuColumnVector.from(concatenatedColumn, field.dataType)
      }
      partitionColumns
    }
  }

  override def close(): Unit = {
    inputBatch.close()
  }
}

/**
 * An iterator for merging and retrieving split ColumnarBatches with partition data.
 *
 * It uses a Queue to fetch ColumnarBatch and partition values and merge them only when
 * required.
 */
class BatchWithPartitionDataIterator(
    batchesWithPartitionData: Seq[BatchWithPartitionData])
  extends GpuColumnarBatchIterator(true) {
  private val pending = new mutable.Queue[BatchWithPartitionData]() ++ batchesWithPartitionData

  override def hasNext: Boolean = pending.nonEmpty

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more split batches with partition data to retrieve.")
    }
    // Retrieves and merges the next split ColumnarBatch with partition data.
    withResource(pending.dequeue()) { splitBatchMerger =>
      splitBatchMerger.mergeWithPartitionData()
    }
  }

  override def doClose(): Unit = {
    pending.foreach(_.close())
  }
}

object BatchWithPartitionDataUtils {
  private val CUDF_COLUMN_SIZE_LIMIT: Long = new RapidsConf(SQLConf.get).cudfColumnSizeLimit

  /**
   * Splits partition data (values and row counts) into smaller batches, ensuring that
   * size of batch is less than cuDF size limit. Then, it utilizes these smaller
   * partitioned batches to split the input batch and merges them to generate
   * an Iterator of split ColumnarBatches.
   *
   * Using an Iterator ensures that the actual merging does not happen until
   * the batch is required, thus avoiding GPU memory wastage.
   *
   * @param batch           Input batch, will be closed after the call returns
   * @param partitionValues Partition values collected from the batch
   * @param partitionRows   Row numbers collected from the batch, and it should have
   *                        the same size with "partitionValues"
   * @param partitionSchema Partition schema
   * @return a new columnar batch iterator with partition values
   */
  def addPartitionValuesToBatch(
      batch: ColumnarBatch,
      partitionRows: Array[Long],
      partitionValues: Array[InternalRow],
      partitionSchema: StructType): GpuColumnarBatchIterator = {
      if (partitionSchema.nonEmpty) {
        withResource(batch) { _ =>
          val partitionedGroups = splitPartitionDataIntoGroups(partitionRows, partitionValues,
            partitionSchema)
          splitAndCombineBatchWithPartitionData(batch, partitionedGroups, partitionSchema)
        }
      } else {
        new SingleGpuColumnarBatchIterator(batch)
      }
  }

  /**
   * Adds a single set of partition values to all rows in a ColumnarBatch.
   * @return a new columnar batch iterator with partition values
   */
  def addSinglePartitionValueToBatch(
      batch: ColumnarBatch,
      partitionValues: InternalRow,
      partitionSchema: StructType): GpuColumnarBatchIterator = {
    addPartitionValuesToBatch(batch, Array(batch.numRows), Array(partitionValues), partitionSchema)
  }

  /**
   * Splits partitions into smaller batches, ensuring that the batch size
   * for each column does not exceed the specified cuDF limit.
   *
   * Data structures:
   *  - sizeOfBatch:   Array that stores the size of batches for each column.
   *  - currentBatch:  Array used to hold the rows and partition values of the current batch.
   *  - resultBatches: Array that stores the resulting batches after splitting.
   *
   * Algorithm:
   *  - Initialize `sizeOfBatch` and `resultBatches`.
   *  - Iterate through `partRows`:
   *    - Get rowsToProcess - This can either be rows from new partition or
   *      rows remaining to be processed if there was a split.
   *    - Iterator through each column:
   *      - Calculate the size of the current partition for the column.
   *      - Check if adding this partition to the current batch would exceed cuDF limit.
   *        - If it does, split the partition:
   *          - Calculate max num of rows we can add to current batch.
   *          - Append entry (InternalRow, Row counts that fit) to the current batch.
   *          - Append current batch to the result batches.
   *          - Exit inner loop, as we cannot add more partitions to current batch
   *            and process remaining rows.
   *        - Otherwise, increase the `sizeOfBatch` for the column.
   *    - If there was no split, append entry (InternalRow, rowsToProcess) to the current batch.
   *      This implies all remaining rows can be added in current batch without exceeding cuDF
   *      limit.
   *
   * Example:
   * {{{
   * Input:
   *    partition rows:   [10, 40, 70, 10, 11]
   *    partition values: [ [abc, ab], [bc, ab], [abc, bc], [aa, cc], [ade, fd] ]
   *    cuDF limit:  300 bytes
   *
   * Result:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ]
   * ]
   * }}}
   *
   * @return An array of batches, containing (row counts, partition values) pairs,
   *         such that each batch's size is less than cuDF limit.
   */
  private def splitPartitionDataIntoGroups(partRows: Array[Long], partValues: Array[InternalRow],
      partSchema: StructType): Array[Array[PartitionRowData]] = {
    val resultBatches = ArrayBuffer[Array[PartitionRowData]]()
    val currentBatch = ArrayBuffer[PartitionRowData]()
    // Initialize an array to store the size of the current batch for each column
    val sizeOfBatch = Array.fill(partSchema.length)(0L)
    var partIndex = 0
    // Variables to keep track of split occurrence and processed rows
    // Note: These variables can be replaced by an Option. Keep them explicit for more clarity.
    var (splitOccurred, processedRows) = (false, 0)
    var rowsInPartition = 0
    while (partIndex < partRows.length) {
      rowsInPartition = if(splitOccurred) {
        // If there has been a split, there are rows remaining in current partition for processing
        rowsInPartition - processedRows
      } else {
        // Else get next partition
        partRows(partIndex).toInt
      }
      splitOccurred = false
      processedRows = 0
      val valuesInPartition = partValues(partIndex)
      // Loop through all columns or until split condition is reached
      var colIndex = 0
      while(colIndex < partSchema.length && !splitOccurred) {
        val field = partSchema(colIndex)
        // Assumption: Columns of other data type would not have fit already. Do nothing.
        if(field.dataType == StringType) {
          val singleValue = valuesInPartition.getString(colIndex)
          val sizeOfSingleValue = singleValue.getBytes("UTF-8").length.toLong
          val sizeOfPartition = rowsInPartition * sizeOfSingleValue
          // Check if adding the partition to the current batch exceeds cuDF limit
          splitOccurred = sizeOfBatch(colIndex) + sizeOfPartition > CUDF_COLUMN_SIZE_LIMIT
          if (splitOccurred) {
            // Calculate max row counts that can fit.
            processedRows =
              ((CUDF_COLUMN_SIZE_LIMIT - sizeOfBatch(colIndex)) / sizeOfSingleValue).toInt
            currentBatch.append(PartitionRowData(valuesInPartition, processedRows))
            // Add current batch to result batches and reset current batch.
            resultBatches.append(currentBatch.toArray)
            currentBatch.clear()
            sizeOfBatch.indices.foreach(i => sizeOfBatch(i) = 0)
          } else {
            sizeOfBatch(colIndex) += sizeOfPartition
          }
        }
        colIndex += 1
      }
      // If there was no split, all rows can be added in current batch and increment partIndex
      // to process next partition.
      if(!splitOccurred) {
        currentBatch.append(PartitionRowData(valuesInPartition, rowsInPartition))
        partIndex += 1
      }
    }
    // Add the last remaining batch to resultBatches
    if (currentBatch.nonEmpty) {
      resultBatches.append(currentBatch.toArray)
    }
    resultBatches.toArray
  }

  /**
   * Splits the input ColumnarBatch into smaller batches, wraps these batches with partition
   * data, and returns an Iterator.
   *
   * @note Partition values are merged with the columnar batches lazily by the resulting Iterator
   *       to save GPU memory.
   *
   * @param batch                     Input ColumnarBatch.
   * @param listOfPartitionedRowsData Array of arrays where each entry contains 'list of partition
   *                                  values and number of times it is replicated'. See example in
   *                                  `splitPartitionDataIntoGroups()`.
   * @param partitionSchema           Schema of partition ColumnVectors.
   * @return An Iterator of combined ColumnarBatches.
   *
   * @see [[com.nvidia.spark.rapids.PartitionRowData]]
   */
  private def splitAndCombineBatchWithPartitionData(
      batch: ColumnarBatch,
      listOfPartitionedRowsData: Array[Array[PartitionRowData]],
      partitionSchema: StructType): BatchWithPartitionDataIterator = {
    val splitIndices = calculateSplitIndices(listOfPartitionedRowsData)
    closeOnExcept(splitColumnarBatch(batch, splitIndices)) { splitColumnarBatches =>
      assert(splitColumnarBatches.length == listOfPartitionedRowsData.length)
      // Combine the split GPU ColumnVectors with partition ColumnVectors.
      val combinedBatches = splitColumnarBatches.zip(listOfPartitionedRowsData).map {
        case (spillableBatch, partitionedRowsData) =>
          new BatchWithPartitionData(spillableBatch, partitionedRowsData, partitionSchema)
      }
      new BatchWithPartitionDataIterator(combinedBatches)
    }
  }

  /**
   * Helper method to calculate split indices for dividing ColumnarBatch based on the row counts.
   *
   * Example,
   * {{{
   * Input - Array of arrays having (row counts, InternalRow) pairs:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ],
   *  [ (4, [ac, gbc]), (7, [aa, tt]), (15, [ate, afd]) ]
   * ]
   *
   * Result -
   *  - Row counts for each batch: [ 10+40+30, 7+10+11, 4+7+15 ] = [ 80, 28, 26 ]
   *  - Split Indices: [ 80, 108 ]
   *  - ColumnarBatch with 134 rows will be split in three batches having
   *    `[ 80, 28, 26 ]` rows in each.
   *}}}
   *
   * @param listOfPartitionedRowsData Array of arrays where each entry contains 'list of partition
   *                                  values and number of times it is replicated'. See example in
   *                                  `splitPartitionDataIntoGroups()`.
   * @return A sequence of split indices.
   */
  private def calculateSplitIndices(
      listOfPartitionedRowsData: Array[Array[PartitionRowData]]): Seq[Int] = {
    // Calculate the row counts for each batch
    val rowCountsForEachBatch = listOfPartitionedRowsData.map(partitionData =>
      partitionData.map {
        case PartitionRowData(_, rowNum) => rowNum
      }.sum
    )
    // Calculate split indices using cumulative sum
    rowCountsForEachBatch.scanLeft(0)(_ + _).drop(1).dropRight(1)
  }

  /**
   * Split a ColumnarBatch into multiple ColumnarBatches based on indices.
   */
  private def splitColumnarBatch(input: ColumnarBatch,
      splitIndices: Seq[Int]): Seq[SpillableColumnarBatch] = {
    if (splitIndices.isEmpty) {
      Seq(SpillableColumnarBatch(GpuColumnVector.incRefCounts(input),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    } else {
      val schema = GpuColumnVector.extractTypes(input)
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
        table.contiguousSplit(splitIndices: _*)
      }
      splitInput.safeMap { ct =>
        SpillableColumnarBatch(ct, schema, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      }
    }
  }
}
