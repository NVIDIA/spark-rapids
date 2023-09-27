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
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}

/**
 * A utility for merging partitioned rows data with a ColumnarBatch.
 *
 * @param inputBatch           Input ColumnarBatch to merge with partition data.
 * @param partitionSchema      Schema of the partitioned data.
 * @param partitionedRowsData  Array of (row counts, InternalRow) pairs.
 */
class ColumnarBatchPartitionMerger(
    inputBatch: SpillableColumnarBatch,
    partitionSchema: StructType,
    partitionedRowsData: Array[(Long, InternalRow)]) {

  /**
   * Merges the partitioned data with the input ColumnarBatch.
   * @return Merged ColumnarBatch.
   */
  def mergeWithPartitionData(): ColumnarBatch = {
    withResource(inputBatch.getColumnarBatch()) { columnarBatch =>
      if (partitionSchema.isEmpty) {
        columnarBatch // No partition columns, return the original batch.
      } else {
        val partitionColumns = buildPartitionColumns()
        val numRows = partitionColumns.head.getRowCount.toInt
        val inputColumns = (0 until columnarBatch.numCols).map(columnarBatch.column)
        val mergedColumns: Seq[SparkVector] = inputColumns ++ partitionColumns
        val mergedBatch = new ColumnarBatch(mergedColumns.toArray, numRows)
        // Increment reference counts
        mergedColumns.foreach {
          case gpuColumn: GpuColumnVector => gpuColumn.incRefCount()
        }
        mergedBatch
      }
    }
  }

  /**
   * Builds GPU-based partition columns by mapping partitioned data to a sequence of
   * GpuColumnVectors.
   * @return A sequence of GpuColumnVectors, one for each partitioned column in the schema.
   */
  private def buildPartitionColumns(): Seq[GpuColumnVector] = {
    closeOnExcept(new Array[GpuColumnVector](partitionSchema.length)) { partitionColumns =>
      for ((field, colIndex) <- partitionSchema.zipWithIndex) {
        val dataType = field.dataType
        // Create an array to hold the individual columns for each partition.
        withResource(new Array[ColumnVector](partitionedRowsData.length)) { singlePartCols =>
          partitionedRowsData.zipWithIndex.foreach { case ((rowNum, valueRow), partId) =>
            val singleValue = valueRow.get(colIndex, dataType)
            withResource(GpuScalar.from(singleValue, dataType)) { singleScalar =>
              // Create a column vector from the GPU scalar, associated with the row number.
              singlePartCols(partId) = ColumnVector.fromScalar(singleScalar, rowNum.toInt)
            }
          }

          // Concatenate the individual columns into a single column vector.
          val concatenatedColumn = if (singlePartCols.length > 1) {
            ColumnVector.concatenate(singlePartCols: _*)
          } else {
            singlePartCols.head
          }
          val gpuColumn = GpuColumnVector.from(concatenatedColumn, field.dataType)
          partitionColumns(colIndex) = gpuColumn.incRefCount()
        }
      }
      partitionColumns
    }
  }
}

/**
 * An iterator for merging and retrieving split ColumnarBatches with partition data.
 *
 * It uses a Queue to fetch ColumnarBatch and partition values and merge them only when
 * required.
 */
class SplitBatchWithPartitionDataIterator(
    splitBatchMergers: Seq[ColumnarBatchPartitionMerger]) extends Iterator[ColumnarBatch] {
  private val pending = new mutable.Queue[ColumnarBatchPartitionMerger]() ++ splitBatchMergers

  def hasNext: Boolean = pending.nonEmpty

  def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more split batches with partition data to retrieve.")
    }
    // Retrieves and merges the next split ColumnarBatch with partition data.
    val splitBatchMerger = pending.dequeue()
    splitBatchMerger.mergeWithPartitionData()
  }
}

object SplitColumnarBatchProcessor {
  val cuDF_LIMIT: Long = (1L << 31) - 1

  /**
   * Splits partition data (values and row counts) into smaller batches, ensuring that
   * size of batch is less than cuDF size limit. Then, it utilizes these smaller
   * partitioned batches to split the input batch and merges them to generate
   * an Iterator of split ColumnarBatches.
   *
   * Using an Iterator ensures that the actual merging does not happen until
   * the batch is required, thus avoiding GPU memory wastage.
   *
   * @param batch           - input batch, will be closed after the call returns
   * @param partitionValues - partition values collected from the batch
   * @param partitionRows   - row numbers collected from the batch, and it should have
   *                          the same size with "partitionValues"
   * @param partitionSchema - the partition schema
   * @return a new columnar batch iterator with partition values
   */
  def addPartitionValuesToBatch(
      batch: ColumnarBatch,
      partitionRows: Array[Long],
      partitionValues: Array[InternalRow],
      partitionSchema: StructType): Iterator[ColumnarBatch] = {
    if (partitionSchema.nonEmpty) {
      val partitionedGroups = splitPartitionDataIntoGroups(partitionRows, partitionValues,
        partitionSchema)
      splitAndCombineBatchWithPartitionData(batch, partitionedGroups, partitionSchema)
    } else {
      new SingleGpuColumnarBatchIterator(batch)
    }
  }

  /***
   * Adds a single set of partition values to all rows in a ColumnarBatch.
   * @return a new columnar batch iterator with partition values
   */
  def addSinglePartitionValueToBatch(
      batch: ColumnarBatch,
      partitionValues: InternalRow,
      partitionSchema: StructType): Iterator[ColumnarBatch] = {
    addPartitionValuesToBatch(batch, Array(batch.numRows), Array(partitionValues), partitionSchema)
  }

  /**
   * Splits partitions into smaller batches, ensuring that the batch size
   * for each column does not exceed the specified cudfLimit.
   *
   * Data structures:
   * - sizeOfBatch: HashMap that stores the size of batches for each column.
   * - currentBatch: Array used to hold the rows and partition values of the current batch.
   * - resultBatches: Array that stores the resulting batches after splitting.
   *
   * Algorithm:
   * - Initialize `sizeOfBatch` and `resultBatches`.
   * - Iterate through `partRows`:
   *   - Add (rows, [values]) to the current batch.
   *   - For each column:
   *     - Calculate the size of the current partition for the column.
   *     - Check if adding this partition to the current batch would exceed cuDF limit.
   *       - If it does, split the partition:
   *         - Update the current batch's last value = (new rows, [values]).
   *         - Add it to the result batches.
   *         - Process the remaining rows again.
   *       - Otherwise, increase the `sizeOfBatch` for the column.
   *
   * Example:
   * Input:
   *  partition rows:   [10, 40, 70, 10, 11]
   *  partition values: [ [abc, ab], [bc, ab], [abc, bc], [aa, cc], [ade, fd] ]
   *  cuDF limit:  300 bytes
   *
   * Result:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ]
   * ]
   *
   * @return An array of batches, containing (row counts, partition values) pairs,
   *         such that each batch's size is less than cudfLimit.
   */
  private def splitPartitionDataIntoGroups(partRows: Array[Long], partValues: Array[InternalRow],
      partSchema: StructType): Array[Array[(Long, InternalRow)]] = {
    val resultBatches = ArrayBuffer[Array[(Long, InternalRow)]]()
    val currentBatch = ArrayBuffer[(Long, InternalRow)]()
    // Initialize a HashMap to store the size of the current batch for each column
    val sizeOfBatch = mutable.HashMap.empty[Int, Long]
    var partIndex = 0
    while (partIndex < partRows.length) {
      val rowsInPartition = partRows(partIndex)
      val valuesInPartition = partValues(partIndex)
      // Add the partition's data to the current batch
      currentBatch.append((rowsInPartition, valuesInPartition))
      partSchema.zipWithIndex.foreach {
        case (field, colIndex) if field.dataType == StringType =>
          val singleValue = valuesInPartition.getString(colIndex)
          val sizeOfSingleValue = singleValue.getBytes("UTF-8").length.toLong
          val sizeOfPartition = rowsInPartition * sizeOfSingleValue
          // Check if adding the partition to the current batch exceeds cuDFLimit
          if (sizeOfBatch.getOrElseUpdate(colIndex, 0L) + sizeOfPartition <= cuDF_LIMIT) {
            sizeOfBatch(colIndex) += sizeOfPartition
          } else {
            // Need to split the current partition
            val newRows = (cuDF_LIMIT - sizeOfBatch(colIndex)) / sizeOfSingleValue
            // Update batches and clear states
            currentBatch.update(currentBatch.length - 1, (newRows, valuesInPartition))
            resultBatches.append(currentBatch.toArray)
            currentBatch.clear()
            sizeOfBatch.clear()
            // Reprocess the current partition with remaining rows
            partRows(partIndex) -= newRows
            partIndex -= 1
          }
        case _ =>
        // Assumption: Columns of other data type would not have fit already. Do nothing.
      }
      partIndex += 1
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
   * Note:
   * - Partition values are not converted to GPU ColumnVectors and merged with the smaller
   *   ColumnarBatch to avoid wasting GPU memory.
   * - The actual creation of GPU ColumnVectors and merging is handled by the
   *   SplitBatchIteratorWithPartitionData lazily.
   *
   * @param batch           Input ColumnarBatch.
   * @param splitRowData    Array of arrays containing split row data (RowNums and InternalRows).
   * @param partitionSchema Schema of partition ColumnVectors.
   * @return An Iterator of combined ColumnarBatches.
   */
  private def splitAndCombineBatchWithPartitionData(
      batch: ColumnarBatch,
      splitRowData: Array[Array[(Long, InternalRow)]],
      partitionSchema: StructType): Iterator[ColumnarBatch] = {
    withResource(batch) { _ =>
      val splitIndices = calculateSplitIndices(splitRowData)
      val splitColumnarBatches = splitColumnarBatch(batch, splitIndices)
      // Combine the split GPU ColumnVectors with partition ColumnVectors.
      val combinedBatches = splitColumnarBatches.zip(splitRowData).map {
        case (splitColumnarBatch, splitRowInfo) =>
          (0 until splitColumnarBatch.numCols).map(splitColumnarBatch.column).foreach {
            case gpuCol: GpuColumnVector => gpuCol.incRefCount()
          }
          val spillableBatch = SpillableColumnarBatch(splitColumnarBatch,
            SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          new ColumnarBatchPartitionMerger(spillableBatch, partitionSchema, splitRowInfo)
      }
      new SplitBatchWithPartitionDataIterator(combinedBatches)
    }
  }

  /**
   * Helper method to calculate split indices for dividing ColumnarBatch based on the row counts.
   *
   * Example,
   * Input - Array of arrays having (row counts, InternalRow) pairs:
   * [
   *  [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   *  [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ],
   *  [ (4, [ac, gbc]), (7, [aa, tt]), (15, [ate, afd]) ]
   * ]
   *
   * Result -
   * Row counts for each batch: [ 10+40+30, 7+10+11, 4+7+15 ] = [ 80, 28, 26 ]
   * Split Indices: [ 80, 108 ]
   * -> A ColumnarBatch with 134 rows will be split in three batches
   * having [ 80, 28, 26 ] rows in each.
   *
   * @param splitRowData Array of arrays having (row counts, InternalRow) pairs for each partition.
   * @return A sequence of split indices.
   */
  private def calculateSplitIndices(splitRowData: Array[Array[(Long, InternalRow)]]): Seq[Int] = {
    // Calculate the row counts for each batch
    val rowCountsForEachBatch = splitRowData.map(partitionData =>
      partitionData.map {
        case (rowNum, _) => rowNum
      }.sum.toInt
    )
    // Calculate split indices using cumulative sum
    rowCountsForEachBatch.scanLeft(0)(_ + _).drop(1).dropRight(1)
  }

  /**
   * Split a ColumnarBatch into multiple ColumnarBatches based on specified row counts.
   */
  private def splitColumnarBatch(input: ColumnarBatch,
      splitIndices: Seq[Int]): Seq[ColumnarBatch] = {
    if (splitIndices.isEmpty) {
      Seq(input)
    } else {
      val schema = GpuColumnVector.extractTypes(input)
      val splitInput = withResource(GpuColumnVector.from(input)) { inputTable =>
        inputTable.contiguousSplit(splitIndices: _*)
      }
      closeOnExcept(splitInput) { _ =>
        splitInput.safeMap { table =>
          GpuColumnVectorFromBuffer.from(table, schema)
        }
      }
    }
  }
}

