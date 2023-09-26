/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}


abstract class SplitInfo() {
  val rowCount: Long
  def buildPartitionColumns(): Seq[GpuColumnVector]
}

case class MultiPartSplitInfo(partRowNumsAndValues: Array[(Long, InternalRow)],
      partSchema: StructType) extends SplitInfo {
  val rowCount: Long = partRowNumsAndValues.map(_._1).sum

  def buildPartitionColumns(): Seq[GpuColumnVector] = {
    // build the partitions vectors for all partitions within each column
    // and concatenate those together then go to the next column
    closeOnExcept(new Array[GpuColumnVector](partSchema.length)) { partsCols =>
      for ((field, colIndex) <- partSchema.zipWithIndex) {
        val dataType = field.dataType
        withResource(new Array[ColumnVector](partRowNumsAndValues.length)) {
          onePartCols =>
          partRowNumsAndValues.zipWithIndex.foreach { case ((rowNum, valueRow), partId) =>
            val singleValue = valueRow.get(colIndex, dataType)
            withResource(GpuScalar.from(singleValue, dataType)) { oneScalar =>
              onePartCols(partId) = ColumnVector.fromScalar(oneScalar, rowNum.toInt)
            }
          }
          val res = if (onePartCols.length > 1) {
            ColumnVector.concatenate(onePartCols: _*)
          } else {
            onePartCols.head
          }
          partsCols(colIndex) = GpuColumnVector.from(res, field.dataType).incRefCount()
        }
      }
      partsCols
    }
  }
}

case class SinglePartSplitInfo(partRowNumsAndValues: (Int, InternalRow),
    partSchema: StructType) extends SplitInfo {
  val rowCount: Long = partRowNumsAndValues._1

  def buildPartitionColumns(): Seq[GpuColumnVector] = {
    var succeeded = false
    val result = new Array[GpuColumnVector](partSchema.length)
    try {
      for ((field, colIndex) <- partSchema.zipWithIndex) {
        val dataType = field.dataType
        partRowNumsAndValues match {
          case (rowNum, valueRow) =>
            val singleValue = valueRow.get (colIndex, dataType)
            val cv = withResource(GpuScalar.from(singleValue, dataType)) {
              oneScalar => ColumnVector.fromScalar(oneScalar, rowNum)
            }
            result (colIndex) = GpuColumnVector.from (cv, dataType).incRefCount ()
        }
      }
      succeeded = true
      result
    } finally {
      if (!succeeded) {
        result.filter(_ != null).safeClose()
      }
    }
  }
}

case class SplitBatchReader(spillableBatch: SpillableColumnarBatch,
                            splitInfoOp: Option[SplitInfo]) {
  def merge(): ColumnarBatch = {
    withResource(spillableBatch.getColumnarBatch()) { colBatch =>
      splitInfoOp match {
        case Some(splitInfo) =>
          val partitionColumns = splitInfo.buildPartitionColumns()
          val rowCounts = partitionColumns.head.getRowCount.toInt
          val splitCols = (0 until colBatch.numCols).map (colBatch.column)
          val resultCols: Seq[SparkVector] = splitCols ++ partitionColumns
          val resultBatch = new ColumnarBatch (resultCols.toArray, rowCounts)
          resultCols.foreach {
            case gpuCol: GpuColumnVector => gpuCol.incRefCount ()
          }
          resultBatch
        case None =>
          colBatch
      }
    }
  }
}

object SplitBatchReader {
  def from(batch: ColumnarBatch,
           splitInfo: SplitInfo): SplitBatchReader = {
    val spillableBatch = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    SplitBatchReader(spillableBatch, Some(splitInfo))
  }

  def from(batch: ColumnarBatch): SplitBatchReader = {
    val spillableBatch = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    SplitBatchReader(spillableBatch, None)
  }
}

class SplitBatchReaderIterator(
  splitBatches: Seq[SplitBatchReader]) extends Iterator[ColumnarBatch] {
  private val pending = new scala.collection.mutable.Queue[SplitBatchReader]() ++ splitBatches

  override def hasNext: Boolean = pending.nonEmpty

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    val sbr = pending.dequeue()
    sbr.merge()
  }
}

/**
 * A wrapper reader that always appends partition values to the ColumnarBatch produced by the input
 * reader `fileReader`. Each scalar value is splatted to a column with the same number of
 * rows as the batch returned by the reader.
 */
class ColumnarPartitionReaderWithPartitionValues(
    fileReader: PartitionReader[ColumnarBatch],
    partitionValues: InternalRow,
    partitionSchema: StructType) extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = fileReader.next()
  private var outputIter: Iterator[ColumnarBatch] = Iterator.empty


  override def get(): ColumnarBatch = {
    if (partitionValues.numFields == 0) {
      fileReader.get()
    } else if (outputIter.hasNext) {
      outputIter.next()
    } else {
      val fileBatch: ColumnarBatch = fileReader.get()
      outputIter = ColumnarPartitionReaderWithPartitionValues.addPartitionValues(fileBatch,
        partitionValues, partitionSchema)
      outputIter.next()
    }
  }

  override def close(): Unit = {
    fileReader.close()
    // TODO: Do we need to close InternalRow?
    // partitionValues.foreach(_.close())
  }
}

object ColumnarPartitionReaderWithPartitionValues {
  def newReader(partFile: PartitionedFile,
      baseReader: PartitionReader[ColumnarBatch],
      partitionSchema: StructType): PartitionReader[ColumnarBatch] = {
    new ColumnarPartitionReaderWithPartitionValues(baseReader, partFile.partitionValues,
      partitionSchema)
  }

  def addPartitionValues(
      fileBatch: ColumnarBatch,
      partitionValues: InternalRow,
      partitionSchema: StructType): SplitBatchReaderIterator = {
    withResource(fileBatch) { _ =>
      val splitRowNums = splitPartitionIntoBatchesScalar(fileBatch.numRows, partitionValues,
        partitionSchema)
      val partitionColumnsBatch = splitRowNums.map(SinglePartSplitInfo(_, partitionSchema))
      addGpuColumnVectorsToBatch(fileBatch, partitionColumnsBatch)
    }
  }

  def addMultiplePartitionValues(
      cb: ColumnarBatch,
      partRows: Array[Long],
      partValues: Array[InternalRow],
      partSchema: StructType): SplitBatchReaderIterator = {
    withResource(cb) { _ =>
      val splitRowNumsSeq = splitPartitionIntoBatches(partRows, partValues, partSchema)
      val batchOfPartCols = splitRowNumsSeq.map(MultiPartSplitInfo(_, partSchema))
      ColumnarPartitionReaderWithPartitionValues.addGpuColumnVectorsToBatch(cb, batchOfPartCols)
    }
  }

  /**
   * Split a ColumnarBatch according to a list of indices.
   *
   * TODO: Check SplillPriority?
   */
  def makeSplits(input: ColumnarBatch,
      rowCounts: Seq[Int]): Seq[ColumnarBatch] = {
    val splitIndices = rowCounts.scanLeft(0)(_ + _).drop(1).dropRight(1)
    val schema = GpuColumnVector.extractTypes(input)
    if (splitIndices.isEmpty) {
      Seq(input)
    } else {
      val splitInput = withResource(GpuColumnVector.from(input)) { table =>
        table.contiguousSplit(splitIndices: _*)
      }
      closeOnExcept(splitInput) { _ =>
        splitInput.safeMap { table =>
          GpuColumnVectorFromBuffer.from(table, schema)
        }
      }
    }
  }

  def addGpuColumnVectorsToBatch(
      fileBatch: ColumnarBatch,
      batchOfPartitionColumns: Seq[SplitInfo]): SplitBatchReaderIterator = {
    val rowCounts = batchOfPartitionColumns.map(_.rowCount.toInt)
    val columnarBatchSeqs = makeSplits(fileBatch, rowCounts)
    // Combine the split GPU ColumnVectors with partition ColumnVectors.
    val splitBatches = columnarBatchSeqs.zip(batchOfPartitionColumns).map {
      case (cb, partCols) =>
        (0 until cb.numCols).map(cb.column).foreach {
          case gpuCol: GpuColumnVector => gpuCol.incRefCount()
        }
        SplitBatchReader.from(cb, partCols)
    }
    new SplitBatchReaderIterator(splitBatches)
  }

  /**
   * Splits the partition into smaller batches such that the batch size
   * does not exceed cudfLimit for any column.
   *
   * Algorithm:
   * - Calculate the max size of single value among partition columns.
   * - Calculate the size of partition = numRows * maxSizeOfSingleValue.
   * - If size of partition exceeds cudfLimit, split the partition into batches.
   *    - Calculate newRows = cudfLimit / maxSizeOfSingleValue.
   *    - Add newRows to resultBatches.
   *    - Update remainingNumRows and remainingSizeOfPartition.
   *    - Repeat until remainingSizeOfPartition is less than cudfLimit.
   *  - Return the array of batch sizes in resultBatches.
   *
   * Example:
   * Input:
   *    numRows: 50
   *    partValues: [ abc, abcdef ]
   *    cudfLimit: 140 bytes
   *
   * Result:
   *    [ 23, 23, 4 ]
   *
   * @return An array of batch sizes, such that the size of each batch is
   *         below cudfLimit.
   */
  private def splitPartitionIntoBatchesScalar(numRows: Int,
      partValues: InternalRow, partitionSchema: StructType): Array[(Int, InternalRow)] = {
    val cuDFLimit: Long = 50000 // (1L << 31) - 1
    // Calculate the maximum size of a String type column.
    // Assumption: Columns of other data type would not have fit already.
    val maxSizeOfSingleValue: Int = partitionSchema.zipWithIndex.collect {
      case (field, colIndex) if field.dataType == StringType =>
        val singleValue = partValues.getString(colIndex)
        val sizeOfSingleValue = singleValue.getBytes("UTF-8").length
        sizeOfSingleValue
      case _ => 0
    }.max

    // Check if the total size of the partition is within the cuDFLimit
    if (numRows * maxSizeOfSingleValue < cuDFLimit) {
      Array((numRows, partValues)) // No need to split, return a single batch
    } else {
      // Need to split the current partition
      val resultBatches = ArrayBuffer[Int]()
      var remainingNumRows = numRows
      var remainingSizeOfPartition = remainingNumRows * maxSizeOfSingleValue
      val newRows = (cuDFLimit / maxSizeOfSingleValue).toInt
      // Split the partition into batches
      while (remainingSizeOfPartition > cuDFLimit) {
        resultBatches.append(newRows)
        remainingNumRows -= newRows
        remainingSizeOfPartition = remainingNumRows * maxSizeOfSingleValue
      }
      // Add the remaining rows as the last batch
      if (remainingNumRows > 0) {
        resultBatches.append(remainingNumRows)
      }
      resultBatches.map(x => (x, partValues)).toArray
    }
  }

  /**
   * Splits partitions into smaller batches such that the batch size
   * does not exceed cudfLimit for any column.
   *
   * Data Structures used:
   * - sizeOfBatch: HashMap to store the size of batches for each column
   * - currentBatch: Array to hold the current batch's rows and partitionValues
   * - resultBatches: Array to store the resulting batches
   *
   * Algorithm:
   * - Initialize `sizeOfBatch` and `resultBatches`.
   * - Iterate through `partRows`:
   *    - Add (rows, [values]) to the current batch.
   *    - For each column:
   *        - Calculate the size of the current partition for the column.
   *        - Check if adding this partition to the current batch exceeds cudfLimit.
   *            - If it does, split the partition:
   *              - Update the current batch's last value = (new rows, [values])
   *              - Add it to the result batches.
   *              - Process the remaining rows again
   *            - Otherwise, increase the `sizeOfBatch` for the column.
   *
   * Example:
   * Input:
   * partRows:   [10, 40, 70, 10, 11]
   * partValues: [ [abc, ab], [bc, ab], [abc, bc], [aa, cc], [ade, fd] ]
   * cudfLimit:  300 bytes
   *
   * Result:
   * [
   * [ (10, [abc, ab]), (40, [bc, ab]), (63, [abc, bc]) ],
   * [ (7, [abc, bc]), (10, [aa, cc]), (11, [ade, fd]) ]
   * ]
   *
   * @return An array of batches, containing (row sizes, partitionValues) pairs,
   *         such that each batch's size is less than cudfLimit.
   */
  private def splitPartitionIntoBatches(partRows: Array[Long], partValues: Array[InternalRow],
      partSchema: StructType): Array[Array[(Long, InternalRow)]] = {
    val cuDFLimit = 50000 // (1L << 31) - 1
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
          if (sizeOfBatch.getOrElseUpdate(colIndex, 0L) + sizeOfPartition <= cuDFLimit) {
            sizeOfBatch(colIndex) += sizeOfPartition
          } else {
            // Need to split the current partition
            val newRows = (cuDFLimit - sizeOfBatch(colIndex)) / sizeOfSingleValue
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
}
