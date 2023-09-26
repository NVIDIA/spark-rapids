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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}



case class SplitBatchReader(spillableBatch: SpillableColumnarBatch,
                            partitionColumns: Seq[GpuColumnVector]) {
  def merge(): ColumnarBatch = {
    withResource(spillableBatch.getColumnarBatch()) { colBatch =>
      if (partitionColumns.isEmpty) {
        colBatch // No need to merge
      } else {
        val rowCounts = partitionColumns.head.getRowCount.toInt
        val splitCols = (0 until colBatch.numCols).map(colBatch.column)
        val resultCols: Seq[ColumnVector] = splitCols ++ partitionColumns
        val resultBatch = new ColumnarBatch(resultCols.toArray, rowCounts)
        resultCols.foreach {
          case gpuCol: GpuColumnVector => gpuCol.incRefCount()
        }
        resultBatch
      }
    }
  }
}

object SplitBatchReader {
  def from(batch: ColumnarBatch,
           partitionColumns: Seq[GpuColumnVector]): SplitBatchReader = {
    val spillableBatch = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    SplitBatchReader(spillableBatch, partitionColumns)
  }

  def from(batch: ColumnarBatch): SplitBatchReader = {
    val spillableBatch = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    SplitBatchReader(spillableBatch, Seq.empty)
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
    partitionValues: Array[Scalar],
    partValueTypes: Array[DataType]) extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = fileReader.next()
  private var outputIter: Iterator[ColumnarBatch] = Iterator.empty


  override def get(): ColumnarBatch = {
    if (partitionValues.isEmpty) {
      fileReader.get()
    } else if (outputIter.hasNext) {
      outputIter.next()
    } else {
      val fileBatch: ColumnarBatch = fileReader.get()
      outputIter = ColumnarPartitionReaderWithPartitionValues.addPartitionValues(fileBatch,
        partitionValues, partValueTypes)
      outputIter.next()
    }
  }

  override def close(): Unit = {
    fileReader.close()
    partitionValues.foreach(_.close())
  }
}

object ColumnarPartitionReaderWithPartitionValues {
  def newReader(partFile: PartitionedFile,
      baseReader: PartitionReader[ColumnarBatch],
      partitionSchema: StructType): PartitionReader[ColumnarBatch] = {
    val partitionValues = partFile.partitionValues.toSeq(partitionSchema)
    val partitionScalars = createPartitionValues(partitionValues, partitionSchema)
    new ColumnarPartitionReaderWithPartitionValues(baseReader, partitionScalars,
      GpuColumnVector.extractTypes(partitionSchema))
  }

  def createPartitionValues(
      partitionValues: Seq[Any],
      partitionSchema: StructType): Array[Scalar] = {
    val partitionScalarTypes = partitionSchema.fields.map(_.dataType)
    partitionValues.zip(partitionScalarTypes).safeMap {
      case (v, t) => GpuScalar.from(v, t)
    }.toArray
  }

  def addPartitionValues(
      fileBatch: ColumnarBatch,
      partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): SplitBatchReaderIterator = {
    withResource(fileBatch) { _ =>
      val partitionColumnsBatch = buildPartitionColumnsBatch(fileBatch.numRows,
        partitionValues, sparkTypes)
      addGpuColumnVectorsToBatch(fileBatch, partitionColumnsBatch)
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
      batchOfPartitionColumns: Seq[Seq[GpuColumnVector]]): SplitBatchReaderIterator = {
    val rowCounts = batchOfPartitionColumns.map(_.headOption.map(_.getRowCount.toInt).getOrElse(0))
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

  private def buildPartitionColumns(
      numRows: Int,
      partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): Seq[GpuColumnVector] = {
    var succeeded = false
    val result = new Array[GpuColumnVector](partitionValues.length)
    try {
      for (i <- result.indices) {
        val cv = GpuColumnVector.from(
          ai.rapids.cudf.ColumnVector.fromScalar(partitionValues(i), numRows), sparkTypes(i))
        result(i) = cv.incRefCount()
      }
      succeeded = true
      result
    } finally {
      if (!succeeded) {
        result.filter(_ != null).safeClose()
      }
    }
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
      partValues: Array[Scalar], sparkTypes: Array[DataType]): Array[Int] = {
    val cuDFLimit: Long = 50000 // (1L << 31) - 1
    // Calculate the maximum size of a String type column.
    // Assumption: Columns of other data type would not have fit already.
    val maxSizeOfSingleValue: Int = sparkTypes.zipWithIndex.collect {
      case (StringType, colIndex) => partValues(colIndex).getUTF8.length
      case _ => 0
    }.max

    // Check if the total size of the partition is within the cuDFLimit
    if (numRows * maxSizeOfSingleValue < cuDFLimit) {
      Array(numRows) // No need to split, return a single batch
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
      resultBatches.toArray
    }
  }

  private def buildPartitionColumnsBatch(numRows: Int, partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): Seq[Seq[GpuColumnVector]] = {
    splitPartitionIntoBatchesScalar(numRows, partitionValues, sparkTypes)
      .map(buildPartitionColumns(_, partitionValues, sparkTypes))
  }
}
