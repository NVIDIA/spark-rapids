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

  override def get(): ColumnarBatch = {
    if (partitionValues.isEmpty) {
      fileReader.get()
    } else {
      val fileBatch: ColumnarBatch = fileReader.get()
      // TODO: Replace usage with `addPartitionValuesIter`
      ColumnarPartitionReaderWithPartitionValues.addPartitionValues(fileBatch,
        partitionValues, partValueTypes)
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
      sparkTypes: Array[DataType]): ColumnarBatch = {
    withResource(fileBatch) { _ =>
      closeOnExcept(buildPartitionColumns(fileBatch.numRows, partitionValues, sparkTypes)) {
        partitionColumns => addGpuColumVectorsToBatch(fileBatch, partitionColumns)
      }
    }
  }

  def addPartitionValuesIter(
      fileBatch: ColumnarBatch,
      partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): Iterator[ColumnarBatch] = {
    withResource(fileBatch) { _ =>
      // TODO: Use closeOnExcept for `partitionColumnsBatch`
      val partitionColumnsBatch = buildPartitionColumnsBatch(fileBatch.numRows, partitionValues,
        sparkTypes)
      addGpuColumVectorsToBatchIter(fileBatch, partitionColumnsBatch)
    }
  }

  /**
   * The caller is responsible for closing the fileBatch passed in.
   */
  def addGpuColumVectorsToBatch(
      fileBatch: ColumnarBatch,
      partitionColumns: Array[GpuColumnVector]): ColumnarBatch = {
    val fileBatchCols = (0 until fileBatch.numCols).map(fileBatch.column)
    val resultCols = fileBatchCols ++ partitionColumns
    val result = new ColumnarBatch(resultCols.toArray, fileBatch.numRows)
    fileBatchCols.foreach(_.asInstanceOf[GpuColumnVector].incRefCount())
    result
  }

  /**
   * Splits each Gpu Column Vector into multiple batches based on row counts.
   *
   * Example,
   * columnVectors = [ Cv('pqr', 55), Cv('111', 55), Cv('ab1', 55)]
   * rowCounts = [ 20, 10, 15, 10 ]
   * splitIndices = [ 20, 30, 35 ]
   * Result:
   *    [
   *      [Cv('pqr',20), Cv('pqr',10), Cv('pqr',15), Cv('pqr',10)],
   *      [Cv('111',20), Cv('111',10), Cv('111',15), Cv('111',10)],
   *      [Cv('ab1',20), Cv('ab1',10), Cv('ab1',15), Cv('ab1',10)]
   *    ]
   */
  private def splitColumnVector(columnVectors: Seq[ColumnVector],
      rowCounts: Array[Long]): Seq[Array[GpuColumnVector]] = {
    // Calculate indices from rowCounts
    val splitIndices = rowCounts.scanLeft(0) {
      case (cumulativeCount, partitionIndex) =>
        // ColumnVector#split() requires indices to be 'int'
        cumulativeCount + partitionIndex.toInt
    }.drop(1).dropRight(1)

    columnVectors.map {
      case gpuColumnVector: GpuColumnVector =>
        val splitColumns = gpuColumnVector.getBase.split(splitIndices: _*)
        splitColumns.map(splitCol => GpuColumnVector.from(splitCol, gpuColumnVector.dataType()))
    }
  }

  def addGpuColumVectorsToBatchIter(
     fileBatch: ColumnarBatch,
     batchOfPartitionColumns: Array[Array[GpuColumnVector]]): Iterator[ColumnarBatch] = {
    val fileBatchCols = (0 until fileBatch.numCols).map(fileBatch.column)
    val rowCounts = batchOfPartitionColumns.map(_.head.getRowCount)
    val columnVectorsSeq = splitColumnVector(fileBatchCols, rowCounts)
    // Combine the split GPU ColumnVectors with partition ColumnVectors.
    batchOfPartitionColumns.zipWithIndex.map {
      case (partitionColumns, index) =>
        // TODO: Can we transpose `columnVectorsSeq` or it would be more expensive?
        val splitCols = columnVectorsSeq.map(col => col(index))
        val resultCols = splitCols ++ partitionColumns
        val batchNumRows = resultCols.headOption.map(_.getRowCount.toInt).getOrElse(0)
        val resultBatch = new ColumnarBatch(resultCols.toArray, batchNumRows)
        splitCols.collect { case gpuCol: GpuColumnVector => gpuCol.incRefCount() }
        resultBatch
    }.toIterator
  }

  private def buildPartitionColumns(
      numRows: Int,
      partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): Array[GpuColumnVector] = {
    var succeeded = false
    val result = new Array[GpuColumnVector](partitionValues.length)
    try {
      for (i <- result.indices) {
        result(i) = GpuColumnVector.from(
          ai.rapids.cudf.ColumnVector.fromScalar(partitionValues(i), numRows), sparkTypes(i))
      }
      succeeded = true
      result
    } finally {
      if (!succeeded) {
        result.filter(_ != null).safeClose()
      }
    }
  }

  // scalastyle:off line.size.limit
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
  // scalastyle:on line.size.limit
  private def splitPartitionIntoBatchesScalar(numRows: Int,
      partValues: Array[Scalar], sparkTypes: Array[DataType]): Array[Int] = {
    val cuDFLimit: Long = (1L << 31) - 1
    // Calculate the maximum size of a String type column.
    // Assumption: Columns of other data type would not have fit already.
    val maxSizeOfSingleValue: Int = sparkTypes.zipWithIndex.collect {
      case (StringType, colIndex) => partValues(colIndex).getUTF8.length
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
      sparkTypes: Array[DataType]): Array[Array[GpuColumnVector]] = {
    splitPartitionIntoBatchesScalar(numRows, partitionValues, sparkTypes)
      .map(buildPartitionColumns(_, partitionValues, sparkTypes))
  }
}
