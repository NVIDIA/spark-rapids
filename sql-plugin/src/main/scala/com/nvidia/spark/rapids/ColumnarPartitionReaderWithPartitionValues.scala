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
import org.apache.spark.sql.vectorized.ColumnarBatch

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
    val partitionColumnsBatch = buildPartitionColumnsBatch(fileBatch.numRows, partitionValues,
      sparkTypes)
    addGpuColumVectorsToBatchIter(fileBatch, partitionColumnsBatch)
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

  def addGpuColumVectorsToBatchIter(
     fileBatch: ColumnarBatch,
     partitionColumnVectors: Array[Array[GpuColumnVector]]): Iterator[ColumnarBatch] = {
    val fileBatchCols = (0 until fileBatch.numCols).map(fileBatch.column)

    // Calculate the cumulative row counts for splitting the GPU ColumnVectors.
    // Note:
    //  1. ai.rapids.cudf.ColumnView#split only accepts 'int' indices
    //  2. Split indices must be in cumulative format
    val cumulativeRowCounts = partitionColumnVectors.scanLeft(0) {
      case (cumulativeCount, partitionColumns) =>
        cumulativeCount + partitionColumns.head.getRowCount.toInt
    }.drop(1).dropRight(1)
    // Split each column into multiple batches based on the split indices
    val splitColumnVectors = fileBatchCols.map {
      case gpuColumnVector: GpuColumnVector =>
        val splitColumns = gpuColumnVector.getBase.split(cumulativeRowCounts: _*)
        splitColumns.map(splitCol => GpuColumnVector.from(splitCol, gpuColumnVector.dataType()))
    }

    // Combine the split GPU ColumnVectors with partition ColumnVectors.
    partitionColumnVectors.zipWithIndex.map {
      case (partitionColumns, index) =>
        val splitCols = splitColumnVectors.map(col => col(index))
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

  /**
   * Splits the numRows into multiple batches such that sum
   * of sizes does not exceed cudf limit
   *
   * Example,
   * Input:
   *  numRows:    50
   *  partValues: [ abc, abcdef ]
   *
   * Assume:
   *  limit:      140 bytes
   *
   * Split Calculation:
   *  Max Value Size: 6 (colIndex=2)
   *  Need to split 3 times
   *
   * Result: [ 23, 23, 4 ]
   *
   * @return An array of rows
   */
  private def splitPartitionIntoBatchesScalar(numRows: Int,
      partValues: Array[Scalar], sparkTypes: Array[DataType]): Array[Int] = {
    // Calculate the max size of String type as this will be the bottleneck
    val maxValueSize = sparkTypes.zipWithIndex.map {
      case (StringType, colIndex) => partValues(colIndex).getUTF8.length
      case _ => 0
    }.max

    val cuDFLimit = (1L << 31) - 1
    val partitionInfo = ArrayBuffer[Int]()
    var remainingNumRows = numRows
    while (cuDFLimit < remainingNumRows * maxValueSize) {
      val newRowNums = (cuDFLimit / maxValueSize).toInt
      remainingNumRows -= newRowNums
      partitionInfo.append(newRowNums)
    }
    partitionInfo.append(remainingNumRows)
    partitionInfo.toArray
  }

  private def buildPartitionColumnsBatch(numRows: Int, partitionValues: Array[Scalar],
      sparkTypes: Array[DataType]): Array[Array[GpuColumnVector]] = {
    splitPartitionIntoBatchesScalar(numRows, partitionValues, sparkTypes)
      .map(buildPartitionColumns(_, partitionValues, sparkTypes))
  }
}
