/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Scalar
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.{DataType, StructType}
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

object ColumnarPartitionReaderWithPartitionValues extends Arm {
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
}
