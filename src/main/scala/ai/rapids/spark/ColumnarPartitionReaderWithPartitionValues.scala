/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.Scalar

import org.apache.spark.sql.sources.v2.reader.PartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch

class ColumnarPartitionReaderWithPartitionValues(
    fileReader: PartitionReader[ColumnarBatch],
    partitionValues: Array[Scalar]) extends PartitionReader[ColumnarBatch] {
  override def next(): Boolean = fileReader.next()

  override def get(): ColumnarBatch = {
    if (partitionValues.isEmpty) {
      fileReader.get()
    } else {
      val fileBatch: ColumnarBatch = fileReader.get()
      var partitionColumns: Array[GpuColumnVector] = null
      try {
        partitionColumns = buildPartitionColumns(fileBatch.numRows)
        val fileBatchCols = (0 until fileBatch.numCols).map(fileBatch.column)
        val resultCols = fileBatchCols ++ partitionColumns
        val result = new ColumnarBatch(resultCols.toArray, fileBatch.numRows)
        fileBatchCols.foreach(_.asInstanceOf[GpuColumnVector].inRefCount())
        partitionColumns = null
        result
      } finally {
        if (fileBatch != null) {
          fileBatch.close()
        }
        if (partitionColumns != null) {
          partitionColumns.foreach(_.close())
        }
      }
    }
  }

  override def close(): Unit = fileReader.close()

  private def buildPartitionColumns(numRows: Int): Array[GpuColumnVector] = {
    var succeeded = false
    val result = new Array[GpuColumnVector](partitionValues.length)
    try {
      for (i <- result.indices) {
        result(i) = GpuColumnVector.from(ai.rapids.cudf.ColumnVector.fromScalar(partitionValues(i), numRows))
      }
      succeeded = true
      result
    } finally {
      if (!succeeded) {
        result.filter(_ != null).foreach(_.close())
      }
    }
  }
}
