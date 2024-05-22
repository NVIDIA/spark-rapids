/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuDeltaParquetFileFormatUtils {
  /**
   * Row number of the row in the file. When used with [[FILE_PATH_COL]] together, it can be used
   * as unique id of a row in file. Currently to correctly calculate this, the called needs to
   * set both [[isSplitable]] to false, and [[RapidsConf.PARQUET_READER_TYPE]] to "PERFILE".
   */
  val METADATA_ROW_IDX_COL: String = "__metadata_row_index"
  val METADATA_ROW_IDX_FIELD: StructField = StructField(METADATA_ROW_IDX_COL, LongType,
    nullable = false)

  /**
   * File path of the file that the row came from.
   */
  val FILE_PATH_COL: String = "_metadata_file_path"
  val FILE_PATH_FIELD: StructField = StructField(FILE_PATH_COL, StringType, nullable = false)

  /**
   * Add a metadata column to the iterator. Currently only support [[METADATA_ROW_IDX_COL]].
   */
  def addMetadataColumnToIterator(
      schema: StructType,
      input: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    val metadataRowIndexCol = schema.fieldNames.indexOf(METADATA_ROW_IDX_COL)
    if (metadataRowIndexCol == -1) {
      return input
    }
    var rowIndex = 0L
    input.map { batch =>
      withResource(batch) { _ =>
        val newBatch = addRowIdxColumn(metadataRowIndexCol, rowIndex, batch)
        rowIndex += batch.numRows()
        newBatch
      }
    }
  }

  private def addRowIdxColumn(
      rowIdxPos: Int,
      rowIdxStart: Long,
      batch: ColumnarBatch): ColumnarBatch = {
    val rowIdxCol = withResource(Scalar.fromLong(rowIdxStart)) { start =>
      GpuColumnVector.from(CudfColumnVector.sequence(start, batch.numRows()),
        METADATA_ROW_IDX_FIELD.dataType)
    }

    closeOnExcept(rowIdxCol) { rowIdxCol =>
      // Replace row_idx column
      val columns = new Array[ColumnVector](batch.numCols())
      for (i <- 0 until batch.numCols()) {
        if (i == rowIdxPos) {
          columns(i) = rowIdxCol
        } else {
          columns(i) = batch.column(i) match {
            case gpuCol: GpuColumnVector => gpuCol.incRefCount()
            case col => col
          }
        }
      }

      new ColumnarBatch(columns, batch.numRows())
    }
  }
}
