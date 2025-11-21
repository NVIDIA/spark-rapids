/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatchRow;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A wrapper class of ColumnarBatch.
 * This class is used to fix the issue of partition writer for Date type.
 */
public class ColumnarBatchForPartitionWriter implements AutoCloseable {

  final ColumnVector[] columns;
  final int numRows;

  public ColumnarBatchForPartitionWriter(ColumnVector[] columns, int numRows) {
    this.columns = columns;
    this.numRows = numRows;
  }

  public Iterator<InternalRow> rowIterator() {
    final int maxRows = this.numRows;

    // here use a fixed ColumnBatchRow to fix the issue of partition writer for Date type.
    final ColumnarBatchRowForPartitionWriter row =
        new ColumnarBatchRowForPartitionWriter(new ColumnarBatchRow(columns));
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        row.setRowId(rowId++);
        return row;
      }
    };
  }

  @Override
  public void close() throws Exception {
    Throwable error = null;

    for (ColumnVector c : columns) {
      try {
        c.close();
      } catch (Exception e) {
        if (error == null) {
          error = e;
        } else {
          error.addSuppressed(e);
        }
      }
    }

    if (error != null) {
      throw new RuntimeException("Errors occurred while closing ColumnVectors", error);
    }
  }
}