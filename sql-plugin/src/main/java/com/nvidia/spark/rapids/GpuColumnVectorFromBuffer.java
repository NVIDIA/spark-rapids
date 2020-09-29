/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.Table;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/** GPU column vector carved from a single buffer, like those from cudf's contiguousSplit. */
public final class GpuColumnVectorFromBuffer extends GpuColumnVector {
  private final DeviceMemoryBuffer buffer;

  /**
   * Get a ColumnarBatch from a set of columns in a contiguous table. This differs from the
   * GpuColumnVector version in that it produces GpuColumnVectorFromBuffer instances that are
   * useful for determining the original buffer from which the column was derived.
   * This will increment the reference count for all columns converted so you will need to close
   * both the table that is passed in and the batch returned to be sure that there are no leaks.
   *
   * @param contigTable contiguous table
   * @return batch of GpuColumnVectorFromBuffer instances derived from the table
   */
  public static ColumnarBatch from(ContiguousTable contigTable) {
    DeviceMemoryBuffer buffer = contigTable.getBuffer();
    Table table = contigTable.getTable();
    long rows = table.getRowCount();
    if (rows != (int) rows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }
    int numColumns = table.getNumberOfColumns();
    GpuColumnVector[] columns = new GpuColumnVector[numColumns];
    try {
      for (int i = 0; i < numColumns; ++i) {
        ColumnVector v = table.getColumn(i);
        DataType type = getSparkTypeFrom(v);
        columns[i] = new GpuColumnVectorFromBuffer(type, v.incRefCount(), buffer);
      }
      return new ColumnarBatch(columns, (int) rows);
    } catch (Exception e) {
      for (GpuColumnVector v : columns) {
        if (v != null) {
          v.close();
        }
      }
      throw e;
    }
  }

  /**
   * Note that this type of [[GpuColumnVector]] takes a single buffer. The buffer is
   * shared between the various components of the vector (data, validity, offsets), and held
   * in one contiguous chunk.
   *
   * This class does not need to override close, because [[columnVector]]
   * already holds references to the buffer, and will be released once
   * [[ColumnVector]] is closed.
   *
   * See [[from]] in this class for more info on its lifecycle.
   *
   * @param type the spark data type for this column
   * @param cudfColumn a ColumnVector instance
   * @param buffer the buffer to hold
   */
  private GpuColumnVectorFromBuffer(DataType type, ColumnVector cudfColumn,
      DeviceMemoryBuffer buffer) {
    super(type, cudfColumn);
    this.buffer = buffer;
  }

  /**
   * Get the underlying contiguous buffer, shared between columns of the original
   * `ContiguousTable`
   * @return contiguous (data, validity and offsets) device memory buffer
   */
  public DeviceMemoryBuffer getBuffer() {
    return buffer;
  }
}
