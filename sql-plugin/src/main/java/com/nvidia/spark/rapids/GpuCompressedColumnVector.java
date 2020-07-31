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

import ai.rapids.cudf.DType;
import ai.rapids.cudf.DeviceMemoryBuffer;
import com.nvidia.spark.rapids.format.ColumnMeta;
import com.nvidia.spark.rapids.format.TableMeta;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A GPU column vector that has been compressed. The columnar data within cannot
 * be accessed directly. This class primarily serves the role of tracking the
 * compressed data and table metadata so it can be decompressed later.
 */
public final class GpuCompressedColumnVector extends ColumnVector {
  private final DeviceMemoryBuffer buffer;
  private final TableMeta tableMeta;

  public static ColumnarBatch from(CompressedTable compressedTable) {
    DeviceMemoryBuffer buffer = compressedTable.buffer();
    TableMeta tableMeta = compressedTable.meta();
    long rows = tableMeta.rowCount();
    if (rows != (int) rows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }

    ColumnMeta columnMeta = new ColumnMeta();
    int numColumns = tableMeta.columnMetasLength();
    ColumnVector[] columns = new ColumnVector[numColumns];
    try {
      for (int i = 0; i < numColumns; ++i) {
        tableMeta.columnMetas(columnMeta, i);
        DType dtype = DType.fromNative(columnMeta.dtype());
        DataType type = GpuColumnVector.getSparkType(dtype);
        DeviceMemoryBuffer slicedBuffer = buffer.slice(0, buffer.getLength());
        columns[i] = new GpuCompressedColumnVector(type, slicedBuffer, tableMeta);
      }
    } catch (Throwable t) {
      for (int i = 0; i < numColumns; ++i) {
        if (columns[i] != null) {
          columns[i].close();
        }
      }
      throw t;
    }

    return new ColumnarBatch(columns, (int) rows);
  }

  private GpuCompressedColumnVector(DataType type, DeviceMemoryBuffer buffer, TableMeta tableMeta) {
    super(type);
    this.buffer = buffer;
    this.tableMeta = tableMeta;
  }

  public DeviceMemoryBuffer getBuffer() {
    return buffer;
  }

  public TableMeta getTableMeta() {
    return tableMeta;
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public boolean hasNull() {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public int numNulls() {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public boolean isNullAt(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public byte getByte(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public short getShort(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public int getInt(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public long getLong(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public float getFloat(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public double getDouble(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new IllegalStateException("column vector is compressed");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new IllegalStateException("column vector is compressed");
  }
}
