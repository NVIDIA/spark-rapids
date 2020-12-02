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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A GPU column vector that has been compressed. The columnar data within cannot
 * be accessed directly. This class primarily serves the role of tracking the
 * compressed data and table metadata so it can be decompressed later.
 */
public final class GpuCompressedColumnVector extends GpuColumnVectorBase {
  private final DeviceMemoryBuffer buffer;
  private final TableMeta tableMeta;

  /**
   * Build a columnar batch from a compressed table.
   * NOTE: The data remains compressed and cannot be accessed directly from the columnar batch.
   */
  public static ColumnarBatch from(CompressedTable compressedTable, DataType[] colTypes) {
    return from(compressedTable.buffer(), compressedTable.meta(), colTypes);
  }

  public static boolean isBatchCompressed(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      return false;
    } else {
      return batch.column(0) instanceof GpuCompressedColumnVector;
    }
  }

  /**
   * This should only ever be called from an assertion.
   */
  private static boolean typeConversionAllowed(ColumnMeta columnMeta, DataType colType) {
    DType dt = DType.fromNative(columnMeta.dtypeId(), columnMeta.dtypeScale());
    if (!dt.isNestedType()) {
      return GpuColumnVector.getNonNestedRapidsType(colType).equals(dt);
    }
    if (colType instanceof MapType) {
      MapType mType = (MapType) colType;
      // list of struct of key/value
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      ColumnMeta structCm = columnMeta.children(0);
      if (structCm.dtypeId() != DType.STRUCT.getTypeId().getNativeId()) {
        return false;
      }
      if (structCm.childrenLength() != 2) {
        return false;
      }
      ColumnMeta keyCm = structCm.children(0);
      if (!typeConversionAllowed(keyCm, mType.keyType())) {
        return false;
      }
      ColumnMeta valCm = structCm.children(1);
      return typeConversionAllowed(valCm, mType.valueType());
    } else if (colType instanceof ArrayType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      ColumnMeta tmp = columnMeta.children(0);
      return typeConversionAllowed(tmp, ((ArrayType) colType).elementType());
    } else if (colType instanceof StructType) {
      if (!(dt.equals(DType.STRUCT))) {
        return false;
      }
      StructType st = (StructType) colType;
      final int numChildren = columnMeta.childrenLength();
      if (numChildren != st.size()) {
        return false;
      }
      for (int childIndex = 0; childIndex < numChildren; childIndex++) {
        ColumnMeta tmp = columnMeta.children(childIndex);
        StructField entry = ((StructType) colType).apply(childIndex);
        if (!typeConversionAllowed(tmp, entry.dataType())) {
          return false;
        }
      }
      return true;
    } else if (colType instanceof BinaryType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      ColumnMeta tmp = columnMeta.children(0);
      return tmp.dtypeId() == DType.INT8.getTypeId().getNativeId() ||
          tmp.dtypeId() == DType.UINT8.getTypeId().getNativeId();
    } else {
      // Unexpected type
      return false;
    }
  }

  /**
   * Build a columnar batch from a compressed data buffer and specified table metadata
   * NOTE: The data remains compressed and cannot be accessed directly from the columnar batch.
   */
  public static ColumnarBatch from(DeviceMemoryBuffer compressedBuffer,
      TableMeta tableMeta,
      DataType[] colTypes) {
    long rows = tableMeta.rowCount();
    if (rows != (int) rows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }

    ColumnMeta columnMeta = new ColumnMeta();
    int numColumns = tableMeta.columnMetasLength();
    assert numColumns == colTypes.length : "Size mismatch on types";
    ColumnVector[] columns = new ColumnVector[numColumns];
    try {
      for (int i = 0; i < numColumns; ++i) {
        tableMeta.columnMetas(columnMeta, i);
        DataType type = colTypes[i];
        assert typeConversionAllowed(columnMeta, type) : "Type conversion is not allowed from " +
            columnMeta + " to " + type + " at index " + i;
        compressedBuffer.incRefCount();
        columns[i] = new GpuCompressedColumnVector(type, compressedBuffer, tableMeta);
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
}
