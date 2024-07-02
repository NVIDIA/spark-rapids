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

package com.nvidia.spark.rapids;

import ai.rapids.cudf.ContiguousTable;
import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.HostMemoryBuffer;
import com.nvidia.spark.rapids.format.TableMeta;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector that tracks a packed (or compressed) table on host. Unlike a normal
 * host column vector, the columnar data within cannot be accessed directly.
 * This is intended to only be used during shuffle after the data is partitioned and
 * before it is serialized.
 */
public final class PackedTableHostColumnVector extends ColumnVector {

  private static final String BAD_ACCESS_MSG = "Column is packed";

  private final TableMeta tableMeta;
  private final HostMemoryBuffer tableBuffer;

  PackedTableHostColumnVector(TableMeta tableMeta, HostMemoryBuffer tableBuffer) {
    super(DataTypes.NullType);
    long rows = tableMeta.rowCount();
    int batchRows = (int) rows;
    if (rows != batchRows) {
      throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
    }
    this.tableMeta = tableMeta;
    this.tableBuffer = tableBuffer;
  }

  /**
   * Create a columnar batch from a table meta and a host buffer.
   * The host buffer will be taken over, so do not use it any longer.
   */
  public static ColumnarBatch from(TableMeta meta, HostMemoryBuffer hostBuf) {
    ColumnVector column = new PackedTableHostColumnVector(meta, hostBuf);
    return new ColumnarBatch(new ColumnVector[] { column }, (int) meta.rowCount());
  }

  private static ColumnarBatch from(TableMeta meta, DeviceMemoryBuffer devBuf) {
    HostMemoryBuffer tableBuf;
    try(HostMemoryBuffer buf = HostMemoryBuffer.allocate(devBuf.getLength())) {
      buf.copyFromDeviceBuffer(devBuf);
      buf.incRefCount();
      tableBuf = buf;
    }
    ColumnVector column = new PackedTableHostColumnVector(meta, tableBuf);
    return new ColumnarBatch(new ColumnVector[] { column }, (int) meta.rowCount());
  }

  /** Both the input table and output batch should be closed. */
  public static ColumnarBatch from(CompressedTable table) {
    return from(table.meta(), table.buffer());
  }

  /** Both the input table and output batch should be closed. */
  public static ColumnarBatch from(ContiguousTable table) {
    return from(MetaUtils.buildTableMeta(0, table), table.getBuffer());
  }

  /** Returns true if this columnar batch uses a packed table on host */
  public static boolean isBatchPackedOnHost(ColumnarBatch batch) {
    return batch.numCols() == 1 && batch.column(0) instanceof PackedTableHostColumnVector;
  }

  public TableMeta getTableMeta() {
    return tableMeta;
  }

  public HostMemoryBuffer getTableBuffer() {
    return tableBuffer;
  }

  @Override
  public void close() {
    if (tableBuffer != null) {
      tableBuffer.close();
    }
  }

  @Override
  public boolean hasNull() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public int numNulls() {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public boolean isNullAt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public byte getByte(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public short getShort(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public int getInt(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public long getLong(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public float getFloat(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public double getDouble(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new IllegalStateException(BAD_ACCESS_MSG);
  }
}
