
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

import ai.rapids.cudf.DeviceMemoryBuffer;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostMemoryBuffer;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Optional;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public final class RapidsHostColumnVector extends ColumnVector {

  /**
   * Get the underlying host cudf columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static ai.rapids.cudf.HostColumnVector[] extractBases(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    ai.rapids.cudf.HostColumnVector[] vectors = new ai.rapids.cudf.HostColumnVector[numColumns];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((RapidsHostColumnVector)batch.column(i)).getBase();
    }
    return vectors;
  }

  /**
   * Get the underlying spark compatible host columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static RapidsHostColumnVector[] extractColumns(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    RapidsHostColumnVector[] vectors = new RapidsHostColumnVector[numColumns];

    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((RapidsHostColumnVector)batch.column(i));
    }
    return vectors;
  }


  private final ai.rapids.cudf.HostColumnVector cudfCv;

  /**
   * Sets up the data type of this column vector.
   */
  RapidsHostColumnVector(DataType type, ai.rapids.cudf.HostColumnVector cudfCv) {
    super(type);
    // TODO need some checks to be sure everything matches
    this.cudfCv = cudfCv;
  }

  public RapidsHostColumnVector incRefCount() {
    // Just pass through the reference counting
    cudfCv.incRefCount();
    return this;
  }

  @Override
  public void close() {
    // Just pass through the reference counting
    cudfCv.close();
  }

  @Override
  public boolean hasNull() {
    return cudfCv.hasNulls();
  }

  @Override
  public int numNulls() {
    return (int) cudfCv.getNullCount();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return cudfCv.isNull(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return cudfCv.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return cudfCv.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return cudfCv.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return cudfCv.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return cudfCv.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return cudfCv.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return cudfCv.getDouble(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new IllegalStateException("Arrays are currently not supported by rapids cudf");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    ai.rapids.cudf.ColumnViewAccess<HostMemoryBuffer> structHcv = cudfCv.getChildColumnViewAccess(0);
    // keys
    ai.rapids.cudf.ColumnViewAccess<HostMemoryBuffer> firstHcv = structHcv.getChildColumnViewAccess(0);
    // values
    ai.rapids.cudf.ColumnViewAccess<HostMemoryBuffer> secondHcv = structHcv.getChildColumnViewAccess(1);

    //first keys column get all buffers
    DeviceMemoryBuffer firstDevData = null;
    DeviceMemoryBuffer firstDevOffset = null;
    DeviceMemoryBuffer firstDevValid = null;
    HostMemoryBuffer firstData = firstHcv.getDataBuffer();
    HostMemoryBuffer firstOffset = firstHcv.getOffsetBuffer();
    HostMemoryBuffer firstValid = firstHcv.getValidityBuffer();
    if (firstData != null) {
      firstDevData = DeviceMemoryBuffer.allocate(firstData.getLength());
      firstDevData.copyFromHostBuffer(0, firstData, 0, firstData.getLength());
    }
    if (firstOffset != null) {
      firstDevOffset = DeviceMemoryBuffer.allocate(firstOffset.getLength());
      firstDevOffset.copyFromHostBuffer(0, firstOffset, 0, firstOffset.getLength());
    }
    if (firstValid != null) {
      firstDevValid = DeviceMemoryBuffer.allocate(firstValid.getLength());
      firstDevValid.copyFromHostBuffer(0, firstValid, 0, firstValid.getLength());
    }
    //second values column get all buffers
    DeviceMemoryBuffer secondDevData = null;
    DeviceMemoryBuffer secondDevOffset = null;
    DeviceMemoryBuffer secondDevValid = null;
    HostMemoryBuffer secondData = secondHcv.getDataBuffer();
    HostMemoryBuffer secondOffset = secondHcv.getOffsetBuffer();
    HostMemoryBuffer secondValid = secondHcv.getValidityBuffer();
    if (secondData != null) {
      secondDevData = DeviceMemoryBuffer.allocate(secondData.getLength());
      secondDevData.copyFromHostBuffer(0, secondData, 0, secondData.getLength());
    }
    if (secondOffset != null) {
      secondDevOffset = DeviceMemoryBuffer.allocate(secondOffset.getLength());
      secondDevOffset.copyFromHostBuffer(0, secondOffset, 0, secondOffset.getLength());
    }
    if (secondValid != null) {
      secondDevValid = DeviceMemoryBuffer.allocate(secondValid.getLength());
      secondDevValid.copyFromHostBuffer(0, secondValid, 0, secondValid.getLength());
    }

    ai.rapids.cudf.ColumnVector firstDevCv = new ai.rapids.cudf.ColumnVector(firstHcv.getDataType(),
        firstHcv.getRowCount(), Optional.of(firstHcv.getNullCount()),
        firstDevData, firstDevValid, firstDevOffset);
    ai.rapids.cudf.ColumnVector secondDevCv = new ai.rapids.cudf.ColumnVector(secondHcv.getDataType(),
        secondHcv.getRowCount(), Optional.of(secondHcv.getNullCount()),
        secondDevData, secondDevValid, secondDevOffset);
    GpuColumnVector finFirstCv = GpuColumnVector.from(firstDevCv);
    GpuColumnVector finSecondCv = GpuColumnVector.from(secondDevCv);
    //TODO: test more that offset and len are right
    return new ColumnarMap(finFirstCv.copyToHost(),finSecondCv.copyToHost(),
        ordinal* DType.INT32.getSizeInBytes(), (ordinal + 1)* DType.INT32.getSizeInBytes());
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new IllegalStateException("The decimal type is currently not supported by rapids cudf");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    // TODO need a cheaper way to go directly to the String
    return UTF8String.fromString(cudfCv.getJavaString(rowId));
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new IllegalStateException("Binary data access is currently not supported by rapids cudf");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new IllegalStateException("Struct and struct like types are currently not supported by rapids cudf");
  }

  public ai.rapids.cudf.HostColumnVector getBase() {
    return cudfCv;
  }

  public long getRowCount() { return cudfCv.getRowCount(); }

  public GpuColumnVector copyToDevice() {
    return new GpuColumnVector(type, cudfCv.copyToDevice());
  }
}
