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

import com.nvidia.spark.Retryable;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.HostMemoryBuffer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * This is a copy of the cudf HostColumnVector.ColumnBuilder class.
 * Moving this here to allow for iterating on host memory oom handling.
 */
public final class RapidsHostColumnBuilder implements AutoCloseable, Retryable {

  private boolean allowGrowth = true;
  private HostColumnVector.DataType dataType;
  private DType type;
  private long currentInitBufferOffset = 0l;
  private HostMemoryBuffer data;
  private HostMemoryBuffer valid;
  private HostMemoryBuffer offsets;
  private long nullCount = 0l;
  private long checkpointNullCount = 0;
  private boolean nullable;
  private long rows;

  private long checkpointRows;
  private long estimatedRows;
  private long rowCapacity = 0L;
  private long validCapacity = 0L;
  private boolean built = false;
  private List<RapidsHostColumnBuilder> childBuilders = new ArrayList<>();
  private Runnable nullHandler;

  // The value of currentIndex can't exceed Int32.Max. Storing currentIndex as a long is to
  // adapt HostMemoryBuffer.setXXX, which requires a long offset.
  private long currentIndex = 0;
  private long checkpointCurrentIndex = 0;

  // Only for Strings: pointer of the byte (data) buffer
  private int currentStringByteIndex = 0;
  private int checkpointCurrentStringByteIndex = 0;

  // Use bit shift instead of multiply to transform row offset to byte offset
  private int bitShiftBySize = 0;
  /**
   * The size in bytes of an offset entry
   */
  static final int OFFSET_SIZE = DType.INT32.getSizeInBytes();
  private static final int bitShiftByOffset = (int) (Math.log(OFFSET_SIZE) / Math.log(2));

  public RapidsHostColumnBuilder(HostColumnVector.DataType dataType, long estimatedRows) {
    this.allowGrowth = true;
    this.dataType = dataType;
    this.type = dataType.getType();
    this.nullable = dataType.isNullable();
    this.rows = 0;
    this.estimatedRows = Math.max(estimatedRows, 1L);
    this.bitShiftBySize = (int) (Math.log(this.type.getSizeInBytes()) / Math.log(2));

    // initialize the null handler according to the data type
    this.setupNullHandler();

    for (int i = 0; i < dataType.getNumChildren(); i++) {
      childBuilders.add(new RapidsHostColumnBuilder(dataType.getChild(i), estimatedRows));
    }
  }

  @Override
  public void checkpoint() {
    checkpointRows = rows;
    checkpointCurrentIndex = currentIndex;
    checkpointCurrentStringByteIndex = currentStringByteIndex;
    checkpointNullCount = nullCount;
    for (RapidsHostColumnBuilder child : childBuilders) {
      child.checkpoint();
    }
  }

  @Override
  public void restore() {
    // May need to reset the validity bits
    if (nullable && (valid != null) && (currentIndex > checkpointCurrentIndex)) {
      for (long i = checkpointCurrentIndex; i < currentIndex; i++) {
        resetNullAt(valid, i);
      }
    }
    currentIndex = checkpointCurrentIndex;
    currentStringByteIndex = checkpointCurrentStringByteIndex;
    nullCount = checkpointNullCount;
    rows = checkpointRows;
    for (RapidsHostColumnBuilder child : childBuilders) {
      child.restore();
    }
  }

  private long getInitBufferOffset() {
    return this.currentInitBufferOffset;
  }

  private void preAllocateOffsets(HostMemoryBuffer initBuffer) {
    long neededSize = (estimatedRows + 1) << bitShiftByOffset;
    offsets = initBuffer.slice(this.currentInitBufferOffset, neededSize);
    offsets.setInt(0, 0);
    this.currentInitBufferOffset += neededSize;
  }

  private void preAllocateData(HostMemoryBuffer initBuffer, long neededSize) {
    data = initBuffer.slice(this.currentInitBufferOffset, neededSize);
    this.currentInitBufferOffset += neededSize;
  }

  private void preAllocateValidity(HostMemoryBuffer initBuffer) {
    // This is the same as ColumnView.getValidityBufferSize
    // number of bytes required = Math.ceil(number of bits / 8)
    long actualBytes = ((estimatedRows) + 7) >> 3;
    // padding to the adding boundary(64 bytes)
    long maskBytes = ((actualBytes + 63) >> 6) << 6;
    valid = initBuffer.slice(this.currentInitBufferOffset, maskBytes);
    this.currentInitBufferOffset += maskBytes;
    valid.setMemory(0, valid.getLength(), (byte) 0xFF);
    validCapacity = estimatedRows;
  }

  public RapidsHostColumnBuilder preAllocateBuffers(HostMemoryBuffer initBuffer, long offset) {
    this.allowGrowth = false;
    this.currentInitBufferOffset = offset;

    if (this.type == DType.LIST) {
      preAllocateOffsets(initBuffer);
    } else if (this.type == DType.STRING) {
      // Initialize data buffer with 20 bytes per string to match spark default.
      preAllocateData(initBuffer, estimatedRows * 20);
      preAllocateOffsets(initBuffer);
    } else if (this.type == DType.STRUCT) {
      // just set rowCapacity below
    } else {
      preAllocateData(initBuffer, estimatedRows << bitShiftBySize);
    }
    rowCapacity = estimatedRows;

    // Pre-allocate validity buffer if needed
    if (this.nullable) {
      preAllocateValidity(initBuffer);
    }

    for (int i = 0; i < dataType.getNumChildren(); i++) {
      childBuilders.get(i).preAllocateBuffers(initBuffer, this.currentInitBufferOffset);
      this.currentInitBufferOffset = childBuilders.get(i).getInitBufferOffset();
    }
    return this;
  }

  public void setAllowGrowth(boolean enable) {
    this.allowGrowth = enable;
    for (RapidsHostColumnBuilder child : childBuilders) {
      child.setAllowGrowth(enable);
    }
  }

  private void setupNullHandler() {
    if (this.type == DType.LIST) {
      this.nullHandler = () -> {
        this.growListBuffersAndRows();
        this.growValidBuffer();
        setNullAt(currentIndex++);
        offsets.setInt(currentIndex << bitShiftByOffset, childBuilders.get(0).getCurrentIndex());
      };
    } else if (this.type == DType.STRING) {
      this.nullHandler = () -> {
        this.growStringBuffersAndRows(0);
        this.growValidBuffer();
        setNullAt(currentIndex++);
        offsets.setInt(currentIndex << bitShiftByOffset, currentStringByteIndex);
      };
    } else if (this.type == DType.STRUCT) {
      this.nullHandler = () -> {
        this.growStructBuffersAndRows();
        this.growValidBuffer();
        setNullAt(currentIndex++);
        for (RapidsHostColumnBuilder childBuilder : childBuilders) {
          childBuilder.appendNull();
        }
      };
    } else {
      this.nullHandler = () -> {
        this.growFixedWidthBuffersAndRows();
        this.growValidBuffer();
        setNullAt(currentIndex++);
      };
    }
  }

  public HostColumnVector build() {
    List<HostColumnVectorCore> hostColumnVectorCoreList = new ArrayList<>();
    for (RapidsHostColumnBuilder childBuilder : childBuilders) {
      hostColumnVectorCoreList.add(childBuilder.buildNestedInternal());
    }
    if (valid != null) {
      // The valid buffer might have been pre-allocated, but never used.  If so, close it.
      if (nullCount == 0) {
        valid.close();
        valid = null;
      } else {
        // Aligns the valid buffer size with other buffers in terms of row size, because it grows lazily.
        growValidBuffer();
      }
    }
    HostColumnVector hostColumnVector = new HostColumnVector(type, rows,
        Optional.of(nullCount), data, valid, offsets, hostColumnVectorCoreList);
    built = true;
    return hostColumnVector;
  }

  private HostColumnVectorCore buildNestedInternal() {
    List<HostColumnVectorCore> hostColumnVectorCoreList = new ArrayList<>();
    for (RapidsHostColumnBuilder childBuilder : childBuilders) {
      hostColumnVectorCoreList.add(childBuilder.buildNestedInternal());
    }
    if (valid != null) {
      // The valid buffer might have been pre-allocated, but never used.  If so, close it.
      if (nullCount == 0) {
        valid.close();
        valid = null;
      } else {
        // Aligns the valid buffer size with other buffers in terms of row size, because it grows lazily.
        growValidBuffer();
      }
    }
    return new HostColumnVectorCore(type, rows, Optional.of(nullCount), data, valid,
        offsets, hostColumnVectorCoreList);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public RapidsHostColumnBuilder appendLists(List... inputLists) {
    for (List inputList : inputLists) {
      // one row
      append(inputList);
    }
    return this;
  }

  public RapidsHostColumnBuilder appendStructValues(List<HostColumnVector.StructData> inputList) {
    for (HostColumnVector.StructData structInput : inputList) {
      // one row
      append(structInput);
    }
    return this;
  }

  public RapidsHostColumnBuilder appendStructValues(HostColumnVector.StructData... inputList) {
    for (HostColumnVector.StructData structInput : inputList) {
      append(structInput);
    }
    return this;
  }

  /**
   * Grows valid buffer lazily. The valid buffer won't be materialized until the first null
   * value appended. This method reuses the rowCapacity to track the sizes of column.
   * Therefore, please call specific growBuffer method to update rowCapacity before calling
   * this method.
   */
  private void growValidBuffer() {
    if (valid == null) {
      // This is the same as ColumnView.getValidityBufferSize
      // number of bytes required = Math.ceil(number of bits / 8)
      long actualBytes = ((rowCapacity) + 7) >> 3;
      // padding to the adding boundary(64 bytes)
      long maskBytes = ((actualBytes + 63) >> 6) << 6;
      valid = HostMemoryBuffer.allocate(maskBytes);
      valid.setMemory(0, valid.getLength(), (byte) 0xFF);
      validCapacity = rowCapacity;
      return;
    }
    if (validCapacity < rowCapacity) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add rows beyond preallocated capacity: " + rowCapacity);
      }

      // This is the same as ColumnView.getValidityBufferSize
      // number of bytes required = Math.ceil(number of bits / 8)
      long actualBytes = ((rowCapacity) + 7) >> 3;
      // padding to the adding boundary(64 bytes)
      long maskBytes = ((actualBytes + 63) >> 6) << 6;
      HostMemoryBuffer newValid = HostMemoryBuffer.allocate(maskBytes);
      newValid.setMemory(0, newValid.getLength(), (byte) 0xFF);
      valid = copyBuffer(newValid, valid);
      validCapacity = rowCapacity;
    }
  }

  /**
   * A method automatically grows data buffer for fixed-width columns as needed along with
   * incrementing the row counts. Please call this method before appending any value or null.
   */
  private void growFixedWidthBuffersAndRows() {
    growFixedWidthBuffersAndRows(1);
  }

  /**
   * A method automatically grows data buffer for fixed-width columns for a given size as needed
   * along with incrementing the row counts. Please call this method before appending
   * multiple values or nulls.
   */
  private void growFixedWidthBuffersAndRows(int numRows) {
    assert rows + numRows <= Integer.MAX_VALUE : "Row count cannot go over Integer.MAX_VALUE";
    rows += numRows;

    if (data == null) {
      long neededSize = Math.max(rows, estimatedRows);
      data = HostMemoryBuffer.allocate(neededSize << bitShiftBySize);
      rowCapacity = neededSize;
    } else if (rows > rowCapacity) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add rows beyond preallocated capacity: " + rowCapacity);
      }
      long neededSize = Math.max(rows, rowCapacity * 2);
      long newCap = Math.min(neededSize, Integer.MAX_VALUE - 1);
      data = copyBuffer(HostMemoryBuffer.allocate(newCap << bitShiftBySize), data);
      rowCapacity = newCap;
    }
  }

  /**
   * A method automatically grows offsets buffer for list columns as needed along with
   * incrementing the row counts. Please call this method before appending any value or null.
   */
  private void growListBuffersAndRows() {
    assert rows + 2 <= Integer.MAX_VALUE : "Row count cannot go over Integer.MAX_VALUE";
    rows++;

    if (offsets == null) {
      offsets = HostMemoryBuffer.allocate((estimatedRows + 1) << bitShiftByOffset);
      offsets.setInt(0, 0);
      rowCapacity = estimatedRows;
    } else if (rows > rowCapacity) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add rows beyond preallocated capacity: " + rowCapacity);
      }
      long newCap = Math.min(rowCapacity * 2, Integer.MAX_VALUE - 2);
      offsets = copyBuffer(HostMemoryBuffer.allocate((newCap + 1) << bitShiftByOffset), offsets);
      rowCapacity = newCap;
    }
  }

  /**
   * A method automatically grows offsets and data buffer for string columns as needed along with
   * incrementing the row counts. Please call this method before appending any value or null.
   *
   * @param stringLength number of bytes required by the next row
   */
  private void growStringBuffersAndRows(int stringLength) {
    assert rows + 2 <= Integer.MAX_VALUE : "Row count cannot go over Integer.MAX_VALUE";
    rows++;

    if (offsets == null) {
      // Initialize data buffer with at least 1 byte in case the first appended value is null.
      data = HostMemoryBuffer.allocate(Math.max(1, stringLength));
      offsets = HostMemoryBuffer.allocate((estimatedRows + 1) << bitShiftByOffset);
      offsets.setInt(0, 0);
      rowCapacity = estimatedRows;
      return;
    }

    if (rows > rowCapacity) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add rows beyond preallocated capacity: " + rowCapacity);
      }
      long newCap = Math.min(rowCapacity * 2, Integer.MAX_VALUE - 2);
      offsets = copyBuffer(HostMemoryBuffer.allocate((newCap + 1) << bitShiftByOffset), offsets);
      rowCapacity = newCap;
    }

    long currentLength = currentStringByteIndex + stringLength;
    if (currentLength > data.getLength()) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add string bytes beyond preallocated capacity: " + data.getLength());
      }

      long requiredLength = data.getLength();
      do {
        requiredLength = requiredLength * 2;
      } while (currentLength > requiredLength);
      data = copyBuffer(HostMemoryBuffer.allocate(requiredLength), data);
    }
  }

  /**
   * For struct columns, we only need to update rows and rowCapacity (for the growth of
   * valid buffer), because struct columns hold no buffer itself.
   * Please call this method before appending any value or null.
   */
  private void growStructBuffersAndRows() {
    assert rows + 1 <= Integer.MAX_VALUE : "Row count cannot go over Integer.MAX_VALUE";
    rows++;

    if (rowCapacity == 0) {
      rowCapacity = estimatedRows;
    } else if (rows > rowCapacity) {
      if (!this.allowGrowth) {
        throw new RapidsHostColumnOverflow (
            "attempt to add row beyond preallocated capacity: " + rowCapacity);
      }
      rowCapacity = Math.min(rowCapacity * 2, Integer.MAX_VALUE - 1);
    }
  }

  private HostMemoryBuffer copyBuffer(HostMemoryBuffer targetBuffer, HostMemoryBuffer buffer) {
    try {
      targetBuffer.copyFromHostBuffer(0, buffer, 0, buffer.getLength());
      buffer.close();
      buffer = targetBuffer;
      targetBuffer = null;
    } finally {
      if (targetBuffer != null) {
        targetBuffer.close();
      }
    }
    return buffer;
  }

  /**
   * Reset the validity bit for the given index (used by restore).
   *
   * @param valid the buffer to reset it in.
   * @param index the index to reset it at.
   */
  static void resetNullAt(HostMemoryBuffer valid, long index) {
    long bucket = index / 8;
    byte currentByte = valid.getByte(bucket);
    int bitmask = (1 << (index % 8)) & 0x00ff;
    currentByte |= bitmask;
    valid.setByte(bucket, currentByte);
  }

  /**
   * Set the validity bit to null for the given index.
   *
   * @param valid the buffer to set it in.
   * @param index the index to set it at.
   * @return 1 if validity changed else 0 if it already was null.
   */
  static int setNullAt(HostMemoryBuffer valid, long index) {
    long bucket = index / 8;
    byte currentByte = valid.getByte(bucket);
    int bitmask = ~(1 << (index % 8));
    int ret = (currentByte >> index) & 0x1;
    currentByte &= bitmask;
    valid.setByte(bucket, currentByte);
    return ret;
  }

  /**
   * Method that sets the null bit in the validity vector
   *
   * @param index the row index at which the null is marked
   */
  private void setNullAt(long index) {
    assert index < rows : "Index for null value should fit the column with " + rows + " rows";
    nullCount += setNullAt(valid, index);
  }

  public final RapidsHostColumnBuilder appendNull() {
    nullHandler.run();
    return this;
  }

  //For structs
  private RapidsHostColumnBuilder append(HostColumnVector.StructData structData) {
    assert type.isNestedType();
    if (type.equals(DType.STRUCT)) {
      if (structData == null || structData.isNull()) {
        return appendNull();
      } else {
        for (int i = 0; i < structData.getNumFields(); i++) {
          RapidsHostColumnBuilder childBuilder = childBuilders.get(i);
          appendChildOrNull(childBuilder, structData.getField(i));
        }
        endStruct();
      }
    }
    return this;
  }

  private boolean allChildrenHaveSameIndex() {
    if (childBuilders.size() > 0) {
      int expected = childBuilders.get(0).getCurrentIndex();
      for (RapidsHostColumnBuilder child : childBuilders) {
        if (child.getCurrentIndex() != expected) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * If you want to build up a struct column you can get each child `builder.getChild(N)` and
   * append to all of them, then when you are done call `endStruct` to update this builder.
   * Do not start to append to the child and then append a null to this without ending the struct
   * first or you might not get the results that you expected.
   *
   * @return this for chaining.
   */
  public RapidsHostColumnBuilder endStruct() {
    assert type.equals(DType.STRUCT) : "This only works for structs";
    assert allChildrenHaveSameIndex() : "Appending structs data appears to be off " +
        childBuilders + " should all have the same currentIndex " + type;
    growStructBuffersAndRows();
    currentIndex++;
    return this;
  }

  /**
   * If you want to build up a list column you can get `builder.getChild(0)` and append to than,
   * then when you are done call `endList` and everything that was appended to that builder
   * will now be in the next list. Do not start to append to the child and then append a null
   * to this without ending the list first or you might not get the results that you expected.
   *
   * @return this for chaining.
   */
  public RapidsHostColumnBuilder endList() {
    assert type.equals(DType.LIST);
    growListBuffersAndRows();
    offsets.setInt(++currentIndex << bitShiftByOffset, childBuilders.get(0).getCurrentIndex());
    return this;
  }

  // For lists
  private <T> RapidsHostColumnBuilder append(List<T> inputList) {
    if (inputList == null) {
      appendNull();
    } else {
      RapidsHostColumnBuilder childBuilder = childBuilders.get(0);
      for (Object listElement : inputList) {
        appendChildOrNull(childBuilder, listElement);
      }
      endList();
    }
    return this;
  }

  private void appendChildOrNull(RapidsHostColumnBuilder childBuilder, Object listElement) {
    if (listElement == null) {
      childBuilder.appendNull();
    } else if (listElement instanceof Integer) {
      childBuilder.append((Integer) listElement);
    } else if (listElement instanceof String) {
      childBuilder.append((String) listElement);
    } else if (listElement instanceof Double) {
      childBuilder.append((Double) listElement);
    } else if (listElement instanceof Float) {
      childBuilder.append((Float) listElement);
    } else if (listElement instanceof Boolean) {
      childBuilder.append((Boolean) listElement);
    } else if (listElement instanceof Long) {
      childBuilder.append((Long) listElement);
    } else if (listElement instanceof Byte) {
      childBuilder.append((Byte) listElement);
    } else if (listElement instanceof Short) {
      childBuilder.append((Short) listElement);
    } else if (listElement instanceof BigDecimal) {
      childBuilder.append((BigDecimal) listElement);
    } else if (listElement instanceof BigInteger) {
      childBuilder.append((BigInteger) listElement);
    } else if (listElement instanceof List) {
      childBuilder.append((List<?>) listElement);
    } else if (listElement instanceof HostColumnVector.StructData) {
      childBuilder.append((HostColumnVector.StructData) listElement);
    } else if (listElement instanceof byte[]) {
      childBuilder.appendUTF8String((byte[]) listElement);
    } else {
      throw new IllegalStateException("Unexpected element type: " + listElement.getClass());
    }
  }

  @Deprecated
  public void incrCurrentIndex() {
    currentIndex = currentIndex + 1;
  }

  public int getCurrentIndex() {
    return (int) currentIndex;
  }

  @Deprecated
  public int getCurrentByteIndex() {
    return currentStringByteIndex;
  }

  public final RapidsHostColumnBuilder append(byte value) {
    growFixedWidthBuffersAndRows();
    assert type.isBackedByByte();
    assert currentIndex < rows;
    data.setByte(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(short value) {
    growFixedWidthBuffersAndRows();
    assert type.isBackedByShort();
    assert currentIndex < rows;
    data.setShort(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(int value) {
    growFixedWidthBuffersAndRows();
    assert type.isBackedByInt();
    assert currentIndex < rows;
    data.setInt(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(long value) {
    growFixedWidthBuffersAndRows();
    assert type.isBackedByLong();
    assert currentIndex < rows;
    data.setLong(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(float value) {
    growFixedWidthBuffersAndRows();
    assert type.equals(DType.FLOAT32);
    assert currentIndex < rows;
    data.setFloat(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(double value) {
    growFixedWidthBuffersAndRows();
    assert type.equals(DType.FLOAT64);
    assert currentIndex < rows;
    data.setDouble(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public final RapidsHostColumnBuilder append(boolean value) {
    growFixedWidthBuffersAndRows();
    assert type.equals(DType.BOOL8);
    assert currentIndex < rows;
    data.setBoolean(currentIndex++ << bitShiftBySize, value);
    return this;
  }

  public RapidsHostColumnBuilder append(BigDecimal value) {
    return append(value.setScale(-type.getScale(), RoundingMode.UNNECESSARY).unscaledValue());
  }

  private static byte[] convertDecimal128FromJavaToCudf(byte[] bytes) {
    byte[] finalBytes = new byte[16]; //hack
    byte lastByte = bytes[0];
    //Convert to 2's complement representation and make sure the sign bit is extended correctly
    byte setByte = (lastByte & 0x80) > 0 ? (byte) 0xff : (byte) 0x00;
    for (int i = bytes.length; i < finalBytes.length; i++) {
      finalBytes[i] = setByte;
    }
    // After setting the sign bits, reverse the rest of the bytes for endianness
    for (int k = 0; k < bytes.length; k++) {
      finalBytes[k] = bytes[bytes.length - k - 1];
    }
    return finalBytes;
  }

  public RapidsHostColumnBuilder append(BigInteger unscaledVal) {
    growFixedWidthBuffersAndRows();
    assert currentIndex < rows;
    if (type.getTypeId() == DType.DTypeEnum.DECIMAL32) {
      data.setInt(currentIndex++ << bitShiftBySize, unscaledVal.intValueExact());
    } else if (type.getTypeId() == DType.DTypeEnum.DECIMAL64) {
      data.setLong(currentIndex++ << bitShiftBySize, unscaledVal.longValueExact());
    } else if (type.getTypeId() == DType.DTypeEnum.DECIMAL128) {
      byte[] unscaledValueBytes = unscaledVal.toByteArray();
      byte[] result = convertDecimal128FromJavaToCudf(unscaledValueBytes);
      data.setBytes(currentIndex++ << bitShiftBySize, result, 0, result.length);
    } else {
      throw new IllegalStateException(type + " is not a supported decimal type.");
    }
    return this;
  }

  public RapidsHostColumnBuilder append(String value) {
    assert value != null : "appendNull must be used to append null strings";
    return appendUTF8String(value.getBytes(StandardCharsets.UTF_8));
  }

  public RapidsHostColumnBuilder appendUTF8String(byte[] value) {
    return appendUTF8String(value, 0, value.length);
  }

  public RapidsHostColumnBuilder appendUTF8String(byte[] value, int srcOffset, int length) {
    assert value != null : "appendNull must be used to append null strings";
    assert srcOffset >= 0;
    assert length >= 0;
    assert value.length + srcOffset <= length;
    assert type.equals(DType.STRING) : " type " + type + " is not String";
    growStringBuffersAndRows(length);
    assert currentIndex < rows;
    if (length > 0) {
      data.setBytes(currentStringByteIndex, value, srcOffset, length);
    }
    currentStringByteIndex += length;
    offsets.setInt(++currentIndex << bitShiftByOffset, currentStringByteIndex);
    return this;
  }

  /**
   * Append multiple non-null byte values.
   */
  public RapidsHostColumnBuilder append(byte[] value, int srcOffset, int length) {
    assert type.isBackedByByte();
    assert srcOffset >= 0;
    assert length >= 0;
    assert length + srcOffset <= value.length;

    if (length > 0) {
      growFixedWidthBuffersAndRows(length);
      assert currentIndex < rows;
      data.setBytes(currentIndex, value, srcOffset, length);
    }
    currentIndex += length;
    return this;
  }

  /**
   * Appends byte to a LIST of INT8/UINT8
   */
  public RapidsHostColumnBuilder appendByteList(byte[] value) {
    return appendByteList(value, 0, value.length);
  }

  /**
   * Appends bytes to a LIST of INT8/UINT8
   */
  public RapidsHostColumnBuilder appendByteList(byte[] value, int srcOffset, int length) {
    assert value != null : "appendNull must be used to append null bytes";
    assert type.equals(DType.LIST) : " type " + type + " is not LIST";
    getChild(0).append(value, srcOffset, length);
    return endList();
  }

  /**
   * Accepts a byte array containing the two's-complement representation of the unscaled value, which
   * is in big-endian byte-order. Then, transforms it into the representation of cuDF Decimal128 for
   * appending.
   * This method is more efficient than `append(BigInteger unscaledVal)` if we can directly access the
   * two's-complement representation of a BigDecimal without encoding via the method `toByteArray`.
   */
  public RapidsHostColumnBuilder appendDecimal128(byte[] binary) {
    growFixedWidthBuffersAndRows();
    assert type.getTypeId().equals(DType.DTypeEnum.DECIMAL128);
    assert currentIndex < rows;
    assert binary.length <= type.getSizeInBytes();
    byte[] cuBinary = convertDecimal128FromJavaToCudf(binary);
    data.setBytes(currentIndex++ << bitShiftBySize, cuBinary, 0, cuBinary.length);
    return this;
  }

  public RapidsHostColumnBuilder getChild(int index) {
    return childBuilders.get(index);
  }

  /**
   * Finish and create the immutable ColumnVector, copied to the device.
   */
  public final ColumnVector buildAndPutOnDevice() {
    try (HostColumnVector tmp = build()) {
      return tmp.copyToDevice();
    }
  }

  @Override
  public void close() {
    if (!built) {
      if (data != null) {
        data.close();
        data = null;
      }
      if (valid != null) {
        valid.close();
        valid = null;
      }
      if (offsets != null) {
        offsets.close();
        offsets = null;
      }
      for (RapidsHostColumnBuilder childBuilder : childBuilders) {
        childBuilder.close();
      }
      built = true;
    }
  }

  @Override
  public String toString() {
    StringJoiner sj = new StringJoiner(",");
    for (RapidsHostColumnBuilder cb : childBuilders) {
      sj.add(cb.toString());
    }
    return "RapidsHostColumnBuilder{" +
        "type=" + type +
        ", children=" + sj +
        ", data=" + data +
        ", valid=" + valid +
        ", currentIndex=" + currentIndex +
        ", nullCount=" + nullCount +
        ", estimatedRows=" + estimatedRows +
        ", populatedRows=" + rows +
        ", built=" + built +
        '}';
  }
}
