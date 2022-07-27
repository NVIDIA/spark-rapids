/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;

/**
 * This is an InternalRow implementation based off of UnsafeRow, but follows a format for use with
 * the row format supported by cudf.  In this format each column is padded to match the alignment
 * needed by it, and validity is placed at the end one byte at a time.
 *
 * It also supports remapping the columns so that if the columns were re-ordered to reduce packing
 * in the format, then they can be mapped back to their original positions.
 *
 * This class is likely to go away once we move to code generation when going directly to an
 * UnsafeRow through code generation. This is rather difficult because of some details in how
 * UnsafeRow works.
 */
public final class CudfUnsafeRow extends InternalRow {


  //////////////////////////////////////////////////////////////////////////////
  // Private fields and methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Address of where the row is stored in off heap memory.
   */
  private long address;

  /**
   * For each column the starting location to read from. The index is the position in
   * the row bytes, not the user facing ordinal.
   */
  private int[] startOffsets;

  /**
   * At what point validity data starts from the beginning of a row's data.
   */
  private int validityOffsetInBytes;

  /**
   * The size of this row's backing data, in bytes.
   */
  private int sizeInBytes;

  /**
   * A mapping from the user facing ordinal to the index in the underlying row.
   */
  private int[] remapping;

  private boolean variableWidthSchema;

  /**
   * Get the address where a field is stored.
   * @param ordinal the user facing ordinal.
   * @return the address of the field.
   */
  private long getFieldAddressFromOrdinal(int ordinal) {
    assertIndexIsValid(ordinal);
    int i = remapping[ordinal];
    return address + startOffsets[i];
  }

  /**
   * Verify that index is valid for this row.
   * @param index in this case the index can be either the user facing ordinal or the index into the
   *              row.
   */
  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < startOffsets.length : "index (" + index + ") should < " + startOffsets.length;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Public methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Construct a new Row. The resulting row won't be usable until `pointTo()` has been called,
   * since the value returned by this constructor is equivalent to a null pointer.
   *
   * @param attributes the schema of what this will hold.  This is the schema of the underlying
   *                    row, so if columns were re-ordered it is the attributes of the reordered
   *                    data.
   * @param remapping a mapping from the user requested column to the underlying column in the
   *                  backing row.
   */
  public CudfUnsafeRow(Attribute[] attributes, int[] remapping) {
    startOffsets = new int[attributes.length];
    JCudfUtil.RowOffsetsCalculator jCudfBuilder =
        JCudfUtil.getRowOffsetsCalculator(attributes, startOffsets);
    this.validityOffsetInBytes = jCudfBuilder.getValidityBytesOffset();
    this.variableWidthSchema = jCudfBuilder.hasVarSizeData();
    this.remapping = remapping;
    assert startOffsets.length == remapping.length;
  }

  // for serializer
  public CudfUnsafeRow() {}

  @Override
  public int numFields() { return startOffsets.length; }

  /**
   * Update this CudfUnsafeRow to point to different backing data.
   *
   * @param address the address in host memory for this.  We should change this to be a
   *                MemoryBuffer class or something like that.
   * @param sizeInBytes the size of this row's backing data, in bytes
   */
  public void pointTo(long address, int sizeInBytes) {
    assert startOffsets != null && startOffsets.length > 0 : "startOffsets not properly initialized";
    assert sizeInBytes % 8 == 0 : "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8";
    this.address = address;
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    // Don't remap the ordinal because it will be remapped in each of the other backing APIs
    return SpecializedGettersReader.read(this, ordinal, dataType, true, true);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    int i = remapping[ordinal];
    assertIndexIsValid(i);
    int validByteIndex = i / 8;
    int validBitIndex = i % 8;
    byte b = Platform.getByte(null, address + validityOffsetInBytes + validByteIndex);
    return ((1 << validBitIndex) & b) == 0;
  }

  @Override
  public void setNullAt(int ordinal) {
    int i = remapping[ordinal];
    assertIndexIsValid(i);
    int validByteIndex = i / 8;
    int validBitIndex = i % 8;
    byte b = Platform.getByte(null, address + validityOffsetInBytes + validByteIndex);
    b = (byte)((b & ~(1 << validBitIndex)) & 0xFF);
    Platform.putByte(null, address + validityOffsetInBytes + validByteIndex, b);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return Platform.getBoolean(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    return Platform.getByte(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    return Platform.getShort(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    return Platform.getInt(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    return Platform.getLong(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    return Platform.getFloat(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    return Platform.getDouble(null, getFieldAddressFromOrdinal(ordinal));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    if (isNullAt(ordinal)) {
      return null;
    }
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(ordinal), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(ordinal), precision, scale);
    } else {
      throw new IllegalArgumentException("NOT IMPLEMENTED YET");
//      byte[] bytes = getBinary(ordinal);
//      BigInteger bigInteger = new BigInteger(bytes);
//      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
//      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    }
    final long columnOffset = getFieldAddressFromOrdinal(ordinal);
    // data format for the fixed-width portion of variable-width data is 4 bytes of offset from the
    // start of the row followed by 4 bytes of length.
    final int offset = Platform.getInt(null, columnOffset);
    final int size = Platform.getInt(null, columnOffset + 4);
    return UTF8String.fromAddress(null, address + offset, size);
  }

  @Override
  public byte[] getBinary(int ordinal) {
//    if (isNullAt(ordinal)) {
//      return null;
//    } else {
//      final long offsetAndSize = getLong(ordinal);
//      final int offset = (int) (offsetAndSize >> 32);
//      final int size = (int) offsetAndSize;
//      final byte[] bytes = new byte[size];
//      Platform.copyMemory(
//          null,
//          address + offset,
//          bytes,
//          Platform.BYTE_ARRAY_OFFSET,
//          size
//      );
//      return bytes;
//    }
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
//    if (isNullAt(ordinal)) {
//      return null;
//    } else {
//      final long offsetAndSize = getLong(ordinal);
//      final int offset = (int) (offsetAndSize >> 32);
//      final int months = Platform.getInt(baseObject, address + offset);
//      final int days = Platform.getInt(baseObject, address + offset + 4);
//      final long microseconds = Platform.getLong(baseObject, address + offset + 8);
//      return new CalendarInterval(months, days, microseconds);
//    }
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  @Override
  public CudfUnsafeRow getStruct(int ordinal, int numFields) {
//    if (isNullAt(ordinal)) {
//      return null;
//    } else {
//      final long offsetAndSize = getLong(ordinal);
//      final int offset = (int) (offsetAndSize >> 32);
//      final int size = (int) offsetAndSize;
//      final UnsafeRow row = new UnsafeRow(numFields);
//      row.pointTo(baseObject, address + offset, size);
//      return row;
//    }
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  @Override
  public ArrayData getArray(int ordinal) {
//    if (isNullAt(ordinal)) {
//      return null;
//    } else {
//      final long offsetAndSize = getLong(ordinal);
//      final int offset = (int) (offsetAndSize >> 32);
//      final int size = (int) offsetAndSize;
//      final UnsafeArrayData array = new UnsafeArrayData();
//      array.pointTo(baseObject, address + offset, size);
//      return array;
//    }
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  @Override
  public MapData getMap(int ordinal) {
//    if (isNullAt(ordinal)) {
//      return null;
//    } else {
//      final long offsetAndSize = getLong(ordinal);
//      final int offset = (int) (offsetAndSize >> 32);
//      final int size = (int) offsetAndSize;
//      final UnsafeMapData map = new UnsafeMapData();
//      map.pointTo(baseObject, address + offset, size);
//      return map;
//    }
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  /**
   * Copies this row, returning a self-contained UnsafeRow that stores its data in an internal
   * byte array rather than referencing data stored in a data page.
   */
  @Override
  public CudfUnsafeRow copy() {
//    UnsafeRow rowCopy = new UnsafeRow(numFields);
//    final byte[] rowDataCopy = new byte[sizeInBytes];
//    Platform.copyMemory(
//        baseObject,
//        address,
//        rowDataCopy,
//        Platform.BYTE_ARRAY_OFFSET,
//        sizeInBytes
//    );
//    rowCopy.pointTo(rowDataCopy, Platform.BYTE_ARRAY_OFFSET, sizeInBytes);
//    return rowCopy;
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
  }

  @Override
  public int hashCode() {
    return Murmur3_x86_32.hashUnsafeWords(null, address, sizeInBytes, 42);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CudfUnsafeRow) {
      CudfUnsafeRow o = (CudfUnsafeRow) other;
      return (sizeInBytes == o.sizeInBytes) &&
          ByteArrayMethods.arrayEquals(null, address, null, o.address, sizeInBytes) &&
          Arrays.equals(remapping, o.remapping);
    }
    return false;
  }

  // This is for debugging
  @Override
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      if (i != 0) build.append(',');
      build.append(java.lang.Long.toHexString(Platform.getLong(null, address + i)));
    }
    build.append(']');
    build.append(" remapped with ");
    build.append(Arrays.toString(remapping));
    return build.toString();
  }

  @Override
  public boolean anyNull() {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET");
//    return BitSetMethods.anySet(baseObject, address, bitSetWidthInBytes / 8);
  }

  public boolean isVariableWidthSchema() {
    return variableWidthSchema;
  }

  public int getValidityOffsetInBytes() {
    return validityOffsetInBytes;
  }

  /**
   * Calculates the offset of the variable width section.
   * This can be used to get the offset of the variable-width data. Note that the data-offset is 1-byte aligned.
   * @return Total bytes used by the fixed width offsets and the validity bytes without row-alignment.
   */
  public int getFixedWidthInBytes() {
    return getValidityOffsetInBytes() + JCudfUtil.calculateBitSetWidthInBytes(numFields());
  }
}
