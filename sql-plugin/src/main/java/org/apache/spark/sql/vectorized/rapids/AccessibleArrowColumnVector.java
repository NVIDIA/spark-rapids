/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.vectorized.rapids;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector backed by Apache Arrow that adds API to get to the Arrow ValueVector.
 * Currently calendar interval type and map type are not supported.
 * Original code copied from Spark ArrowColumnVector.
 */
public final class AccessibleArrowColumnVector extends ColumnVector {
  private final AccessibleArrowVectorAccessor accessor;
  private AccessibleArrowColumnVector[] childColumns;

  public ValueVector getArrowValueVector() {
    return accessor.vector;
  }

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    accessor.close();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getMap(rowId);
  }

  @Override
  public AccessibleArrowColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  public AccessibleArrowColumnVector(ValueVector vector) {
    super(ArrowUtils.fromArrowField(vector.getField()));

    if (vector instanceof BitVector) {
      accessor = new AccessibleBooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new AccessibleByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new AccessibleShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new AccessibleIntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new AccessibleLongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new AccessibleFloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new AccessibleDoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new AccessibleDecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new AccessibleStringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new AccessibleBinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new AccessibleDateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new AccessibleTimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof MapVector) {
      MapVector mapVector = (MapVector) vector;
      accessor = new AccessibleMapAccessor(mapVector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new AccessibleArrayAccessor(listVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new AccessibleStructAccessor(structVector);

      childColumns = new AccessibleArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new AccessibleArrowColumnVector(structVector.getVectorById(i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class AccessibleArrowVectorAccessor {
    private final ValueVector vector;

    AccessibleArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class AccessibleBooleanAccessor extends AccessibleArrowVectorAccessor {

    private final BitVector accessor;

    AccessibleBooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class AccessibleByteAccessor extends AccessibleArrowVectorAccessor {

    private final TinyIntVector accessor;

    AccessibleByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleShortAccessor extends AccessibleArrowVectorAccessor {

    private final SmallIntVector accessor;

    AccessibleShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleIntAccessor extends AccessibleArrowVectorAccessor {

    private final IntVector accessor;

    AccessibleIntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleLongAccessor extends AccessibleArrowVectorAccessor {

    private final BigIntVector accessor;

    AccessibleLongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleFloatAccessor extends AccessibleArrowVectorAccessor {

    private final Float4Vector accessor;

    AccessibleFloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleDoubleAccessor extends AccessibleArrowVectorAccessor {

    private final Float8Vector accessor;

    AccessibleDoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleDecimalAccessor extends AccessibleArrowVectorAccessor {

    private final DecimalVector accessor;

    AccessibleDecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static class AccessibleStringAccessor extends AccessibleArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    AccessibleStringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          stringResult.end - stringResult.start);
      }
    }
  }

  private static class AccessibleBinaryAccessor extends AccessibleArrowVectorAccessor {

    private final VarBinaryVector accessor;

    AccessibleBinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class AccessibleDateAccessor extends AccessibleArrowVectorAccessor {

    private final DateDayVector accessor;

    AccessibleDateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleTimestampAccessor extends AccessibleArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    AccessibleTimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class AccessibleArrayAccessor extends AccessibleArrowVectorAccessor {

    private final ListVector accessor;
    private final AccessibleArrowColumnVector arrayData;

    AccessibleArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new AccessibleArrowColumnVector(vector.getDataVector());
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a AccessibleArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  private static class AccessibleStructAccessor extends AccessibleArrowVectorAccessor {

    AccessibleStructAccessor(StructVector vector) {
      super(vector);
    }
  }

  private static class AccessibleMapAccessor extends AccessibleArrowVectorAccessor {
    private final MapVector accessor;
    private final AccessibleArrowColumnVector keys;
    private final AccessibleArrowColumnVector values;

    AccessibleMapAccessor(MapVector vector) {
      super(vector);
      this.accessor = vector;
      StructVector entries = (StructVector) vector.getDataVector();
      this.keys = new AccessibleArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
      this.values = new AccessibleArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
    }

    @Override
    final ColumnarMap getMap(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      int offset = accessor.getOffsetBuffer().getInt(index);
      int length = accessor.getInnerValueCountAt(rowId);
      return new ColumnarMap(keys, values, offset, length);
    }
  }
}
