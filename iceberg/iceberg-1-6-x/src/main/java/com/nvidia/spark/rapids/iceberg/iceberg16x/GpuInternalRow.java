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

package com.nvidia.spark.rapids.iceberg.iceberg16x;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.time.LocalDate;

public class GpuInternalRow extends InternalRow {
  private final InternalRow wrapped;

  public GpuInternalRow(InternalRow row) {
    this.wrapped = row;
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (wrapped.isNullAt(ordinal)) return null;
    if (dataType instanceof DateType) {
      return LocalDate.ofEpochDay(getInt(ordinal));
    } else if (dataType instanceof StringType) {
      UTF8String s = getUTF8String(ordinal);
      return s == null ? null : s.toString();
    } else if (dataType instanceof DecimalType) {
      DecimalType dt = (DecimalType) dataType;
      return getDecimal(ordinal, dt.precision(), dt.scale()).toJavaBigDecimal();
    } else {
      return wrapped.get(ordinal, dataType);
    }
  }

  @Override public boolean isNullAt(int ordinal) { return wrapped.isNullAt(ordinal); }
  @Override public boolean getBoolean(int ordinal) { return wrapped.getBoolean(ordinal); }
  @Override public byte getByte(int ordinal) { return wrapped.getByte(ordinal); }
  @Override public short getShort(int ordinal) { return wrapped.getShort(ordinal); }
  @Override public int getInt(int ordinal) { return wrapped.getInt(ordinal); }
  @Override public long getLong(int ordinal) { return wrapped.getLong(ordinal); }
  @Override public float getFloat(int ordinal) { return wrapped.getFloat(ordinal); }
  @Override public double getDouble(int ordinal) { return wrapped.getDouble(ordinal); }
  @Override public Decimal getDecimal(int ordinal, int precision, int scale) { return wrapped.getDecimal(ordinal, precision, scale); }
  @Override public UTF8String getUTF8String(int ordinal) { return wrapped.getUTF8String(ordinal); }
  @Override public byte[] getBinary(int ordinal) { return wrapped.getBinary(ordinal); }
  @Override public CalendarInterval getInterval(int ordinal) { return wrapped.getInterval(ordinal); }
  @Override public InternalRow getStruct(int ordinal, int numFields) { return wrapped.getStruct(ordinal, numFields); }
  @Override public ArrayData getArray(int ordinal) { return wrapped.getArray(ordinal); }
  @Override public MapData getMap(int ordinal) { return wrapped.getMap(ordinal); }
  @Override public int numFields() { return wrapped.numFields(); }
  @Override public void setNullAt(int i) { wrapped.setNullAt(i); }
  @Override public void update(int i, Object value) { wrapped.update(i, value); }
  @Override public InternalRow copy() { return wrapped.copy(); }
}
