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

package com.nvidia.spark.rapids;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Wrapper of a RapidsHostColumnVector, which will check nulls in each "getXXX" call and
 * return the default value of a type when trying to read a null value.
 * The performance may not be good enough, so use it only when there is no other ways.
 */
public class RapidsNullSafeHostColumnVector extends ColumnVector {
    private final RapidsHostColumnVector rapidsHcv;

    public RapidsNullSafeHostColumnVector(RapidsHostColumnVector rapidsHcv) {
        super(rapidsHcv.dataType());
        this.rapidsHcv = rapidsHcv;
    }

    public final RapidsNullSafeHostColumnVector incRefCount() {
        // Just pass through the reference counting
        rapidsHcv.incRefCount();
        return this;
    }

    public final ai.rapids.cudf.HostColumnVector getBase() {
        return rapidsHcv.getBase();
    }

    @Override
    public void close() {
        rapidsHcv.close();
    }

    @Override
    public boolean hasNull() {
        return rapidsHcv.hasNull();
    }

    @Override
    public int numNulls() {
        return rapidsHcv.numNulls();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return rapidsHcv.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return isNullAt(rowId) ? false : rapidsHcv.getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return isNullAt(rowId) ? 0 : rapidsHcv.getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return isNullAt(rowId) ? 0 : rapidsHcv.getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return isNullAt(rowId) ? 0 : rapidsHcv.getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return isNullAt(rowId) ? 0L : rapidsHcv.getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return isNullAt(rowId) ? 0.0F : rapidsHcv.getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return isNullAt(rowId) ? 0.0 : rapidsHcv.getDouble(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        return isNullAt(rowId) ? null : rapidsHcv.getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int ordinal) {
        return isNullAt(ordinal) ? null : rapidsHcv.getMap(ordinal);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        return isNullAt(rowId) ? null : rapidsHcv.getDecimal(rowId, precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        return isNullAt(rowId) ? null : rapidsHcv.getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return isNullAt(rowId) ? null : rapidsHcv.getBinary(rowId);
    }

    @Override
    public ColumnVector getChild(int ordinal) {
        return rapidsHcv.getChild(ordinal);
    }
}
