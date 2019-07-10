/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark;

import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.TimeUnit;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public final class GpuColumnVector extends ColumnVector {

    public static final class GpuColumnarBatchBuilder implements AutoCloseable {
        private final ai.rapids.cudf.ColumnVector.Builder[] builders;
        private final StructField[] fields;

        public GpuColumnarBatchBuilder(StructType schema, int rows) {
            fields = schema.fields();
            int len = fields.length;
            builders = new ai.rapids.cudf.ColumnVector.Builder[len];
            boolean success = false;
            try {
                for (int i = 0; i < len; i++) {
                    StructField field = fields[i];
                    DType type = getRapidsType(field);
                    TimeUnit units = getTimeUnits(field);
                    builders[i] = ai.rapids.cudf.ColumnVector.builder(type, units, rows);
                    success = true;
                }
            } finally {
                if (!success) {
                    for (ai.rapids.cudf.ColumnVector.Builder b: builders) {
                        if (b != null) {
                            b.close();
                        }
                    }
                }
            }
        }

        public ai.rapids.cudf.ColumnVector.Builder builder(int i) {
            return builders[i];
        }

        public ColumnarBatch build(int rows) {
            ColumnVector[] vectors = new ColumnVector[builders.length];
            boolean success = false;
            try {
                for (int i = 0; i < builders.length; i++) {
                    ai.rapids.cudf.ColumnVector cv = builders[i].build();
                    vectors[i] = new GpuColumnVector(fields[i].dataType(), cv);
                    cv.ensureOnDevice();
                    builders[i] = null;
                }
                ColumnarBatch ret = new ColumnarBatch(vectors, rows);
                success = true;
                return ret;
            } finally {
                if (!success) {
                    for (ColumnVector vec: vectors) {
                        if (vec != null) {
                            vec.close();
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            for (ai.rapids.cudf.ColumnVector.Builder b: builders) {
                if (b != null) {
                    b.close();
                }
            }
        }
    }

    public static TimeUnit getTimeUnits(StructField field) {
        DataType type = field.dataType();
        return getTimeUnits(type);
    }

    public static TimeUnit getTimeUnits(DataType type) {
        if (type instanceof TimestampType) {
            return TimeUnit.MICROSECONDS;
        }
        return TimeUnit.NONE;
    }

    public static TimeUnit getTimeUnits(DType type) {
        if (type == DType.TIMESTAMP) {
            return TimeUnit.MICROSECONDS;
        }
        return TimeUnit.NONE;
    }

    public static DType getRapidsType(StructField field) {
        DataType type = field.dataType();
        return getRapidsType(type);
    }

    public static DType getRapidsType(DataType type) {
        if (type instanceof LongType) {
            return DType.INT64;
        } else if (type instanceof DoubleType) {
            return DType.FLOAT64;
        } else if (type instanceof ByteType) {
            return DType.INT8;
        } else if (type instanceof BooleanType) {
            return DType.BOOL8;
        } else if (type instanceof ShortType) {
            return DType.INT16;
        } else if (type instanceof IntegerType) {
            return DType.INT32;
        } else if (type instanceof FloatType) {
            return DType.FLOAT32;
        } else if (type instanceof DateType) {
            return DType.DATE32;
        } else if (type instanceof TimestampType) {
            return DType.TIMESTAMP;
        } else if (type instanceof StringType) {
            return DType.STRING; // TODO what do we want to do about STRING_CATEGORY???
        }
        throw new IllegalStateException(type + " is not supported for GPU processing yet.");
    }

    private static DataType getSparkType(DType type) {
        switch (type) {
            case BOOL8:
                return DataTypes.BooleanType;
            case INT8:
                return DataTypes.ByteType;
            case INT16:
                return DataTypes.ShortType;
            case INT32:
                return DataTypes.IntegerType;
            case INT64:
                return DataTypes.LongType;
            case FLOAT32:
                return DataTypes.FloatType;
            case FLOAT64:
                return DataTypes.DoubleType;
            case DATE32:
                return DataTypes.DateType;
            case TIMESTAMP:
                return DataTypes.TimestampType; // TODO need to verify that the TimeUnits are correct
            case STRING: //Fall through
            case STRING_CATEGORY:
                return DataTypes.StringType;
            default:
                throw new IllegalStateException(type + " is not supported by spark yet.");

        }
    }

    public static ColumnarBatch from(Table table) {
        int numColumns = table.getNumberOfColumns();
        ColumnVector[] columns = new ColumnVector[numColumns];
        boolean success = false;
        try {
            for (int i = 0; i < numColumns; i++) {
                columns[i] = from(table.getColumn(i).incRefCount());
            }
            long rows = table.getRowCount();
            if (rows != (int) rows) {
                throw new IllegalStateException("Cannot support a batch larger that MAX INT rows");
            }
            ColumnarBatch ret = new ColumnarBatch(columns, (int)rows);
            success = true;
            return ret;
        } finally {
            if (!success) {
                for (ColumnVector cv: columns) {
                    if (cv != null) {
                        cv.close();
                    }
                }
            }
        }
    }

    public static GpuColumnVector from(ai.rapids.cudf.ColumnVector cudfCv) {
       return new GpuColumnVector(getSparkType(cudfCv.getType()), cudfCv);
    }

    private final ai.rapids.cudf.ColumnVector cudfCv;
    private int refCount = 1;

    /**
     * Sets up the data type of this column vector.
     */
    private GpuColumnVector(DataType type, ai.rapids.cudf.ColumnVector cudfCv) {
        super(type);
        // TODO need some checks to be sure everything matches
        this.cudfCv = cudfCv;
    }

    public GpuColumnVector inRefCount() {
        refCount++;
        return this;
    }

    @Override
    public void close() {
        refCount--;
        if (refCount == 0) {
            cudfCv.close();
        }
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
        throw new IllegalStateException("Maps are currently not supported by rapids cudf");
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

    public ai.rapids.cudf.ColumnVector getBase() {
        return cudfCv;
    }
}
