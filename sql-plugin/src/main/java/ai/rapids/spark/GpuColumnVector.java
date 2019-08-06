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
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.TimeUnit;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
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

    /**
     * A collection of builders for building up columnar data.
     * @param schema the schema of the batch.
     * @param rows the maximum number of rows in this batch.
     * @param batch if this is going to copy a ColumnarBatch in a non GPU format that batch
     *              we are going to copy. If not this may be null. This is used to get an idea
     *              of how big to allocate buffers that do not necessarily correspond to the
     *              number of rows.
     */
    public GpuColumnarBatchBuilder(StructType schema, int rows, ColumnarBatch batch) {
      fields = schema.fields();
      int len = fields.length;
      builders = new ai.rapids.cudf.ColumnVector.Builder[len];
      boolean success = false;
      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          DType type = getRapidsType(field);
          TimeUnit units = getTimeUnits(field);
          if (type == DType.STRING) {
            // If we cannot know the exact size, assume the string is small and allocate
            // 8 bytes per row.  The buffer of the builder will grow as needed if it is
            // too small.
            int bufferSize = rows * 8;
            if (batch != null) {
              ColumnVector cv = batch.column(i);
              if (cv instanceof WritableColumnVector) {
                WritableColumnVector wcv = (WritableColumnVector)cv;
                if (!wcv.hasDictionary()) {
                  bufferSize = wcv.getArrayOffset(rows-1) +
                      wcv.getArrayLength(rows - 1);
                }
              }
            }
            builders[i] = ai.rapids.cudf.ColumnVector.builder(type, rows, bufferSize);
          } else {
            builders[i] = ai.rapids.cudf.ColumnVector.builder(type, units, rows);
          }
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

  private static DType toRapidsOrNull(DataType type) {
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
    return null;
  }

  public static boolean isSupportedType(DataType type) {
    return toRapidsOrNull(type) != null;
  }

  public static DType getRapidsType(StructField field) {
    DataType type = field.dataType();
    return getRapidsType(type);
  }

  public static DType getRapidsType(DataType type) {
    DType result = toRapidsOrNull(type);
    if (result == null) {
      throw new IllegalArgumentException(type + " is not supported for GPU processing yet.");
    }
    return result;
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
        throw new IllegalArgumentException(type + " is not supported by spark yet.");

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

  public static GpuColumnVector from(Scalar scalar, int count) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar, count));
  }

  private final ai.rapids.cudf.ColumnVector cudfCv;

  /**
   * Sets up the data type of this column vector.
   */
  private GpuColumnVector(DataType type, ai.rapids.cudf.ColumnVector cudfCv) {
    super(type);
    // TODO need some checks to be sure everything matches
    this.cudfCv = cudfCv;
  }

  public GpuColumnVector inRefCount() {
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
