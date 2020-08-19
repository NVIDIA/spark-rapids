/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnViewAccess;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.List;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public class GpuColumnVector extends GpuColumnVectorBase {

  public static final class GpuColumnarBatchBuilder implements AutoCloseable {
    private final ai.rapids.cudf.HostColumnVector.Builder[] builders;
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
      builders = new ai.rapids.cudf.HostColumnVector.Builder[len];
      boolean success = false;
      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          DType type = getRapidsType(field);
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
            builders[i] = ai.rapids.cudf.HostColumnVector.builder(rows, bufferSize);
          } else {
            builders[i] = ai.rapids.cudf.HostColumnVector.builder(type, rows);
          }
          success = true;
        }
      } finally {
        if (!success) {
          for (ai.rapids.cudf.HostColumnVector.Builder b: builders) {
            if (b != null) {
              b.close();
            }
          }
        }
      }
    }

    public ai.rapids.cudf.HostColumnVector.Builder builder(int i) {
      return builders[i];
    }

    public ColumnarBatch build(int rows) {
      ColumnVector[] vectors = new ColumnVector[builders.length];
      boolean success = false;
      try {
        for (int i = 0; i < builders.length; i++) {
          ai.rapids.cudf.ColumnVector cv = builders[i].buildAndPutOnDevice();
          vectors[i] = new GpuColumnVector(fields[i].dataType(), cv);
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
      for (ai.rapids.cudf.HostColumnVector.Builder b: builders) {
        if (b != null) {
          b.close();
        }
      }
    }
  }

  private static final DType toRapidsOrNull(DataType type) {
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
      return DType.TIMESTAMP_DAYS;
    } else if (type instanceof TimestampType) {
      return DType.TIMESTAMP_MICROSECONDS;
    } else if (type instanceof StringType) {
      return DType.STRING;
    }
    return null;
  }

  public static final boolean isSupportedType(DataType type) {
    return toRapidsOrNull(type) != null;
  }

  public static final DType getRapidsType(StructField field) {
    DataType type = field.dataType();
    return getRapidsType(type);
  }

  public static final DType getRapidsType(DataType type) {
    DType result = toRapidsOrNull(type);
    if (result == null) {
      throw new IllegalArgumentException(type + " is not supported for GPU processing yet.");
    }
    return result;
  }

  static final DataType getSparkType(DType type) {
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
      case TIMESTAMP_DAYS:
        return DataTypes.DateType;
      case TIMESTAMP_MICROSECONDS:
        return DataTypes.TimestampType;
      case STRING:
        return DataTypes.StringType;
      default:
        throw new IllegalArgumentException(type + " is not supported by spark yet.");
    }
  }

  protected static final <T> DataType getSparkTypeFrom(ColumnViewAccess<T> access) {
    DType type = access.getDataType();
    if (type == DType.LIST) {
      try (ColumnViewAccess<T> child = access.getChildColumnViewAccess(0)) {
        return new ArrayType(getSparkTypeFrom(child), true);
      }
    } else {
      return getSparkType(type);
    }
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static final ColumnarBatch emptyBatch(StructType schema) {
    return new GpuColumnarBatchBuilder(schema, 0, null).build(0);
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static final ColumnarBatch emptyBatch(List<Attribute> format) {
    StructType schema = new StructType();
    for (Attribute attribute: format) {
      schema = schema.add(new StructField(attribute.name(),
          attribute.dataType(),
          attribute.nullable(),
          null));
    }
    return emptyBatch(schema);
  }


  /**
   * Convert a spark schema into a cudf schema
   * @param input the spark schema to convert
   * @return the cudf schema
   */
  public static final Schema from(StructType input) {
    Schema.Builder builder = Schema.builder();
    input.foreach(f -> builder.column(GpuColumnVector.getRapidsType(f.dataType()), f.name()));
    return builder.build();
  }

  /**
   * Convert a ColumnarBatch to a table. The table will increment the reference count for all of
   * the columns in the batch, so you will need to close both the batch passed in and the table
   * returned to avoid any memory leaks.
   */
  public static final Table from(ColumnarBatch batch) {
    return new Table(extractBases(batch));
  }

  /**
   * Convert a Table to a ColumnarBatch.  The columns in the table will have their reference counts
   * incremented so you will need to close both the table passed in and the batch returned to
   * not have any leaks.
   */
  public static final ColumnarBatch from(Table table) {
    return from(table, 0, table.getNumberOfColumns());
  }

  /**
   * Get a ColumnarBatch from a set of columns in the Table. This gets the columns
   * starting at startColIndex and going until but not including untilColIndex. This will
   * increment the reference count for all columns converted so you will need to close
   * both the table that is passed in and the batch returned to be sure that there are no leaks.
   *
   * @param table a table of vectors
   * @param startColIndex index of the first vector you want in the final ColumnarBatch
   * @param untilColIndex until index of the columns. (ie doesn't include that column num)
   * @return a ColumnarBatch of the vectors from the table
   */
  public static final ColumnarBatch from(Table table, int startColIndex, int untilColIndex) {
    assert table != null : "Table cannot be null";
    int numColumns = untilColIndex - startColIndex;
    ColumnVector[] columns = new ColumnVector[numColumns];
    int finalLoc = 0;
    boolean success = false;
    try {
      for (int i = startColIndex; i < untilColIndex; i++) {
        columns[finalLoc] = from(table.getColumn(i).incRefCount());
        finalLoc++;
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

  /**
   * Converts a cudf internal vector to a spark compatible vector. No reference counts
   * are incremented so you need to either close the returned value or the input value,
   * but not both.
   */
  public static final GpuColumnVector from(ai.rapids.cudf.ColumnVector cudfCv) {
    return new GpuColumnVector(getSparkTypeFrom(cudfCv), cudfCv);
  }

  public static final GpuColumnVector from(Scalar scalar, int count) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar, count));
  }

  /**
   * Get the underlying cudf columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static final ai.rapids.cudf.ColumnVector[] extractBases(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    ai.rapids.cudf.ColumnVector[] vectors = new ai.rapids.cudf.ColumnVector[numColumns];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i)).getBase();
    }
    return vectors;
  }

  /**
   * Get the underlying spark compatible columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static final GpuColumnVector[] extractColumns(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    GpuColumnVector[] vectors = new GpuColumnVector[numColumns];

    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i));
    }
    return vectors;
  }

  private final ai.rapids.cudf.ColumnVector cudfCv;

  /**
   * Take an INT32 column vector and return a host side int array.  Don't use this for anything
   * too large.  Note that this ignores validity totally.
   */
  public static final int[] toIntArray(ai.rapids.cudf.ColumnVector vec) {
    assert vec.getType() == DType.INT32;
    int rowCount = (int)vec.getRowCount();
    int[] output = new int[rowCount];
    try (HostColumnVector h = vec.copyToHost()) {
      for (int i = 0; i < rowCount; i++) {
        output[i] = h.getInt(i);
      }
    }
    return output;
  }

  /**
   * Sets up the data type of this column vector.
   */
  GpuColumnVector(DataType type, ai.rapids.cudf.ColumnVector cudfCv) {
    super(type);
    // TODO need some checks to be sure everything matches
    this.cudfCv = cudfCv;
  }

  public final GpuColumnVector incRefCount() {
    // Just pass through the reference counting
    cudfCv.incRefCount();
    return this;
  }

  @Override
  public final void close() {
    // Just pass through the reference counting
    cudfCv.close();
  }

  @Override
  public final boolean hasNull() {
    return cudfCv.hasNulls();
  }

  @Override
  public final int numNulls() {
    return (int) cudfCv.getNullCount();
  }

  public static final long getTotalDeviceMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() > 0) {
      if (batch.column(0) instanceof GpuCompressedColumnVector) {
        GpuCompressedColumnVector gccv = (GpuCompressedColumnVector) batch.column(0);
        sum += gccv.getBuffer().getLength();
      } else {
        for (int i = 0; i < batch.numCols(); i++) {
          sum += ((GpuColumnVector) batch.column(i)).getBase().getDeviceMemorySize();
        }
      }
    }
    return sum;
  }

  public static final long getTotalDeviceMemoryUsed(GpuColumnVector[] cv) {
    long sum = 0;
    for (int i = 0; i < cv.length; i++){
      sum += cv[i].getBase().getDeviceMemorySize();
    }
    return sum;
  }

  public static final long getTotalDeviceMemoryUsed(Table tb) {
    long sum = 0;
    int len = tb.getNumberOfColumns();
    for (int i = 0; i < len; i++) {
      sum += tb.getColumn(i).getDeviceMemorySize();
    }
    return sum;
  }

  public final ai.rapids.cudf.ColumnVector getBase() {
    return cudfCv;
  }

  public final long getRowCount() { return cudfCv.getRowCount(); }

  public final RapidsHostColumnVector copyToHost() {
    return new RapidsHostColumnVector(type, cudfCv.copyToHost());
  }

  @Override
  public final String toString() {
    return getBase().toString();
  }
}
