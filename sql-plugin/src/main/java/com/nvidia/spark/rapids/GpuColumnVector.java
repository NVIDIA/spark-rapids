/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.ArrowColumnBuilder;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Arrays;
import java.util.List;

/**
 * A GPU accelerated version of the Spark ColumnVector.
 * Most of the standard Spark APIs should never be called, as they assume that the data
 * is on the host, and we want to keep as much of the data on the device as possible.
 * We also provide GPU accelerated versions of the transitions to and from rows.
 */
public class GpuColumnVector extends GpuColumnVectorBase {

  /**
   * Print to standard error the contents of a table. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the table to print out.
   * @param table the table to print out.
   */
  public static synchronized void debug(String name, Table table) {
    System.err.println("DEBUG " + name + " " + table);
    for (int col = 0; col < table.getNumberOfColumns(); col++) {
      debug(String.valueOf(col), table.getColumn(col));
    }
  }

  /**
   * Print to standard error the contents of a column. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the column to print out.
   * @param col the column to print out.
   */
  public static synchronized void debug(String name, ai.rapids.cudf.ColumnVector col) {
    try (HostColumnVector hostCol = col.copyToHost()) {
      debug(name, hostCol);
    }
  }

  private static String hexString(byte[] bytes) {
    StringBuilder str = new StringBuilder();
    for (byte b : bytes) {
      str.append(String.format("%02x", b&0xff));
    }
    return str.toString();
  }

  /**
   * Print to standard error the contents of a column. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the column to print out.
   * @param hostCol the column to print out.
   */
  public static synchronized void debug(String name, HostColumnVector hostCol) {
    DType type = hostCol.getType();
    System.err.println("COLUMN " + name + " " + type);
    if (type.getTypeId() == DType.DTypeEnum.DECIMAL64) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " " + hostCol.getBigDecimal(i));
        }
      }
    } else if (DType.STRING.equals(type)) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " \"" + hostCol.getJavaString(i) + "\" " +
              hexString(hostCol.getUTF8(i)));
        }
      }
    } else if (DType.INT32.equals(type)
        || DType.INT8.equals(type)
        || DType.INT16.equals(type)
        || DType.INT64.equals(type)
        || DType.TIMESTAMP_DAYS.equals(type)
        || DType.TIMESTAMP_SECONDS.equals(type)
        || DType.TIMESTAMP_MICROSECONDS.equals(type)
        || DType.TIMESTAMP_MILLISECONDS.equals(type)
        || DType.TIMESTAMP_NANOSECONDS.equals(type)) {
      debugInteger(hostCol, type);
    } else if (DType.BOOL8.equals(type)) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " " + hostCol.getBoolean(i));
        }
      }
    } else if (DType.FLOAT64.equals(type)) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " " + hostCol.getDouble(i));
        }
      }
    } else if (DType.FLOAT32.equals(type)) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " " + hostCol.getFloat(i));
        }
      }
    } else {
      System.err.println("TYPE " + type + " NOT SUPPORTED FOR DEBUG PRINT");
    }
  }

  private static void debugInteger(HostColumnVector hostCol, DType intType) {
    for (int i = 0; i < hostCol.getRowCount(); i++) {
      if (hostCol.isNull(i)) {
        System.err.println(i + " NULL");
      } else {
        final int sizeInBytes = intType.getSizeInBytes();
        final Object value;
        switch (sizeInBytes) {
        case Byte.BYTES:
          value = hostCol.getByte(i);
          break;
        case Short.BYTES:
          value = hostCol.getShort(i);
          break;
        case Integer.BYTES:
          value = hostCol.getInt(i);
          break;
        case Long.BYTES:
          value = hostCol.getLong(i);
          break;
        default:
          throw new IllegalArgumentException("INFEASIBLE: Unsupported integer-like type " + intType);
        }
        System.err.println(i + " " + value);
      }
    }
  }

  private static HostColumnVector.DataType convertFrom(DataType spark, boolean nullable) {
    if (spark instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) spark;
      return new HostColumnVector.ListType(nullable,
          convertFrom(arrayType.elementType(), arrayType.containsNull()));
    } else if (spark instanceof MapType) {
      MapType mapType = (MapType) spark;
      return new HostColumnVector.ListType(nullable,
          new HostColumnVector.StructType(false, Arrays.asList(
              convertFrom(mapType.keyType(), false),
              convertFrom(mapType.valueType(), mapType.valueContainsNull())
          )));
    } else if (spark instanceof StructType) {
      StructType stType = (StructType) spark;
      HostColumnVector.DataType[] children = new HostColumnVector.DataType[stType.size()];
      StructField[] fields = stType.fields();
      for (int i = 0; i < children.length; i++) {
        children[i] = convertFrom(fields[i].dataType(), fields[i].nullable());
      }
      return new HostColumnVector.StructType(nullable, children);
    } else {
      // Only works for basic types
      return new HostColumnVector.BasicType(nullable, getNonNestedRapidsType(spark));
    }
  }

  public static abstract class GpuColumnarBatchBuilderBase implements AutoCloseable {
    protected StructField[] fields;

    public abstract void close();
    public abstract void copyColumnar(ColumnVector cv, int colNum, boolean nullable, int rows);

    protected abstract ColumnVector buildAndPutOnDevice(int builderIndex);
    protected abstract int buildersLength();

    public ColumnarBatch build(int rows) {
      int buildersLen = buildersLength();
      ColumnVector[] vectors = new ColumnVector[buildersLen];
      boolean success = false;
      try {
        for (int i = 0; i < buildersLen; i++) {
          vectors[i] = buildAndPutOnDevice(i);
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
  }

  public static final class GpuArrowColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
    private final ai.rapids.cudf.ArrowColumnBuilder[] builders;

    /**
     * A collection of builders for building up columnar data from Arrow data.
     * @param schema the schema of the batch.
     * @param rows the maximum number of rows in this batch.
     * @param batch if this is going to copy a ColumnarBatch in a non GPU format that batch
     *              we are going to copy. If not this may be null. This is used to get an idea
     *              of how big to allocate buffers that do not necessarily correspond to the
     *              number of rows.
     */
    public GpuArrowColumnarBatchBuilder(StructType schema, int rows, ColumnarBatch batch) {
      fields = schema.fields();
      int len = fields.length;
      builders = new ai.rapids.cudf.ArrowColumnBuilder[len];
      boolean success = false;

      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          builders[i] = new ArrowColumnBuilder(convertFrom(field.dataType(), field.nullable()));
        }
        success = true;
      } finally {
        if (!success) {
          for (ai.rapids.cudf.ArrowColumnBuilder b: builders) {
            if (b != null) {
              b.close();
            }
          }
        }
      }
    }

    protected int buildersLength() {
      return builders.length;
    }

    protected ColumnVector buildAndPutOnDevice(int builderIndex) {
      ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
      GpuColumnVector gcv = new GpuColumnVector(fields[builderIndex].dataType(), cv);
      builders[builderIndex] = null;
      return gcv;
    }

    public void copyColumnar(ColumnVector cv, int colNum, boolean nullable, int rows) {
      HostColumnarToGpu.arrowColumnarCopy(cv, builder(colNum), nullable, rows);
    }

    public ai.rapids.cudf.ArrowColumnBuilder builder(int i) {
      return builders[i];
    }

    @Override
    public void close() {
      for (ai.rapids.cudf.ArrowColumnBuilder b: builders) {
        if (b != null) {
          b.close();
        }
      }
    }
  }

  public static final class GpuColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
    private final ai.rapids.cudf.HostColumnVector.ColumnBuilder[] builders;

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
      builders = new ai.rapids.cudf.HostColumnVector.ColumnBuilder[len];
      boolean success = false;
      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          builders[i] = new HostColumnVector.ColumnBuilder(convertFrom(field.dataType(), field.nullable()), rows);
        }
        success = true;
      } finally {
        if (!success) {
          for (ai.rapids.cudf.HostColumnVector.ColumnBuilder b: builders) {
            if (b != null) {
              b.close();
            }
          }
        }
      }
    }

    public void copyColumnar(ColumnVector cv, int colNum, boolean nullable, int rows) {
      HostColumnarToGpu.columnarCopy(cv, builder(colNum), nullable, rows);
    }

    public ai.rapids.cudf.HostColumnVector.ColumnBuilder builder(int i) {
      return builders[i];
    }

    protected int buildersLength() {
      return builders.length;
    }

    protected ColumnVector buildAndPutOnDevice(int builderIndex) {
      ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
      GpuColumnVector gcv = new GpuColumnVector(fields[builderIndex].dataType(), cv);
      builders[builderIndex] = null;
      return gcv;
    }

    public HostColumnVector[] buildHostColumns() {
      HostColumnVector[] vectors = new HostColumnVector[builders.length];
      try {
        for (int i = 0; i < builders.length; i++) {
          vectors[i] = builders[i].build();
          builders[i] = null;
        }
        HostColumnVector[] result = vectors;
        vectors = null;
        return result;
      } finally {
        if (vectors != null) {
          for (HostColumnVector v : vectors) {
            if (v != null) {
              v.close();
            }
          }
        }
      }
    }

    @Override
    public void close() {
      for (ai.rapids.cudf.HostColumnVector.ColumnBuilder b: builders) {
        if (b != null) {
          b.close();
        }
      }
    }
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
      return DType.TIMESTAMP_DAYS;
    } else if (type instanceof TimestampType) {
      return DType.TIMESTAMP_MICROSECONDS;
    } else if (type instanceof StringType) {
      return DType.STRING;
    } else if (type instanceof NullType) {
      // INT8 is used for both in this case
      return DType.INT8;
    } else if (type instanceof DecimalType) {
      // Decimal supportable check has been conducted in the GPU plan overriding stage.
      // So, we don't have to handle decimal-supportable problem at here.
      DecimalType dt = (DecimalType) type;
      if (dt.precision() > DType.DECIMAL64_MAX_PRECISION) {
        return null;
      } else {
        // Map all DecimalType to DECIMAL64, in case of underlying DType transaction.
        return DType.create(DType.DTypeEnum.DECIMAL64, -dt.scale());
      }
    }
    return null;
  }

  public static boolean isNonNestedSupportedType(DataType type) {
    return toRapidsOrNull(type) != null;
  }

  public static DType getNonNestedRapidsType(DataType type) {
    DType result = toRapidsOrNull(type);
    if (result == null) {
      throw new IllegalArgumentException(type + " is not supported for GPU processing yet.");
    }
    return result;
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static ColumnarBatch emptyBatch(StructType schema) {
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0, null)) {
      return builder.build(0);
    }
  }

  /**
   * Create an empty batch from the given format.  This should be used very sparingly because
   * returning an empty batch from an operator is almost always the wrong thing to do.
   */
  public static ColumnarBatch emptyBatch(List<Attribute> format) {
    return emptyBatch(structFromAttributes(format));
  }


  /**
   * Create empty host column vectors from the given format.  This should only be necessary
   * when serializing an empty broadcast table.
   */
  public static HostColumnVector[] emptyHostColumns(StructType schema) {
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0, null)) {
      return builder.buildHostColumns();
    }
  }

  /**
   * Create empty host column vectors from the given format.  This should only be necessary
   * when serializing an empty broadcast table.
   */
  public static HostColumnVector[] emptyHostColumns(List<Attribute> format) {
    return emptyHostColumns(structFromAttributes(format));
  }

  private static StructType structFromAttributes(List<Attribute> format) {
    StructField[] fields = new StructField[format.size()];
    int i = 0;
    for (Attribute attribute: format) {
      fields[i++] = new StructField(
          attribute.name(),
          attribute.dataType(),
          attribute.nullable(),
          null);
    }
    return new StructType(fields);
  }

  /**
   * Convert a Spark schema into a cudf schema
   * @param input the Spark schema to convert
   * @return the cudf schema
   */
  public static Schema from(StructType input) {
    Schema.Builder builder = Schema.builder();
    input.foreach(f -> builder.column(GpuColumnVector.getNonNestedRapidsType(f.dataType()), f.name()));
    return builder.build();
  }

  /**
   * Convert a ColumnarBatch to a table. The table will increment the reference count for all of
   * the columns in the batch, so you will need to close both the batch passed in and the table
   * returned to avoid any memory leaks.
   */
  public static Table from(ColumnarBatch batch) {
    return new Table(extractBases(batch));
  }

  /**
   * Get the data types for a batch.
   */
  public static DataType[] extractTypes(ColumnarBatch batch) {
    DataType[] ret = new DataType[batch.numCols()];
    for (int i = 0; i < batch.numCols(); i++) {
      ret[i] = batch.column(i).dataType();
    }
    return ret;
  }

  /**
   * Get the data types for a struct.
   */
  public static DataType[] extractTypes(StructType st) {
    DataType[] ret = new DataType[st.size()];
    for (int i = 0; i < st.size(); i++) {
      ret[i] = st.apply(i).dataType();
    }
    return ret;
  }

  /**
   * Convert a Table to a ColumnarBatch.  The columns in the table will have their reference counts
   * incremented so you will need to close both the table passed in and the batch returned to
   * not have any leaks.
   * @param colTypes the types of the columns that should be returned.
   */
  public static ColumnarBatch from(Table table, DataType[] colTypes) {
    return from(table, colTypes, 0, table.getNumberOfColumns());
  }

  /**
   * Returns true if the cudf column can be used for the specified Spark type.
   */
  private static boolean typeConversionAllowed(ColumnView cv, DataType colType) {
    DType dt = cv.getType();
    if (!dt.isNestedType()) {
      return getNonNestedRapidsType(colType).equals(dt);
    }
    if (colType instanceof MapType) {
      MapType mType = (MapType) colType;
      // list of struct of key/value
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView structCv = cv.getChildColumnView(0)) {
        if (!(structCv.getType().equals(DType.STRUCT))) {
          return false;
        }
        if (structCv.getNumChildren() != 2) {
          return false;
        }
        try (ColumnView keyCv = structCv.getChildColumnView(0)) {
          if (!typeConversionAllowed(keyCv, mType.keyType())) {
            return false;
          }
        }
        try (ColumnView valCv = structCv.getChildColumnView(1)) {
          return typeConversionAllowed(valCv, mType.valueType());
        }
      }
    } else if (colType instanceof ArrayType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView tmp = cv.getChildColumnView(0)) {
        return typeConversionAllowed(tmp, ((ArrayType) colType).elementType());
      }
    } else if (colType instanceof StructType) {
      if (!(dt.equals(DType.STRUCT))) {
        return false;
      }
      StructType st = (StructType) colType;
      final int numChildren = cv.getNumChildren();
      if (numChildren != st.size()) {
        return false;
      }
      for (int childIndex = 0; childIndex < numChildren; childIndex++) {
        try (ColumnView tmp = cv.getChildColumnView(childIndex)) {
          StructField entry = ((StructType) colType).apply(childIndex);
          if (!typeConversionAllowed(tmp, entry.dataType())) {
            return false;
          }
        }
      }
      return true;
    } else if (colType instanceof BinaryType) {
      if (!(dt.equals(DType.LIST))) {
        return false;
      }
      try (ColumnView tmp = cv.getChildColumnView(0)) {
        DType tmpType = tmp.getType();
        return tmpType.equals(DType.INT8) || tmpType.equals(DType.UINT8);
      }
    } else {
      // Unexpected type
      return false;
    }
  }

  /**
   * This should only ever be called from an assertion. This is to avoid the performance overhead
   * of doing the complicated check in production.  Sadly this means that we don't get to give a
   * clear message about what part of the check failed, so the assertions that use this should
   * include in the message both types so a user can see what is different about them.
   */
  static boolean typeConversionAllowed(Table table, DataType[] colTypes) {
    final int numColumns = table.getNumberOfColumns();
    if (numColumns != colTypes.length) {
      return false;
    }
    boolean ret = true;
    for (int colIndex = 0; colIndex < numColumns; colIndex++) {
      ret = ret && typeConversionAllowed(table.getColumn(colIndex), colTypes[colIndex]);
    }
    return ret;
  }

  /**
   * Get a ColumnarBatch from a set of columns in the Table. This gets the columns
   * starting at startColIndex and going until but not including untilColIndex. This will
   * increment the reference count for all columns converted so you will need to close
   * both the table that is passed in and the batch returned to be sure that there are no leaks.
   *
   * @param table a table of vectors
   * @param colTypes List of the column data types in the table passed in
   * @param startColIndex index of the first vector you want in the final ColumnarBatch
   * @param untilColIndex until index of the columns. (ie doesn't include that column num)
   * @return a ColumnarBatch of the vectors from the table
   */
  public static ColumnarBatch from(Table table, DataType[] colTypes, int startColIndex, int untilColIndex) {
    assert table != null : "Table cannot be null";
    assert typeConversionAllowed(table, colTypes) : "Type conversion is not allowed from " + table +
        " to " + Arrays.toString(colTypes);
    int numColumns = untilColIndex - startColIndex;
    ColumnVector[] columns = new ColumnVector[numColumns];
    int finalLoc = 0;
    boolean success = false;
    try {
      for (int i = startColIndex; i < untilColIndex; i++) {
        columns[finalLoc] = from(table.getColumn(i).incRefCount(), colTypes[i]);
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
   * Converts a cudf internal vector to a Spark compatible vector. No reference counts
   * are incremented so you need to either close the returned value or the input value,
   * but not both.
   */
  public static GpuColumnVector from(ai.rapids.cudf.ColumnVector cudfCv, DataType type) {
    assert typeConversionAllowed(cudfCv, type) : "Type conversion is not allowed from " + cudfCv +
        " to " + type;
    return new GpuColumnVector(type, cudfCv);
  }

  /**
   * Converts a cudf internal vector to a Spark compatible vector. No reference counts
   * are incremented so you need to either close the returned value or the input value,
   * but not both. This conversion performs an unconditional check that the types are
   * convertible rather than an assertion check.
   * @throws IllegalArgumentException if the type conversion check fails
   */
  public static GpuColumnVector fromChecked(ai.rapids.cudf.ColumnVector cudfCv, DataType type) {
    if (!typeConversionAllowed(cudfCv, type)) {
      throw new IllegalArgumentException("Type conversion is not allowed from " + cudfCv +
          " to " + type);
    }
    return new GpuColumnVector(type, cudfCv);
  }

  public static GpuColumnVector from(Scalar scalar, int count, DataType sparkType) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar, count), sparkType);
  }

  /**
   * Get the underlying cudf columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static ai.rapids.cudf.ColumnVector[] extractBases(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    ai.rapids.cudf.ColumnVector[] vectors = new ai.rapids.cudf.ColumnVector[numColumns];
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i)).getBase();
    }
    return vectors;
  }

  /**
   * Get the underlying Spark compatible columns from the batch.  This does not increment any
   * reference counts so if you want to use these columns after the batch is closed
   * you will need to do that on your own.
   */
  public static GpuColumnVector[] extractColumns(ColumnarBatch batch) {
    int numColumns = batch.numCols();
    GpuColumnVector[] vectors = new GpuColumnVector[numColumns];

    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = ((GpuColumnVector)batch.column(i));
    }
    return vectors;
  }

  /**
   * Convert the table into columns and return them, outside of a ColumnarBatch.
   * @param colType the types of the columns.
   */
  public static GpuColumnVector[] extractColumns(Table table, DataType[] colType) {
    try (ColumnarBatch batch = from(table, colType)) {
      return extractColumns(batch);
    }
  }

  private final ai.rapids.cudf.ColumnVector cudfCv;

  /**
   * Take an INT32 column vector and return a host side int array.  Don't use this for anything
   * too large.  Note that this ignores validity totally.
   */
  public static int[] toIntArray(ai.rapids.cudf.ColumnVector vec) {
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

  public static long getTotalDeviceMemoryUsed(ColumnarBatch batch) {
    long sum = 0;
    if (batch.numCols() > 0) {
      if (batch.column(0) instanceof WithTableBuffer) {
        WithTableBuffer wtb = (WithTableBuffer) batch.column(0);
        sum += wtb.getTableBuffer().getLength();
      } else {
        for (int i = 0; i < batch.numCols(); i++) {
          sum += ((GpuColumnVector) batch.column(i)).getBase().getDeviceMemorySize();
        }
      }
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(GpuColumnVector[] cv) {
    long sum = 0;
    for (int i = 0; i < cv.length; i++){
      sum += cv[i].getBase().getDeviceMemorySize();
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(Table tb) {
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
