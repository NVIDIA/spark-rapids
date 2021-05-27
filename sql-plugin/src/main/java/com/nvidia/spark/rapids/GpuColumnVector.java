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

import ai.rapids.cudf.BaseDeviceMemoryBuffer;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.ArrowColumnBuilder;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.HostColumnVectorCore;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Schema;
import ai.rapids.cudf.Table;

import org.apache.arrow.memory.ReferenceManager;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
   * Print to standard error the contents of a table. Note that this should never be
   * called from production code, as it is very slow.  Also note that this is not production
   * code.  You might need/want to update how the data shows up or add in support for more
   * types as this really is just for debugging.
   * @param name the name of the table to print out.
   * @param cb the batch to print out.
   */
  public static synchronized void debug(String name, ColumnarBatch cb) {
    try (Table table = from(cb)) {
      debug(name, table);
    }
  }

  private static synchronized void debugGPUAddrs(String name, ai.rapids.cudf.ColumnView col) {
    try (BaseDeviceMemoryBuffer data = col.getData();
         BaseDeviceMemoryBuffer validity = col.getValid()) {
      System.err.println("GPU COLUMN " + name + " - NC: " + col.getNullCount()
          + " DATA: " + data + " VAL: " + validity);
    }
    if (col.getType() == DType.STRUCT) {
      for (int i = 0; i < col.getNumChildren(); i++) {
        try (ColumnView child = col.getChildColumnView(i)) {
          debugGPUAddrs(name + ":CHILD_" + i, child);
        }
      }
    } else if (col.getType() == DType.LIST) {
      try (ColumnView child = col.getChildColumnView(0)) {
        debugGPUAddrs(name + ":DATA", child);
      }
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
  public static synchronized void debug(String name, ai.rapids.cudf.ColumnView col) {
    debugGPUAddrs(name, col);
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
  public static synchronized void debug(String name, HostColumnVectorCore hostCol) {
    DType type = hostCol.getType();
    System.err.println("COLUMN " + name + " - " + type);
    if (type.isDecimalType()) {
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
    } else if (DType.STRUCT.equals(type)) {
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } // The struct child columns are printed out later on.
      }
      for (int i = 0; i < hostCol.getNumChildren(); i++) {
        debug(name + ":CHILD_" + i, hostCol.getChildColumnView(i));
      }
    } else if (DType.LIST.equals(type)) {
      System.err.println("OFFSETS");
      for (int i = 0; i < hostCol.getRowCount(); i++) {
        if (hostCol.isNull(i)) {
          System.err.println(i + " NULL");
        } else {
          System.err.println(i + " [" + hostCol.getStartListOffset(i) + " - " +
              hostCol.getEndListOffset(i) + ")");
        }
      }
      debug(name + ":DATA", hostCol.getChildColumnView(0));
    } else {
      System.err.println("TYPE " + type + " NOT SUPPORTED FOR DEBUG PRINT");
    }
  }

  private static void debugInteger(HostColumnVectorCore hostCol, DType intType) {
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

    private final ArrowBufReferenceHolder[] referenceHolders;

    /**
     * A collection of builders for building up columnar data from Arrow data.
     * @param schema the schema of the batch.
     */
    public GpuArrowColumnarBatchBuilder(StructType schema) {
      fields = schema.fields();
      int len = fields.length;
      builders = new ai.rapids.cudf.ArrowColumnBuilder[len];
      referenceHolders = new ArrowBufReferenceHolder[len];
      boolean success = false;

      try {
        for (int i = 0; i < len; i++) {
          StructField field = fields[i];
          builders[i] = new ArrowColumnBuilder(convertFrom(field.dataType(), field.nullable()));
          referenceHolders[i] = new ArrowBufReferenceHolder();
        }
        success = true;
      } finally {
        if (!success) {
          close();
        }
      }
    }

    protected int buildersLength() {
      return builders.length;
    }

    protected ColumnVector buildAndPutOnDevice(int builderIndex) {
      ai.rapids.cudf.ColumnVector cv = builders[builderIndex].buildAndPutOnDevice();
      GpuColumnVector gcv = new GpuColumnVector(fields[builderIndex].dataType(), cv);
      referenceHolders[builderIndex].releaseReferences();
      builders[builderIndex] = null;
      return gcv;
    }

    public void copyColumnar(ColumnVector cv, int colNum, boolean ignored, int rows) {
      referenceHolders[colNum].addReferences(
        HostColumnarToGpu.arrowColumnarCopy(cv, builder(colNum), rows)
      );
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
      for (ArrowBufReferenceHolder holder: referenceHolders) {
        holder.releaseReferences();
      }
    }
  }

  public static final class GpuColumnarBatchBuilder extends GpuColumnarBatchBuilderBase {
    private final ai.rapids.cudf.HostColumnVector.ColumnBuilder[] builders;

    /**
     * A collection of builders for building up columnar data.
     * @param schema the schema of the batch.
     * @param rows the maximum number of rows in this batch.
     */
    public GpuColumnarBatchBuilder(StructType schema, int rows) {
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

  private static final class ArrowBufReferenceHolder {
    private final List<ReferenceManager> references = new ArrayList<>();

    public void addReferences(List<ReferenceManager> refs) {
      references.addAll(refs);
      refs.forEach(ReferenceManager::retain);
    }

    public void releaseReferences() {
      if (references.isEmpty()) {
        return;
      }
      for (ReferenceManager ref: references) {
        ref.release();
      }
      references.clear();
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
        return DecimalUtil.createCudfDecimal(dt.precision(), dt.scale());
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
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0)) {
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
    try (GpuColumnarBatchBuilder builder = new GpuColumnarBatchBuilder(schema, 0)) {
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
  static boolean typeConversionAllowed(ColumnView cv, DataType colType) {
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

  static boolean typeConversionAllowed(Table table, DataType[] colTypes, int startCol, int endCol) {
    final int numColumns = endCol - startCol;
    assert numColumns == colTypes.length: "The number of columns and the number of types don't match";
    boolean ret = true;
    for (int colIndex = startCol; colIndex < endCol; colIndex++) {
      boolean t = typeConversionAllowed(table.getColumn(colIndex), colTypes[colIndex - startCol]);
      ret = ret && t;
    }
    return ret;
  }

  /**
   * This should only ever be called from an assertion. This is to avoid the performance overhead
   * of doing the complicated check in production.  Sadly this means that we don't get to give a
   * clear message about what part of the check failed, so the assertions that use this should
   * include in the message both types so a user can see what is different about them.
   */
  static boolean typeConversionAllowed(Table table, DataType[] colTypes) {
    final int numColumns = table.getNumberOfColumns();
    assert numColumns == colTypes.length: "The number of columns and the number of types don't " +
        "match " + table + " " + Arrays.toString(colTypes);
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
   * @param colTypes List of the column data types for the returned batch. It matches startColIndex
   *                 to untilColIndex instead of everything in table.
   * @param startColIndex index of the first vector you want in the final ColumnarBatch
   * @param untilColIndex until index of the columns. (ie doesn't include that column num)
   * @return a ColumnarBatch of the vectors from the table
   */
  public static ColumnarBatch from(Table table, DataType[] colTypes, int startColIndex, int untilColIndex) {
    assert table != null : "Table cannot be null";
    assert typeConversionAllowed(table, colTypes, startColIndex, untilColIndex) :
        "Type conversion is not allowed from " + table + " to " + Arrays.toString(colTypes) +
            " columns " + startColIndex + " to " + untilColIndex;
    int numColumns = untilColIndex - startColIndex;
    ColumnVector[] columns = new ColumnVector[numColumns];
    int finalLoc = 0;
    boolean success = false;
    try {
      for (int i = startColIndex; i < untilColIndex; i++) {
        columns[finalLoc] = from(table.getColumn(i).incRefCount(), colTypes[i - startColIndex]);
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
    assert typeConversionAllowed(cudfCv, type) : "Type conversion is not allowed from " +
        buildColumnTypeString(cudfCv) + " to " + type + " expected " + buildColumnTypeString(type);
    return new GpuColumnVector(type, cudfCv);
  }

  private static String buildColumnTypeString(ai.rapids.cudf.ColumnView view) {
    DType type = view.getType();
    if (type.isNestedType()) {
      StringBuilder sb = new StringBuilder(type.toString());
      sb.append("(");
      for (int i = 0; i < view.getNumChildren(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        try (ColumnView childView = view.getChildColumnView(i)) {
          sb.append(buildColumnTypeString(childView));
        }
      }
      sb.append(")");
      return sb.toString();
    }
    return type.toString();
  }

  private static String buildColumnTypeString(DataType sparkType) {
    DType dtype = toRapidsOrNull(sparkType);
    if (dtype != null) {
      return dtype.toString();
    }
    StringBuilder sb = new StringBuilder();
    if (sparkType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) sparkType;
      sb.append("LIST(");
      sb.append(buildColumnTypeString(arrayType.elementType()));
      sb.append(")");
    } else if (sparkType instanceof MapType) {
      MapType mapType = (MapType) sparkType;
      sb.append("LIST(STRUCT(");
      sb.append(buildColumnTypeString(mapType.keyType()));
      sb.append(",");
      sb.append(buildColumnTypeString(mapType.valueType()));
      sb.append("))");
    } else if (sparkType instanceof StructType) {
      StructType structType = (StructType) sparkType;
      sb.append(structType.iterator().map(f -> buildColumnTypeString(f.dataType()))
          .mkString("STRUCT(", ",", ")"));
    } else {
      throw new IllegalArgumentException("Unexpected data type: " + sparkType);
    }
    return sb.toString();
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
      throw new IllegalArgumentException("Type conversion error to " + type +
          ": expected cudf type " + buildColumnTypeString(type) +
          " found cudf type " + buildColumnTypeString(cudfCv));
    }
    return new GpuColumnVector(type, cudfCv);
  }

  public static GpuColumnVector from(Scalar scalar, int count, DataType sparkType) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar, count), sparkType);
  }

  /**
   * Creates a GpuColumnVector from a GpuScalar
   *
   * @param scalar the input GpuScalar
   * @param count the row number of the output column
   * @param sparkType the type of the output column
   * @return a GpuColumnVector. It should be closed to avoid memory leak.
   */
  public static GpuColumnVector from(GpuScalar scalar, int count, DataType sparkType) {
    return from(ai.rapids.cudf.ColumnVector.fromScalar(scalar.getBase(), count), sparkType);
  }

  /**
   * Creates a cudf ColumnVector where the elements are filled with nulls.
   *
   * NOTE: Besides the non-nested types, the array type is supported.
   *
   * @param count the row number of the output column
   * @param sparkType the expected data type of the output column
   * @return a ColumnVector filled with nulls. It should be closed to avoid memory leak.
   */
  public static ai.rapids.cudf.ColumnVector columnVectorFromNull(int count, DataType sparkType) {
    try (Scalar s = GpuScalar.from(null, sparkType)) {
      return ai.rapids.cudf.ColumnVector.fromScalar(s, count);
    }
  }

  /**
   * Creates a GpuColumnVector where the elements are filled with nulls.
   *
   * NOTE: Besides the non-nested types, the array type is supported.
   *
   * @param count the row number of the output column
   * @param sparkType the data type of the output column
   * @return a GpuColumnVector filled with nulls. It should be closed to avoid memory leak.
   */
  public static GpuColumnVector fromNull(int count, DataType sparkType) {
    return GpuColumnVector.from(columnVectorFromNull(count, sparkType), sparkType);
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
   * Increment the reference count for all columns in the input batch.
   */
  public static ColumnarBatch incRefCounts(ColumnarBatch batch) {
    for (ai.rapids.cudf.ColumnVector cv: extractBases(batch)) {
      cv.incRefCount();
    }
    return batch;
  }

  /**
   * Take the columns from all of the batches passed in and put them in a single batch. The order
   * of the columns is preserved.
   * <br/>
   * For example if we had <pre>combineColumns({A, B}, {C, D})</pre> The result would be a single
   * batch with <pre>{A, B, C, D}</pre>
   */
  public static ColumnarBatch combineColumns(ColumnarBatch ... batches) {
    boolean isFirst = true;
    int numRows = 0;
    ArrayList<ColumnVector> columns = new ArrayList<>();
    for (ColumnarBatch cb: batches) {
      if (isFirst) {
        numRows = cb.numRows();
        isFirst = false;
      } else {
        assert cb.numRows() == numRows : "Rows do not match expected " + numRows + " found " +
            cb.numRows();
      }
      int numColumns = cb.numCols();
      for (int i = 0; i < numColumns; i++) {
        columns.add(cb.column(i));
      }
    }
    ColumnarBatch ret = new ColumnarBatch(columns.toArray(new ColumnVector[columns.size()]), numRows);
    return incRefCounts(ret);
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
        HashSet<Long> found = new HashSet<>();
        for (int i = 0; i < batch.numCols(); i++) {
          ai.rapids.cudf.ColumnVector cv = ((GpuColumnVector)batch.column(i)).getBase();
          long id = cv.getNativeView();
          if (found.add(id)) {
            sum += cv.getDeviceMemorySize();
          }
        }
      }
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(GpuColumnVector[] vectors) {
    long sum = 0;
    HashSet<Long> found = new HashSet<>();
    for (GpuColumnVector vector : vectors) {
      ai.rapids.cudf.ColumnVector cv = vector.getBase();
      long id = cv.getNativeView();
      if (found.add(id)) {
        sum += cv.getDeviceMemorySize();
      }
    }
    return sum;
  }

  public static long getTotalDeviceMemoryUsed(Table table) {
    long sum = 0;
    int len = table.getNumberOfColumns();
    // Deduplicate columns that are the same
    HashSet<Long> found = new HashSet<>();
    for (int i = 0; i < len; i++) {
      ai.rapids.cudf.ColumnVector cv = table.getColumn(i);
      long id = cv.getNativeView();
      if (found.add(id)) {
        sum += cv.getDeviceMemorySize();
      }
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
