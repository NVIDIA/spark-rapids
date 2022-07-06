/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.DType;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.*;

public final class JCudfUtil {
  /**
   * Each JCUDF Row is 64-bit aligned.
   */
  public static final int JCUDF_ROW_ALIGNMENT = 8;
  /**
   * For Variable length fields, a record of (int offset, int elements) is 32-bit aligned.
   */
  public static final int JCUDF_VAR_LENGTH_FIELD_ALIGNMENT = 4;
  /**
   * Default alignment for data of variable length size. i.e., Struct data is 64-bit aligned.
   */
  public static final int JCUDF_VAR_LENGTH_DATA_DEFAULT_ALIGNMENT = 8;
  /**
   * The size of the variable length record.
   * It has (Int offset, Int length/elements).
   */
  public static final int JCUDF_VAR_LENGTH_FIELD_SIZE = 8;
  /**
   * An estimate of the String data size in bytes.
   */
  public static final int JCUDF_TYPE_STRING_LENGTH_ESTIMATE = 20;
  public static final int JCUDF_STRING_TYPE_ALIGNMENT = 1;

  /**
   * The maximum number of bytes of fixed columns that can be supported by the optimized CUDF
   * kernel.
   * The fixed-width optimized cudf kernel only supports up to 1.5 KB per row.
   */
  public static final int JCUDF_OPTIMIZED_CUDF_KERNEL_MAX_BYTES = 1536;

  /**
   * Spark by default limits codegen to 100 fields "spark.sql.codegen.maxFields".
   * The maximum is {@code 1536 - 8} with 64-bit alignment.
   */
  public static final int JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED =
      JCUDF_OPTIMIZED_CUDF_KERNEL_MAX_BYTES - JCUDF_ROW_ALIGNMENT;

  // Used to negate the alignment of variable-sized
  public static final int JCUDF_MAX_TYPE_ORDER = 16;


  public static final Set<DataType> fixedLengthDataTypes;

  // Decimal type of precision 8 bytes is also fixed-length.
  // See {@link #isFixedLength(DataType)}.
  static {
    fixedLengthDataTypes = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
                BooleanType, DateType, TimestampType
            )));
  }

  public static int alignOffset(int offset, int alignment) {
    return (offset + alignment - 1) & -alignment;
  }

  public static int calculateBitSetWidthInBytes(int numFields) {
    return (numFields + 7)/ 8;
  }

  public static boolean isFixedLength(DataType dt) {
    if (dt instanceof DecimalType) {
      return ((DecimalType) dt).precision() <= Decimal.MAX_LONG_DIGITS();
    }
    return fixedLengthDataTypes.contains(dt);
  }

  /**
   * Given a spark SQL datatype, it returns the size in bytes.
   *
   * @param dt Spark datatype.
   * @return size in bytes of dt
   */
  public static int getSizeForFixedLengthDataType(DataType dt) {
    if (dt instanceof DecimalType) {
      int precision = ((DecimalType) dt).precision();
      if (precision <= Decimal.MAX_INT_DIGITS()) {
        return 4;
      }
    }
    return dt.defaultSize();
  }

  /**
   * A method that represents the order of datatypes.
   * It is used to order the datatypes in order to generate a packed schema.
   * We should be careful on how we want to sort the references to variable-width columns.
   * Currently, we sort columns by the descending order of their data length.
   * For variable width data types, reference to variable-width data are 4-bytes aligned.
   * However, var-width data should be placed in ascending order of their alignment to reduce
   * packing following the validity bytes.
   *
   * For example, Strings are 1-byte aligned while Structs are 8-byte aligned. Therefore, Struct
   * columns are aligned first in the JCUDF.
   * TypeOrder(Integer) = 40
   * TypeOrder(String)  = 40 - 1 = 39
   * TypeOrder(Struct)  = 40 - 8 = 32
   *
   * @param dt the type of the data column.
   * @return the size of the datatype in bytes multiplied by 10 if dt is a fixed size type.
   *         Otherwise, it subtracts the alignment from 40.
   */
  public static int getDataTypeOrder(DataType dt) {
    if (isFixedLength(dt)) {
      // for fixed sized data, order is equivalent to the type size
      return getSizeForFixedLengthDataType(dt) * 10;
    }
    // For variable data types, return negative number to indicate alignment.
    // The bigger the alignment the higher the order of the data type.
    DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dt);
    int orderVal = JCUDF_VAR_LENGTH_FIELD_ALIGNMENT * 10;
    return orderVal - getDataAlignmentForDataType(rapidsType);
        //getDataAlignmentForDataType(rapidsType) - JCUDF_MAX_TYPE_ORDER;
  }

  /**
   * Sets the alignment for variable length data types.
   * This is used to align the data in the variable width section of the JCUDF row.
   * For fixed-length types, the alignment is equivalent to the type size in bytes.
   *
   * @param rapidsType a variable length data type.
   * @return the alignment in bytes of the RAPIDS type in the variable width JCUDF.
   */
  public static int getDataAlignmentForDataType(DType rapidsType) {
    switch (rapidsType.getTypeId()) {
      case STRING:
        // Strings are 1-byte aligned
        return JCUDF_STRING_TYPE_ALIGNMENT;
      case LIST:
        // Data aligned depends on the datatype. return 8 bytes for now.
      case STRUCT:
        // Structs are 8-bytes aligned
        return JCUDF_VAR_LENGTH_DATA_DEFAULT_ALIGNMENT;
      default:
        // all other types are aligned based on the size.
        return rapidsType.getSizeInBytes();
    }
  }

  public static int[] getPackedSchema(Attribute[] attributes) {
    Tuple2[] tuples = new Tuple2[attributes.length];
    int[] result = new int[attributes.length];
    return result;
  }

  /**
   * Sets an estimate size in bytes for variable-length data types.
   * This is used to get a rough estimate of the bytes needed to allocate JCUDF row.
   *
   * @param rapidsType a variable length data type.
   * @return size in bytes of the variable length datatype.
   */
  public static int getEstimateSizeForVarLengthTypes(DType rapidsType) {
    switch (rapidsType.getTypeId()) {
      case STRING:
        return JCUDF_TYPE_STRING_LENGTH_ESTIMATE;
      case LIST:
      case STRUCT:
        // Return 32 bytes for other types until we decide on how to handle each type.
      default:
        throw new IllegalArgumentException(rapidsType + " estimated size is not supported yet.");
    }
  }

  /**
   * Checks if CUDF Kernel can apply optimized row conversion on the Spark schema represented by
   * {@link CudfUnsafeRow}. Note that the calculation assumes that the {@code cudfUnsafeRow}
   * is not usable yet (i.e., {@link CudfUnsafeRow#pointTo(long, int)} is not called yet).
   *
   * The fixed-width optimized cudf kernel only supports up to 1.5 KB per row.
   * For fixed-size rows, we know the exact number of bytes used by the columns and the validity
   * bytes. We can use that value to branch over the size of the output to know which kernel to
   * call.
   *
   * @param cudfUnsafeRow the unsafeRow object that represents the Spark SQL Schema.
   * @return true if {@link CudfUnsafeRow#getFixedWidthSizeInBytes()} is less than
   *         {@link #JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED} which is 1528 Bytes.
   */
  public static boolean fitsOptimizedConversion(
      CudfUnsafeRow cudfUnsafeRow) {
    return !cudfUnsafeRow.isVariableWidthSchema() &&
        cudfUnsafeRow.getVariableSizeDataOffset() < JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED;
  }

  /**
   * @see #fitsOptimizedConversion(CudfUnsafeRow)
   * In some situations, we can calculate an estimate size for a schema withoput constructing
   * the object yet. In that case, we can use {@link JCudfRowVisitor} which already processed the
   * schema.
   */
  public static boolean fitsOptimizedConversion(
      JCudfRowOffsetsEstimator cudfRowOffsetMock) {
    return !cudfRowOffsetMock.hasVarSizeData() &&
        cudfRowOffsetMock.getCurrentByteOffset() < JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED;
  }

  /**
   * A helper class to calculate the columns, validity, and variable width offsets.
   * This helper is used to get an estimate size for the row including variable-sized rows.
   * The implementation does not make any assumptions whether the schema is packed or not.
   * Unlike {@link JCudfRowVisitor}, this implementation does not cache any calculations.
   */
  public static class JCudfRowOffsetsEstimator {
    Attribute[] attributes;
    int byteCursor = 0;
    int varSizeColIndex;
    int validityBytesOffset = 0;

    private JCudfRowOffsetsEstimator(Attribute[] attrArr) {
      this.attributes = attrArr;
      resetMeta();
    }

    private int calcColOffset(int ind) {
      int res;
      Attribute attr = attributes[ind];
      DataType dataType = attr.dataType();
      DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dataType);
      if (isFixedLength(dataType)) {
        int length = rapidsType.getSizeInBytes();
        res = alignOffset(byteCursor, length);
        byteCursor = res + length;
        return res;
      }
      if (!hasVarSizeData()) {
        // set the index to the first variable size column.
        varSizeColIndex = ind;
      }
      res = alignOffset(byteCursor, JCUDF_VAR_LENGTH_FIELD_ALIGNMENT);
      byteCursor = res + JCUDF_VAR_LENGTH_FIELD_SIZE;
      return res;
    }

    /**
     * Sets the column offsets
     * @param offsets array containing the offset for all columns
     * @return the current byte offset of the row.
     */
    public int setColumnOffsets(int[] offsets) {
      resetMeta();
      for (int i = 0; i < attributes.length; i++) {
        offsets[i] = calcColOffset(i);
      }
      return byteCursor;
    }

    private void resetMeta() {
      varSizeColIndex = attributes.length;
      byteCursor = 0;
    }

    private void setValidityBits(int fieldsCount){
      validityBytesOffset = byteCursor;
      byteCursor += calculateBitSetWidthInBytes(fieldsCount);
    }

    public int getEstimateSize() {
      resetMeta();
      for (int i = 0; i < attributes.length; i++) {
        calcColOffset(i);
      }
      // set the validity size
      setValidityBits(attributes.length);
      if (hasVarSizeData()) {
        for (int ind = varSizeColIndex; ind < attributes.length; ind++) {
          DataType dataType = attributes[ind].dataType();
          if (isFixedLength(dataType)) {
            continue;
          }
          DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dataType);
          addVarSizeData(rapidsType);
        }
      }
      byteCursor = alignOffset(byteCursor, JCUDF_ROW_ALIGNMENT);
      return byteCursor;
    }
    private int addVarSizeData(DType rapidsType) {
      int length = getEstimateSizeForVarLengthTypes(rapidsType);
      // For variable-length, alignment and length are not equivalent.
      int alignment = getDataAlignmentForDataType(rapidsType);
      int res = alignOffset(byteCursor, alignment);
      byteCursor = res + length;
      return res;
    }

    public boolean hasVarSizeData() {
      return attributes.length > 0 && varSizeColIndex != attributes.length;
    }

    protected int getCurrentByteOffset() {
      return byteCursor;
    }
  }

  /**
   * A helper class to calculate the columns, validity, and the variable width offsets.
   * The variable size data offset will fast-forward to calculate all offsets and cache the results
   * for optimization purpose (trade-off is more memory usage).
   * The size of the type is cached in {@link #typesLength}. The array will have negative values for
   * variable-size data types.
   * The implementation does not make any assumptions whether the schema is packed or not.
   */
  public static class JCudfRowVisitor {
    Attribute[] attributes;
    // caches the offsets of the columns.
    int[] offsets;
    // caches the size of the column
    int[] typesLength;
    int validitySizeInBytes;
    // number of bytes used by the fixed size section (fixed fields + validity bytes)
    int fixedWidthSizeInBytes;
    // counter used to advance the offset as fields get added
    int byteCursor = 0;
    // index used to iterate through the columns.
    int currColInd = -1;
    // where the validity starts
    int validityBytesOffset = 0;
    private JCudfRowVisitor(Attribute[] attrArr) {
      this.attributes = attrArr;
    }

    private void setValidityBits(int fieldsCount){
      validityBytesOffset = byteCursor;
      validitySizeInBytes = calculateBitSetWidthInBytes(fieldsCount);
      byteCursor += validitySizeInBytes;
    }

    /**
     * Calculate the offset of the given ind.
     * The #typesLength caches the size of the column.
     * If the data type is variable-size data, #typesLength will have a negative value that can be
     * used to look-up the type. String has a length of -15.
     *
     * @param ind index of the column
     * @return the offset of the colum.
     */
    private int calcColOffset(int ind) {
      int res;
      Attribute attr = attributes[ind];
      DataType dataType = attr.dataType();
      DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dataType);
      if (isFixedLength(dataType)) {
        typesLength[ind] = rapidsType.getSizeInBytes();
        res = alignOffset(byteCursor, typesLength[ind]);
        byteCursor = res + typesLength[ind];
        return res;
      }
      // For variable size data, return negative value to distinguish fixed Vs Non-fixed types.
      typesLength[ind] = getDataAlignmentForDataType(rapidsType) - JCUDF_MAX_TYPE_ORDER;
      res = alignOffset(byteCursor, JCUDF_VAR_LENGTH_FIELD_ALIGNMENT);
      byteCursor = res + JCUDF_VAR_LENGTH_FIELD_SIZE;
      return res;
    }

    private void initCudfRowData() {
      offsets = new int[attributes.length];
      typesLength = new int[attributes.length];
      fixedWidthSizeInBytes = 0;

      for (int i = 0 ; i < attributes.length; i++) {
        offsets[i] = calcColOffset(i);
      }
      setValidityBits(attributes.length);
      // add this point we know where exactly the width size section starts
      fixedWidthSizeInBytes = byteCursor;
    }

    /**
     * Advances the index to point to the next column.
     */
    public void getNextCol() {
      if (currColInd >= attributes.length - 1) {
        throw new RuntimeException("Accessing Out of bound column");
      }
      currColInd++;
    }

    public int getVariableDataOffset() {
      return fixedWidthSizeInBytes;
    }

    public int getColLength() {
      return typesLength[currColInd];
    }

    public int getColOffset() {
      return offsets[currColInd];
    }

    public int getValiditySizeInBytes() {
      return validitySizeInBytes;
    }

    public int getValidityBytesOffset() {
      return validityBytesOffset;
    }
  }

  public static JCudfRowVisitor getJCudfRowVisitor(Attribute[] attributes) {
    JCudfRowVisitor cudfRowVisitor = new JCudfRowVisitor(attributes);
    cudfRowVisitor.initCudfRowData();
    return cudfRowVisitor;
  }

  public static JCudfRowOffsetsEstimator getJCudfRowEstimator(Attribute[] attributes) {
    return new JCudfRowOffsetsEstimator(attributes);
  }
}
