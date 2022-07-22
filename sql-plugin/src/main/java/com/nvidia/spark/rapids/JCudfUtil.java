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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
   * The maximum buffer size allocated to copy JCudf row.
   */
  public static final long JCUDF_MAX_DATA_BUFFER_LENGTH =
      Integer.MAX_VALUE - (JCUDF_ROW_ALIGNMENT - 1);

  /**
   * Spark by default limits codegen to 100 fields "spark.sql.codegen.maxFields".
   * The maximum is {@code 1536 - 7} with 64-bit alignment.
   */
  public static final int JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED =
      JCUDF_OPTIMIZED_CUDF_KERNEL_MAX_BYTES - (JCUDF_ROW_ALIGNMENT - 1);

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
    return (offset - 1 + alignment) & -alignment;
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
   * A method used to order the columns to pack the schema based on the JCudf specifications.
   * It sorts fixed-width columns by their length.
   * Variable-length columns store 8 bytes, but are 4 byte aligned since they are two 4-byte
   * values.
   * However, if both datatypes are variable-length, then we want the type with larger alignment to
   * come last for better packing of the data.
   *
   * @param dt1 first datatype to compare.
   * @param dt2 second datatype to compare.
   * @return true when dt1 has more precedence than dt2.
   */
  public static boolean compareDataTypePrecedence(DataType dt1, DataType dt2) {
    int refSize1;
    int refSize2;
    int dataAlignSize1 = 0;
    int dataAlignSize2 = 0;
    if (isFixedLength(dt1)){
      refSize1 = getSizeForFixedLengthDataType(dt1);
    } else {
      refSize1 = JCUDF_VAR_LENGTH_FIELD_ALIGNMENT;
      dataAlignSize1 = getDataAlignmentForDataType(GpuColumnVector.getNonNestedRapidsType(dt1));
    }
    if (isFixedLength(dt2)){
      refSize2 = getSizeForFixedLengthDataType(dt2);
    } else {
      refSize2 = JCUDF_VAR_LENGTH_FIELD_ALIGNMENT;
      dataAlignSize2 = getDataAlignmentForDataType(GpuColumnVector.getNonNestedRapidsType(dt2));
    }
    if (dataAlignSize2 > 0 && dataAlignSize1 > 0) { // both types are variable length
      return dataAlignSize1 < dataAlignSize2;
    }
    return refSize1 > refSize2;
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

  /**
   * Sets an estimate size in bytes for variable-length data types.
   * This is used to get a rough estimate of the bytes needed to allocate JCUDF row.
   *
   * @param rapidsType a variable length data type.
   * @return size in bytes of the variable length datatype.
   */
  public static int getEstimateSizeForVarLengthTypes(DType rapidsType) {
    if (rapidsType.equals(DType.STRING)) {
      return JCUDF_TYPE_STRING_LENGTH_ESTIMATE;
    }
    throw new IllegalArgumentException(rapidsType + " estimated size is not supported yet.");
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
   * Note that Spark by default limits codegen to 100 fields {@code spark.sql.codegen.maxFields}".
   *
   * @param cudfUnsafeRow the unsafeRow object that represents the Spark SQL Schema.
   * @return true if {@link CudfUnsafeRow#getFixedWidthInBytes()} is less than
   *         {@link #JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED} which is 1529 Bytes to account for
   *         the alignment.
   */
  public static boolean fitsOptimizedConversion(
      CudfUnsafeRow cudfUnsafeRow) {
    return !cudfUnsafeRow.isVariableWidthSchema() &&
        cudfUnsafeRow.getFixedWidthInBytes() < JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED;
  }

  /**
   * @see #fitsOptimizedConversion(CudfUnsafeRow)
   * In some situations, we can calculate an estimate size for a schema without constructing
   * the object yet. In that case, we can use {@link RowBuilder} which iterates through the
   * columns to get an estimate of the bytes occupied by the entire row.
   */
  public static boolean fitsOptimizedConversion(
      RowOffsetsCalculator cudfRowOffsetMock) {
    return !cudfRowOffsetMock.hasVarSizeData() &&
        cudfRowOffsetMock.getEstimateSize() < JCUDF_MAX_FIXED_ROW_SIZE_FITS_OPTIMIZED;
  }

  /**
   * A helper class to calculate the columns offsets and validity size/offset.
   * It is also used to get a rough estimate of the memory size used by {@link CudfUnsafeRow}.
   * The implementation saves the offset calculations only if {@link #offsets} is not null. This
   * implementation avoids allocating an array when caching the offsets is not needed.
   */
  public static class RowOffsetsCalculator {
    private final Attribute[] attributes;
    private int[] offsets;
    // moving cursor pointing to the current offset.
    private int byteCursor;
    // the first column that has var-width size.
    private int varSizeColIndex;
    // At what point validity data starts.
    private int validityBytesOffset;
    // Caches the estimate size (8-bytes aligned). Calculating and reading this field is
    // synchronized.
    private int estimateRowSize;

    private RowOffsetsCalculator(Attribute[] attrArr, int[] offsetArr) {
      // offsetArr can be null.
      // This is in the context of constructing an object to get an estimate of the memory needed
      // by the row.
      this.attributes = attrArr;
      resetMeta();
      initFixedWidthSection(offsetArr);
    }

    /**
     * Handles iterating on all the columns to advance the {@link #byteCursor}.
     * Then, it calculates the start offset of the validity and its size in bytes.
     * This method updates the following: {@link #byteCursor}, {@link #offsets},
     * {@link #varSizeColIndex}, and all the fields related to the validity
     *
     * @param offsetArr if not NULL, it will be used to save the column offsets.
     */
    private void initFixedWidthSection(int[] offsetArr) {
      if (offsetArr == null) {
        for (int i = 0; i < attributes.length; i++) {
          advanceColCursorAndGet(i);
        }
      } else {
        this.offsets = offsetArr;
        for (int i = 0; i < attributes.length; i++) {
          offsets[i] = advanceColCursorAndGet(i);
        }
      }
      // set the validity size, and mark where it starts
      setValidityBits(attributes.length);
    }

    /**
     * Given the column-index, it increments the {@link #byteCursor} by the length of the datatype
     * of that column.
     * It takes into considerations the alignment of the column.
     * Note that this method expects sequential increments to the column. Accessing columns in
     * random order would result in incorrect calculations.
     *
     * @param ind the index of the column in the schema.
     * @return At what point the column starts.
     */
    private int advanceColCursorAndGet(int ind) {
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

    private void resetMeta() {
      varSizeColIndex = attributes.length;
      byteCursor = 0;
      estimateRowSize = -1;
      validityBytesOffset = 0;
    }

    private void setValidityBits(int fieldsCount) {
      validityBytesOffset = byteCursor;
      byteCursor += calculateBitSetWidthInBytes(fieldsCount);
    }

    /**
     * This is used internally to estimate the memory needed by the row.
     * The value calculated is 8-bytes aligned.
     * It loops on variable-width columns using a moving-cursor that represents the offset
     * of the data.
     * Note that the calculation requires that {@link #initFixedWidthSection(int[])} has
     * been called at some point before. Otherwise, the {@link #byteCursor} will be 0.
     */
    private void calculateEstimateSize() {
      // at this point the cursor points to the beginning of the variableWidth section
      estimateRowSize = byteCursor;
      if (hasVarSizeData()) {
        for (int ind = varSizeColIndex; ind < attributes.length; ind++) {
          DataType dataType = attributes[ind].dataType();
          if (isFixedLength(dataType)) {
            continue;
          }
          DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dataType);
          estimateRowSize = advanceDataCursorAndGet(estimateRowSize, rapidsType);
        }
      }
      // for row estimate, we need just to align to row alignment
      estimateRowSize = alignOffset(estimateRowSize, JCUDF_ROW_ALIGNMENT);
    }

    /**
     * Increments the data cursor by the length of the column type.
     * It takes into considerations the datatype alignment.
     * See {@link #getDataAlignmentForDataType(DType)}.
     *
     * @param dataOffsetCursor current offset of the data values.
     * @param rapidsType the type of the current column.
     * @return data cursor (1-byte aligned) after advancing by the length of the data.
     */
    private int advanceDataCursorAndGet(int dataOffsetCursor, DType rapidsType) {
      // For variable-length, alignment and length are not equivalent.
      int length = getEstimateSizeForVarLengthTypes(rapidsType);
      int alignment = getDataAlignmentForDataType(rapidsType);
      dataOffsetCursor = alignOffset(dataOffsetCursor, alignment);
      return dataOffsetCursor + length;
    }

    /**
     * Used to check whether a row is variable-width or not.
     * This assumes that an empty schema is "fixed-width".
     *
     * @return true if the schema is non-empty and at least one column is variable-width size.
     */
    public boolean hasVarSizeData() {
      return attributes.length > 0 && varSizeColIndex != attributes.length;
    }

    public int getValidityBytesOffset() {
      return validityBytesOffset;
    }

    /**
     * Reads the estimate memory that may be used by the row.
     * This method is thread-safe, but it is not expected that multiple-threads are accessing the
     * same row object concurrently.
     *
     * @return the estimate memory occupied by the row in bytes (8-bytes aligned).
     */
    public synchronized int getEstimateSize() {
      if (estimateRowSize < 0) {
        // not initialized yet
        calculateEstimateSize();
      }
      return estimateRowSize;
    }
  }

  /**
   * A helper class to calculate columns and validity offsets along with Code-generation that is
   * used to copy row from {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow} to
   * {@link CudfUnsafeRow}.
   * The implementation assumes that columns are: accessed sequentially; and they are not
   * necessarily packed.
   * This class does not need to cache the offsets/types since all the calculations are consumed
   * inside a loop.
   * It is important to note that failing to iterate on all the columns will break the offsets of
   * the validity bytes and the variable-width data.
   * Note: Although building the code using {@code String.format()} looks more clean and readable,
   * it is not preferred in terms of performance.
   */
  public static class RowBuilder {
    private static final String COPY_STRING_DATA_METHOD_NAME = "copyUTF8StringInto";
    public static final String GET_CUDF_ROW_SIZE_METHOD_NAME = "getCudfRowLength";

    public static final String COPY_STRING_DATA_CODE = String.join("\n"
        , "private int " + COPY_STRING_DATA_METHOD_NAME + "("
        , "    int ordinal, Object src, long baseOffset, int sparkValidityOffset,"
        , "    long startAddress, long bufferEndAddress, int cudfColOffset, int dataDstOffset) {"
        , "  final long unsafeCudfColOffset = startAddress + cudfColOffset;"
        , "  // check that the field is null"
        , "  if (org.apache.spark.unsafe.bitset.BitSetMethods.isSet(src, baseOffset, ordinal)) {"
        , "    // The string is null. This assumes that we keep the offset position."
        , "    // The size should be 0, the address is 0 since no data is stored."
        , "    Platform.putInt(null, unsafeCudfColOffset, 0);"
        , "    Platform.putInt(null, unsafeCudfColOffset + 4, 0);"
        , "    return 0;"
        , "  }"
        , "  // calculate the spark field offset"
        , "  long sparkFieldOffset = baseOffset + sparkValidityOffset + (ordinal * 8);"
        , "  // get the offset and the size from Spark UnsafeFormat"
        , "  final long strOffsetAndSize = Platform.getLong(src, sparkFieldOffset);"
        , "  final int strOffset = (int) (strOffsetAndSize >> 32);"
        , "  final int strSize = (int) strOffsetAndSize;"
        , "  // verify that the strSize will not cause to write past buffer"
        , "  long cudfDstAddress = startAddress + dataDstOffset;"
        , "  long newDataOffset = cudfDstAddress + strSize;"
        , "  if (newDataOffset > bufferEndAddress) {"
        , "    return Integer.MIN_VALUE;"
        , "  }"
        , "  // set the offset and length in the fixed width section."
        , "  Platform.putInt(null, unsafeCudfColOffset, dataDstOffset);"
        , "  Platform.putInt(null, unsafeCudfColOffset + 4, strSize);"
        , "  // copy the data"
        , "  Platform.copyMemory("
        , "      src, baseOffset + strOffset, null, cudfDstAddress, strSize);"
        , "  return strSize;"
        , "}");

    public static final String GET_CUDF_ROW_SIZE_CODE = String.join("\n"
        , "private int "+ GET_CUDF_ROW_SIZE_METHOD_NAME + "(int bytesOffset) {"
        , " return (bytesOffset +"
            + (JCUDF_ROW_ALIGNMENT - 1) + ") & -" + JCUDF_ROW_ALIGNMENT + ";"
        , "}");

    private final Attribute[] attributes;
    // size of the validity bytes.
    private int validitySizeInBytes;
    // number of bytes used by the fixed size section (fixed fields + validity bytes)
    private int fixedWidthSizeInBytes;
    // moving cursor pointing to the last byte in the row as columns are visited.
    private int byteCursor = 0;
    // where the validity starts
    private int validityBytesOffset = 0;

    private RowBuilder(Attribute[] attrArr) {
      this.attributes = attrArr;
    }

    private void setValidityBits(int fieldsCount){
      validityBytesOffset = byteCursor;
      validitySizeInBytes = calculateBitSetWidthInBytes(fieldsCount);
      byteCursor += validitySizeInBytes;
      fixedWidthSizeInBytes = byteCursor;
    }

    /**
     * Given a column-index, it calculates the offset of the column (including relevant alignments)
     * and returns code string used to copy the column from
     * {@link org.apache.spark.sql.catalyst.expressions.UnsafeRow} to {@link CudfUnsafeRow}.
     * The columns have to be accessed sequentially to update the moving-cursor {@link #byteCursor}.
     *
     * @param colIndex the sequential index of the column in the schema
     * @param rowBase the Spark row object address.
     * @param rowBaseOffset relative offset from the beginning of the row object address.
     * @param sparkValidityOffset the size of the Spark validity bytes. It is 8-bytes aligned.
     * @param cudfAddress the start address of the cudf row.
     * @param cudfDataCursor the variable used by the generated-code to point to the address of
     *                       the next variable-width data.
     * @return generated-code to copy the column from the source unsafeRow to the Cudf destination
     *         row.
     */
    public String generateCopyCodeColumn(
        int colIndex, String rowBase, String rowBaseOffset, String sparkValidityOffset,
        String cudfAddress, String cudfEndAddress, String cudfDataCursor) {
      Attribute attr = attributes[colIndex];
      DataType dataType = attr.dataType();
      DType rapidsType = GpuColumnVector.getNonNestedRapidsType(dataType);
      String colIndAsStr = String.valueOf(colIndex);
      String result;
      int columnSize = rapidsType.getSizeInBytes();
      if (columnSize == 0) { // this can be STRING, LIST, or STRUCT
        columnSize = JCUDF_VAR_LENGTH_FIELD_SIZE;
        byteCursor = alignOffset(byteCursor, JCUDF_VAR_LENGTH_FIELD_ALIGNMENT);
        if (rapidsType.equals(DType.STRING)) {
          result =
            cudfDataCursor
                + " += "
                + COPY_STRING_DATA_METHOD_NAME + "("
                + String.join(", ", colIndAsStr, rowBase, rowBaseOffset,
                      sparkValidityOffset, cudfAddress, cudfEndAddress,
                      String.valueOf(byteCursor), cudfDataCursor)
                + ");\n"
                + "if (" + cudfDataCursor + " < 0) {\n"
                + "  return " + cudfDataCursor + ";\n"
                + "}";
        } else {
          // we support String only for now.
          throw new IllegalStateException(
              "column index " + colIndAsStr + ", DType:" + rapidsType + " NOT SUPPORTED YET" );
        }
      } else { // other types including fixed-width size
        byteCursor = alignOffset(byteCursor, columnSize);
        String cudfDstOffset = cudfAddress + " + " + byteCursor;
        String sparkSrcOffset =
            rowBase + ", " + rowBaseOffset + " + " + sparkValidityOffset
                + " + (" + colIndAsStr + " * 8)";
        switch (columnSize) {
          case 1:
            result = "Platform.putByte(null, " + cudfDstOffset
                + ", Platform.getByte(" + sparkSrcOffset + "));";
            break;
          case 2:
            result = "Platform.putShort(null, " + cudfDstOffset
                + ", Platform.getShort(" + sparkSrcOffset + "));";
            break;
          case 4:
            result = "Platform.putInt(null, " + cudfDstOffset
                + ", Platform.getInt(" + sparkSrcOffset + "));";
            break;
          case 8:
            result = "Platform.putLong(null, " + cudfDstOffset
                + ", Platform.getLong(" + sparkSrcOffset + "));";
            break;
          default:
            throw new IllegalStateException(
                "column index " + colIndAsStr + ", DType:" + rapidsType + " NOT SUPPORTED YET" );
        }
      }
      // update cursor
      byteCursor += columnSize;
      if (colIndex == attributes.length - 1) {
        // this is the last column.
        // we can mark the validityBytes offsets
        setValidityBits(attributes.length);
      }
      return result;
    }


    public int getVariableDataOffset() {
      return fixedWidthSizeInBytes;
    }

    public int getValiditySizeInBytes() {
      return validitySizeInBytes;
    }

    public int getValidityBytesOffset() {
      return validityBytesOffset;
    }

    // used for testing
    public int getByteCursor() {
      return byteCursor;
    }
  }

  public static RowBuilder getRowBuilder(Attribute[] attributes) {
    return new RowBuilder(attributes);
  }

  public static RowOffsetsCalculator getRowOffsetsCalculator(Attribute[] attributes) {
    return getRowOffsetsCalculator(attributes, null);
  }

  public static RowOffsetsCalculator getRowOffsetsCalculator(Attribute[] attributes, int[] offsets) {
    return new RowOffsetsCalculator(attributes, offsets);
  }
}
