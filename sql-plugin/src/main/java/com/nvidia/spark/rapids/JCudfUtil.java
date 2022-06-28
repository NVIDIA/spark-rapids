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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StringType;

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
   * The order of all fixed length types is positive, while the variable length data types are
   * represented by negative values.
   * For fixed columns, alignment is equivalent to the type size in bytes.
   * For variable length data types, the order is a function of the equivalent data alignment.
   * For example, Strings are 1-byte aligned while Structs are 8-byte aligned. Therefore, Struct
   * columns are aligned first in the JCUDF.
   * TypeOrder(String) = 1 - 16 = -15
   * TypeOrder(Struct) = 8 - 16 = -8
   *
   * @param dt the type of the data column.
   * @return the size of the datatype in bytes if dt is a fixed size typ. Otherwise, it returns
   *         a negative value (dataType_alignment - 16).
   */
  public static int getDataTypeOrder(DataType dt) {
    if (isFixedLength(dt)) {
      // for fixed sized data, order is equivalent to the type size
      return getSizeForFixedLengthDataType(dt);
    }
    // for variable data types, return negative number to indicate alignment
    int alignment = JCUDF_VAR_LENGTH_DATA_DEFAULT_ALIGNMENT;
    if (dt instanceof StringType) {
      // Strings are 1 byte aligned
      alignment = 1;
    }
//    else if (dt instanceof ArrayType) {
//      // for Arrays, the alignment depends on the Array type
//      // array of byte, is 1 byte aligned, array[int] is aligned by 4,
//      // Array[Long] or Array[Struct] alignment = 8;
//      // Array[int], alignment = 4;
//      // Array[byte], alignment = 1;
//    }
    // the bigger the alignment the higher the order of the data type.
    return alignment - 16;
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
        return 1;
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
    switch (rapidsType.getTypeId()) {
      case STRING:
        return JCUDF_TYPE_STRING_LENGTH_ESTIMATE;
      case LIST:
      case STRUCT:
        // Return 32 bytes for other types until we decide on how to handle each type.
        return 32;
      default:
        throw new IllegalArgumentException(rapidsType + " estimated size is not supported yet.");
    }
  }

  /**
   * Checks if CUDF Kernel can apply optimized row conversion on the Spark schema represented by
   * {@link CudfUnsafeRow}.
   * The fixed-width optimized cudf kernel only supports up to 1.5 KB per row which means at
   * Spark by default limits codegen to 100 fields "spark.sql.codegen.maxFields".
   * So, we are going to be cautious and start with that until we have tested it more.
   * The rough initial estimate size of the CudfUnsafeRow during initialization
   * {@link CudfUnsafeRow#getInitialSizeEstimate()}.
   * Note that the latter does not include alignment of the variable-width data types.
   *
   * @param cudfUnsafeRow the unsafeRow object that represents the Spark SQL Schema.
   * @return true if {@link CudfUnsafeRow#getInitialSizeEstimate()} is less than 1.1 KB.
   */
  public static boolean fitsOptimizedConversion(CudfUnsafeRow cudfUnsafeRow) {
    return cudfUnsafeRow.getInitialSizeEstimate() < 1152;
  }
}
