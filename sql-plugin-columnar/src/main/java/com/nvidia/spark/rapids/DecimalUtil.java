/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.DecimalUtils;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;

import scala.Option;

public final class DecimalUtil {
  private static final DecimalType BOOLEAN_DECIMAL = DataTypes.createDecimalType(1, 0);

  private DecimalUtil() {}

  public static DType createCudfDecimal(DecimalType dt) {
    return DecimalUtils.createDecimalType(dt.precision(), dt.scale());
  }

  public static ColumnVector outOfBounds(ColumnView input, DecimalType to) {
    return DecimalUtils.outOfBounds(input, to.precision(), to.scale());
  }

  /**
   * Return the size in bytes of the fixed-width data types.
   * WARNING: Do not use this method for variable-width data types.
   */
  public static int getDataTypeSize(DataType dt) {
    if (dt instanceof DecimalType && ((DecimalType) dt).precision() <= Decimal.MAX_INT_DIGITS()) {
      return 4;
    }
    return dt.defaultSize();
  }

  public static Option<DecimalType> optionallyAsDecimalType(DataType t) {
    if (t instanceof DecimalType) {
      return Option.apply((DecimalType) t);
    } else if (t instanceof ByteType) {
      return decimalTypeFor(DType.INT8);
    } else if (t instanceof ShortType) {
      return decimalTypeFor(DType.INT16);
    } else if (t instanceof IntegerType) {
      return decimalTypeFor(DType.INT32);
    } else if (t instanceof LongType) {
      return decimalTypeFor(DType.INT64);
    } else if (t instanceof BooleanType) {
      return Option.apply(BOOLEAN_DECIMAL);
    }
    return Option.empty();
  }

  public static DecimalType asDecimalType(DataType t) {
    Option<DecimalType> dt = optionallyAsDecimalType(t);
    if (dt.isDefined()) {
      return dt.get();
    }
    throw new IllegalArgumentException(
        "Internal Error: type " + t + " cannot automatically be cast to a supported DecimalType");
  }

  private static Option<DecimalType> decimalTypeFor(DType dtype) {
    return Option.apply(DataTypes.createDecimalType(dtype.getPrecisionForInt(), 0));
  }
}
