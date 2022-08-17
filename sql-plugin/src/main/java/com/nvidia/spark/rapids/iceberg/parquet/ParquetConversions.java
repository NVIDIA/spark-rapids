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

package com.nvidia.spark.rapids.iceberg.parquet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.apache.iceberg.types.Type;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/** Derived from Apache Iceberg's ParquetConversions class. */
public class ParquetConversions {
  private ParquetConversions() {
  }

  static Function<Object, Object> converterFromParquet(PrimitiveType parquetType, Type icebergType) {
    Function<Object, Object> fromParquet = converterFromParquet(parquetType);
    if (icebergType != null) {
      if (icebergType.typeId() == Type.TypeID.LONG &&
          parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32) {
        return value -> ((Integer) fromParquet.apply(value)).longValue();
      } else if (icebergType.typeId() == Type.TypeID.DOUBLE &&
          parquetType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FLOAT) {
        return value -> ((Float) fromParquet.apply(value)).doubleValue();
      }
    }

    return fromParquet;
  }

  static Function<Object, Object> converterFromParquet(PrimitiveType type) {
    if (type.getOriginalType() != null) {
      switch (type.getOriginalType()) {
        case UTF8:
          // decode to CharSequence to avoid copying into a new String
          return binary -> StandardCharsets.UTF_8.decode(((Binary) binary).toByteBuffer());
        case DECIMAL:
          int scale = type.getDecimalMetadata().getScale();
          switch (type.getPrimitiveTypeName()) {
            case INT32:
            case INT64:
              return num -> BigDecimal.valueOf(((Number) num).longValue(), scale);
            case FIXED_LEN_BYTE_ARRAY:
            case BINARY:
              return bin -> new BigDecimal(new BigInteger(((Binary) bin).getBytes()), scale);
            default:
              throw new IllegalArgumentException(
                  "Unsupported primitive type for decimal: " + type.getPrimitiveTypeName());
          }
        default:
      }
    }

    switch (type.getPrimitiveTypeName()) {
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return binary -> ByteBuffer.wrap(((Binary) binary).getBytes());
      default:
    }

    return obj -> obj;
  }
}
