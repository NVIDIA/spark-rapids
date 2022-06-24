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

package com.nvidia.spark.rapids.iceberg.spark;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;

/**
 * Helper methods for working with Spark/Hive metadata.
 */
public class SparkSchemaUtil {
  private SparkSchemaUtil() {
  }

  /**
   * Convert a {@link Schema} to a {@link DataType Spark type}.
   *
   * @param schema a Schema
   * @return the equivalent Spark type
   * @throws IllegalArgumentException if the type cannot be converted to Spark
   */
  public static StructType convert(Schema schema) {
    return (StructType) TypeUtil.visit(schema, new TypeToSparkType());
  }

  /**
   * Convert a {@link Type} to a {@link DataType Spark type}.
   *
   * @param type a Type
   * @return the equivalent Spark type
   * @throws IllegalArgumentException if the type cannot be converted to Spark
   */
  public static DataType convert(Type type) {
    return TypeUtil.visit(type, new TypeToSparkType());
  }

  public static StructType convertWithoutConstants(Schema schema, Map<Integer, ?> idToConstant) {
    return (StructType) TypeUtil.visit(schema, new TypeToSparkType() {
      @Override
      public DataType struct(Types.StructType struct, List<DataType> fieldResults) {
        List<Types.NestedField> fields = struct.fields();

        List<StructField> sparkFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
        for (int i = 0; i < fields.size(); i += 1) {
          Types.NestedField field = fields.get(i);
          // skip fields that are constants
          if (idToConstant.containsKey(field.fieldId())) {
            continue;
          }
          DataType type = fieldResults.get(i);
          StructField sparkField = StructField.apply(
              field.name(), type, field.isOptional(), Metadata.empty());
          if (field.doc() != null) {
            sparkField = sparkField.withComment(field.doc());
          }
          sparkFields.add(sparkField);
        }

        return StructType$.MODULE$.apply(sparkFields);
      }
    });
  }

  /**
   * Estimate approximate table size based on Spark schema and total records.
   *
   * @param tableSchema  Spark schema
   * @param totalRecords total records in the table
   * @return approximate size based on table schema
   */
  public static long estimateSize(StructType tableSchema, long totalRecords) {
    if (totalRecords == Long.MAX_VALUE) {
      return totalRecords;
    }

    long result;
    try {
      result = LongMath.checkedMultiply(tableSchema.defaultSize(), totalRecords);
    } catch (ArithmeticException e) {
      result = Long.MAX_VALUE;
    }
    return result;
  }

  public static void validateMetadataColumnReferences(Schema tableSchema, Schema readSchema) {
    List<String> conflictingColumnNames = readSchema.columns().stream()
        .map(Types.NestedField::name)
        .filter(name -> MetadataColumns.isMetadataColumn(name) && tableSchema.findField(name) != null)
        .collect(Collectors.toList());

    ValidationException.check(
        conflictingColumnNames.isEmpty(),
        "Table column names conflict with names reserved for Iceberg metadata columns: %s.\n" +
            "Please, use ALTER TABLE statements to rename the conflicting table columns.",
        conflictingColumnNames);
  }

  public static Map<Integer, String> indexQuotedNameById(Schema schema) {
    Function<String, String> quotingFunc = name -> String.format("`%s`", name.replace("`", "``"));
    return TypeUtil.indexQuotedNameById(schema.asStruct(), quotingFunc);
  }
}
