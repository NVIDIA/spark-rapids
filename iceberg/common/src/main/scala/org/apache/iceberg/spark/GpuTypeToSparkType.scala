/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark

import scala.collection.JavaConverters._

import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.spark.GpuTypeToSparkType._
import org.apache.iceberg.types.{Type, Types, TypeUtil}

import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types._

object GpuTypeToSparkType {
  // Metadata keys for nested type field IDs (maps and arrays)
  val MAP_KEY_FIELD_ID_METADATA_KEY = "parquet.field.id.map.key"
  val MAP_VALUE_FIELD_ID_METADATA_KEY = "parquet.field.id.map.value"
  val ARRAY_ELEMENT_FIELD_ID_METADATA_KEY = "parquet.field.id.array.element"

  def toSparkType(schema: Schema): StructType = {
    TypeUtil.visit(schema, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  def toSparkType(icebergStruct: Types.StructType): StructType = {
    TypeUtil.visit(icebergStruct, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  /**
   * Build metadata for a field, including its field ID and optionally nested type field IDs.
   * For maps, this includes key and value field IDs.
   * For arrays, this includes element field ID.
   */
  private[iceberg] def fieldMetadataOf(fieldId: Int, icebergType: Type): Metadata = {
    val builder = new MetadataBuilder()
      .putLong(FIELD_ID_METADATA_KEY, fieldId)

    if (MetadataColumns.metadataFieldIds().contains(fieldId)) {
      builder.putBoolean(METADATA_COL_ATTR_KEY, true)
    }

    // Add nested type field IDs
    icebergType match {
      case mapType: Types.MapType =>
        builder.putLong(MAP_KEY_FIELD_ID_METADATA_KEY, mapType.keyId())
        builder.putLong(MAP_VALUE_FIELD_ID_METADATA_KEY, mapType.valueId())
      case listType: Types.ListType =>
        builder.putLong(ARRAY_ELEMENT_FIELD_ID_METADATA_KEY, listType.elementId())
      case _ =>
    }

    builder.build()
  }

  // Backward compatible version without type
  private[iceberg] def fieldMetadataOf(fieldId: Int): Metadata = {
    val builder = new MetadataBuilder()
      .putLong(FIELD_ID_METADATA_KEY, fieldId)

    if (MetadataColumns.metadataFieldIds().contains(fieldId)) {
      builder.putBoolean(METADATA_COL_ATTR_KEY, true)
    }

    builder.build()
  }
}

class GpuTypeToSparkType extends TypeToSparkType {
  override def struct(struct: Types.StructType,
                      fieldResults: java.util.List[DataType]): DataType = {

    val sparkFields = struct.fields().asScala
      .zip(fieldResults.asScala)
      .map {
        case (field, fieldResult) =>
          // Include nested type field IDs for maps and arrays
          val metadata = fieldMetadataOf(field.fieldId(), field.`type`())
          var sparkField = StructField(field.name(), fieldResult, field.isOptional, metadata)
          if (field.doc() != null) {
            sparkField = sparkField.withComment(field.doc())
          }
          sparkField
      }

    StructType(sparkFields.toSeq)
  }
}
