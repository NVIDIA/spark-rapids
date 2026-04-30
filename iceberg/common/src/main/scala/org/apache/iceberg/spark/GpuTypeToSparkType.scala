/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.SchemaUtils._
import org.apache.iceberg.{MetadataColumns, Schema}
import org.apache.iceberg.spark.GpuTypeToSparkType.fieldMetadataOf
import org.apache.iceberg.types.{Types, TypeUtil}

import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types._

object GpuTypeToSparkType {
  def toSparkType(schema: Schema): StructType = {
    TypeUtil.visit(schema, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  def toSparkType(icebergStruct: Types.StructType): StructType = {
    TypeUtil.visit(icebergStruct, new GpuTypeToSparkType).asInstanceOf[StructType]
  }

  private[iceberg] def fieldMetadataOf(fieldId: Int): Metadata = {
    val builder = new MetadataBuilder()
    .putLong(FIELD_ID_METADATA_KEY, fieldId)

    if (MetadataColumns.metadataFieldIds().contains(fieldId)) {
      builder.putBoolean(METADATA_COL_ATTR_KEY, true)
    }

    builder.build()
  }

  private def nestedMetadataJson(fieldType: org.apache.iceberg.types.Type): Option[String] = {
    val builder = new MetadataBuilder()
    appendNestedFieldIdMetadata(builder, fieldType)
    val json = builder.build().json
    if (json == "{}") {
      None
    } else {
      Some(json)
    }
  }

  private def appendNestedFieldIdMetadata(
      builder: MetadataBuilder,
      fieldType: org.apache.iceberg.types.Type): Unit = {
    fieldType match {
      case list: Types.ListType =>
        val elementField = list.fields().asScala.head
        builder.putLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY, elementField.fieldId())
        nestedMetadataJson(elementField.`type`())
          .foreach(builder.putString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY, _))
      case map: Types.MapType =>
        val fields = map.fields().asScala
        val keyField = fields.head
        val valueField = fields(1)
        builder.putLong(MAP_KEY_FIELD_ID_METADATA_KEY, keyField.fieldId())
        nestedMetadataJson(keyField.`type`())
          .foreach(builder.putString(MAP_KEY_NESTED_IDS_METADATA_KEY, _))
        builder.putLong(MAP_VALUE_FIELD_ID_METADATA_KEY, valueField.fieldId())
        nestedMetadataJson(valueField.`type`())
          .foreach(builder.putString(MAP_VALUE_NESTED_IDS_METADATA_KEY, _))
      case _ =>
    }
  }
}

class GpuTypeToSparkType extends TypeToSparkType {
  override def struct(struct: Types.StructType,
                      fieldResults: java.util.List[DataType]): DataType = {

    val sparkFields = struct.fields().asScala
      .zip(fieldResults.asScala)
      .map {
        case (field, fieldResult) =>
          val metadataBuilder = new MetadataBuilder().withMetadata(fieldMetadataOf(field.fieldId()))
          GpuTypeToSparkType.appendNestedFieldIdMetadata(metadataBuilder, field.`type`())
          val metadata = metadataBuilder.build()
          var sparkField = StructField(field.name(), fieldResult, field.isOptional, metadata)
          if (field.doc() != null) {
            sparkField = sparkField.withComment(field.doc())
          }
          sparkField
      }

    StructType(sparkFields.toSeq)
  }
}
