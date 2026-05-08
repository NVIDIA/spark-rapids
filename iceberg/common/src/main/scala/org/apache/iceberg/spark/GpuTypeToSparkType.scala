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
import org.apache.iceberg.spark.GpuTypeToSparkType.fieldMetadataOf
import org.apache.iceberg.types.{Type, Types, TypeUtil}

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
}

class GpuTypeToSparkType extends TypeUtil.SchemaVisitor[DataType] {
  override def schema(schema: Schema, struct: DataType): DataType = struct

  override def struct(struct: Types.StructType,
                      fieldResults: java.util.List[DataType]): DataType = {

    val sparkFields = struct.fields().asScala
      .zip(fieldResults.asScala)
      .map {
        case (field, fieldResult) =>
          val metadata = fieldMetadataOf(field.fieldId())
          var sparkField = StructField(field.name(), fieldResult, field.isOptional, metadata)
          if (field.doc() != null) {
            sparkField = sparkField.withComment(field.doc())
          }
          sparkField
      }

    StructType(sparkFields.toSeq)
  }

  override def field(field: Types.NestedField, fieldResult: DataType): DataType = fieldResult

  override def list(list: Types.ListType, elementResult: DataType): DataType =
    ArrayType(elementResult, list.isElementOptional)

  override def map(
      map: Types.MapType,
      keyResult: DataType,
      valueResult: DataType): DataType =
    MapType(keyResult, valueResult, map.isValueOptional)

  override def primitive(primitive: Type.PrimitiveType): DataType = {
    primitive.typeId() match {
      case Type.TypeID.BOOLEAN => BooleanType
      case Type.TypeID.INTEGER => IntegerType
      case Type.TypeID.LONG => LongType
      case Type.TypeID.FLOAT => FloatType
      case Type.TypeID.DOUBLE => DoubleType
      case Type.TypeID.DATE => DateType
      case Type.TypeID.TIME =>
        throw new UnsupportedOperationException("Spark does not support time fields")
      case Type.TypeID.TIMESTAMP =>
        val timestamp = primitive.asInstanceOf[Types.TimestampType]
        if (timestamp.shouldAdjustToUTC) {
          TimestampType
        } else {
          TimestampNTZType
        }
      case Type.TypeID.STRING => StringType
      case Type.TypeID.UUID => StringType
      case Type.TypeID.FIXED => BinaryType
      case Type.TypeID.BINARY => BinaryType
      case Type.TypeID.DECIMAL =>
        val decimal = primitive.asInstanceOf[Types.DecimalType]
        DecimalType(decimal.precision(), decimal.scale())
      case _ =>
        throw new UnsupportedOperationException(s"Cannot convert $primitive to Spark type")
    }
  }
}
