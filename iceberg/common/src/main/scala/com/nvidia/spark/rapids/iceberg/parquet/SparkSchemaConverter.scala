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


package com.nvidia.spark.rapids.iceberg.parquet

import java.util.{List => JList}

import scala.annotation.nowarn

import org.apache.iceberg.parquet.TypeWithSchemaVisitor
import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type => ParquetType}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.{Type, Types}
import org.apache.iceberg.types.Type.TypeID

import org.apache.spark.sql.types._


/** Generate the Spark schema corresponding to a Parquet schema and expected Iceberg schema */
class SparkSchemaConverter extends TypeWithSchemaVisitor[DataType] {

  override def message(iStruct: Types.StructType, message: MessageType, fields: JList[DataType])
  : DataType = struct(iStruct, message, fields)

  override def struct(iStruct: Types.StructType, struct: GroupType, fieldTypes: JList[DataType])
    : DataType = {
    import scala.collection.JavaConverters._

    // Handle case where struct is null (whole struct missing from file)
    if (struct == null) {
      if (iStruct == null) {
        return new StructType()
      }
      // Return struct with all expected fields (will be filled with nulls by post-processor)
      val icebergFields = iStruct.fields().asScala
      val fields = icebergFields.zipWithIndex.map { case (icebergField, i) =>
        StructField(icebergField.name(), fieldTypes.get(i), icebergField.isOptional, Metadata.empty)
      }
      return new StructType(fields.toArray)
    }

    // Use parquet field names and nullability, with types from fieldTypes (in iceberg order)
    // The visitor visits fields in iceberg (expected) order, so fieldTypes[i] corresponds to
    // the type for icebergFields[i].
    val parquetFields = struct.getFields.asScala
    val icebergFields = if (iStruct != null) iStruct.fields().asScala else Seq.empty

    // Check if all parquet fields have IDs for ID-based matching
    val allParquetFieldsHaveIds = parquetFields.forall(pf => pf.getId != null)

    val fields = if (allParquetFieldsHaveIds && iStruct != null && parquetFields.size == icebergFields.size) {
      // Map parquet field IDs to their positions in iceberg schema to get correct fieldTypes index
      parquetFields.map { parquetField =>
        val parquetId = parquetField.getId.intValue()
        val icebergIdx = icebergFields.indexWhere(_.fieldId() == parquetId)
        require(icebergIdx >= 0, s"No iceberg field found for parquet field with ID $parquetId")
        require(!parquetField.isRepetition(ParquetType.Repetition.REPEATED),
          s"Fields cannot have repetition REPEATED: ${parquetField}")
        val isNullable = parquetField.isRepetition(ParquetType.Repetition.OPTIONAL)
        StructField(parquetField.getName, fieldTypes.get(icebergIdx), isNullable, Metadata.empty)
      }
    } else if (!allParquetFieldsHaveIds || iStruct == null) {
      // Position-based matching for internal structures (map key-value) or when no iceberg schema
      parquetFields.zipWithIndex.map { case (parquetField, i) =>
        require(!parquetField.isRepetition(ParquetType.Repetition.REPEATED),
          s"Fields cannot have repetition REPEATED: ${parquetField}")
        val isNullable = parquetField.isRepetition(ParquetType.Repetition.OPTIONAL)
        require(i < fieldTypes.size(), s"Parquet field at index $i has no corresponding type")
        StructField(parquetField.getName, fieldTypes.get(i), isNullable, Metadata.empty)
      }
    } else {
      // Schema evolution: different field counts, use ID-based matching
      val icebergIdToIdx: Map[Int, Int] = icebergFields.zipWithIndex.map { case (f, i) =>
        f.fieldId() -> i
      }.toMap

      parquetFields.flatMap { parquetField =>
        Option(parquetField.getId).flatMap { id =>
          icebergIdToIdx.get(id.intValue()).map { icebergIdx =>
            require(!parquetField.isRepetition(ParquetType.Repetition.REPEATED),
              s"Fields cannot have repetition REPEATED: ${parquetField}")
            val isNullable = parquetField.isRepetition(ParquetType.Repetition.OPTIONAL)
            StructField(parquetField.getName, fieldTypes.get(icebergIdx), isNullable, Metadata.empty)
          }
        }
      }
    }

    new StructType(fields.toArray)
  }


  override def list(iList: Types.ListType, array: GroupType, elementType: DataType): DataType = {
    // Handle null array (list not in file)
    if (array == null) {
      val isNullable = iList.isElementOptional
      return new ArrayType(elementType, isNullable)
    }
    val repeated = array.getType(0).asGroupType
    val element = repeated.getType(0)
    require(!element.isRepetition(ParquetType.Repetition.REPEATED),
      s"Elements cannot have repetition REPEATED: $element")
    val isNullable = element.isRepetition(ParquetType.Repetition.OPTIONAL)
    new ArrayType(elementType, isNullable)
  }

  override def map(iMap: Types.MapType, map: GroupType, keyType: DataType, valueType: DataType):
  DataType = {
    // Handle null map (map not in file)
    if (map == null) {
      val isValueNullable = iMap.isValueOptional
      return new MapType(keyType, valueType, isValueNullable)
    }
    val keyValue = map.getType(0).asGroupType
    val value = keyValue.getType(1)

    require(!value.isRepetition(ParquetType.Repetition.REPEATED),
      s"Values cannot have repetition REPEATED: ${value}")

    val isValueNullable = value.isRepetition(ParquetType.Repetition.OPTIONAL)
    new MapType(keyType, valueType, isValueNullable)
  }

  @nowarn("cat=deprecation")
  override def primitive(iPrimitive: Type.PrimitiveType, primitiveType: PrimitiveType): DataType = {
    // Handle case where primitiveType is null (field in expected schema but not in file)
    // This can happen during schema evolution when a column is added
    if (primitiveType == null) {
      // Return the Spark type for the expected Iceberg type
      // The post-processor will fill this column with nulls or constants
      return SparkSchemaUtil.convert(iPrimitive)
    }

    // Handle case where iPrimitive is null (field in file but not in expected schema)
    // This shouldn't normally happen since we visit the expected schema
    if (iPrimitive == null) {
      // Fall back to converting from Parquet type directly
      import org.apache.iceberg.shaded.org.apache.parquet.schema.LogicalTypeAnnotation
      val logicalType = primitiveType.getLogicalTypeAnnotation

      return primitiveType.getPrimitiveTypeName match {
        case PrimitiveType.PrimitiveTypeName.BOOLEAN => BooleanType
        case PrimitiveType.PrimitiveTypeName.INT32 => IntegerType
        case PrimitiveType.PrimitiveTypeName.INT64 => LongType
        case PrimitiveType.PrimitiveTypeName.FLOAT => FloatType
        case PrimitiveType.PrimitiveTypeName.DOUBLE => DoubleType
        case PrimitiveType.PrimitiveTypeName.BINARY =>
          if (logicalType != null && logicalType.isInstanceOf[LogicalTypeAnnotation.StringLogicalTypeAnnotation]) {
            StringType
          } else {
            BinaryType
          }
        case PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
          if (logicalType != null && logicalType.isInstanceOf[LogicalTypeAnnotation.DecimalLogicalTypeAnnotation]) {
            val decimalAnnotation = logicalType.asInstanceOf[LogicalTypeAnnotation.DecimalLogicalTypeAnnotation]
            DecimalType(decimalAnnotation.getPrecision, decimalAnnotation.getScale)
          } else {
            BinaryType
          }
        case _ => throw new UnsupportedOperationException(
          s"Unsupported Parquet primitive type: ${primitiveType.getPrimitiveTypeName}")
      }
    }

    // If up-casts are needed, load as the pre-cast Spark type, and this will be up-cast in
    iPrimitive.typeId match {
      case TypeID.LONG =>
        if (primitiveType.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
          IntegerType
        } else {
          LongType
        }
      case TypeID.DOUBLE =>
        if (primitiveType.getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT) {
          FloatType
        } else {
          DoubleType
        }
      case TypeID.DECIMAL =>
        val metadata = primitiveType.getDecimalMetadata
        DecimalType(metadata.getPrecision, metadata.getScale)
      case _ =>
        SparkSchemaUtil.convert(iPrimitive)
    }
  }
}