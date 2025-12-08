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

import org.apache.iceberg.parquet.TypeWithSchemaVisitor
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.shaded.org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type => ParquetType}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.{Type, Types}
import org.apache.spark.sql.types._

import java.util.{List => JList}
import scala.annotation.nowarn


/** Generate the Spark schema corresponding to a Parquet schema and expected Iceberg schema */
class SparkSchemaConverter extends TypeWithSchemaVisitor[DataType] {

  override def message(iStruct: Types.StructType, message: MessageType, fields: JList[DataType])
  : DataType = struct(iStruct, message, fields)

  override def struct(iStruct: Types.StructType, struct: GroupType, fieldTypes: JList[DataType])
    : DataType = {

    val parquetFields = struct.getFields
    val fields: JList[StructField] = Lists.newArrayListWithExpectedSize(fieldTypes.size)
    for (i <- 0 until parquetFields.size) {
      val parquetField = parquetFields.get(i)
      require(!parquetField.isRepetition(ParquetType.Repetition.REPEATED),
        s"Fields cannot have repetition REPEATED: ${parquetField}")
      val isNullable = parquetField.isRepetition(ParquetType.Repetition.OPTIONAL)
      val field = StructField(parquetField.getName, fieldTypes.get(i), isNullable,
        Metadata.empty)
      fields.add(field)
    }
    new StructType(fields.toArray(new Array[StructField](0)))
  }


  override def list(iList: Types.ListType, array: GroupType, elementType: DataType): DataType = {
    val repeated = array.getType(0).asGroupType
    val element = repeated.getType(0)
    require(!element.isRepetition(ParquetType.Repetition.REPEATED),
      s"Elements cannot have repetition REPEATED: $element")
    val isNullable = element.isRepetition(ParquetType.Repetition.OPTIONAL)
    new ArrayType(elementType, isNullable)
  }

  override def map(iMap: Types.MapType,  map: GroupType, keyType: DataType, valueType: DataType):
  DataType = {
    val keyValue = map.getType(0).asGroupType
    val value = keyValue.getType(1)

    require(!value.isRepetition(ParquetType.Repetition.REPEATED),
      s"Values cannot have repetition REPEATED: ${value}")

    val isValueNullable = value.isRepetition(ParquetType.Repetition.OPTIONAL)
    new MapType(keyType, valueType, isValueNullable)
  }

  @nowarn("cat=deprecation")
  override def primitive(iPrimitive: Type.PrimitiveType, primitiveType: PrimitiveType): DataType = {
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