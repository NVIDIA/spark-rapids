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

package org.apache.spark.sql.rapids

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{ArrayType, ByteType, CalendarIntervalType, DataType, DataTypes, Decimal, DecimalType, IntegerType, LongType, MapType, NullType, StructField, StructType, UserDefinedType}

object PCBSSchemaConverter {
  val intervalStructType = new StructType()
      .add("_days", IntegerType)
      .add("_months", IntegerType)
      .add("_ms", LongType)

  def getSupportedDataType(
      curId: AtomicLong,
      dataType: DataType,
      mapping: mutable.HashMap[DataType, DataType]): DataType = {
    dataType match {
      case CalendarIntervalType =>
        intervalStructType
      case NullType =>
        ByteType
      case s: StructType =>
        val newStructType = StructType(
          s.indices.map { index =>
            StructField(curId.getAndIncrement().toString,
              getSupportedDataType(curId, s.fields(index).dataType, mapping),
              s.fields(index).nullable, s.fields(index).metadata)
          })
        mapping.put(s, newStructType)
        newStructType
      case a@ArrayType(elementType, nullable) =>
        val newArrayType =
          ArrayType(getSupportedDataType(curId, elementType, mapping), nullable)
        mapping.put(a, newArrayType)
        newArrayType
      case m@MapType(keyType, valueType, nullable) =>
        val newKeyType = getSupportedDataType(curId, keyType, mapping)
        val newValueType = getSupportedDataType(curId, valueType, mapping)
        val mapType = MapType(newKeyType, newValueType, nullable)
        mapping.put(m, mapType)
        mapType
      case d: DecimalType if d.scale < 0 =>
        val newType = if (d.precision <= Decimal.MAX_INT_DIGITS) {
          IntegerType
        } else {
          LongType
        }
        newType
      case _ =>
        dataType
    }
  }

  def getSupportedSchemaFromUnsupported(
      cachedAttributes: Seq[Attribute],
      mapping: mutable.HashMap[DataType, DataType]): Seq[Attribute] = {

    // We only handle CalendarIntervalType, Decimals and NullType ATM convert it to a supported type
    val curId = new AtomicLong()
    cachedAttributes.map {
      attribute =>
        val name = s"_col${curId.getAndIncrement()}"
        attribute.dataType match {
          case CalendarIntervalType =>
            AttributeReference(name, intervalStructType,
              attribute.nullable, metadata = attribute.metadata)(attribute.exprId)
                .asInstanceOf[Attribute]
          case NullType =>
            AttributeReference(name, DataTypes.ByteType,
              nullable = true, metadata =
                attribute.metadata)(attribute.exprId).asInstanceOf[Attribute]
          case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | DecimalType() =>
            AttributeReference(name,
              getSupportedDataType(curId, attribute.dataType, mapping),
              attribute.nullable, attribute.metadata)(attribute.exprId)
          case udt: UserDefinedType[_] =>
            AttributeReference(name,
              getSupportedDataType(curId, udt.sqlType, mapping),
              attribute.nullable, attribute.metadata)(attribute.exprId)
          case _ =>
            attribute.withName(name)
        }
    }
  }

}
