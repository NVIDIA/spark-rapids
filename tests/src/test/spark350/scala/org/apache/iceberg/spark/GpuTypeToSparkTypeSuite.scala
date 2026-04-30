/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark

import com.nvidia.spark.rapids.SchemaUtils._
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, Metadata, MetadataBuilder, StructType}

class GpuTypeToSparkTypeSuite extends AnyFunSuite {

  test("nestedMetadataJson returns None for primitives") {
    assert(GpuTypeToSparkType.nestedMetadataJson(Types.IntegerType.get()).isEmpty)
    assert(GpuTypeToSparkType.nestedMetadataJson(Types.StringType.get()).isEmpty)
  }

  test("nestedMetadataJson for a flat list records only the element id") {
    val listType = Types.ListType.ofOptional(11, Types.IntegerType.get())
    val md = parse(GpuTypeToSparkType.nestedMetadataJson(listType))

    assert(md.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 11L)
    assert(!md.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
  }

  test("nestedMetadataJson for a flat map records only the key/value ids") {
    val mapType = Types.MapType.ofOptional(
      21, 22, Types.StringType.get(), Types.IntegerType.get())
    val md = parse(GpuTypeToSparkType.nestedMetadataJson(mapType))

    assert(md.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 21L)
    assert(md.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 22L)
    assert(!md.contains(MAP_KEY_NESTED_IDS_METADATA_KEY))
    assert(!md.contains(MAP_VALUE_NESTED_IDS_METADATA_KEY))
  }

  test("nestedMetadataJson recurses through list-of-list") {
    val inner = Types.ListType.ofOptional(31, Types.IntegerType.get())
    val outer = Types.ListType.ofOptional(30, inner)
    val md = parse(GpuTypeToSparkType.nestedMetadataJson(outer))

    assert(md.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 30L)
    val innerJson = md.getString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY)
    val innerMd = Metadata.fromJson(innerJson)
    assert(innerMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 31L)
    assert(!innerMd.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))
  }

  test("nestedMetadataJson recurses through map-of-(list, list)") {
    val keyList = Types.ListType.ofOptional(41, Types.StringType.get())
    val valueList = Types.ListType.ofOptional(42, Types.IntegerType.get())
    val mapType = Types.MapType.ofOptional(43, 44, keyList, valueList)
    val md = parse(GpuTypeToSparkType.nestedMetadataJson(mapType))

    assert(md.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 43L)
    assert(md.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 44L)

    val keyMd = Metadata.fromJson(md.getString(MAP_KEY_NESTED_IDS_METADATA_KEY))
    assert(keyMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 41L)

    val valueMd = Metadata.fromJson(md.getString(MAP_VALUE_NESTED_IDS_METADATA_KEY))
    assert(valueMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 42L)
  }

  test("appendNestedFieldIdMetadata is a no-op for primitives") {
    val builder = new MetadataBuilder()
    GpuTypeToSparkType.appendNestedFieldIdMetadata(builder, Types.IntegerType.get())
    assert(builder.build().json == "{}")
  }

  test("toSparkType attaches nested field-id metadata to the parent StructField") {
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "tags",
        Types.ListType.ofOptional(3, Types.StringType.get())),
      Types.NestedField.optional(4, "props",
        Types.MapType.ofOptional(5, 6, Types.StringType.get(), Types.IntegerType.get())))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)

    val idField = sparkSchema("id")
    assert(idField.dataType == IntegerType)
    assert(idField.metadata.getLong(FIELD_ID_METADATA_KEY) == 1L)

    val tagsField = sparkSchema("tags")
    assert(tagsField.dataType.isInstanceOf[ArrayType])
    assert(tagsField.metadata.getLong(FIELD_ID_METADATA_KEY) == 2L)
    assert(tagsField.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 3L)
    assert(!tagsField.metadata.contains(LIST_ELEMENT_NESTED_IDS_METADATA_KEY))

    val propsField = sparkSchema("props")
    assert(propsField.dataType.isInstanceOf[MapType])
    assert(propsField.metadata.getLong(FIELD_ID_METADATA_KEY) == 4L)
    assert(propsField.metadata.getLong(MAP_KEY_FIELD_ID_METADATA_KEY) == 5L)
    assert(propsField.metadata.getLong(MAP_VALUE_FIELD_ID_METADATA_KEY) == 6L)
  }

  test("toSparkType serializes deeply-nested ids as JSON sub-Metadata") {
    val schema = new Schema(
      Types.NestedField.optional(10, "nested_lists",
        Types.ListType.ofOptional(11,
          Types.ListType.ofOptional(12, Types.IntegerType.get()))))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)
    val field = sparkSchema("nested_lists")

    assert(field.metadata.getLong(FIELD_ID_METADATA_KEY) == 10L)
    assert(field.metadata.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 11L)
    val nestedJson = field.metadata.getString(LIST_ELEMENT_NESTED_IDS_METADATA_KEY)
    val nestedMd = Metadata.fromJson(nestedJson)
    assert(nestedMd.getLong(LIST_ELEMENT_FIELD_ID_METADATA_KEY) == 12L)
  }

  test("toSparkType handles a struct of struct preserving each child's id") {
    val innerStruct = Types.StructType.of(
      Types.NestedField.required(101, "inner_a", Types.IntegerType.get()),
      Types.NestedField.optional(102, "inner_b", Types.StringType.get()))
    val schema = new Schema(
      Types.NestedField.optional(100, "outer", innerStruct))

    val sparkSchema = GpuTypeToSparkType.toSparkType(schema)
    val outer = sparkSchema("outer")
    assert(outer.metadata.getLong(FIELD_ID_METADATA_KEY) == 100L)

    val outerStruct = outer.dataType.asInstanceOf[StructType]
    assert(outerStruct("inner_a").metadata.getLong(FIELD_ID_METADATA_KEY) == 101L)
    assert(outerStruct("inner_b").metadata.getLong(FIELD_ID_METADATA_KEY) == 102L)
  }

  private def parse(json: Option[String]): Metadata = {
    Metadata.fromJson(json.getOrElse(
      throw new AssertionError("expected nested metadata JSON, got None")))
  }
}
