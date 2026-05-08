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

package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{
  BoundReference,
  GetArrayStructFields,
  GetStructField
}
import org.apache.spark.sql.rapids.{
  GpuGetArrayStructFieldsMeta,
  GpuGetStructFieldMeta
}
import org.apache.spark.sql.types._

class StructFieldProjectionSuite extends AnyFunSuite {
  private val originalStruct = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", StringType),
    StructField("c", LongType)))

  test("struct field meta keeps ordinal for matching converted child schema") {
    val child = BoundReference(0, originalStruct, nullable = true)
    val sparkExpr = GetStructField(child, ordinal = 1, Some("b"))

    assert(GpuGetStructFieldMeta.effectiveOrdinal(sparkExpr, child) == 1)
    GpuGetStructFieldMeta.resolveField(sparkExpr, child.dataType) match {
      case Right((ordinal, field)) =>
        assert(ordinal == 1)
        assert(field == originalStruct.fields(1))
      case Left(reason) =>
        fail(reason)
    }
  }

  test("struct field meta resolves ordinal from projected child schema") {
    val originalChild = BoundReference(0, originalStruct, nullable = true)
    val projectedStruct = StructType(Seq(originalStruct.fields(2)))
    val projectedChild = BoundReference(0, projectedStruct, nullable = true)
    val sparkExpr = GetStructField(originalChild, ordinal = 2, Some("c"))

    assert(GpuGetStructFieldMeta.effectiveOrdinal(sparkExpr, projectedChild) == 0)
    GpuGetStructFieldMeta.resolveField(sparkExpr, projectedChild.dataType) match {
      case Right((ordinal, field)) =>
        assert(ordinal == 0)
        assert(field == projectedStruct.fields(0))
      case Left(reason) =>
        fail(reason)
    }
  }

  test("array struct field meta resolves ordinal and numFields from projected child schema") {
    val originalArrayType = ArrayType(originalStruct, containsNull = true)
    val originalChild = BoundReference(0, originalArrayType, nullable = true)
    val projectedStruct = StructType(Seq(originalStruct.fields(1)))
    val projectedChild = BoundReference(0,
      ArrayType(projectedStruct, containsNull = true), nullable = true)
    val sparkExpr = GetArrayStructFields(
      originalChild,
      field = originalStruct.fields(1),
      ordinal = 1,
      numFields = originalStruct.fields.length,
      containsNull = true)

    assert(GpuGetArrayStructFieldsMeta.effectiveOrdinal(sparkExpr, projectedChild) == 0)
    assert(GpuGetArrayStructFieldsMeta.effectiveField(sparkExpr, projectedChild) ==
      projectedStruct.fields(0))
    assert(GpuGetArrayStructFieldsMeta.effectiveNumFields(sparkExpr, projectedChild) == 1)
  }

  test("struct field meta rejects a converted child schema without the requested field") {
    val originalChild = BoundReference(0, originalStruct, nullable = true)
    val projectedStruct = StructType(Seq(originalStruct.fields(0)))
    val sparkExpr = GetStructField(originalChild, ordinal = 2, Some("c"))

    val error = GpuGetStructFieldMeta.resolveField(sparkExpr, projectedStruct).left.toOption
    assert(error.exists(_.contains("field 'c' is not present")))
  }
}
