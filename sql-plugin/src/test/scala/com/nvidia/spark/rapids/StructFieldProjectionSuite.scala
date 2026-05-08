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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, GetArrayStructFields, GetStructField}
import org.apache.spark.sql.rapids.GpuStructFieldRemap
import org.apache.spark.sql.types._

class StructFieldProjectionSuite extends AnyFunSuite {
  private val originalStruct = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", StringType),
    StructField("c", LongType)))

  test("resolveField keeps ordinal when converted child schema matches the original") {
    val sparkExpr = GetStructField(
      BoundReference(0, originalStruct, nullable = true), ordinal = 1, Some("b"))

    GpuStructFieldRemap.resolveField(
      sparkExpr.child.dataType, originalStruct, sparkExpr.ordinal, "GetStructField") match {
      case Right((ordinal, field)) =>
        assert(ordinal == 1)
        assert(field == originalStruct.fields(1))
      case Left(reason) => fail(reason)
    }
  }

  test("resolveField remaps ordinal when converted child schema drops earlier fields") {
    val projectedStruct = StructType(Seq(originalStruct.fields(2)))
    val sparkExpr = GetStructField(
      BoundReference(0, originalStruct, nullable = true), ordinal = 2, Some("c"))

    GpuStructFieldRemap.resolveField(
      sparkExpr.child.dataType, projectedStruct, sparkExpr.ordinal, "GetStructField") match {
      case Right((ordinal, field)) =>
        assert(ordinal == 0)
        assert(field == projectedStruct.fields(0))
      case Left(reason) => fail(reason)
    }
  }

  test("resolveArrayStructField returns ordinal, field and numFields from converted schema") {
    val originalArrayType = ArrayType(originalStruct, containsNull = true)
    val projectedStruct = StructType(Seq(originalStruct.fields(1)))
    val sparkExpr = GetArrayStructFields(
      BoundReference(0, originalArrayType, nullable = true),
      field = originalStruct.fields(1),
      ordinal = 1,
      numFields = originalStruct.fields.length,
      containsNull = true)

    GpuStructFieldRemap.resolveArrayStructField(
      sparkExpr.child.dataType, ArrayType(projectedStruct, containsNull = true),
      sparkExpr.ordinal, "GetArrayStructFields") match {
      case Right((ordinal, field, numFields)) =>
        assert(ordinal == 0)
        assert(field == projectedStruct.fields(0))
        assert(numFields == 1)
      case Left(reason) => fail(reason)
    }
  }

  test("resolveField rejects a converted child schema without the requested field") {
    val projectedStruct = StructType(Seq(originalStruct.fields(0)))
    val sparkExpr = GetStructField(
      BoundReference(0, originalStruct, nullable = true), ordinal = 2, Some("c"))

    val error = GpuStructFieldRemap.resolveField(
      sparkExpr.child.dataType, projectedStruct, sparkExpr.ordinal, "GetStructField")
      .left.toOption
    assert(error.exists(_.contains("field 'c' is not present")))
  }
}
