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
  AttributeReference,
  GetArrayStructFields,
  GetStructField
}
import org.apache.spark.sql.rapids.{
  GpuGetArrayStructFieldsMeta,
  GpuGetStructFieldMeta,
  GpuStructFieldOrdinalTag
}
import org.apache.spark.sql.types._

class StructFieldOrdinalTagSuite extends AnyFunSuite {

  private val threeFieldStruct = StructType(Seq(
    StructField("a", IntegerType, nullable = true),
    StructField("b", StringType, nullable = true),
    StructField("c", DoubleType, nullable = true)))

  private val structAttr = AttributeReference("s", threeFieldStruct, nullable = true)()

  private val arrayOfStruct = ArrayType(threeFieldStruct, containsNull = false)
  private val arrayAttr = AttributeReference("arr", arrayOfStruct, nullable = true)()

  // ---------- GetStructField ordinal tag ----------

  test("effectiveOrdinal returns original ordinal when no tag is set") {
    val gsf = GetStructField(structAttr, 2, Some("c"))
    assert(GpuGetStructFieldMeta.effectiveOrdinal(gsf) === 2)
  }

  test("effectiveOrdinal returns tagged ordinal when tag is set") {
    val gsf = GetStructField(structAttr, 2, Some("c"))
    gsf.setTagValue(GpuStructFieldOrdinalTag.PRUNED_ORDINAL_TAG, 0)
    assert(GpuGetStructFieldMeta.effectiveOrdinal(gsf) === 0)
  }

  // ---------- effectiveNumFields ----------

  test("effectiveNumFields returns original numFields when no tag") {
    val gasf = GetArrayStructFields(arrayAttr, threeFieldStruct(1), 1, 3, false)
    val result = GpuGetArrayStructFieldsMeta.effectiveNumFields(arrayAttr, gasf, -1)
    assert(result === 3)
  }

  test("effectiveNumFields derives from child type when tag is active") {
    val prunedStruct = StructType(Seq(
      StructField("b", StringType, nullable = true)))
    val prunedArrayType = ArrayType(prunedStruct, containsNull = false)
    val prunedChild = AttributeReference("arr", prunedArrayType, nullable = true)()

    val gasf = GetArrayStructFields(arrayAttr, threeFieldStruct(1), 1, 3, false)
    val result = GpuGetArrayStructFieldsMeta.effectiveNumFields(prunedChild, gasf, 0)
    assert(result === 1)
  }

  test("effectiveNumFields falls back to expr.numFields for non-array child type") {
    val nonArrayChild = AttributeReference("x", IntegerType, nullable = true)()
    val gasf = GetArrayStructFields(arrayAttr, threeFieldStruct(0), 0, 3, false)
    val result = GpuGetArrayStructFieldsMeta.effectiveNumFields(nonArrayChild, gasf, 0)
    assert(result === 3)
  }

}
