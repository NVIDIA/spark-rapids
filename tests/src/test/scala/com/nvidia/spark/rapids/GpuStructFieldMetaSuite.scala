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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetStructField}
import org.apache.spark.sql.rapids.{GpuGetStructField, GpuGetStructFieldMeta}
import org.apache.spark.sql.types._

class GpuStructFieldMetaSuite extends AnyFunSuite {
  private val originalStruct = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", StringType),
    StructField("c", LongType)))

  private def conf: RapidsConf = new RapidsConf(Map.empty[String, String])

  test("tagForGpu remaps ordinal when child schema is projected") {
    val projectedStruct = StructType(Seq(originalStruct.fields(1), originalStruct.fields(2)))
    val originalChild = AttributeReference("s", originalStruct, nullable = true)()
    val sparkExpr = GetStructField(originalChild, ordinal = 2, Some("c"))

    val meta = GpuOverrides.wrapExpr(sparkExpr, conf, None)
      .asInstanceOf[GpuGetStructFieldMeta]
    // simulate an upstream expression that produces a narrower converted child schema
    meta.childExprs.head.overrideDataType(projectedStruct)
    meta.tagForGpu()

    assert(meta.canThisBeReplaced,
      s"meta should be replaceable for projected schema, explain: ${meta.explain(all = false)}")
    // field "c" survives the projection with the same dataType, so no override is needed
    assert(meta.typeMeta.dataType.contains(LongType))

    // pass a child whose dataType reflects the projection; the meta must remap ordinal 2 → 1
    val projectedChild = AttributeReference("s_proj", projectedStruct, nullable = true)()
    val gpuExpr = meta.convertToGpu(projectedChild).asInstanceOf[GpuGetStructField]
    assert(gpuExpr.ordinal == 1)
    assert(gpuExpr.name.contains("c"))
  }

  test("tagForGpu marks willNotWork when projected schema drops the requested field") {
    val projectedStruct = StructType(Seq(originalStruct.fields(0)))
    val originalChild = AttributeReference("s", originalStruct, nullable = true)()
    val sparkExpr = GetStructField(originalChild, ordinal = 2, Some("c"))

    val meta = GpuOverrides.wrapExpr(sparkExpr, conf, None)
      .asInstanceOf[GpuGetStructFieldMeta]
    meta.childExprs.head.overrideDataType(projectedStruct)
    meta.tagForGpu()

    assert(!meta.canThisBeReplaced)
    val explain = meta.explain(all = false)
    assert(explain.contains("field 'c' is not present"),
      s"expected explain to mention missing field, got: $explain")
  }
}
