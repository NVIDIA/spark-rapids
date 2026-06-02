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

import ai.rapids.cudf
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, JsonToStructs}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Runtime CPU fallback for `from_json` (JsonToStructs).
 *
 * cuDF's JSON reader detects a nested schema-category mismatch (a value whose JSON category
 * disagrees with the requested struct schema) at column granularity and nulls the whole column.
 * That over-nulls mixed-row batches relative to Spark's per-row depth-1 nulling -- e.g. one
 * well-formed row plus one row whose nested value disagrees with the schema, where Spark keeps the
 * first row and nulls only the second. Rather than emit results that diverge from CPU Spark, we
 * recompute the entire batch with Spark's own JsonToStructs. Only batches that actually contain a
 * mismatch take this path; clean batches stay on the GPU. A per-row diagnostic from cuDF would let
 * the GPU null only the offending rows and avoid the fallback (spark-rapids-jni#4536 / #4645).
 */
object GpuJsonToStructsCpuFallback {
  def fallbackToCpu(
      input: cudf.ColumnVector,
      schema: StructType,
      options: Map[String, String],
      timeZoneId: Option[String]): cudf.ColumnVector = {
    val cpuExpr = JsonToStructs(schema, options,
      BoundReference(0, StringType, nullable = true), timeZoneId)

    // The output is a single struct column, carried as the one field of a wrapper row so the
    // row-to-column converter (which materializes a StructType of top-level columns) can build it.
    val wrapperSchema = StructType(Array(StructField("v", schema, nullable = true)))

    val rows = withResource(input.copyToHost()) { host =>
      val numRows = host.getRowCount.toInt
      val out = new Array[InternalRow](numRows)
      var i = 0
      while (i < numRows) {
        val json = if (host.isNull(i)) null else UTF8String.fromBytes(host.getUTF8(i))
        out(i) = InternalRow(cpuExpr.eval(InternalRow(json)))
        i += 1
      }
      out
    }

    val converter = new GpuRowToColumnConverter(wrapperSchema)
    withResource(converter.convertBatch(rows, wrapperSchema)) { batch =>
      batch.column(0).asInstanceOf[GpuColumnVector].getBase.incRefCount()
    }
  }
}
