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
{"spark": "400"}
{"spark": "400db173"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "412"}
{"spark": "413"}
{"spark": "420"}
{"spark": "500"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{LongType, StructField, StructType, VariantType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.types.variant.VariantBuilder

class CachedBatchWriterVariantSuite extends SparkQueryCompareTestSuite {
  import CachedBatchWriterVariantSuite._

  // ColumnarBatchRow.copy() does not take Variant on Spark 4.0 and 4.1.0-4.1.1, so the
  // columnar entry has to carry its row into the next batch without copying it.
  test("columnar entry carries a Variant row into the next batch with its own values") {
    // Each row is estimated at 2056 bytes, so a 16 KiB budget holds seven of them.
    val numRows = 30
    val attrs = Seq(AttributeReference("id", LongType, nullable = false)(),
      AttributeReference("v", VariantType, nullable = false)())
    // The CPU session sends the columnar entry to the CPU writer whatever the GPU supports.
    withCpuSparkSession(spark => {
      val ser = new ParquetCachedBatchSerializer
      val conf = TrampolineUtil.getSparkConf(spark)
      // A ColumnarBatch is not serializable, so the task builds its own.
      val input = spark.sparkContext.parallelize(0 until numRows, 1)
        .mapPartitions(ids => Iterator(variantBatch(ids.toList)))
      val cached = ser.convertColumnarBatchToCachedBatch(input, attrs, MEMORY_ONLY, conf)
        .collect().toList
      assert(cached.length > 2, "the rows should span several batches")
      val decoded = ser.convertCachedBatchToInternalRow(
        spark.sparkContext.parallelize(cached, numSlices = 1), attrs, attrs, conf)
        .map(_.copy()).collect()
      assert(decoded.length == numRows)
      decoded.zipWithIndex.foreach { case (row, id) =>
        val expected = variantOf(id)
        val actual = row.getVariant(1)
        assert((row.getLong(0), actual.getValue.toSeq, actual.getMetadata.toSeq) ==
          (id.toLong, expected.getValue.toSeq, expected.getMetadata.toSeq))
      }
    }, new SparkConf().set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, "16k"))
  }
}

object CachedBatchWriterVariantSuite {
  private val schema = StructType(Seq(StructField("id", LongType, nullable = false),
    StructField("v", VariantType, nullable = false)))

  private def variantOf(id: Int) =
    VariantBuilder.parseJson(s"""{"id": $id, "s": "${"%04d".format(id)}"}""", false)

  private def variantBatch(ids: Seq[Int]): ColumnarBatch = {
    val cols = OnHeapColumnVector.allocateColumns(ids.length, schema)
    ids.zipWithIndex.foreach { case (id, i) =>
      val variant = variantOf(id)
      cols(0).putLong(i, id.toLong)
      cols(1).getChild(0).putByteArray(i, variant.getValue)
      cols(1).getChild(1).putByteArray(i, variant.getMetadata)
    }
    new ColumnarBatch(Array[ColumnVector](cols: _*), ids.length)
  }
}
