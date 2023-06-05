/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import com.nvidia.spark.ParquetCachedBatchSerializer
import com.nvidia.spark.rapids.{FuzzerUtils, SparkQueryCompareTestSuite}

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

/**
 * Unit tests for cached batch writing
 */
class CachedBatchWriterSuite extends SparkQueryCompareTestSuite {

  private def writeAndConsumeEmptyBatch(spark: SparkSession): Unit = {
    SparkSession.setActiveSession(spark)
    val ser = new ParquetCachedBatchSerializer
    val schema = Seq(AttributeReference("_col0", IntegerType, true)())
    val cb0 = FuzzerUtils.createColumnarBatch(schema.toStructType, 0)
    val cb = FuzzerUtils.createColumnarBatch(schema.toStructType, 10)
    val rdd = spark.sparkContext.parallelize(Seq(cb, cb0))
    val storageLevel = MEMORY_ONLY
    val conf = spark.sqlContext.conf
    val cachedRdd = ser.convertColumnarBatchToCachedBatch(rdd, schema, storageLevel, conf)
    val cbRdd = ser.convertCachedBatchToColumnarBatch(cachedRdd, schema, schema, conf)
    val part = cbRdd.partitions.head
    val context = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    TaskContext.setTaskContext(context)
    val batches = cbRdd.compute(part, context)
    // Read the batches to consume the cache
    while (batches.hasNext) {
      batches.next()
    }
  }

  test("cache empty columnar batch on GPU") {
    withGpuSparkSession(writeAndConsumeEmptyBatch)
  }
}
