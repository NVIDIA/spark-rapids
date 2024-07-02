/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.RmmSpark

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, ExprId, SortOrder, SpecificInternalRow}
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsDiskBlockManager}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class ShufflePartitionerRetrySuite extends RmmSparkRetrySuiteBase {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
      .column(9, null.asInstanceOf[java.lang.Integer], 8, 7, 6, 5, 4, 3, 2, 1)
      .column("nine", "eight", null, null, "six", "five", "four", "three", "two", "one")
      .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType))
    }
  }

  private def testRoundRobinPartitioner(partNum: Int) = {
    val conf = new SparkConf()
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf), new RapidsDiskBlockManager(conf))
      val rrp = GpuRoundRobinPartitioning(partNum)
      // batch will be closed within columnarEvalAny
      val batch = buildBatch
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      var ret: Array[(ColumnarBatch, Int)] = null
      try {
        ret = rrp.columnarEvalAny(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
        assert(partNum === ret.size)
      } finally {
        if (ret != null) {
          ret.map(_._1).safeClose()
        }
      }
    }
  }

  test("GPU range partition with retry") {
    val conf = new SparkConf()
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf), new RapidsDiskBlockManager(conf))
      // Initialize range bounds
      val fieldTypes: Array[DataType] = Array(IntegerType)
      val bounds = new SpecificInternalRow(fieldTypes)
      bounds.setInt(0, 3)
      // Initialize GPU sorter
      val ref = GpuBoundReference(0, IntegerType, nullable = true)(ExprId(0), "a")
      val sortOrder = SortOrder(ref, Ascending)
      val attrs = AttributeReference(ref.name, ref.dataType, ref.nullable)()
      val gpuSorter = new GpuSorter(Seq(sortOrder), Array(attrs))

      val rp = GpuRangePartitioner(Array.apply(bounds), gpuSorter)
      // batch will be closed within columnarEvalAny
      val batch = buildBatch
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      var ret: Array[(ColumnarBatch, Int)] = null
      try {
        ret = rp.columnarEvalAny(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
        assert(ret.length === 2)
      } finally {
        if (ret != null) {
          ret.map(_._1).safeClose()
        }
      }
    }
  }

  test("GPU round robin partition with retry using multiple partition") {
    testRoundRobinPartitioner(4)
  }

  test("GPU round robin partitioner with retry using 1 partition") {
    testRoundRobinPartitioner(1)
  }
}
