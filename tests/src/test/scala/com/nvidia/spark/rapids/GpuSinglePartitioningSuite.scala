/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, Table}
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuSinglePartitioningSuite extends FunSuite with Arm {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table)
    }
  }

  test("generates contiguous split uncompressed") {
    val conf = new SparkConf().set("spark.shuffle.manager", GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf), Cuda.memGetInfo())
      val partitioner = GpuSinglePartitioning(Nil)
      withResource(buildBatch()) { expected =>
        // partition will consume batch, so make a new batch with incremented refcounts
        val columns = GpuColumnVector.extractColumns(expected)
        columns.foreach(_.incRefCount())
        val batch = new ColumnarBatch(columns.toArray, expected.numRows)
        val result = partitioner.columnarEval(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
        try {
          assertResult(1)(result.length)
          assertResult(0)(result.head._2)
          val resultBatch = result.head._1
          // verify this is a contiguous split table
          assert(resultBatch.column(0).isInstanceOf[GpuColumnVectorFromBuffer])
          TestUtils.compareBatches(expected, resultBatch)
        } finally {
          result.foreach(_._1.close())
        }
      }
    }
  }
}
