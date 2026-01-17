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
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.GpuMetric

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark 4.1+ implementation of union for GpuUnionExec.
 *
 * Uses partitioner-aware union (SPARK-52921) which groups partitions at corresponding
 * indices across child RDDs, preserving partition alignment. Falls back to concatenation 
 * if partition counts differ.
 */
object GpuUnionExecShim {
  def unionColumnarRdds(
      sc: SparkContext,
      rdds: Seq[RDD[ColumnarBatch]],
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): RDD[ColumnarBatch] = {
    val nonEmptyRdds = rdds.filter(!_.partitions.isEmpty)
    val partitionCounts = nonEmptyRdds.map(_.getNumPartitions).distinct
    if (nonEmptyRdds.nonEmpty && partitionCounts.size == 1) {
      new GpuPartitionerAwareUnionRDD(sc, nonEmptyRdds, partitionCounts.head,
        numOutputRows, numOutputBatches)
    } else {
      // Fall back to concatenation similar to pre-4.1 implementation
      sc.union(nonEmptyRdds).map { batch =>
        numOutputBatches += 1
        numOutputRows += batch.numRows
        batch
      }
    }
  }
}
