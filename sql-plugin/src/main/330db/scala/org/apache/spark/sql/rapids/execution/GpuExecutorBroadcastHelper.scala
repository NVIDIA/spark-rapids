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

package org.apache.spark.sql.rapids.execution

import com.nvidia.spark.rapids.{Arm, ConcatAndConsumeAll, GpuColumnVector, GpuMetric, GpuShuffleCoalesceIterator, HostShuffleCoalesceIterator}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuExecutorBroadcastHelper extends Arm {

  // Because of the nature of the executor-side broadcast, every executor needs to read the 
  // shuffle data directly from the executor that produces the broadcast data. 
  // Note that we can't call mapPartitions here because we're on an executor, and we don't 
  // have access to a SparkContext.
  private def shuffleCoalesceIterator(
    buildRelation: RDD[ColumnarBatch],
    buildSchema: StructType,
    metricsMap: Map[String, GpuMetric],
    targetSize: Long): Iterator[ColumnarBatch] = {

    val dataTypes = GpuColumnVector.extractTypes(buildSchema)

    // Use the GPU Shuffle Coalesce iterator to concatenate and load batches onto the 
    // host as needed. Since we don't have GpuShuffleCoalesceExec in the plan for the 
    // executor broadcast scenario, we have to use that logic here to efficiently 
    // grab and release the semaphore while doing I/O
    val iter = buildRelation.partitions.map { part =>
      buildRelation.iterator(part, TaskContext.get())
    }.reduceLeft(_ ++ _)
    new GpuShuffleCoalesceIterator(
      new HostShuffleCoalesceIterator(iter, targetSize, dataTypes, metricsMap),
        dataTypes, metricsMap).asInstanceOf[Iterator[ColumnarBatch]]
  }

  /**
   * Given an RDD of ColumnarBatch containing broadcast data from a shuffle, get the 
   * fully coalesced ColumnarBatch loaded on to the GPU.
   *
   * @param buildRelation - the executor broadcast produced by a shuffle exchange
   * @param buildSchema - the schema expected for the output
   * @param buildOutput - the output attributes expected in case we receieve an empty RDD
   * @param metricsMap - metrics to populate while the shuffle coalesce iterator loads 
   *                     data from I/O to the device
   * @param targetSize - the target batch size in bytes for loading data on to the GPU 
   */
  def getExecutorBroadcastBatch(
      buildRelation: RDD[ColumnarBatch], 
      buildSchema: StructType,
      buildOutput: Seq[Attribute],
      metricsMap: Map[String, GpuMetric],
      targetSize: Long): ColumnarBatch = {

    val it = shuffleCoalesceIterator(buildRelation, buildSchema, metricsMap, targetSize)
    // In a broadcast hash join, right now the GPU expects a single batch
    ConcatAndConsumeAll.getSingleBatchWithVerification(it, buildOutput)
  }

}