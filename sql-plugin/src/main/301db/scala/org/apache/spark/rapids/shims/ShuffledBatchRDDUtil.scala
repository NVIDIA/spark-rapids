/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.shims

import com.nvidia.spark.rapids.shims.SparkShimImpl

import org.apache.spark.{MapOutputTrackerMaster, Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, PartialMapperPartitionSpec, PartialReducerPartitionSpec}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDDPartition
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Some APIs for the ShuffledBatchRDD are only accessible from org.apache.spark...
 * This code tries to match the Spark code as closely as possible. Fixing a compiler or IDE
 * warning is not always the best thing here because if it changes how it matches up with the
 * Spark code it may make it harder to maintain as thing change in Spark.
 */
object ShuffledBatchRDDUtil {
  def preferredLocations(
      partition: Partition,
      dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch]): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)
    }
  }

  def getReaderAndPartSize(
      split: Partition,
      context: TaskContext,
      dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      sqlMetricsReporter: SQLShuffleReadMetricsReporter):
  (ShuffleReader[Nothing, Nothing], Long) = {
    val shim = SparkShimImpl
    split.asInstanceOf[ShuffledBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
        val reader = SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)
        val blocksByAddress = shim.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId, 0, Int.MaxValue, startReducerIndex, endReducerIndex)
        val partitionSize = blocksByAddress.flatMap(_._2).map(_._2).sum
        (reader, partitionSize)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        val reader = SparkEnv.get.shuffleManager.getReaderForRange(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)
        val blocksByAddress = shim.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId, 0, Int.MaxValue, reducerIndex,
          reducerIndex + 1)
        val partitionSize = blocksByAddress.flatMap(_._2)
            .filter(tuple => tuple._3 >= startMapIndex && tuple._3 < endMapIndex)
            .map(_._2).sum
        (reader, partitionSize)
      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        val reader = SparkEnv.get.shuffleManager.getReaderForRange(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)
        val blocksByAddress = shim.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId, 0, Int.MaxValue, startReducerIndex, endReducerIndex)
        val partitionSize = blocksByAddress.flatMap(_._2)
            .filter(_._3 == mapIndex)
            .map(_._2).sum
        (reader, partitionSize)
    }
  }
}
