/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import org.apache.spark.{MapOutputTrackerMaster, Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.execution.{CoalescedMapperPartitionSpec, CoalescedPartitionSpec, PartialMapperPartitionSpec, PartialReducerPartitionSpec}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDDPartition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId}

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
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  private def getPartitionSize(
      blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]): Long = {
    blocksByAddress.flatMap { case (_, blockInfos) =>
      blockInfos.map { case (_, size, _) => size }
    }.sum
  }

  def getReaderAndPartSize(
      split: Partition,
      context: TaskContext,
      dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      sqlMetricsReporter: SQLShuffleReadMetricsReporter):
  (ShuffleReader[Nothing, Nothing], Long) = {
    split.asInstanceOf[ShuffledBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        val reader = SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId,
          0,
          Int.MaxValue,
          startReducerIndex,
          endReducerIndex)
        (reader, getPartitionSize(blocksByAddress))
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        val reader = SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1)
        (reader, getPartitionSize(blocksByAddress))
      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        val reader = SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex)
        (reader, getPartitionSize(blocksByAddress))
      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        val reader = SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter)
        val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId,
          startMapIndex,
          endMapIndex,
          0,
          numReducers)
        (reader, getPartitionSize(blocksByAddress))
    }
  }
}
