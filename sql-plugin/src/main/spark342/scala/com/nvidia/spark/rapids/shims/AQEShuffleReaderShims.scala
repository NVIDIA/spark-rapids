/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "342"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.{CoalescedMapperPartitionSpec, CoalescedPartitionSpec, PartialMapperPartitionSpec, ShufflePartitionSpec}

object AQEShuffleReaderShims {

  def isLocalReader(partitionSpecs: Seq[ShufflePartitionSpec]): Boolean =
    partitionSpecs.exists(_.isInstanceOf[PartialMapperPartitionSpec]) ||
      partitionSpecs.exists(_.isInstanceOf[CoalescedMapperPartitionSpec])

  def isCoalescedSpec(spec: ShufflePartitionSpec): Boolean = spec match {
    case CoalescedPartitionSpec(0, 0, _) => true
    case s: CoalescedPartitionSpec => s.endReducerIndex - s.startReducerIndex > 1
    case _ => false
  }

  def coalescedReadOutputPartitioning(partitionSpecs: Seq[ShufflePartitionSpec],
      outputPartitioning: Partitioning): Partitioning = {
    // For coalesced shuffle read, the data distribution is not changed, only the number of
    // partitions is changed.
    outputPartitioning match {
      case h: HashPartitioning =>
        val partitions = partitionSpecs.map {
          case CoalescedPartitionSpec(start, end, _) => CoalescedBoundary(start, end)
          // Can not happend due to isCoalescedRead
          case unexpected =>
            throw SparkException.internalError(s"Unexpected ShufflePartitionSpec: $unexpected")
        }
        CurrentOrigin.withOrigin(h.origin)(CoalescedHashPartitioning(h, partitions))
      case r: RangePartitioning =>
        CurrentOrigin.withOrigin(r.origin)(r.copy(numPartitions = partitionSpecs.length))
      // This can only happen for `REBALANCE_PARTITIONS_BY_NONE`, which uses
      // `RoundRobinPartitioning` but we don't need to retain the number of partitions.
      case r: RoundRobinPartitioning =>
        r.copy(numPartitions = partitionSpecs.length)
      case other @ SinglePartition =>
        throw new IllegalStateException(
          "Unexpected partitioning for coalesced shuffle read: " + other)
      case _ =>
        // Spark plugins may have custom partitioning and may replace this operator
        // during the postStageOptimization phase, so return UnknownPartitioning here
        // rather than throw an exception
        UnknownPartitioning(partitionSpecs.length)
    }
  }
}