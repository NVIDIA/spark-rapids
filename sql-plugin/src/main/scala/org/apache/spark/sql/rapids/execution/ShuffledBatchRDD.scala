/*
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

// Most of this code came from ShuffledRowRDD in spark, with minor modifications

// In order to have metrics and preferred locations work we need to be in a spark sql package
package org.apache.spark.sql.rapids.execution

import java.util

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.{GpuMetric, NvtxWithMetrics, ShimLoader}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{CoalescedPartitioner, CoalescedPartitionSpec, PartialMapperPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class ShuffledBatchRDDPartition(index: Int, spec: ShufflePartitionSpec) extends Partition

/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 */
class BatchPartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
class CoalescedBatchPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = parentPartitionMapping(parent.getPartition(key))

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedBatchPartitioner =>
      c.parent == parent && util.Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * parent.hashCode() + util.Arrays.hashCode(partitionStartIndices)
  }
}

/**
 * This is a specialized version of `org.apache.spark.rdd.ShuffledRDD` that is optimized for
 * shuffling `ColumnarBatch` instead of Java key-value pairs.
 *
 * This RDD takes a `ShuffleDependency` (`dependency`),
 * and an optional array of partition start indices as input arguments
 * (`specifiedPartitionStartIndices`).
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner` is the original partitioner used to partition
 * map output, and `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions
 * (i.e. the number of partitions of the map output).
 *
 * When `specifiedPartitionStartIndices` is defined, `specifiedPartitionStartIndices.length`
 * will be the number of post-shuffle partitions. For this case, the `i`th post-shuffle
 * partition includes `specifiedPartitionStartIndices[i]` to
 * `specifiedPartitionStartIndices[i+1] - 1` (inclusive).
 *
 * When `specifiedPartitionStartIndices` is not defined, there will be
 * `dependency.partitioner.numPartitions` post-shuffle partitions. For this case,
 * a post-shuffle partition is created for every pre-shuffle partition.
 */
class ShuffledBatchRDD(
    var dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])
    extends RDD[ColumnarBatch](dependency.rdd.context, Nil) {

  def this(
      dependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      metrics: Map[String, SQLMetric]) = {
    this(dependency, metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }

  override def getDependencies = List(dependency)

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledBatchRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledBatchRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val shuffledBatchPartition = split.asInstanceOf[ShuffledBatchRDDPartition]
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val shim = ShimLoader.getSparkShims
    val shuffleManagerShims = shim.getShuffleManagerShims()
    val (reader, partitionSize) = shuffledBatchPartition.spec match {
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

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
        val reader = shuffleManagerShims.getReader(
          SparkEnv.get.shuffleManager,
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)
        val blocksByAddress = shim.getMapSizesByExecutorId(
          dependency.shuffleHandle.shuffleId, 0, Int.MaxValue, reducerIndex, reducerIndex + 1)
        val partitionSize = blocksByAddress.flatMap(_._2)
            .filter(tuple => tuple._3 >= startMapIndex && tuple._3 < endMapIndex)
            .map(_._2).sum
        (reader, partitionSize)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        val reader = shuffleManagerShims.getReader(
          SparkEnv.get.shuffleManager,
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
    metrics(GpuMetric.NUM_PARTITIONS).add(1)
    metrics(GpuMetric.PARTITION_SIZE).add(partitionSize)
    reader.read().asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
