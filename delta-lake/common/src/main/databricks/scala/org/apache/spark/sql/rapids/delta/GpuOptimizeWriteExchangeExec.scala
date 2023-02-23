/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
 * This file was derived from OptimizeWriteExchange.scala
 * in the Delta Lake project at https://github.com/delta-io/delta
 * (pending at https://github.com/delta-io/delta/pull/1198).
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.rapids.delta

import scala.concurrent.Future
import scala.concurrent.duration.Duration

import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids.{GpuColumnarBatchSerializer, GpuExec, GpuMetric, GpuPartitioning, GpuRoundRobinPartitioning}
import com.nvidia.spark.rapids.delta.RapidsDeltaSQLConf

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.metric.{SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.rapids.execution.{GpuShuffleExchangeExecBase, ShuffledBatchRDD}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ThreadUtils

/**
 * Execution plan to repartition and rebalance the data for Optimize Write.
 * The plan will be added on top of the query plan of each write job to avoid any interference
 * from Adaptive Query Execution.
 *
 * @param partitioning Partitioning to use data exchange.
 * @param child Input plan of write job.
 */
case class GpuOptimizeWriteExchangeExec(
    partitioning: GpuPartitioning,
    override val child: SparkPlan) extends Exchange with GpuExec {
  import GpuMetric._

  // Use 150% of target file size hint config considering parquet compression.
  // Still the result file can be smaller/larger than the config due to data skew or
  // variable compression ratio for each data type.
  final val PARQUET_COMPRESSION_RATIO = 1.5

  // Dummy partitioning because:
  // 1) The exact output partitioning is determined at query runtime
  // 2) optimizeWrite is always placed right after the top node(DeltaInvariantChecker),
  //    there is no parent plan to refer to outputPartitioning
  override def outputPartitioning: Partitioning = UnknownPartitioning(partitioning.numPartitions)

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL, "data size"),
    "dataReadSize" -> createSizeMetric(MODERATE_LEVEL, "data read size"),
    "rapidsShuffleSerializationTime" ->
        createNanoTimingMetric(DEBUG_LEVEL, "rs. serialization time"),
    "rapidsShuffleDeserializationTime" ->
        createNanoTimingMetric(DEBUG_LEVEL, "rs. deserialization time"),
    "rapidsShuffleWriteTime" ->
        createNanoTimingMetric(ESSENTIAL_LEVEL, "rs. shuffle write time"),
    "rapidsShuffleCombineTime" ->
        createNanoTimingMetric(DEBUG_LEVEL, "rs. shuffle combine time"),
    "rapidsShuffleWriteIoTime" ->
        createNanoTimingMetric(DEBUG_LEVEL, "rs. shuffle write io time"),
    "rapidsShuffleReadTime" ->
        createNanoTimingMetric(ESSENTIAL_LEVEL, "rs. shuffle read time")
  ) ++ GpuMetric.wrap(readMetrics) ++ GpuMetric.wrap(writeMetrics)

  override lazy val allMetrics: Map[String, GpuMetric] = {
    Map(
      PARTITION_SIZE -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_PARTITION_SIZE),
      NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
      NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
      NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES)
    ) ++ additionalMetrics
  }

  private lazy val serializer: Serializer =
    new GpuColumnarBatchSerializer(gpuLongMetric("dataSize"))

  @transient lazy val inputRDD: RDD[ColumnarBatch] = child.executeColumnar()

  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }


  @transient lazy val shuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val dep = GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputRDD,
      child.output,
      partitioning,
      child.output.map(_.dataType).toArray,
      serializer,
      useGPUShuffle=partitioning.usesGPUShuffle,
      useMultiThreadedShuffle=partitioning.usesMultiThreadedShuffle,
      metrics=allMetrics,
      writeMetrics=writeMetrics,
      additionalMetrics=additionalMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Collect execution statistics, these will be used to adjust/decide how to split files
    val stats = ThreadUtils.awaitResult(mapOutputStatisticsFuture, Duration.Inf)
    if (stats == null) {
      new ShuffledBatchRDD(shuffleDependency, metrics)
    } else {
      try {
        val partitionSpecs = Some(rebalancePartitions(stats))
        new ShuffledBatchRDD(shuffleDependency, metrics, partitionSpecs.get.toArray)
      } catch {
        case e: Throwable =>
          logWarning("Failed to apply OptimizeWrite.", e)
          new ShuffledBatchRDD(shuffleDependency, metrics)
      }
    }
  }

  private def rebalancePartitions(stats: MapOutputStatistics): Seq[ShufflePartitionSpec] = {
    val binSize = ByteUnit.BYTE.convertFrom(
      conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_BIN_SIZE), ByteUnit.MiB)
    val smallPartitionFactor =
      conf.getConf(RapidsDeltaSQLConf.OPTIMIZE_WRITE_SMALL_PARTITION_FACTOR)
    val mergedPartitionFactor =
      conf.getConf(RapidsDeltaSQLConf.OPTIMIZE_WRITE_MERGED_PARTITION_FACTOR)
    val bytesByPartitionId = stats.bytesByPartitionId
    val targetPartitionSize = (binSize * PARQUET_COMPRESSION_RATIO).toLong

    val splitPartitions = if (partitioning.isInstanceOf[GpuRoundRobinPartitioning]) {
      DeltaShufflePartitionsUtil.splitSizeListByTargetSize(
        bytesByPartitionId,
        targetPartitionSize,
        smallPartitionFactor,
        mergedPartitionFactor)
    } else {
      // For partitioned data, do not coalesce small partitions as it will hurt parallelism.
      // Eg. a partition containing 100 partition keys => a task will write 100 files.
      Seq.range(0, bytesByPartitionId.length).toArray
    }

    def optimizeSkewedPartition(reduceIndex: Int): Seq[ShufflePartitionSpec] = {
      val partitionSize = bytesByPartitionId(reduceIndex)
      if (partitionSize > targetPartitionSize) {
        val shuffleId = shuffleDependency.shuffleId
        val newPartitionSpec = DeltaShufflePartitionsUtil.createSkewPartitionSpecs(
          shuffleId,
          reduceIndex,
          targetPartitionSize,
          smallPartitionFactor,
          mergedPartitionFactor)

        if (newPartitionSpec.isEmpty) {
          CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
        } else {
          logDebug(s"[OptimizeWrite] Partition $reduceIndex is skew, " +
              s"split it into ${newPartitionSpec.get.size} parts.")
          newPartitionSpec.get
        }
      } else if (partitionSize > 0) {
        CoalescedPartitionSpec(reduceIndex, reduceIndex + 1) :: Nil
      } else {
        Nil
      }
    }

    // Transform the partitions to the ranges.
    // e.g. [0, 3, 6, 7, 10] -> [[0, 3), [3, 6), [6, 7), [7, 10)]
    (splitPartitions :+ stats.bytesByPartitionId.length).sliding(2).flatMap { k =>
      if (k.head == k.last - 1) {
        // If not a merged partition, split it if needed.
        optimizeSkewedPartition(k.head)
      } else {
        CoalescedPartitionSpec(k.head, k.last) :: Nil
      }
    }.toList
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): GpuOptimizeWriteExchangeExec = {
    copy(child = newChild)
  }
}
