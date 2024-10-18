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
/*** spark-rapids-shim-json-lines
{"spark": "350db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.rapids.shims

import com.nvidia.spark.rapids.GpuPartitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveRepartitioningStatus
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDD

case class GpuShuffleExchangeExec(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin)(
    cpuOutputPartitioning: Partitioning)
  extends GpuDatabricksShuffleExchangeExecBase(gpuOutputPartitioning, child, shuffleOrigin)(
    cpuOutputPartitioning) {

  override def getShuffleRDD(
      partitionSpecs: Array[ShufflePartitionSpec],
      lazyFetching: Boolean): RDD[_] = {
    new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics, partitionSpecs)
  }

  // DB SPECIFIC - throw if called since we don't know how its used
  override def withNewNumPartitions(numPartitions: Int): ShuffleExchangeLike = {
    throw new UnsupportedOperationException
  }

  // DB SPECIFIC - throw if called since we don't know how its used
  override def targetOutputPartitioning: Partitioning = {
    throw new UnsupportedOperationException
  }

  def adaptiveRepartitioningStatus(): AdaptiveRepartitioningStatus = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  def repartition(numPartitions: Int,updatedRepartitioningStatus: AdaptiveRepartitioningStatus):
     ShuffleExchangeLike = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }
}
