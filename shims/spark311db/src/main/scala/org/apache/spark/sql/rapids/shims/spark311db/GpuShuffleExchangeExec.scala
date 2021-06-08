/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids.shims.spark311db

import scala.concurrent.Future

import org.apache.spark.MapOutputStatistics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase

case class GpuShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    shuffleOrigin: ShuffleOrigin)
  extends GpuShuffleExchangeExecBase(outputPartitioning, child) with ShuffleExchangeLike {

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  override def doMapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputBatchRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependencyColumnar)
    }
  }

  override def numMappers: Int = shuffleDependencyColumnar.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependencyColumnar.partitioner.numPartitions

  override def getShuffleRDD(
      partitionSpecs: Array[ShufflePartitionSpec],
      partitionSizes: Option[Array[Long]]): RDD[_] = {
    throw new UnsupportedOperationException
  }

  // DB SPECIFIC - throw if called since we don't know how its used
  override def withNewOutputPartitioning(outputPartitioning: Partitioning) = {
    throw new UnsupportedOperationException
  }

  override def runtimeStatistics: Statistics = {
    // note that Spark will only use the sizeInBytes statistic but making the rowCount
    // available here means that we can more easily reference it in GpuOverrides when
    // planning future query stages when AQE is on
    Statistics(
      sizeInBytes = metrics("dataSize").value,
      rowCount = Some(metrics("numOutputRows").value)
    )
  }
}
