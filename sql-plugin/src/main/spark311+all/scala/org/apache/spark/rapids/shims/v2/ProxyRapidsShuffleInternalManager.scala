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

package org.apache.spark.rapids.shims.v2

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle.{ShuffleBlockResolver, ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.rapids.VisibleShuffleManager

class ProxyRapidsShuffleInternalManager(conf: SparkConf, override val isDriver: Boolean)
    extends VisibleShuffleManager with Proxy {

  override lazy val self: VisibleShuffleManager =
    ShimLoader.newInternalShuffleManager(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    self.registerShuffle(shuffleId, dependency)
  }

  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter
  ): ShuffleReader[K, C] = {
    self.getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context,
      metrics)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    self.getWriter(handle, mapId, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    self.unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    self.shuffleBlockResolver
  }

  override def stop(): Unit = {
    self.stop()
  }
}
