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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.ShimLoader

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._


/**
 * Trait that makes it easy to check whether we are dealing with the
 * a RAPIDS Shuffle Manager
 */
trait RapidsShuffleManagerLike {
  def isDriver: Boolean
  def initialize: Unit
}

/**
 * A simple proxy wrapper allowing to delay loading of the
 * real implementation to a later point when ShimLoader
 * has already updated Spark classloaders.
 *
 * @param conf
 * @param isDriver
 */
class ProxyRapidsShuffleInternalManagerBase(
    conf: SparkConf,
    override val isDriver: Boolean
) extends RapidsShuffleManagerLike with ShuffleManager {

  // touched in the plugin code after the shim initialization
  // is complete
  private lazy val realImpl = ShimLoader.newInternalShuffleManager(conf, isDriver)
    .asInstanceOf[ShuffleManager]

  // This function touches the lazy val `self` so we actually instantiate
  // the manager. This is called from both the driver and executor.
  // In the driver, it's mostly to display information on how to enable/disable the manager,
  // in the executor, the UCXShuffleTransport starts and allocates memory at this time.
  override def initialize: Unit = realImpl

  /**
   * Return the real implementation
   */
  def getRealImpl: ShuffleManager = realImpl

  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    realImpl.getWriter(handle, mapId, context, metrics)
  }

  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    realImpl.getReader(handle,
      startMapIndex, endMapIndex, startPartition, endPartition,
      context, metrics)
  }

  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]
  ): ShuffleHandle = {
    realImpl.registerShuffle(shuffleId, dependency)
  }

  def unregisterShuffle(shuffleId: Int): Boolean = realImpl.unregisterShuffle(shuffleId)

  def shuffleBlockResolver: ShuffleBlockResolver = realImpl.shuffleBlockResolver

  def stop(): Unit = realImpl.stop()
}

