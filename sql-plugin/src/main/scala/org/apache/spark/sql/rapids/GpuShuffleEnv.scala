/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.util.Locale

import com.nvidia.spark.rapids._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

class GpuShuffleEnv(rapidsConf: RapidsConf) extends Logging {
  private var shuffleCatalog: ShuffleBufferCatalog = _
  private var shuffleReceivedBufferCatalog: ShuffleReceivedBufferCatalog = _

  private lazy val conf = SparkEnv.get.conf

  private lazy val isRapidsShuffleConfigured: Boolean = {
    conf.contains("spark.shuffle.manager") &&
      conf.get("spark.shuffle.manager") == GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS
  }

  lazy val rapidsShuffleCodec: Option[TableCompressionCodec] = {
    val codecName = rapidsConf.shuffleCompressionCodec.toLowerCase(Locale.ROOT)
    if (codecName == "none") {
      None
    } else {
      Some(TableCompressionCodec.getCodec(codecName))
    }
  }

  def init(): Unit = {
    if (isRapidsShuffleConfigured) {
      val diskBlockManager = new RapidsDiskBlockManager(conf)
      shuffleCatalog =
          new ShuffleBufferCatalog(RapidsBufferCatalog.singleton, diskBlockManager)
      shuffleReceivedBufferCatalog =
          new ShuffleReceivedBufferCatalog(RapidsBufferCatalog.singleton)
    }
  }

  def getCatalog: ShuffleBufferCatalog = shuffleCatalog

  def getReceivedCatalog: ShuffleReceivedBufferCatalog = shuffleReceivedBufferCatalog

  def getShuffleFetchTimeoutSeconds: Long = {
    conf.getTimeAsSeconds("spark.network.timeout", "120s")
  }
}

object GpuShuffleEnv extends Logging {
  val RAPIDS_SHUFFLE_CLASS: String = ShimLoader.getRapidsShuffleManagerClass
  val RAPIDS_SHUFFLE_INTERNAL: String = ShimLoader.getRapidsShuffleInternalClass

  @volatile private var env: GpuShuffleEnv = _

  def shutdown() = {
    // check for nulls in tests
    Option(SparkEnv.get)
      .map(_.shuffleManager)
      .collect { case sm: VisibleShuffleManager => sm }
      .foreach(_.stop())

    // when we shut down, make sure we clear `env`, as the convention is that
    // `GpuShuffleEnv.init` will be called to re-establish it
    env = null
  }

  //
  // Functions below get called from the driver or executors
  //

  def isExternalShuffleEnabled: Boolean = {
    SparkEnv.get.blockManager.externalShuffleServiceEnabled
  }

  //
  // The actual instantiation of the RAPIDS Shuffle Manager is lazy, and
  // this forces the initialization when we know we are ready in the driver and executor.
  //
  def initShuffleManager(): Unit = {
    SparkEnv.get.shuffleManager match {
      case visibleMgr: VisibleShuffleManager=>
        visibleMgr.initialize
      case _ =>
        throw new IllegalStateException(s"Cannot initialize the RAPIDS Shuffle Manager")
    }
  }

  def isRapidsShuffleAvailable: Boolean = {
    // the driver has `mgr` defined when this is checked
    val sparkEnv = SparkEnv.get
    val isRapidsManager = sparkEnv.shuffleManager.isInstanceOf[VisibleShuffleManager]
    if (isRapidsManager) {
      validateRapidsShuffleManager(sparkEnv.shuffleManager.getClass.getName)
    }
    // executors have `env` defined when this is checked
    // in tests
    val isConfiguredInEnv = Option(env).map(_.isRapidsShuffleConfigured).getOrElse(false)
    (isConfiguredInEnv || isRapidsManager) && !isExternalShuffleEnabled
  }

  def shouldUseRapidsShuffle(conf: RapidsConf): Boolean = {
    conf.shuffleManagerEnabled && isRapidsShuffleAvailable
  }

  def getCatalog: ShuffleBufferCatalog = if (env == null) {
    null
  } else {
    env.getCatalog
  }

  private def validateRapidsShuffleManager(shuffManagerClassName: String): Unit = {
    val shuffleManagerStr = ShimLoader.getRapidsShuffleManagerClass
    if (shuffManagerClassName != shuffleManagerStr) {
      throw new IllegalStateException(s"RapidsShuffleManager class mismatch (" +
          s"${shuffManagerClassName} != $shuffleManagerStr). " +
          s"Check that configuration setting spark.shuffle.manager is correct for the Spark " +
          s"version being used.")
    }
  }

  //
  // Functions below only get called from the executor
  //

  def init(conf: RapidsConf): Unit = {
    val shuffleEnv = new GpuShuffleEnv(conf)
    shuffleEnv.init()
    env = shuffleEnv
  }

  def getReceivedCatalog: ShuffleReceivedBufferCatalog = env.getReceivedCatalog

  def rapidsShuffleCodec: Option[TableCompressionCodec] = env.rapidsShuffleCodec

  def shuffleFetchTimeoutSeconds: Long = env.getShuffleFetchTimeoutSeconds
}
