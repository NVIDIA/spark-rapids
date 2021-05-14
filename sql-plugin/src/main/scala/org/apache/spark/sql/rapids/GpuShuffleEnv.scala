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

  lazy val isRapidsShuffleConfigured: Boolean = {
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
  val RAPIDS_SHUFFLE_CLASS: String = ShimLoader.getSparkShims.getRapidsShuffleManagerClass

  var mgr: Option[RapidsShuffleInternalManagerBase] = None

  @volatile private var env: GpuShuffleEnv = _

  //
  // Functions below get called from the driver or executors
  //

  def isRapidsShuffleEnabled: Boolean = {
    val isRapidsManager = mgr.isDefined
    val externalShuffle = SparkEnv.get.blockManager.externalShuffleServiceEnabled
    isRapidsManager && !externalShuffle
  }

  def setRapidsShuffleManager(
    managerOpt: Option[RapidsShuffleInternalManagerBase] = None): Unit = {
    if (managerOpt.isDefined) {
      val manager = managerOpt.get
      assert(manager.getClass.getCanonicalName == GpuShuffleEnv.RAPIDS_SHUFFLE_CLASS)
      logInfo("RapidsShuffleManager is initialized")
    }
    mgr = managerOpt
  }

  def getCatalog: ShuffleBufferCatalog = if (env == null) {
    null
  } else {
    env.getCatalog
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
