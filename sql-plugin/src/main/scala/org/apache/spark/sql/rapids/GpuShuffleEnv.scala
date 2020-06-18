/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{CudaMemInfo, Rmm}
import com.nvidia.spark.RapidsShuffleManager
import com.nvidia.spark.rapids._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object GpuShuffleEnv extends Logging {
  private val RAPIDS_SHUFFLE_CLASS = classOf[RapidsShuffleManager].getCanonicalName
  private var isRapidsShuffleManagerInitialized: Boolean  = false

  private val catalog = new RapidsBufferCatalog
  private var shuffleCatalog: ShuffleBufferCatalog = _
  private var shuffleReceivedBufferCatalog: ShuffleReceivedBufferCatalog = _
  private var deviceStorage: RapidsDeviceMemoryStore = _
  private var hostStorage: RapidsHostMemoryStore = _
  private var diskStorage: RapidsDiskStore = _
  private var memoryEventHandler: DeviceMemoryEventHandler = _

  def isRapidsShuffleConfigured(conf: SparkConf): Boolean =
    conf.contains("spark.shuffle.manager") &&
      conf.get("spark.shuffle.manager") == RAPIDS_SHUFFLE_CLASS

  // the shuffle plugin will call this on initialize
  def setRapidsShuffleManagerInitialized(initialized: Boolean, className: String): Unit = {
    assert(className == RAPIDS_SHUFFLE_CLASS)
    logInfo("RapidsShuffleManager is initialized")
    isRapidsShuffleManagerInitialized = initialized
  }

  lazy val isRapidsShuffleEnabled: Boolean = {
    val env = SparkEnv.get
    val isRapidsManager = isRapidsShuffleManagerInitialized
    val externalShuffle = env.blockManager.externalShuffleServiceEnabled
    isRapidsManager && !externalShuffle
  }

  def initStorage(conf: RapidsConf, devInfo: CudaMemInfo): Unit = {
    val sparkConf = SparkEnv.get.conf
    if (isRapidsShuffleConfigured(sparkConf)) {
      assert(memoryEventHandler == null)
      deviceStorage = new RapidsDeviceMemoryStore(catalog)
      hostStorage = new RapidsHostMemoryStore(catalog, conf.hostSpillStorageSize)
      val diskBlockManager = new RapidsDiskBlockManager(sparkConf)
      diskStorage = new RapidsDiskStore(catalog, diskBlockManager)
      deviceStorage.setSpillStore(hostStorage)
      hostStorage.setSpillStore(diskStorage)

      val spillStart = (devInfo.total * conf.rmmSpillAsyncStart).toLong
      val spillStop = (devInfo.total * conf.rmmSpillAsyncStop).toLong
      logInfo("Installing GPU memory handler to start spill at " +
          s"${Utils.bytesToString(spillStart)} and stop at " +
          s"${Utils.bytesToString(spillStop)}")
      memoryEventHandler = new DeviceMemoryEventHandler(deviceStorage, spillStart, spillStop)
      Rmm.setEventHandler(memoryEventHandler)

      shuffleCatalog = new ShuffleBufferCatalog(catalog, diskBlockManager)
      shuffleReceivedBufferCatalog = new ShuffleReceivedBufferCatalog(catalog, diskBlockManager)
    }
  }

  def closeStorage(): Unit = {
    logInfo("Closing shuffle storage")
    if (memoryEventHandler != null) {
      // Workaround for shutdown ordering problems where device buffers allocated with this handler
      // are being freed after the handler is destroyed
      //Rmm.clearEventHandler()
      memoryEventHandler = null
    }
    if (deviceStorage != null) {
      deviceStorage.close()
      deviceStorage = null
    }
    if (hostStorage != null) {
      hostStorage.close()
      hostStorage = null
    }
    if (diskStorage != null) {
      diskStorage.close()
      diskStorage = null
    }
  }

  def getCatalog: ShuffleBufferCatalog = shuffleCatalog

  def getReceivedCatalog: ShuffleReceivedBufferCatalog = shuffleReceivedBufferCatalog

  def getDeviceStorage: RapidsDeviceMemoryStore = deviceStorage
}
