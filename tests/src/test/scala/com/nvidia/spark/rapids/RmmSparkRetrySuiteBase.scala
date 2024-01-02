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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.mockito.Mockito.spy
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession

trait RmmSparkRetrySuiteBase extends AnyFunSuite with BeforeAndAfterEach {
  private var rmmWasInitialized = false
  protected var deviceStorage: RapidsDeviceMemoryStore = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    RmmSpark.clearEventHandler()
    if (!Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    }
    deviceStorage = spy(new RapidsDeviceMemoryStore())
    val hostStore = new RapidsHostMemoryStore(Some(1L * 1024 * 1024))
    deviceStorage.setSpillStore(hostStore)
    val catalog = new RapidsBufferCatalog(deviceStorage, hostStore)
    // set these against the singleton so we close them later
    RapidsBufferCatalog.setDeviceStorage(deviceStorage)
    RapidsBufferCatalog.setHostStorage(hostStore)
    RapidsBufferCatalog.setCatalog(catalog)
    val mockEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(mockEventHandler)
    RmmSpark.currentThreadIsDedicatedToTask(1)
    HostAlloc.initialize(-1)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    RmmSpark.removeAllCurrentThreadAssociation()
    RmmSpark.clearEventHandler()
    RapidsBufferCatalog.close()
    GpuSemaphore.shutdown()
    if (rmmWasInitialized) {
      Rmm.shutdown()
    }
    HostAlloc.initialize(-1)
  }

  private class BaseRmmEventHandler extends RmmEventHandler {
    override def getAllocThresholds: Array[Long] = null
    override def getDeallocThresholds: Array[Long] = null
    override def onAllocThreshold(totalAllocSize: Long): Unit = {}
    override def onDeallocThreshold(totalAllocSize: Long): Unit = {}
    override def onAllocFailure(sizeRequested: Long, retryCount: Int): Boolean = {
      false
    }
  }
}
