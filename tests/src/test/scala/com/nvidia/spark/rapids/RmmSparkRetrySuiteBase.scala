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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{Rmm, RmmAllocationMode, RmmEventHandler}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.sql.SparkSession

class RmmSparkRetrySuiteBase extends FunSuite with BeforeAndAfterEach {
  private var rmmWasInitialized = false

  override def beforeEach(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    if (!Rmm.isInitialized) {
      rmmWasInitialized = true
      Rmm.initialize(RmmAllocationMode.CUDA_DEFAULT, null, 512 * 1024 * 1024)
    }
    val deviceStorage = new RapidsDeviceMemoryStore()
    val catalog = new RapidsBufferCatalog(deviceStorage)
    RapidsBufferCatalog.setDeviceStorage(deviceStorage)
    RapidsBufferCatalog.setCatalog(catalog)
    val mockEventHandler = new BaseRmmEventHandler()
    RmmSpark.setEventHandler(mockEventHandler)
    RmmSpark.associateThreadWithTask(RmmSpark.getCurrentThreadId, 1)
  }

  override def afterEach(): Unit = {
    RmmSpark.removeThreadAssociation(RmmSpark.getCurrentThreadId)
    RmmSpark.clearEventHandler()
    RapidsBufferCatalog.close()
    if (rmmWasInitialized) {
      Rmm.shutdown()
    }
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
