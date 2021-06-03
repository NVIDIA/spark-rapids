/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer}
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil

class GpuDeviceManagerSuite extends FunSuite with Arm {

  test("RMM pool limit") {
    TrampolineUtil.cleanupAnyExistingSession()
    val totalGpuSize = Cuda.memGetInfo().total
    val initPoolFraction = 0.1
    val maxPoolFraction = 0.2
    // we need to reduce the minAllocFraction for this test since the
    // initial allocation here is less than the default minimum
    val minPoolFraction = 0.01
    val conf = new SparkConf()
        .set(RapidsConf.POOLED_MEM.key, "true")
        .set(RapidsConf.RMM_POOL.key, "ARENA")
        .set(RapidsConf.RMM_ALLOC_FRACTION.key, initPoolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_MIN_FRACTION.key, minPoolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_MAX_FRACTION.key, maxPoolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_RESERVE.key, "0")
    try {
      TestUtils.withGpuSparkSession(conf) { _ =>
        val initPoolSize = (totalGpuSize * initPoolFraction).toLong
        val allocSize = Math.max(initPoolSize - 1024 * 1024, 0)
        // initial allocation should fit within initial pool size
        withResource(DeviceMemoryBuffer.allocate(allocSize)) { _ =>
          // this should grow the pool
          withResource(DeviceMemoryBuffer.allocate(allocSize / 2)) { _ =>
            assertThrows[OutOfMemoryError] {
              // this should exceed the specified pool limit
              DeviceMemoryBuffer.allocate(allocSize).close()
            }
          }
        }
      }
    } finally {
      TrampolineUtil.cleanupAnyExistingSession()
    }
  }

  test("RMM reserve larger than max") {
    TrampolineUtil.cleanupAnyExistingSession()
    val rapidsConf = new RapidsConf(Map(RapidsConf.RMM_ALLOC_RESERVE.key -> "200g"))
    assertThrows[IllegalArgumentException] {
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    }
  }

  test("RMM init equals max") {
    TrampolineUtil.cleanupAnyExistingSession()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_RESERVE.key -> "0",
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.3",
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> "0.3"))
    try {
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    } finally {
      GpuDeviceManager.shutdown()
    }
  }
}
