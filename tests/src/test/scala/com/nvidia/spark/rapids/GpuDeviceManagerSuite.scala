/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.Arm.withResource
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.execution.TrampolineUtil

object TestMemoryChecker extends MemoryChecker {
  private var availMemBytes: Option[Long] = None

  override def getAvailableMemoryBytes(rapidsConf: RapidsConf): Option[Long] = availMemBytes

  def setAvailableMemoryBytes(b: Option[Long]): Unit = availMemBytes = b
}

class GpuDeviceManagerSuite extends AnyFunSuite with BeforeAndAfter {

  before {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  after {
    TrampolineUtil.cleanupAnyExistingSession()
  }

  test("RMM pool size") {
    val freeGpuSize = Cuda.memGetInfo().free
    val poolFraction = 0.1
    val maxPoolFraction = 0.2
    // we need to reduce the minAllocFraction for this test since the
    // pool allocation here is less than the default minimum
    val minPoolFraction = 0.01
    val conf = new SparkConf()
        .set(RapidsConf.RMM_POOL.key, "ARENA")
        .set(RapidsConf.RMM_ALLOC_FRACTION.key, poolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_MIN_FRACTION.key, minPoolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_MAX_FRACTION.key, maxPoolFraction.toString)
        .set(RapidsConf.RMM_ALLOC_RESERVE.key, "0")
    TestUtils.withGpuSparkSession(conf) { _ =>
      val poolSize = (freeGpuSize * poolFraction).toLong
      val allocSize = poolSize * 3 / 4
      assert(allocSize > 0)
      // initial allocation should fit within pool size
      withResource(DeviceMemoryBuffer.allocate(allocSize)) { _ =>
        assertThrows[OutOfMemoryError] {
          // this should exceed the specified pool size
          DeviceMemoryBuffer.allocate(allocSize).close()
        }
      }
    }
  }

  test("RMM reserve larger than max") {
    val rapidsConf = new RapidsConf(Map(RapidsConf.RMM_ALLOC_RESERVE.key -> "200g"))
    assertThrows[IllegalArgumentException] {
      SparkSession.builder().master("local[1]").getOrCreate()
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    }
  }

  test("RMM pool size equals max") {
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.RMM_ALLOC_RESERVE.key -> "0",
      RapidsConf.RMM_ALLOC_FRACTION.key -> "0.3",
      RapidsConf.RMM_ALLOC_MAX_FRACTION.key -> "0.3"))
    try {
      SparkSession.builder().master("local[1]").getOrCreate()
      GpuDeviceManager.initializeMemory(None, Some(rapidsConf))
    } finally {
      GpuDeviceManager.shutdown()
    }
  }

  test("RMM get memory limits zero config") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(sparkConf)
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    // 4GB minMemoryLimit - 15MB totalOverhead
    val expectedNonPinned = (4L * 1024 * 1024 * 1024) - (15L * 1024 * 1024)

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("RMM get memory limits zero config with host mem") {
    val pySparkOverhead = (2L * 1024 * 1024 * 1024).toString
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverhead)
    val rapidsConf = new RapidsConf(sparkConf)
    val hostBytes = 16L * 1024 * 1024 * 1024 // 16GB
    TestMemoryChecker.setAvailableMemoryBytes(Some(hostBytes))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    // .8 * (16GB minMemoryLimit - 1GB heapSize - 2GB pyspark) - 15MB totalOverhead
    val expectedNonPinned = (.8 * ((16L - 3L) * 1024 * 1024 * 1024)).toLong - (15L * 1024 * 1024)

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("RMM get memory limits off heap configured") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_SIZE.key -> "16g"
    ))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
      TestMemoryChecker)

    // 16GB minMemoryLimit - 15MB totalOverhead
    val expectedNonPinned = (16L * 1024 * 1024 * 1024) - (15L * 1024 * 1024)

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("RMM get memory limits memoryOverhead configured") {
    val sparkOverhead = (8L * 1024 * 1024 * 1024).toString
    val sparkConf = new SparkConf()
      .set("spark.executor.memoryOverhead", sparkOverhead)
    val rapidsConf = new RapidsConf(sparkConf)
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    // 8GB sparkOverhead - 15MB totalOverhead
    val expectedNonPinned = (8L * 1024 * 1024 * 1024).toLong - (15L * 1024 * 1024)

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("RMM get memory limits pinned config only") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.PINNED_POOL_SIZE.key -> "2gb"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    val expectedPinned = 2L * 1024 * 1024 * 1024 // 2GB
    // 4GB minMemoryLimit - 15MB totalOverhead - pinned
    val expectedNonPinned = (4L * 1024 * 1024 * 1024) - (15L * 1024 * 1024) - expectedPinned

    assertResult(expectedPinned)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("RMM get memory limits pinned config above memLimit") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.PINNED_POOL_SIZE.key -> "8gb"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    // 4GB minMemoryLimit - 15MB totalOverhead
    val expectedPinned = (4L * 1024 * 1024 * 1024) - (15L * 1024 * 1024)

    assertResult(expectedPinned)(pinnedSize)
    assertResult(0)(nonPinnedSize)
  }

  test("RMM get memory limits zero config with host mem with heap size set") {
    val pySparkOverhead = (2L * 1024 * 1024 * 1024).toString
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverhead)
      .set("spark.executor.memory", "2g")
    val rapidsConf = new RapidsConf(sparkConf)
    val hostBytes = 16L * 1024 * 1024 * 1024 // 16GB
    TestMemoryChecker.setAvailableMemoryBytes(Some(hostBytes))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    // .8 * (16GB minMemoryLimit - 2GB heapSize - 2GB pyspark) - 15MB totalOverhead
    val expectedNonPinned = (.8 * ((16L - 4L) * 1024 * 1024 * 1024)).toLong - (15L * 1024 * 1024)

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }
}
