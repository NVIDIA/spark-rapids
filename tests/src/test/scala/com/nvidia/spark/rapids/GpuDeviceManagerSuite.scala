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
import org.apache.spark.network.util.ByteUnit
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

  def toBytes: String => Long = ConfHelper.byteFromString(_, ByteUnit.BYTE)

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

  test("get host memory limits zero config off heap disabled") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_ENABLED.key -> "false"))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    assertResult(0)(pinnedSize)
    assertResult(-1)(nonPinnedSize)
  }

  test("get host memory limits zero config") {
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(sparkConf)
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    val minMem = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = minMem - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits zero config with host mem") {
    val pySparkOverheadStr = "2g"
    val sparkOffHeapSizeStr = "1g"
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", sparkOffHeapSizeStr)
    val rapidsConf = new RapidsConf(sparkConf)
    val availableHostMem = toBytes("16g")
    TestMemoryChecker.setAvailableMemoryBytes(Some(availableHostMem))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    val heapSize = toBytes("1g") // default
    val pySparkOverhead = toBytes(pySparkOverheadStr)
    val sparkOffHeapSize = toBytes(sparkOffHeapSizeStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = (.8 * (availableHostMem - heapSize - pySparkOverhead -
      sparkOffHeapSize)).toLong - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits off heap configured") {
    val offHeapLimitStr = "16g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.OFF_HEAP_LIMIT_SIZE.key -> offHeapLimitStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
      TestMemoryChecker)

    val offHeapLimit = toBytes(offHeapLimitStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = offHeapLimit - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits memoryOverhead configured") {
    val sparkOverheadStr = "8g"
    val sparkConf = new SparkConf()
      .set("spark.executor.memoryOverhead", sparkOverheadStr)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.executor.pyspark.memory", "1g") // should be ignored here
    val rapidsConf = new RapidsConf(sparkConf)
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    val sparkOverhead = toBytes(sparkOverheadStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = sparkOverhead - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits pinned config only") {
    val pinnedSizeStr = "2g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.PINNED_POOL_SIZE.key -> pinnedSizeStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    val expectedPinned = toBytes(pinnedSizeStr)
    val minMemLimit = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = minMemLimit - totalOverhead - expectedPinned

    assertResult(expectedPinned)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }

  test("get host memory limits pinned config above memLimit") {
    val pinnedSizeStr = "8g"
    val sparkConf = new SparkConf()
    val rapidsConf = new RapidsConf(Map(
      RapidsConf.PINNED_POOL_SIZE.key -> pinnedSizeStr))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)

    val minMemLimit = toBytes("4g")
    val totalOverhead = toBytes("15m") // default
    val expectedPinned = minMemLimit - totalOverhead

    assertResult(expectedPinned)(pinnedSize)
    assertResult(0)(nonPinnedSize)
  }

  test("get host memory limits zero config with host mem with heap size set") {
    val pySparkOverheadStr = "2g"
    val heapSizeStr = "2g"
    val sparkConf = new SparkConf()
      .set("spark.executor.pyspark.memory", pySparkOverheadStr)
      .set("spark.executor.memory", heapSizeStr)
    val rapidsConf = new RapidsConf(sparkConf)
    val hostBytes = toBytes("16g")
    TestMemoryChecker.setAvailableMemoryBytes(Some(hostBytes))
    val (pinnedSize, nonPinnedSize) =
      GpuDeviceManager.getPinnedPoolAndOffHeapLimits(rapidsConf, sparkConf,
        TestMemoryChecker)
    TestMemoryChecker.setAvailableMemoryBytes(None)

    val pySparkOverhead = toBytes(pySparkOverheadStr)
    val heapSize = toBytes(heapSizeStr)
    val totalOverhead = toBytes("15m") // default
    val expectedNonPinned = (.8 * (hostBytes - heapSize - pySparkOverhead)).toLong - totalOverhead

    assertResult(0)(pinnedSize)
    assertResult(expectedNonPinned)(nonPinnedSize)
  }
}
