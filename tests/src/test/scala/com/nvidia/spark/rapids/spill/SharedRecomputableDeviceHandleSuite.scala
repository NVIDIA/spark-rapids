/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.spill

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.Arm.withResource

class SharedRecomputableDeviceHandleSuite extends SpillUnitTestBase {
  private class TestResource(val id: Int, closedIds: ArrayBuffer[Int]) extends AutoCloseable {
    private var closed = false

    def isClosed: Boolean = closed

    override def close(): Unit = {
      if (closed) {
        throw new IllegalStateException(s"resource $id closed twice")
      }
      closed = true
      closedIds += id
    }
  }

  test("recomputable handle uses pin count for spillability and rebuilds after spill") {
    val closedIds = ArrayBuffer[Int]()
    var buildCount = 0

    def buildResource(): TestResource = {
      buildCount += 1
      new TestResource(buildCount, closedIds)
    }

    withResource(
      new SharedRecomputableDeviceHandle(1024L, buildResource(), () => buildResource())) {
      handle =>
        SpillFramework.stores.deviceStore.track(handle)
        assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
        assert(handle.spillable)

        withResource(handle.acquire()) { lease =>
          assertResult(1)(lease.resource.id)
          assert(!handle.spillable)
          assertResult(0L)(SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
        }

        assert(handle.spillable)
        assertResult(handle.approxSizeInBytes)(
          SpillFramework.stores.deviceStore.spill(handle.approxSizeInBytes))
        assertResult(Seq(1))(closedIds.toSeq)
        assertResult(0)(SpillFramework.stores.deviceStore.numHandles)

        withResource(handle.acquire()) { lease =>
          assertResult(2)(lease.resource.id)
          assert(!lease.resource.isClosed)
        }

        assertResult(2)(buildCount)
        assertResult(1)(SpillFramework.stores.deviceStore.numHandles)
    }

    assertResult(Seq(1, 2))(closedIds.sorted.toSeq)
  }

  test("releaseSpilled only closes evicted generations") {
    val closedIds = ArrayBuffer[Int]()
    var buildCount = 0

    def buildResource(): TestResource = {
      buildCount += 1
      new TestResource(buildCount, closedIds)
    }

    withResource(
      new SharedRecomputableDeviceHandle(1024L, buildResource(), () => buildResource())) {
      handle =>
        SpillFramework.stores.deviceStore.track(handle)
        assertResult(handle.approxSizeInBytes)(handle.spill())

        withResource(handle.acquire()) { lease =>
          assertResult(2)(lease.resource.id)
          assert(!lease.resource.isClosed)
        }

        assertResult(handle.approxSizeInBytes)(handle.spill())
        handle.releaseSpilled()
        assertResult(Seq(1, 2))(closedIds.toSeq.sorted)

        withResource(handle.acquire()) { lease =>
          assertResult(3)(lease.resource.id)
          assert(!lease.resource.isClosed)
        }
    }

    assertResult(Seq(1, 2, 3))(closedIds.sorted.toSeq)
  }

  test("concurrent acquires only rebuild once after eviction") {
    val closedIds = ArrayBuffer[Int]()
    var buildCount = 0
    val rebuildStarted = new CountDownLatch(1)
    val allowRebuild = new CountDownLatch(1)

    def buildResource(): TestResource = synchronized {
      buildCount += 1
      val id = buildCount
      if (id > 1) {
        rebuildStarted.countDown()
        assert(allowRebuild.await(30, TimeUnit.SECONDS))
      }
      new TestResource(id, closedIds)
    }

    withResource(
      new SharedRecomputableDeviceHandle(1024L, buildResource(), () => buildResource())) {
      handle =>
        SpillFramework.stores.deviceStore.track(handle)
        assertResult(handle.approxSizeInBytes)(handle.spill())

        val pool = Executors.newFixedThreadPool(2)
        try {
          val futures = (0 until 2).map { _ =>
            pool.submit(new Callable[Int] {
              override def call(): Int = {
                withResource(handle.acquire()) { lease =>
                  lease.resource.id
                }
              }
            })
          }

          assert(rebuildStarted.await(30, TimeUnit.SECONDS))
          allowRebuild.countDown()

          val ids = futures.map(_.get(30, TimeUnit.SECONDS)).sorted
          assertResult(Seq(2, 2))(ids)
          assertResult(2)(buildCount)
        } finally {
          pool.shutdownNow()
          assert(pool.awaitTermination(30, TimeUnit.SECONDS))
        }
    }

    assertResult(Seq(1, 2))(closedIds.sorted.toSeq)
  }
}
