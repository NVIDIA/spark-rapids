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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.nvidia.spark.rapids.{RmmSparkRetrySuiteBase, ScalableTaskCompletion, TaskRegistryTracker}

import org.apache.spark.sql.SparkSession

class BounceBufferPoolSuite extends RmmSparkRetrySuiteBase {

  override def beforeEach(): Unit = {
    super.beforeEach()
    ScalableTaskCompletion.reset()
    TaskRegistryTracker.clearRegistry()
  }

  override def afterEach(): Unit = {
    try {
      ScalableTaskCompletion.reset()
      TaskRegistryTracker.clearRegistry()
    } finally {
      super.afterEach()
    }
  }

  test("TaskRegistryTracker identifies dedicated task threads") {
    val spark = SparkSession.builder()
        .master("local[1]")
        .appName("BounceBufferPoolSuite")
        .getOrCreate()

    try {
      spark.sparkContext.parallelize(Seq(1), 1).foreachPartition { _ =>
        TaskRegistryTracker.clearRegistry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.registerThreadForRetry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.registerDedicatedThreadForRetry()
        assert(TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)

        TaskRegistryTracker.clearRegistry()
        assert(!TaskRegistryTracker.isCurrentThreadRegisteredAsDedicatedTaskThread)
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
    }
  }

  test("nextBuffer does not mark unregistered threads as waiting on the RMM pool") {
    val pool = new BounceBufferPool[TestBuffer](1L, 1, _ => new TestBuffer)
    val firstBuffer = pool.nextBuffer()
    var firstBufferReturned = false
    val started = new CountDownLatch(1)
    val acquired = new AtomicReference[BounceBuffer[TestBuffer]]()
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread("bounce-buffer-pool-test-waiter") {
      override def run(): Unit = {
        started.countDown()
        try {
          acquired.set(pool.nextBuffer())
        } catch {
          case t: Throwable => failure.set(t)
        }
      }
    }

    try {
      waiter.start()
      assert(started.await(5, TimeUnit.SECONDS))
      waitUntilBlockedOrDone(waiter, failure)
      assertNoFailure(failure)

      firstBuffer.close()
      firstBufferReturned = true
      waiter.join(TimeUnit.SECONDS.toMillis(5))

      assert(!waiter.isAlive)
      assertNoFailure(failure)
      assert(acquired.get() != null)
    } finally {
      if (!firstBufferReturned) {
        firstBuffer.close()
      }
      val acquiredBuffer = acquired.get()
      if (acquiredBuffer != null) {
        acquiredBuffer.close()
      }
      pool.close()
      if (waiter.isAlive) {
        waiter.join(TimeUnit.SECONDS.toMillis(5))
      }
    }
  }

  private def waitUntilBlockedOrDone(
      thread: Thread,
      failure: AtomicReference[Throwable]): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
    while (thread.isAlive &&
        failure.get() == null &&
        thread.getState != Thread.State.WAITING &&
        System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
  }

  private def assertNoFailure(failure: AtomicReference[Throwable]): Unit = {
    val error = failure.get()
    if (error != null) {
      throw error
    }
  }

  private class TestBuffer extends AutoCloseable {
    override def close(): Unit = {}
  }
}
