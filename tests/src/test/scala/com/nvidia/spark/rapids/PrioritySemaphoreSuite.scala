/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class PrioritySemaphoreSuite extends AnyFunSuite {
  type TestPrioritySemaphore = PrioritySemaphore[Long]

  test("tryAcquire should return true if permits are available") {
    val semaphore = new TestPrioritySemaphore(10)

    assert(semaphore.tryAcquire(5))
    assert(semaphore.tryAcquire(3))
    assert(semaphore.tryAcquire(2))
    assert(!semaphore.tryAcquire(1))
  }

  test("acquire and release should work correctly") {
    val semaphore = new TestPrioritySemaphore(1)

    assert(semaphore.tryAcquire(1))

    val latch = new CountDownLatch(1)
    val t = new Thread(() => {
      try {
        semaphore.acquire(1, 1)
        fail("Should not acquire permit")
      } catch {
        case _: InterruptedException =>
          semaphore.acquire(1, 1)
      } finally {
        latch.countDown()
      }
    })
    t.start()

    Thread.sleep(100)
    t.interrupt()

    semaphore.release(1)

    latch.await(1, TimeUnit.SECONDS)
  }

  test("multiple threads should handle permits and priority correctly") {
    val semaphore = new TestPrioritySemaphore(0)
    val latch = new CountDownLatch(3)
    val results = new java.util.ArrayList[Int]()

    def taskWithPriority(priority: Int) = new Runnable {
      override def run(): Unit = {
        try {
          semaphore.acquire(1, priority)
          results.add(priority)
          semaphore.release(1)
        } finally {
          latch.countDown()
        }
      }
    }

    new Thread(taskWithPriority(2)).start()
    new Thread(taskWithPriority(1)).start()
    new Thread(taskWithPriority(3)).start()

    Thread.sleep(100)
    semaphore.release(1)

    latch.await(1, TimeUnit.SECONDS)
    assert(results.asScala.toList == List(3, 2, 1))
  }
}
