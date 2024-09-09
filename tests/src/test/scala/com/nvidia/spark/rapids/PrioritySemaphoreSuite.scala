/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class PrioritySemaphoreSuite extends AnyFunSuite {
  type TestPrioritySemaphore = PrioritySemaphore[Long]

  test("tryAcquire should return true if permits are available") {
    val semaphore = new TestPrioritySemaphore(10)

    assert(semaphore.tryAcquire(5, 0))
    assert(semaphore.tryAcquire(3, 0))
    assert(semaphore.tryAcquire(2, 0))
    assert(!semaphore.tryAcquire(1, 0))
  }

  test("acquire and release should work correctly") {
    val semaphore = new TestPrioritySemaphore(1)

    assert(semaphore.tryAcquire(1, 0))

    val t = new Thread(() => {
      try {
        semaphore.acquire(1, 1)
        fail("Should not acquire permit")
      } catch {
        case _: InterruptedException =>
          semaphore.acquire(1, 1)
      }
    })
    t.start()

    Thread.sleep(100)
    t.interrupt()

    semaphore.release(1)

    t.join(1000)
  }

  test("multiple threads should handle permits and priority correctly") {
    val semaphore = new TestPrioritySemaphore(0)
    val results = new java.util.ArrayList[Int]()

    def taskWithPriority(priority: Int) = new Runnable {
      override def run(): Unit = {
        semaphore.acquire(1, priority)
        results.add(priority)
        semaphore.release(1)
      }
    }

    val threads = List(
      new Thread(taskWithPriority(2)),
      new Thread(taskWithPriority(1)),
      new Thread(taskWithPriority(3))
    )
    threads.foreach(_.start)

    Thread.sleep(100)
    semaphore.release(1)

    threads.foreach(_.join(1000))
    assert(results.asScala.toList == List(3, 2, 1))
  }

  test("low priority thread cannot surpass high priority thread") {
    val semaphore = new TestPrioritySemaphore(10)
    semaphore.acquire(5, 0)
    val t = new Thread(() => {
      semaphore.acquire(10, 2)
      semaphore.release(10)
    })
    t.start()
    Thread.sleep(100)

    // Here, there should be 5 available permits, but a thread with higher priority (2)
    // is waiting to acquire, therefore we should get rejected here
    assert(!semaphore.tryAcquire(5, 0))
    semaphore.release(5)
    t.join(1000)
    // After the high priority thread finishes, we can acquire with lower priority
    assert(semaphore.tryAcquire(5, 0))
  }
}
