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

package com.nvidia.spark.rapids.io.async

import java.util.concurrent.{ExecutionException, Executors, ExecutorService, TimeUnit}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class TrafficControllerSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var throttle: HostMemoryThrottle = _
  private var controller: TrafficController = _
  private var executor: ExecutorService = _

  override def beforeEach(): Unit = {
    throttle = new HostMemoryThrottle(100)
    controller = new TrafficController(throttle)
    executor = Executors.newSingleThreadExecutor()
  }

  override def afterEach(): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }

  class TestTask(taskMemoryBytes: Long) extends Task[Unit](taskMemoryBytes, () => {}) {}

  test("schedule tasks without blocking") {
    val taskMemoryBytes = 50
    val t1 = new TestTask(taskMemoryBytes)
    controller.blockUntilRunnable(t1)
    assertResult(1)(controller.numScheduledTasks)
    assertResult(taskMemoryBytes)(throttle.getTotalHostMemoryBytes)

    val t2 = new TestTask(50)
    controller.blockUntilRunnable(t2)
    assertResult(2)(controller.numScheduledTasks)
    assertResult(2 * taskMemoryBytes)(throttle.getTotalHostMemoryBytes)

    controller.taskCompleted(t1)
    assertResult(1)(controller.numScheduledTasks)
    assertResult(taskMemoryBytes)(throttle.getTotalHostMemoryBytes)
  }

  test("schedule task with blocking") {
    val taskMemoryBytes = 50
    val t1 = new TestTask(taskMemoryBytes)
    controller.blockUntilRunnable(t1)

    val t2 = new TestTask(taskMemoryBytes)
    controller.blockUntilRunnable(t2)

    val t3 = new TestTask(taskMemoryBytes)
    val f = executor.submit(new Runnable {
      override def run(): Unit = controller.blockUntilRunnable(t3)
    })
    Thread.sleep(100)
    assert(!f.isDone)

    controller.taskCompleted(t1)
    f.get(1, TimeUnit.SECONDS)
  }

  test("shutdown while blocking") {
    val t1 = new TestTask(10)
    controller.blockUntilRunnable(t1)

    val t2 = new TestTask(110)

    val f = executor.submit(new Runnable {
      override def run(): Unit = {
        controller.blockUntilRunnable(t2)
      }
    })

    executor.shutdownNow()
    try {
      f.get(1, TimeUnit.SECONDS)
      fail("Should be interrupted")
    } catch {
      case ee: ExecutionException =>
        assert(ee.getCause.isInstanceOf[InterruptedException])
      case _: Throwable => fail("Should be interrupted")
    }
  }
}
