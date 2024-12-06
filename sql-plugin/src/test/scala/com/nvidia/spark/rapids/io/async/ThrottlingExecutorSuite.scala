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

import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, Future, RejectedExecutionException, TimeUnit}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

class ThrottlingExecutorSuite extends AnyFunSuite with BeforeAndAfterEach {

  // Some tests might take longer than usual in the limited CI environment.
  // Use a long timeout to avoid flakiness.
  val longTimeoutSec = 5

  var throttle: HostMemoryThrottle = _
  var trafficController: TrafficController = _
  var executor: ThrottlingExecutor = _

  class TestTask extends Callable[Unit] {
    val latch = new CountDownLatch(1)
    override def call(): Unit = {
      latch.await()
    }
  }

  override def beforeEach(): Unit = {
    throttle = new HostMemoryThrottle(100)
    trafficController = new TrafficController(throttle)
    executor = new ThrottlingExecutor(
      Executors.newSingleThreadExecutor(),
      trafficController
    )
  }

  override def afterEach(): Unit = {
    executor.shutdownNow(longTimeoutSec, TimeUnit.SECONDS)
  }

  test("tasks submitted should update the state") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val future2 = executor.submit(task2, 20)
    assertResult(2)(trafficController.numScheduledTasks)
    assertResult(30)(throttle.getTotalHostMemoryBytes)

    task1.latch.countDown()
    future1.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(20)(throttle.getTotalHostMemoryBytes)

    task2.latch.countDown()
    future2.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("tasks submission fails if total weight exceeds maxWeight") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val task2Weight = 100
    val exec = Executors.newSingleThreadExecutor()
    val future2 = exec.submit(new Runnable {
      override def run(): Unit = executor.submit(task2, task2Weight)
    })
    Thread.sleep(100)
    assert(!future2.isDone)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    task1.latch.countDown()
    future1.get(longTimeoutSec, TimeUnit.SECONDS)
    future2.get(longTimeoutSec, TimeUnit.SECONDS)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(task2Weight)(throttle.getTotalHostMemoryBytes)
  }

  test("submit one task heavier than maxWeight") {
    val future = executor.submit(() => Thread.sleep(10), throttle.maxInFlightHostMemoryBytes + 1)
    future.get(longTimeoutSec, TimeUnit.SECONDS)
    assert(future.isDone)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("submit multiple tasks such that total weight does not exceed maxWeight") {
    val numTasks = 10
    val taskRunTime = 10
    var future: Future[Unit] = null
    for (_ <- 0 to numTasks) {
      future = executor.submit(() => Thread.sleep(taskRunTime), 1)
    }
    // Give enough time for all tasks to complete
    future.get(numTasks * taskRunTime * 5, TimeUnit.MILLISECONDS)
    assertResult(0)(trafficController.numScheduledTasks)
    assertResult(0)(throttle.getTotalHostMemoryBytes)
  }

  test("shutdown while a task is blocked") {
    val task1 = new TestTask
    val future1 = executor.submit(task1, 10)
    assertResult(1)(trafficController.numScheduledTasks)
    assertResult(10)(throttle.getTotalHostMemoryBytes)

    val task2 = new TestTask
    val task2Weight = 100
    val exec = Executors.newSingleThreadExecutor()
    val future2 = exec.submit(new Runnable {
      override def run(): Unit = executor.submit(task2, task2Weight)
    })
    executor.shutdownNow(longTimeoutSec, TimeUnit.SECONDS)

    def assertCause(t: Throwable, cause: Class[_]): Unit = {
      assert(t.getCause != null)
      assert(cause.isInstance(t.getCause))
    }

    val e1 = intercept[ExecutionException](future1.get())
    assertCause(e1, classOf[InterruptedException])
    val e2 = intercept[ExecutionException](future2.get())
    assertCause(e2, classOf[RejectedExecutionException])
  }
}
