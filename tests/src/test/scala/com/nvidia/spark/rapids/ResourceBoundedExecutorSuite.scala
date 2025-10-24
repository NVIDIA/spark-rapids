/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{Future => JFuture}

import scala.collection.mutable

import com.nvidia.spark.rapids.io.async._
import org.scalatest.funsuite.AnyFunSuite

class ResourceBoundedExecutorSuite extends AnyFunSuite with RmmSparkRetrySuiteBase {

  private def createSingleThreadedBoundedExecutor(
      maxThreadNumber: Int,
      memorySize: Long = 100L << 20,
      waitResourceTimeoutMs: Long = 5000L): ResourceBoundedThreadExecutor = {
    ResourceBoundedThreadExecutor[Long]("single-thread-executor",
      new HostMemoryPool(memorySize),
      maxThreadNumber = maxThreadNumber,
      waitResourceTimeoutMs = waitResourceTimeoutMs)
  }

  /**
   * Builds a dummy function that simulates the CPU task, which returns the timestamp of the
   * completion of the task.
   */
  private def buildDummyFn(sleepMs: Long = 5L): () => Long = {
    () => {
      // Make sure the task execution slower than the task submission, so that the execution
      // order will be determined by the task priority.
      Thread.sleep(sleepMs)
      System.nanoTime()
    }
  }

  // The test uses a single-threaded executor to capture execution order, submitting tasks with
  // different priorities and memory requirements, then verifying the actual execution sequence
  // matches the expected priority-based task scheduling behavior.
  test("AsyncCpuTask task priority") {
    // Create a single-threaded bounded executor to control the execution order of tasks.
    val executor = createSingleThreadedBoundedExecutor(maxThreadNumber = 1)
    // Test the task scheduling based on priority:
    // Tasks with higher priority should be executed first even if they are submitted later.
    // Execution order: 4, 2, 3, 5, 1
    val executionPriority = Seq(2, 4, 3, 1, 5)
    executor.submit(AsyncRunner.newCpuTask(buildDummyFn(),1 << 20, priority = 1))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    var results = executionPriority.map { p =>
      p -> executor.submit(AsyncRunner.newCpuTask(buildDummyFn(), 1 << 20, priority = p))
    }.sortBy(_._1).map {
      case (_, future) => future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) > results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }

    // Test priority penalty by memory usage:
    // Tasks requiring less resource should be executed first even if they are submitted later.
    // Execution order: 1, 2, 3, 4, 5
    results = (1 to 5).map { i =>
      executor.submit(AsyncRunner.newCpuTask(buildDummyFn(), memoryBytes = i << 20))
    }.map {
      future => future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
    // Execution order: 1, 5, 4, 3, 2
    executor.submit(AsyncRunner.newCpuTask(buildDummyFn(), memoryBytes = 5 << 20))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    results = (4 to 1 by -1).map { i =>
      executor.submit(AsyncRunner.newCpuTask(buildDummyFn(), memoryBytes = i << 20))
    }.map {
      future => future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) > results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }

    // Comprehensive test for task priority and memory usage (including unbounded tasks):
    // Execution order: 1, 4, 6, 2, 5, 3
    val futures = mutable.ArrayBuffer[JFuture[AsyncResult[Long]]]()
    // (1) priority = 5 - 1KB = 4
    futures += executor.submit(AsyncRunner.newCpuTask(buildDummyFn(),
      memoryBytes = 100 << 10, priority = 5L))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    // (2) priority = 3 - 1KB = 2
    futures += executor.submit(AsyncRunner.newCpuTask(buildDummyFn(),
      memoryBytes = 1 << 10, priority = 3L))
    // (3) priority = 4 - 50KB = -46
    futures += executor.submit(AsyncRunner.newCpuTask(buildDummyFn(),
      memoryBytes = 50 << 10, priority = 4L))
    // (4) Unbounded task with the highest priority
    futures += executor.submit(AsyncRunner.newUnboundedTask(buildDummyFn()))
    // (5) priority = 7 - 10KB = -3
    futures += executor.submit(AsyncRunner.newCpuTask(buildDummyFn(),
      memoryBytes = 10 << 10, priority = 7L))
    // (6) Unbounded task with the highest priority
    futures += executor.submit(AsyncRunner.newUnboundedTask(buildDummyFn()))
    results = Array(1, 4, 6, 2, 5, 3).zip(futures).sortBy(_._1).map {
      case (_, fut) => fut.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
  }
}
