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

import com.nvidia.spark.rapids.io.async.{AsyncResult, AsyncTask, HostMemoryPool, ResourceBoundedThreadExecutor}
import org.scalatest.funsuite.AnyFunSuite

class ResourceBoundedExecutorSuite extends AnyFunSuite with RmmSparkRetrySuiteBase {

  // The test uses a single-threaded executor to capture execution order, submitting tasks with
  // different priorities and memory requirements, then verifying the actual execution sequence
  // matches the expected priority-based task scheduling behavior.
  test("AsyncCpuTask task priority") {
    new HostMemoryPool(10L << 20)

    val executor = ResourceBoundedThreadExecutor[Int]("single-thread-executor",
      new HostMemoryPool(100L << 20),
      maxThreadNumber = 1,
      waitResourceTimeoutMs = 0,
      retryPriorityAdjust = 0.0f)

    // The queue serves as an execution order tracker for verifying priority-based task
    // scheduling behavior, which records the actual sequence of task execution.
    val queue = mutable.Queue[Int]()

    def fnBuilder(index: Int): () => Int = {
      () => {
        queue.enqueue(index)
        // Make sure the task execution slower than the task submission, so that the execution
        // order will be determined by the task priority.
        Thread.sleep(10)
        index
      }
    }

    // Test the task scheduling based on priority:
    // Tasks with higher priority should be executed first even if they are submitted later.
    // Submit order: 2, 3, 5, 4, 1, 6
    // Execution order: 2, 6, 5, 4, 3, 1
    queue.clear()
    executor.submit(AsyncTask.newCpuTask(fnBuilder(2), memoryBytes = 1 << 20, priority = 2))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    Seq(3, 5, 4, 1, 6).map { i =>
      executor.submit(AsyncTask.newCpuTask(fnBuilder(i), memoryBytes = 1 << 20, priority = i))
    }.foreach { future => future.get() }
    assertResult(Array(2, 6, 5, 4, 3, 1))(queue.toArray)

    // Test priority penalty by memory usage:
    // Tasks requiring less resource should be executed first even if they are submitted later.
    // Submit order: 1, 2, 3, 4, 5
    // Execution order: 1, 2, 3, 4, 5
    queue.clear()
    (1 to 5).map { i =>
      executor.submit(AsyncTask.newCpuTask(fnBuilder(i), memoryBytes = i << 20))
    }.foreach { future => future.get() }
    assertResult(Array(1, 2, 3, 4, 5))(queue.toArray)
    // Submit order: 5, 4, 3, 2, 1
    // Execution order: 5, 1, 2, 3, 4
    queue.clear()
    executor.submit(AsyncTask.newCpuTask(fnBuilder(5), memoryBytes = 5 << 20))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    (4 to 1 by -1).map { i =>
      executor.submit(AsyncTask.newCpuTask(fnBuilder(i), memoryBytes = i << 20))
    }.foreach { future => future.get() }
    assertResult(Array(5, 1, 2, 3, 4))(queue.toArray)

    // Comprehensive test for task priority and memory usage (including unbounded tasks):
    // Submit order: 1, 2, 3, 4, 5, 6
    // Execution order: 1, 4, 6, 2, 5, 3
    queue.clear()
    val futures = mutable.ArrayBuffer[JFuture[AsyncResult[Int]]]()
    // priority = 5 - log10(1 << 20) = -1.02
    futures += executor.submit(AsyncTask.newCpuTask(fnBuilder(1),
      memoryBytes = 1 << 20, priority = 5.0f))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    // priority = 3 - log10(1 << 10) = -0.01
    futures += executor.submit(AsyncTask.newCpuTask(fnBuilder(2),
      memoryBytes = 1 << 10, priority = 3.0f))
    // priority = 4 - log10(50 << 10) = -0.71
    futures += executor.submit(AsyncTask.newCpuTask(fnBuilder(3),
      memoryBytes = 50 << 10, priority = 4.0f))
    // Unbounded task with the highest priority
    futures += executor.submit(AsyncTask.newUnboundedTask(fnBuilder(4)))
    // priority = 7 - log10(10 << 20) = -0.02
    futures += executor.submit(AsyncTask.newCpuTask(fnBuilder(5),
      memoryBytes = 10 << 20, priority = 7.0f))
    // Unbounded task with the highest priority
    futures += executor.submit(AsyncTask.newUnboundedTask(fnBuilder(6)))
    futures.foreach(_.get())
    assertResult(Array(1, 4, 6, 2, 5, 3))(queue.toArray)
  }
}
