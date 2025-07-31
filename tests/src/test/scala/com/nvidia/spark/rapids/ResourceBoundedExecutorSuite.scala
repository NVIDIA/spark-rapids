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
import scala.util.Random

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
      waitResourceTimeoutMs = waitResourceTimeoutMs,
      retryPriorityAdjust = 0.0)
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
    executor.submit(AsyncTask.newCpuTask(buildDummyFn(),1 << 20, priority = 1))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    var results = executionPriority.map { p =>
      p -> executor.submit(AsyncTask.newCpuTask(buildDummyFn(), 1 << 20, priority = p))
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
      executor.submit(AsyncTask.newCpuTask(buildDummyFn(), memoryBytes = i << 20))
    }.map {
      future => future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
    // Execution order: 1, 5, 4, 3, 2
    executor.submit(AsyncTask.newCpuTask(buildDummyFn(), memoryBytes = 5 << 20))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    results = (4 to 1 by -1).map { i =>
      executor.submit(AsyncTask.newCpuTask(buildDummyFn(), memoryBytes = i << 20))
    }.map {
      future => future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) > results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }

    // Comprehensive test for task priority and memory usage (including unbounded tasks):
    // Execution order: 1, 4, 6, 2, 5, 3
    val futures = mutable.ArrayBuffer[JFuture[AsyncResult[Long]]]()
    // priority = 5 - log10(1 << 20) = -1.02
    futures += executor.submit(AsyncTask.newCpuTask(buildDummyFn(),
      memoryBytes = 1 << 20, priority = 5.0f))
    // Guarantee the first task is executed first to avoid uncertain execution order.
    Thread.sleep(1)
    // priority = 3 - log10(1 << 10) = -0.01
    futures += executor.submit(AsyncTask.newCpuTask(buildDummyFn(),
      memoryBytes = 1 << 10, priority = 3.0f))
    // priority = 4 - log10(50 << 10) = -0.71
    futures += executor.submit(AsyncTask.newCpuTask(buildDummyFn(),
      memoryBytes = 50 << 10, priority = 4.0f))
    // Unbounded task with the highest priority
    futures += executor.submit(AsyncTask.newUnboundedTask(buildDummyFn()))
    // priority = 7 - log10(10 << 20) = -0.02
    futures += executor.submit(AsyncTask.newCpuTask(buildDummyFn(),
      memoryBytes = 10 << 20, priority = 7.0f))
    // Unbounded task with the highest priority
    futures += executor.submit(AsyncTask.newUnboundedTask(buildDummyFn()))
    results = Array(1, 4, 6, 2, 5, 3).zip(futures).sortBy(_._1).map {
      case (_, fut) => fut.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
  }

  // The test uses a single-threaded executor to capture execution order, submitting grouped tasks
  // with globally generated GroupPriority, then verifying the actual execution sequence
  // matches the expected priority-based task scheduling behavior over grouped tasks.
  test("GroupedAsyncTask should run in order") {
    // Create a single-threaded bounded executor to control the execution order of tasks.
    val executor = createSingleThreadedBoundedExecutor(maxThreadNumber = 1)

    class DummyGroupedTask(
        taskIndex: Int,
        groupIndex: Int,
        groupSize: Int,
        groupPriority: Double) extends GroupedAsyncTask[Long] {

      override protected val sharedState: GroupSharedState = {
        GroupTaskHelpers.newSharedState(groupSize)
      }

      override def resource: TaskResource = TaskResource.newCpuResource(1, Some(groupSize))

      // Guarantee tasks are scheduled in the order of the global unique index.
      override val priority: Double = groupPriority - taskIndex

      private val dummyFn: () => Long = buildDummyFn()

      override protected def callImpl(): Long = dummyFn()
    }

    val taskQueue = mutable.ArrayBuffer[(Int, DummyGroupedTask)]()
    (0 until 10).foreach { g =>
      val priority = GroupTaskHelpers.generateGroupPriority
      (0 until 5).foreach { i =>
        taskQueue += Random.nextInt() -> new DummyGroupedTask(i, g, 5, priority)
      }
    }
    val submits = taskQueue.zipWithIndex.sortBy(_._1._1).map { case ((_, task), i) =>
      (i, executor.submit(task))
    }
    val results = submits.drop(1).sortBy(_._1).map(_._2.get().data)
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
  }

  // The test uses a two-pipe bounded executor to simulate the simple concurrent execution,
  // submitting tasks with different memory requirements, then verifying the actual execution
  // order of HostMemoryPool matches the expected priority-based task scheduling behavior.
  test("HostMemoryPool in parallel: task should be blocked when resource is not enough") {
    // Create a two-pipe bounded executor to simulate the simple concurrent execution.
    val executor = createSingleThreadedBoundedExecutor(maxThreadNumber = 2,
      memorySize = 100,
      waitResourceTimeoutMs = 1000000L)
    val asyncTasks = Seq(
      AsyncTask.newCpuTask(buildDummyFn(sleepMs = 3), memoryBytes = 10), // 0 -> 3
      AsyncTask.newCpuTask(buildDummyFn(sleepMs = 5), memoryBytes = 20), // 0 -> 5
      AsyncTask.newCpuTask(buildDummyFn(sleepMs = 4), memoryBytes = 40), // 3 -> 7
      AsyncTask.newCpuTask(buildDummyFn(sleepMs = 5), memoryBytes = 45), // 5 -> 10
      AsyncTask.newCpuTask(buildDummyFn(sleepMs = 2), memoryBytes = 70), // 10 -> 12
    )
    val results = asyncTasks.map(task => executor.submit(task)).map { future =>
      future.get().data
    }
    (0 until results.length - 1).foreach { i =>
      require(results(i) < results(i + 1), s"Unexpected order of wallTime: ${results.toList}")
    }
  }
}
