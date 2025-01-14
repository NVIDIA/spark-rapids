/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}

import org.apache.spark.sql.rapids.{ColumnarWriteTaskStatsTracker, GpuWriteTaskStatsTracker}

/**
 * Thin wrapper around an ExecutorService that adds throttling.
 *
 * The given executor is owned by this class and will be shutdown when this class is shutdown.
 */
class ThrottlingExecutor(executor: ExecutorService, throttler: TrafficController,
    statsTrackers: Seq[ColumnarWriteTaskStatsTracker]) {

  private var numTasksScheduled = 0
  private var accumulatedThrottleTimeNs = 0L
  private var minThrottleTimeNs = Long.MaxValue
  private var maxThrottleTimeNs = 0L

  private def blockUntilTaskRunnable(task: Task[_]): Unit = {
    val blockStart = System.nanoTime()
    throttler.blockUntilRunnable(task)
    val blockTimeNs = System.nanoTime() - blockStart
    accumulatedThrottleTimeNs += blockTimeNs
    minThrottleTimeNs = Math.min(minThrottleTimeNs, blockTimeNs)
    maxThrottleTimeNs = Math.max(maxThrottleTimeNs, blockTimeNs)
    numTasksScheduled += 1
    updateMetrics()
  }

  def submit[T](callable: Callable[T], hostMemoryBytes: Long): Future[T] = {
    val task = new Task[T](hostMemoryBytes, callable)
    blockUntilTaskRunnable(task)

    executor.submit(() => {
      try {
        task.call()
      } finally {
        throttler.taskCompleted(task)
      }
    })
  }

  def updateMetrics(): Unit = {
    statsTrackers.foreach {
      case gpuStatsTracker: GpuWriteTaskStatsTracker => gpuStatsTracker.setAsyncWriteThrottleTimes(
        numTasksScheduled, accumulatedThrottleTimeNs, minThrottleTimeNs, maxThrottleTimeNs)
      case _ =>
    }
  }

  def shutdownNow(timeout: Long, timeUnit: TimeUnit): Unit = {
    updateMetrics()
    executor.shutdownNow()
    executor.awaitTermination(timeout, timeUnit)
  }
}
