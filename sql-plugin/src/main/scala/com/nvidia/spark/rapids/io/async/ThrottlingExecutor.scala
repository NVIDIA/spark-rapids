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

import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}

/**
 * Thin wrapper around an ExecutorService that adds throttling.
 */
class ThrottlingExecutor(
    val executor: ExecutorService, throttler: TrafficController) {

  def submit[T](callable: Callable[T], hostMemoryBytes: Long): Future[T] = {
    val task = new Task[T](hostMemoryBytes, callable)
    throttler.blockUntilRunnable(task)
    executor.submit(() => {
      try {
        task.call()
      } finally {
        throttler.taskCompleted(task)
      }
    })
  }

  def shutdownNow(timeout: Long, timeUnit: TimeUnit): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(timeout, timeUnit)
  }
}
