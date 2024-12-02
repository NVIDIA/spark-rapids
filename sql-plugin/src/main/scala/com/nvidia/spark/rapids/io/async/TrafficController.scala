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

import java.util.concurrent.Callable
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import com.nvidia.spark.rapids.RapidsConf

/**
 * Simple wrapper around a [[Callable]] that also keeps track of the host memory bytes used by
 * the task.
 *
 * Note: we may want to add more metadata to the task in the future, such as the device memory,
 * as we implement more throttling strategies.
 */
class Task[T](val hostMemoryBytes: Long, callable: Callable[T]) extends Callable[T] {
  override def call(): T = callable.call()
}

/**
 * Throttle interface to be implemented by different throttling strategies.
 *
 * Currently, only HostMemoryThrottle is implemented, which limits the maximum in-flight host
 * memory bytes. In the future, we can add more throttling strategies, such as limiting the
 * device memory usage, the number of tasks, etc.
 */
trait Throttle {

  /**
   * Returns true if the task can be accepted, false otherwise.
   * TrafficController will block the task from being scheduled until this method returns true.
   */
  def canAccept[T](task: Task[T]): Boolean

  /**
   * Callback to be called when a task is scheduled.
   */
  def taskScheduled[T](task: Task[T]): Unit

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def taskCompleted[T](task: Task[T]): Unit
}

/**
 * Throttle implementation that limits the total host memory used by the in-flight tasks.
 */
class HostMemoryThrottle(val maxInFlightHostMemoryBytes: Long) extends Throttle {
  private var totalHostMemoryBytes: Long = 0

  override def canAccept[T](task: Task[T]): Boolean = {
    totalHostMemoryBytes + task.hostMemoryBytes <= maxInFlightHostMemoryBytes
  }

  override def taskScheduled[T](task: Task[T]): Unit = {
    totalHostMemoryBytes += task.hostMemoryBytes
  }

  override def taskCompleted[T](task: Task[T]): Unit = {
    totalHostMemoryBytes -= task.hostMemoryBytes
  }

  def getTotalHostMemoryBytes: Long = totalHostMemoryBytes
}

/**
 * TrafficController is responsible for blocking tasks from being scheduled when the throttle
 * is exceeded. It also keeps track of the number of tasks that are currently scheduled.
 *
 * This class is thread-safe as it is used by multiple tasks.
 */
class TrafficController protected[rapids] (@GuardedBy("lock") throttle: Throttle) {

  @GuardedBy("lock")
  private var numTasks: Int = 0

  private val lock = new ReentrantLock()
  private val canBeScheduled = lock.newCondition()

  /**
   * Blocks the task from being scheduled until the throttle allows it. If there is no task
   * currently scheduled, the task is scheduled immediately even if the throttle is exceeded.
   */
  def blockUntilRunnable[T](task: Task[T]): Unit = {
    lock.lockInterruptibly()
    try {
      while (numTasks > 0 && !throttle.canAccept(task)) {
        condition.await()
      }
      numTasks += 1
      throttle.taskScheduled(task)
    } finally {
      lock.unlock()
    }
  }

  def taskCompleted[T](task: Task[T]): Unit = {
    lock.lockInterruptibly()
    try {
      numTasks -= 1
      throttle.taskCompleted(task)
      condition.signal()
    } finally {
      lock.unlock()
    }
  }

  def numScheduledTasks: Int = {
    lock.lockInterruptibly()
    try {
      numTasks
    } finally {
      lock.unlock()
    }
  }
}

object TrafficController {

  @GuardedBy("this")
  private var instance: TrafficController = _

  /**
   * Initializes the TrafficController singleton instance.
   * This is called once per executor.
   */
  def initialize(conf: RapidsConf): Unit = synchronized {
    if (conf.isAsyncOutputWriteEnabled && instance == null) {
      instance = new TrafficController(
        new HostMemoryThrottle(conf.asyncWriteMaxInFlightHostMemoryBytes))
    }
  }

  def getInstance: Option[TrafficController] = synchronized {
    Option(instance)
  }

  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance = null
    }
  }
}
