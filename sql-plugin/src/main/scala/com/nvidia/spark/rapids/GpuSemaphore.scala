/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import org.apache.commons.lang3.mutable.MutableInt

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

object GpuSemaphore {
  private val enabled = {
    val propstr = System.getProperty("com.nvidia.spark.rapids.semaphore.enabled")
    if (propstr != null) {
      java.lang.Boolean.parseBoolean(propstr)
    } else {
      true
    }
  }

  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile private var instance: GpuSemaphore = _

  private def getInstance: GpuSemaphore = {
    if (instance == null) {
      GpuSemaphore.synchronized {
        // The instance is trying to be used before it is initialized.
        // Since we don't have access to a configuration object here,
        // default to only one task per GPU behavior.
        if (instance == null) {
          initialize(1)
        }
      }
    }
    instance
  }

  /**
   * Initializes the GPU task semaphore.
   * @param tasksPerGpu number of tasks that will be allowed to use the GPU concurrently.
   */
  def initialize(tasksPerGpu: Int): Unit = synchronized {
    if (enabled) {
      if (instance != null) {
        throw new IllegalStateException("already initialized")
      }
      instance = new GpuSemaphore(tasksPerGpu)
    }
  }

  /**
   * Tasks must call this when they begin to use the GPU.
   * If the task has not already acquired the GPU semaphore then it is acquired,
   * blocking if necessary.
   * NOTE: A task completion listener will automatically be installed to ensure
   *       the semaphore is always released by the time the task completes.
   */
  def acquireIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.acquireIfNecessary(context, None)
    }
  }

  /**
   * Tasks must call this when they begin to use the GPU.
   * If the task has not already acquired the GPU semaphore then it is acquired,
   * blocking if necessary.
   * NOTE: A task completion listener will automatically be installed to ensure
   *       the semaphore is always released by the time the task completes.
   */
  def acquireIfNecessary(context: TaskContext, waitMetric: GpuMetric): Unit = {
    if (enabled && context != null) {
      getInstance.acquireIfNecessary(context, Some(waitMetric))
    }
  }

  /**
   * Tasks must call this when they are finished using the GPU.
   */
  def releaseIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.releaseIfNecessary(context)
    }
  }

  /**
   * Uninitialize the GPU semaphore.
   * NOTE: This does not wait for active tasks to release!
   */
  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
    }
  }
}

private final class GpuSemaphore(tasksPerGpu: Int) extends Logging with Arm {
  private val semaphore = new Semaphore(tasksPerGpu)
  // Map to track which tasks have acquired the semaphore.
  private val activeTasks = new ConcurrentHashMap[Long, MutableInt]

  def acquireIfNecessary(context: TaskContext, waitMetric: Option[GpuMetric] = None): Unit = {
    withResource(NvtxWithMetrics.apply("Acquire GPU", NvtxColor.RED, waitMetric)) { _ =>
      val taskAttemptId = context.taskAttemptId()
      val refs = activeTasks.get(taskAttemptId)
      if (refs == null || refs.getValue == 0) {
        logDebug(s"Task $taskAttemptId acquiring GPU")
        semaphore.acquire()
        if (refs != null) {
          refs.increment()
        } else {
          // first time this task has been seen
          activeTasks.put(taskAttemptId, new MutableInt(1))
          context.addTaskCompletionListener[Unit](completeTask)
        }
        GpuDeviceManager.initializeFromTask()
      }
    }
  }

  def releaseIfNecessary(context: TaskContext): Unit = {
    val nvtxRange = new NvtxRange("Release GPU", NvtxColor.RED)
    try {
      val taskAttemptId = context.taskAttemptId()
      val refs = activeTasks.get(taskAttemptId)
      if (refs != null && refs.getValue > 0) {
        if (refs.decrementAndGet() == 0) {
          logDebug(s"Task $taskAttemptId releasing GPU")
          semaphore.release()
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  def completeTask(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    val refs = activeTasks.remove(taskAttemptId)
    if (refs == null) {
      throw new IllegalStateException(s"Completion of unknown task $taskAttemptId")
    }
    if (refs.getValue > 0) {
      logDebug(s"Task $taskAttemptId releasing GPU")
      semaphore.release()
    }
  }

  def shutdown(): Unit = {
    if (!activeTasks.isEmpty) {
      logDebug(s"shutting down with ${activeTasks.size} tasks still registered")
    }
  }
}
