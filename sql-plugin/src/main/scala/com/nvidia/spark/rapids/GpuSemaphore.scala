/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.commons.lang3.mutable.MutableInt

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuTaskMetrics

object GpuSemaphore {
  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile private var instance: GpuSemaphore = _

  private def getInstance: GpuSemaphore = {
    if (instance == null) {
      GpuSemaphore.synchronized {
        // The instance is trying to be used before it is initialized.
        // Since we don't have access to a configuration object here,
        // default to only one task per GPU behavior.
        if (instance == null) {
          initialize()
        }
      }
    }
    instance
  }

  /**
   * Initializes the GPU task semaphore.
   */
  def initialize(): Unit = synchronized {
    if (instance != null) {
      throw new IllegalStateException("already initialized")
    }
    instance = new GpuSemaphore()
  }

  /**
   * Tasks must call this when they begin to use the GPU.
   * If the task has not already acquired the GPU semaphore then it is acquired,
   * blocking if necessary.
   * NOTE: A task completion listener will automatically be installed to ensure
   *       the semaphore is always released by the time the task completes.
   */
  def acquireIfNecessary(context: TaskContext): Unit = {
    if (context != null) {
      getInstance.acquireIfNecessary(context)
    }
  }

  /**
   * Tasks must call this when they are finished using the GPU.
   */
  def releaseIfNecessary(context: TaskContext): Unit = {
    if (context != null) {
      getInstance.releaseIfNecessary(context)
    }
  }

  /**
   * Dumps the stack traces for any tasks that have accessed the GPU semaphore
   * and have not completed. The output includes whether the task has the GPU semaphore
   * held at the time of the stack trace.
   */
  def dumpActiveStackTracesToLog(): Unit = {
    getInstance.dumpActiveStackTracesToLog()
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

  private val MAX_PERMITS = 1000

  private def computeNumPermits(conf: SQLConf): Int = {
    val concurrentStr = conf.getConfString(RapidsConf.CONCURRENT_GPU_TASKS.key, null)
    val concurrentInt = Option(concurrentStr)
      .map(ConfHelper.toInteger(_, RapidsConf.CONCURRENT_GPU_TASKS.key))
      .getOrElse(RapidsConf.CONCURRENT_GPU_TASKS.defaultValue)
    // concurrentInt <= 0 is the same as 1 (fail to the safest value)
    // concurrentInt > MAX_PERMITS becomes the same as MAX_PERMITS
    // (who has more than 1000 threads anyways).
    val permits = MAX_PERMITS / math.min(math.max(concurrentInt, 1), MAX_PERMITS)
    math.max(permits, 1)
  }
}

private final class GpuSemaphore() extends Logging {
  import GpuSemaphore._
  private val semaphore = new Semaphore(MAX_PERMITS)

  // Map to track which tasks have acquired the semaphore.
  case class TaskInfo(count: MutableInt, thread: Thread, numPermits: Int)
  private val activeTasks = new ConcurrentHashMap[Long, TaskInfo]

  def acquireIfNecessary(context: TaskContext): Unit = {
    GpuTaskMetrics.get.semWaitTime {
      val taskAttemptId = context.taskAttemptId()
      val refs = activeTasks.get(taskAttemptId)
      if (refs == null || refs.count.getValue == 0) {
        val permits = if (refs == null) {
          computeNumPermits(SQLConf.get)
        } else {
          refs.numPermits
        }
        logDebug(s"Task $taskAttemptId acquiring GPU with $permits permits")
        semaphore.acquire(permits)
        RmmSpark.associateCurrentThreadWithTask(taskAttemptId)
        if (refs != null) {
          refs.count.increment()
        } else {
          // first time this task has been seen
          activeTasks.put(
            taskAttemptId,
            TaskInfo(new MutableInt(1), Thread.currentThread(), permits))
          context.addTaskCompletionListener[Unit](completeTask)
        }
        GpuDeviceManager.initializeFromTask()
      } else {
        // Already had the semaphore, but we don't know if the thread is new or not
        RmmSpark.associateCurrentThreadWithTask(taskAttemptId)
      }
    }
  }

  def releaseIfNecessary(context: TaskContext): Unit = {
    val nvtxRange = new NvtxRange("Release GPU", NvtxColor.RED)
    try {
      val taskAttemptId = context.taskAttemptId()
      GpuTaskMetrics.get.updateRetry(taskAttemptId)
      RmmSpark.removeCurrentThreadAssociation()
      val refs = activeTasks.get(taskAttemptId)
      if (refs != null && refs.count.getValue > 0) {
        if (refs.count.decrementAndGet() == 0) {
          logDebug(s"Task $taskAttemptId releasing GPU with ${refs.numPermits} permits")
          semaphore.release(refs.numPermits)
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  def completeTask(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    GpuTaskMetrics.get.updateRetry(taskAttemptId)
    RmmSpark.taskDone(taskAttemptId)
    val refs = activeTasks.remove(taskAttemptId)
    if (refs == null) {
      throw new IllegalStateException(s"Completion of unknown task $taskAttemptId")
    }
    if (refs.count.getValue > 0) {
      logDebug(s"Task $taskAttemptId releasing GPU with ${refs.numPermits} permits")
      semaphore.release(refs.numPermits)
    }
  }

  def dumpActiveStackTracesToLog(): Unit = {
    try {
      val stackTracesSemaphoreHeld = new mutable.ArrayBuffer[String]()
      val otherStackTraces = new mutable.ArrayBuffer[String]()
      activeTasks.forEach { (taskAttemptId, taskInfo) =>
        val sb = new mutable.StringBuilder()
        val semaphoreHeld = taskInfo.count.getValue > 0
        taskInfo.thread.getStackTrace.foreach { stackTraceElement =>
          sb.append("    " + stackTraceElement + "\n")
        }
        if (semaphoreHeld) {
          stackTracesSemaphoreHeld.append(
            s"Semaphore held. " +
              s"Stack trace for task attempt id $taskAttemptId:\n${sb.toString()}")
        } else {
          otherStackTraces.append(
            s"Semaphore not held. " +
              s"Stack trace for task attempt id $taskAttemptId:\n${sb.toString()}")
        }
      }
      logWarning(s"Dumping stack traces. The semaphore sees ${activeTasks.size()} tasks, " +
        s"${stackTracesSemaphoreHeld.size} are holding onto the semaphore. " +
        stackTracesSemaphoreHeld.mkString("\n", "\n", "\n") +
        otherStackTraces.mkString("\n", "\n", "\n"))
    } catch {
      case t: Throwable =>
        logWarning("Unable to obtain stack traces in the semaphore.", t)
    }
  }

  def shutdown(): Unit = {
    if (!activeTasks.isEmpty) {
      logDebug(s"shutting down with ${activeTasks.size} tasks still registered")
    }
  }
}
