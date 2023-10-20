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

import java.util
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, Semaphore}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.RmmSpark

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

  def computeNumPermits(conf: SQLConf): Int = {
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

private final class TaskInfo(val taskId: Long) extends Logging {
  private val blockedThreads = new LinkedBlockingQueue[Thread]()
  private val activeThreads = new util.LinkedHashSet[Thread]()
  private lazy val numPermits = GpuSemaphore.computeNumPermits(SQLConf.get)
  private var hasSemaphore = false

  def isHoldingSemaphore: Boolean = synchronized {
    hasSemaphore
  }

  def getActiveThreads: Seq[Thread] = synchronized {
    val ret = ArrayBuffer.empty[Thread]
    activeThreads.forEach { item =>
      ret += item
    }
    ret
  }

  def addCurrentThread(): Unit = synchronized {
    // All threads start out in blocked, but will move out of it when
    // they call blockUntilReady.
    val t = Thread.currentThread()
    blockedThreads.add(t)
  }

  private def moveToActive(t: Thread): Unit = synchronized {
    if (!hasSemaphore) {
      throw new IllegalStateException("Should not move to active without holding the semaphore")
    }
    blockedThreads.remove(t)
    activeThreads.add(t)
  }

  def blockUntilReady(semaphore: Semaphore): Unit = {
    val t = Thread.currentThread()
    var done = false
    var shouldBlockOnSemaphore = false
    while (!done) {
      try {
        synchronized {
          done = hasSemaphore
          if (done) {
            moveToActive(t)
          }
          shouldBlockOnSemaphore = t == blockedThreads.peek
          if (!done && !shouldBlockOnSemaphore) {
            wait()
            if (hasSemaphore) {
              moveToActive(t)
              done = true
            }
          }
        }
        if (!done && shouldBlockOnSemaphore) {
          semaphore.acquire(numPermits)
          synchronized {
            hasSemaphore = true
            moveToActive(t)
            notifyAll()
            done = true
          }
        }
      } catch {
        case throwable: Throwable =>
          synchronized {
            // a thread is exiting because of an exception, so we want to reset things if needed.
            blockedThreads.remove(t)
            activeThreads.remove(t)
            if (!hasSemaphore && shouldBlockOnSemaphore) {
              // wake up the other threads so a new thread tries to get the semaphore
              notifyAll()
            }
          }
          throw throwable
      }
    }
  }

  def releaseSemaphore(semaphore: Semaphore): Unit = synchronized {
    val t = Thread.currentThread()
    activeThreads.remove(t)
    if (hasSemaphore) {
      semaphore.release(numPermits)
      hasSemaphore = false
    }
    // This is only an issue because we are on the thread that is supposedly blocked.
    // So it is really more of a sanity test. In reality there should be no threads
    // that are blocked, but one might have been added here when the semaphore was held
    // and now it is being released so it will block.
    if (blockedThreads.remove(t)) {
      throw new IllegalStateException(s"$t tried to release the semaphore when it is blocked!!!")
    }
  }
}

private final class GpuSemaphore() extends Logging {
  import GpuSemaphore._
  private val semaphore = new Semaphore(MAX_PERMITS)
  // Keep track of all tasks that are both active on the GPU and blocked waiting on the GPU
  private val tasks = new ConcurrentHashMap[Long, TaskInfo]

  def acquireIfNecessary(context: TaskContext): Unit = {
    GpuTaskMetrics.get.semWaitTime {
      val taskAttemptId = context.taskAttemptId()
      val taskInfo = tasks.computeIfAbsent(taskAttemptId, key => {
        onTaskCompletion(context, completeTask)
        new TaskInfo(key)
      })
      taskInfo.addCurrentThread()
      taskInfo.blockUntilReady(semaphore)
      RmmSpark.associateCurrentThreadWithTask(taskAttemptId)
      GpuDeviceManager.initializeFromTask()
    }
  }

  def releaseIfNecessary(context: TaskContext): Unit = {
    val nvtxRange = new NvtxRange("Release GPU", NvtxColor.RED)
    try {
      val taskAttemptId = context.taskAttemptId()
      GpuTaskMetrics.get.updateRetry(taskAttemptId)
      RmmSpark.removeCurrentThreadAssociation()
      val taskInfo = tasks.get(taskAttemptId)
      if (taskInfo != null) {
        taskInfo.releaseSemaphore(semaphore)
      }
    } finally {
      nvtxRange.close()
    }
  }

  def completeTask(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    GpuTaskMetrics.get.updateRetry(taskAttemptId)
    RmmSpark.taskDone(taskAttemptId)
    val refs = tasks.remove(taskAttemptId)
    if (refs == null) {
      throw new IllegalStateException(s"Completion of unknown task $taskAttemptId")
    }
    refs.releaseSemaphore(semaphore)
  }

  def dumpActiveStackTracesToLog(): Unit = {
    try {
      val stackTracesSemaphoreHeld = new mutable.ArrayBuffer[String]()
      val otherStackTraces = new mutable.ArrayBuffer[String]()
      tasks.forEach { (taskAttemptId, taskInfo) =>
        val semaphoreHeld = taskInfo.isHoldingSemaphore
        taskInfo.getActiveThreads.foreach { thread =>
          val sb = new mutable.StringBuilder()
          thread.getStackTrace.foreach { stackTraceElement =>
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
      }
      logWarning(s"Dumping stack traces. The semaphore sees ${tasks.size()} tasks, " +
        s"${stackTracesSemaphoreHeld.size} threads are holding onto the semaphore. " +
        stackTracesSemaphoreHeld.mkString("\n", "\n", "\n") +
        otherStackTraces.mkString("\n", "\n", "\n"))
    } catch {
      case t: Throwable =>
        logWarning("Unable to obtain stack traces in the semaphore.", t)
    }
  }

  def shutdown(): Unit = {
    if (!tasks.isEmpty) {
      logDebug(s"shutting down with ${tasks.size} tasks still registered")
    }
  }
}