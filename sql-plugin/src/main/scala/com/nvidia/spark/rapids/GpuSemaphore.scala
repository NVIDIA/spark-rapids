/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuTaskMetrics

/**
 * The result of trying to acquire a semaphore could be
 * `SemaphoreAcquired` or `AcquireFailed`.
 */
sealed trait TryAcquireResult

/**
 * The Semaphore was successfully acquired.
 */
case object SemaphoreAcquired extends TryAcquireResult

/**
 * To acquire the semaphore this thread would have to block.
 * @param numWaitingTasks the number of tasks waiting at the time the request was made.
 *                        Note that this can change very quickly.
 */
case class AcquireFailed(numWaitingTasks: Int) extends TryAcquireResult

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
   * A thread may try to acquire the semaphore without blocking on it. NOTE: A task completion
   * listener will automatically be installed to ensure the semaphore is always released by the
   * time the task completes.
   */
  def tryAcquire(context: TaskContext): TryAcquireResult = {
    if (context != null) {
      getInstance.tryAcquire(context)
    } else {
      // For unit tests that might try with no context
      SemaphoreAcquired
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

/**
 * This represents the state associated with a given task. A task can have multiple threads
 * associated with it. That tends to happen when there is a UDF in an external language
 * a.k.a python.  In that case a writer thread is created to feed the python process and
 * the original thread is used as a reader thread that pulls data from the python process.
 * For the GPU semaphore to avoid deadlocks we either allow all threads associated with a task
 * on the GPU or none of them. But this requires coordination to block all of them or wake up
 * all of them. That is the primary job of this class.
 *
 * It should be noted that there is no special coordination when releasing the semaphore. This
 * can result in one thread running on the GPU when it thinks it has the semaphore, but does
 * not. As the semaphore is used as a first line of defense to avoid using too much GPU memory
 * this is considered to be okay as there are other mechanisms in place, and it should be rather
 * rare.
 */
private final class SemaphoreTaskInfo() extends Logging {
  /**
   * This holds threads that are not on the GPU yet. Most of the time they are
   * blocked waiting for the semaphore to let them on, but it may hold one
   * briefly even when this task is holding the semaphore. This is a queue
   * mostly to give us a simple way to elect one thread to block on the semaphore
   * while the others will block with a call to `wait`. There should typically be
   * very few threads in here, if any.
   */
  private val blockedThreads = new LinkedBlockingQueue[Thread]()
  /**
   * All threads that are currently active on the GPU. This is mostly used for
   * debugging. It is a `Set` to avoid duplicates, not for performance because there
   * should be very few in here at a time.
   */
  private val activeThreads = new util.LinkedHashSet[Thread]()
  private lazy val numPermits = GpuSemaphore.computeNumPermits(SQLConf.get)
  /**
   * If this task holds the GPU semaphore or not.
   */
  private var hasSemaphore = false

  /**
   * Does this task have the GPU semaphore or not. Be careful because it can change at
   * any point in time. So only use it for logging.
   */
  def isHoldingSemaphore: Boolean = synchronized {
    hasSemaphore
  }

  /**
   * Get the list of threads currently running on the GPU Semaphore for this task. Be
   * careful because these can change at any point in time. So only use it for logging.
   */
  def getActiveThreads: Seq[Thread] = synchronized {
    val ret = ArrayBuffer.empty[Thread]
    activeThreads.forEach { item =>
      ret += item
    }
    ret.toSeq
  }

  private def moveToActive(t: Thread): Unit = synchronized {
    if (!hasSemaphore) {
      throw new IllegalStateException("Should not move to active without holding the semaphore")
    }
    blockedThreads.remove(t)
    activeThreads.add(t)
  }

  /**
   * Block the current thread until we have the semaphore.
   * @param semaphore what we are going to wait on.
   */
  def blockUntilReady(semaphore: Semaphore): Unit = {
    val t = Thread.currentThread()
    // All threads start out in blocked, but will move out of it inside of the while loop.
    synchronized {
      blockedThreads.add(t)
    }
    var done = false
    var shouldBlockOnSemaphore = false
    while (!done) {
      try {
        synchronized {
          // This thread can continue if this task owns the GPU semaphore. When that happens
          // move the state of the thread from blocked to active.
          done = hasSemaphore
          if (done) {
            moveToActive(t)
          }
          // Only one thread can block on the semaphore itself, we pick the first thread in
          // blockedThread to be that one. This is arbitrary and does not matter, it is just
          // simple to do.
          shouldBlockOnSemaphore = t == blockedThreads.peek
          if (!done && !shouldBlockOnSemaphore) {
            // If we need to block and are not blocking on the semaphore we will wait
            // on this class until the task has the semaphore and we wake up.
            wait()
            if (hasSemaphore) {
              moveToActive(t)
              done = true
            }
          }
        }
        if (!done && shouldBlockOnSemaphore) {
          // We cannot be in a synchronized block and wait on the semaphore
          // so we have to release it and grab it again afterwards.
          semaphore.acquire(numPermits)
          synchronized {
            // We now own the semaphore so we need to wake up all of the other tasks that are
            // waiting.
            hasSemaphore = true
            moveToActive(t)
            notifyAll()
            done = true
          }
        }
      } catch {
        case throwable: Throwable =>
          synchronized {
            // a thread is exiting because of an exception, so we want to reset things,
            // and possibly elect another thread to wait on the semaphore.
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

  def tryAcquire(semaphore: Semaphore): Boolean = synchronized {
    val t = Thread.currentThread()
    if (hasSemaphore) {
      activeThreads.add(t)
      true
    } else {
      if (blockedThreads.size() == 0) {
        // No other threads for this task are waiting, so we might be able to grab this directly
        val ret = semaphore.tryAcquire(numPermits)
        if (ret) {
          hasSemaphore = true
          activeThreads.add(t)
          // no need to notify because there are no other threads and we are holding the lock
          // to ensure that.
        }
        ret
      } else {
        false
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
    // It should be impossible for the current thread to be blocked when releasing the semaphore
    // because no blocked thread should ever leave `blockUntilReady`, which is where we put it in
    // the blocked state. So this is just a sanity test that we didn't do something stupid.
    if (blockedThreads.remove(t)) {
      throw new IllegalStateException(s"$t tried to release the semaphore when it is blocked!!!")
    }
  }
}

private final class GpuSemaphore() extends Logging {
  import GpuSemaphore._
  private val semaphore = new Semaphore(MAX_PERMITS)
  // Keep track of all tasks that are both active on the GPU and blocked waiting on the GPU
  private val tasks = new ConcurrentHashMap[Long, SemaphoreTaskInfo]

  def tryAcquire(context: TaskContext): TryAcquireResult = {
    // Make sure that the thread/task is registered before we try and block
    TaskRegistryTracker.registerThreadForRetry()
    val taskAttemptId = context.taskAttemptId()
    val taskInfo = tasks.computeIfAbsent(taskAttemptId, _ => {
      onTaskCompletion(context, completeTask)
      new SemaphoreTaskInfo()
    })
    if (taskInfo.tryAcquire(semaphore)) {
      GpuDeviceManager.initializeFromTask()
      SemaphoreAcquired
    } else {
      // We need to get the number of tasks that are still waiting
      var numWaiting = 0
      tasks.values().forEach { ti =>
        if (ti.isHoldingSemaphore) {
          numWaiting += 1
        }
      }
      AcquireFailed(numWaiting)
    }
  }

  def acquireIfNecessary(context: TaskContext): Unit = {
    // Make sure that the thread/task is registered before we try and block
    TaskRegistryTracker.registerThreadForRetry()
    GpuTaskMetrics.get.semWaitTime {
      val taskAttemptId = context.taskAttemptId()
      val taskInfo = tasks.computeIfAbsent(taskAttemptId, _ => {
        onTaskCompletion(context, completeTask)
        new SemaphoreTaskInfo()
      })
      taskInfo.blockUntilReady(semaphore)
      GpuDeviceManager.initializeFromTask()
    }
  }

  def releaseIfNecessary(context: TaskContext): Unit = {
    val nvtxRange = new NvtxRange("Release GPU", NvtxColor.RED)
    try {
      val taskAttemptId = context.taskAttemptId()
      GpuTaskMetrics.get.updateRetry(taskAttemptId)
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