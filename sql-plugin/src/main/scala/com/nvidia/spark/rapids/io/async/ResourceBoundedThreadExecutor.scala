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

package com.nvidia.spark.rapids.io.async

import java.util.concurrent.{BlockingQueue, Callable, Future, FutureTask, PriorityBlockingQueue, RunnableFuture, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.internal.Logging

/**
 * A FutureTask wrapper is used by ResourceBoundedThreadExecutor to manage the execution of
 * resource-aware asynchronous tasks. It allows the executor to:
 *   - Track whether the task is currently holding a resource.
 *   - Adjust the task's scheduling priority dynamically.
 *   - Mark completion and exception state for robust scheduling and error handling.
 *   - Integrate with resource management callbacks for proper resource release.
 *
 * @param runner the AsyncRunner to be executed and tracked
 * @tparam T the result type returned by the AsyncRunner
 */
class RapidsFutureTask[T](val runner: AsyncRunner[T]) extends FutureTask[AsyncResult[T]](runner) {
  private[async] var priority: Double = runner.priority
  private var resourceFulfilled: Boolean = false
  private var completed: Boolean = false
  private var caughtException: Boolean = false

  override def run(): Unit = if (!caughtException) {
    require(!completed, "Task has already been completed")
    if (resourceFulfilled) {
      // pass the schedule time to the task metrics builder
      runner.metricsBuilder.setScheduleTimeMs(scheduleTime)
      super.run()
      completed = true
    }
  }

  def holdResource(elapsedNs: Long): Unit = {
    require(!completed, "Task has already been completed")
    require(!resourceFulfilled, "Cannot hold resource that is already held")
    resourceFulfilled = true
    scheduleTime += elapsedNs
  }

  def releaseResource(force: Boolean = false): Unit = {
    require(resourceFulfilled, "Cannot release resource that was not held")
    resourceFulfilled = false
    // Release intermediately if the task supports that, or it has to be (like exception occurred)
    if (force || !runner.holdResourceAfterCompletion) {
      runner.releaseResourceCallback()
    } else {
      // Otherwise, we  mark the task for deferred release.
      runner.needDecayRelease = true
    }
  }

  /**
   * Adjusts the task's priority by a specified delta and appends the last wait time to the
   * schedule time.
   */
  def adjustPriority(delta: Double, lastWaitTime: Long): Double = {
    require(!completed, "Task has already been completed")
    scheduleTime += lastWaitTime
    priority += delta
    priority
  }

  override def setException(e: Throwable): Unit = {
    caughtException = true
    runner.execException = Some(e)
    completed = true
    super.setException(e)
  }

  // Indicates whether it has acquired the necessary resources to run.
  def isResourceFulfilled: Boolean = resourceFulfilled

  def isCompleted: Boolean = completed

  private[async] var scheduleTime: Long = 0L
}

/**
 * Comparator for RapidsFutureTask that orders tasks by priority in descending order.
 * Higher priority tasks (larger priority values) are ordered before lower priority tasks.
 * This enables priority-based scheduling in PriorityBlockingQueue where tasks with
 * higher priority are executed first.
 *
 * @tparam T the result type of the RapidsFutureTask
 */
class RapidsFutureTaskComparator[T] extends java.util.Comparator[RapidsFutureTask[T]] {
  override def compare(o1: RapidsFutureTask[T], o2: RapidsFutureTask[T]): Int = {
    if (o1.isResourceFulfilled) {
      // o1 is holding resource, so o1 should come before o2
      -1
    } else if (o2.isResourceFulfilled) {
      // o2 is holding resource, so o2 should come before o1
      1
    } else {
      // both tasks are not holding resources, compare by priority
      (-o1.priority).compareTo(-o2.priority)
    }
  }
}

/**
 * A thread pool executor that integrates with resource management for executing AsyncRunners.
 *
 * This executor provides resource-aware task scheduling by:
 *   - Acquiring resources before task execution through the ResourcePool
 *   - Tracking resource usage and releasing resources after task completion
 *   - Supporting priority-based task ordering via PriorityBlockingQueue
 *   - Retrying tasks that fail to acquire resources with adjusted priority
 *   - Managing resource lifecycles including immediate and deferred release strategies
 *
 * Tasks that cannot acquire sufficient resources within timeout are bypassed during execution and
 * re-queued with adjusted priority to prevent starvation. The executor works exclusively
 * with AsyncRunner instances and their corresponding RapidsFutureTask wrappers.
 *
 * Resource release timing is controlled by the AsyncRunner's `holdResourceAfterCompletion` flag:
 * immediate release when false (default) or deferred release when true, with exceptions always
 * triggering immediate release regardless of the flag.
 *
 * @param mgr the ResourcePool for managing resource acquisition and release
 * @param waitResourceTimeoutMs timeout in milliseconds for resource acquisition
 * @param retryPriorityAdjust priority adjustment applied to tasks that fail resource acquisition
 * @param corePoolSize the core number of threads in the pool
 * @param maximumPoolSize the maximum number of threads in the pool
 * @param workQueue the queue for holding tasks before execution
 * @param threadFactory the factory for creating new threads
 * @param keepAliveTime the time to keep idle threads alive
 */
class ResourceBoundedThreadExecutor(mgr: ResourcePool,
    waitResourceTimeoutMs: Long,
    retryPriorityAdjust: Double,
    corePoolSize: Int,
    maximumPoolSize: Int,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    keepAliveTime: Long = 100L) extends ThreadPoolExecutor(corePoolSize,
  maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, workQueue, threadFactory) with Logging {

  logInfo(s"Creating ResourceBoundedThreadExecutor with resourcePool: ${mgr.toString}, " +
    s"corePoolSize: $corePoolSize, maximumPoolSize: $maximumPoolSize, " +
    s"waitResourceTimeoutMs: $waitResourceTimeoutMs," +
      s" retryPriorityAdjustment: $retryPriorityAdjust")

  override def submit[T](fn: Callable[T]): Future[T] = {
    fn match {
      case task: AsyncRunner[_] =>
        //register the resource release callback
        task.releaseResourceCallback = () => mgr.releaseResource(task)
        super.submit(task)
      case f =>
        throw new IllegalArgumentException(
          "ResourceBoundedThreadExecutor only accepts AsyncRunner," +
              s" but got: ${f.getClass.getName}")
    }
  }

  // This method is only for the extensions of RapidsFutureTask.
  override def submit[T](r: Runnable, result: T): Future[T] = {
    r match {
      case futTask: RapidsFutureTask[_] =>
        //register the resource release callback
        futTask.runner.releaseResourceCallback = () => mgr.releaseResource(futTask.runner)
        super.submit(futTask, null.asInstanceOf[T])
      case _ =>
        throw new UnsupportedOperationException("only accepts AsyncRunner or RapidsFutureTask")
    }
  }

  override def submit(r: Runnable): Future[_] = {
    throw new UnsupportedOperationException("only accepts AsyncRunner or RapidsFutureTask")
  }

  override protected def newTaskFor[T](fn: Callable[T]): RunnableFuture[T] = {
    fn match {
      case task: AsyncRunner[_] =>
        new RapidsFutureTask(task)
      case f =>
        throw new RuntimeException(s"Unexpected functor: ${f.getClass.getName}")
    }
  }

  override protected def newTaskFor[T](r: Runnable, result: T): RunnableFuture[T] = {
    r match {
      case futTask: RapidsFutureTask[_] =>
        futTask.asInstanceOf[RunnableFuture[T]]
      case task: AsyncRunner[_] =>
        new RapidsFutureTask(task).asInstanceOf[RunnableFuture[T]]
      case f =>
        throw new RuntimeException(s"Unexpected runnable: ${f.getClass.getName}")
    }
  }

  override def beforeExecute(t: Thread, r: Runnable): Unit = {
    r match {
      case fut: RapidsFutureTask[_] =>
        mgr.acquireResource(fut.runner, waitResourceTimeoutMs) match {
          case s: AcquireSuccessful =>
            fut.holdResource(s.elapsedTime)
          case AcquireFailed =>
            // bypass the execution via not holding the resource
          case AcquireExcepted(exception) =>
            logError(s"Invalid resource request for task ${fut.runner}: ${exception.getMessage}")
            // setException will unblock the corresponding waiting thread by failing it
            fut.setException(exception)
        }
      case _ =>
        throw new RuntimeException(s"Unexpected runnable: ${r.getClass.getName}")
    }
  }

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    r match {
      case fut: RapidsFutureTask[_] =>
        // Release the held resource if it was acquired.
        if (fut.isResourceFulfilled) {
          fut.releaseResource(t != null)
        }
        // If the task failed to acquire enough resource, we bypass the execution and re-add it to
        // the task queue with a priority penalty to avoid starvation.
        if (t == null && !fut.isCompleted) {
          // IMPORTANT: Ensure the priority penalty >=1000 in case the timeout task being
          // polled recursively.
          val priorPenalty = -retryPriorityAdjust min -1e3
          fut.adjustPriority(priorPenalty, waitResourceTimeoutMs * 1000000L)
          logWarning(s"Re-add timeout task: scheduleTime(${fut.scheduleTime / 1e9}s), " +
              s"new priority(${fut.priority})")
          require(workQueue.add(fut),
            s"Failed to re-add task ${fut.runner} to the work queue after execution")
        }
      case _ =>
        throw new RuntimeException(s"Unexpected runnable: ${r.getClass.getName}")
    }
  }
}

object ResourceBoundedThreadExecutor {
  def apply[T](name: String,
      pool: ResourcePool,
      maxThreadNumber: Int,
      waitResourceTimeoutMs: Long = 60 * 1000L,
      retryPriorityAdjust: Double = 0.0): ResourceBoundedThreadExecutor = {
    val taskQueue = new PriorityBlockingQueue(10000, new RapidsFutureTaskComparator[T])
    val threadFactory: ThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(name)
        .build()

    new ResourceBoundedThreadExecutor(pool,
      waitResourceTimeoutMs,
      retryPriorityAdjust,
      corePoolSize = maxThreadNumber,
      maximumPoolSize = maxThreadNumber,
      workQueue = taskQueue.asInstanceOf[BlockingQueue[Runnable]],
      threadFactory = threadFactory)
  }
}
