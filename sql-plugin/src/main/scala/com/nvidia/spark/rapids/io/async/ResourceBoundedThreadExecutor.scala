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
 * A RapidsFutureTask wrapper is used by ResourceBoundedThreadExecutor to manage the execution of
 * resource-aware asynchronous tasks. It allows the executor to:
 *   - Track whether the task is currently holding a resource.
 *   - [NOT enabled] Adjust the task's scheduling priority dynamically.
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

  def resourceAcquired(waitTimeNs: Long): Unit = {
    require(!completed, "Task has already been completed")
    require(!resourceFulfilled, "Cannot hold resource that is already held")
    resourceFulfilled = true
    scheduleTime += waitTimeNs
  }

  def releaseResource(force: Boolean = false): Unit = {
    require(resourceFulfilled, "Cannot release resource that was not held")
    resourceFulfilled = false
    // releaseResourceCallback may be null under certain circumstance like if the runner is
    // UnboundedAsyncRunner
    Option(runner.releaseResourceCallback).foreach { cb =>
      runner.result match {
        // Immediately release the resource if the task did NOT build result successfully
        case None => cb()
        // Immediately release the resource if the task built a FastReleaseResult
        case Some(_: FastReleaseResult[T]) => cb()
        // Immediately release the resource if forced
        case _ if force => cb()
        // Otherwise, defer the release
        case _ =>
      }
    }
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

  override def toString: String = {
    s"RapidsFutureTask(runner=$runner, priority=$priority, " +
        s"resourceFulfilled=$resourceFulfilled, completed=$completed, " +
        s"caughtException=$caughtException, scheduleTime=${scheduleTime / 1e9}s)"
  }

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
 *   - Acquiring resources before runner execution through the ResourcePool
 *   - Tracking resource usage and releasing resources after task completion
 *   - Supporting priority-based runner ordering via PriorityBlockingQueue
 *   - Managing resource lifecycles including immediate and deferred release strategies
 *
 * AsyncRunners that cannot acquire sufficient resources within timeout are bypassed during
 * execution and re-queued to prevent starvation. The executor works exclusively with AsyncRunner
 * instances and their corresponding RapidsFutureTask wrappers.
 *
 * Resource release timing can be controlled by the implementation of the AsyncRunner.
 * Runners are able to provide callbacks for resource lifecycle management and supports both
 * immediate and deferred resource release patterns based on the corresponding AsyncResult
 * implementation.
 *
 * @param mgr the ResourcePool for managing resource acquisition and release
 * @param timeoutMs timeout in milliseconds for resource acquisition
 * @param corePoolSize the core number of threads in the pool
 * @param maximumPoolSize the maximum number of threads in the pool
 * @param workQueue the queue for holding tasks before execution
 * @param threadFactory the factory for creating new threads
 * @param keepAliveTime the time to keep idle threads alive
 */
class ResourceBoundedThreadExecutor(mgr: ResourcePool,
    timeoutMs: Long,
    corePoolSize: Int,
    maximumPoolSize: Int,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    keepAliveTime: Long = 100L) extends ThreadPoolExecutor(corePoolSize,
  maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, workQueue, threadFactory) with Logging {

  logInfo(s"Creating ResourceBoundedThreadExecutor with resourcePool: ${mgr.toString}, " +
      s"corePoolSize: $corePoolSize, maximumPoolSize: $maximumPoolSize, " +
      s"waitResourceTimeoutMs: $timeoutMs")

  override def submit[T](fn: Callable[T]): Future[T] = {
    fn match {
      // quick path for UnboundedAsyncRunner that does not need resource management
      case runner: UnboundedAsyncRunner[_] =>
        super.submit(runner)
      case runner: AsyncRunner[_] =>
        //register the resource release callback
        runner.releaseResourceCallback = () => mgr.releaseResource(runner)
        super.submit(runner)
      case f =>
        throw new IllegalArgumentException(
          "ResourceBoundedThreadExecutor only accepts AsyncRunner," +
              s" but got: ${f.getClass.getName}")
    }
  }

  // This method is only for the extensions of RapidsFutureTask.
  override def submit[T](r: Runnable, result: T): Future[T] = {
    r match {
      // quick path for UnboundedAsyncRunner that does not need resource management
      case ft: RapidsFutureTask[_] if ft.runner.isInstanceOf[UnboundedAsyncRunner[_]] =>
        super.submit(ft, null.asInstanceOf[T])
      case ft: RapidsFutureTask[_] =>
        //register the resource release callback
        ft.runner.releaseResourceCallback = () => mgr.releaseResource(ft.runner)
        super.submit(ft, null.asInstanceOf[T])
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
        mgr.acquireResource(fut.runner, timeoutMs) match {
          case s: AcquireSuccessful =>
            fut.resourceAcquired(s.elapsedTime)
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
        // the task queue.
        if (t == null && !fut.isCompleted) {
          fut.scheduleTime += timeoutMs * 1000000L
          val reAddSuccess = workQueue.add(fut)
          require(reAddSuccess, s"Failed to re-add $fut to the work queue after execution")
          logDebug(s"Re-add timeout runner: $fut")
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
      waitResourceTimeoutMs: Long): ResourceBoundedThreadExecutor = {
    val taskQueue = new PriorityBlockingQueue(10000, new RapidsFutureTaskComparator[T])
    val threadFactory: ThreadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(name)
        .build()

    new ResourceBoundedThreadExecutor(pool,
      waitResourceTimeoutMs,
      corePoolSize = maxThreadNumber,
      maximumPoolSize = maxThreadNumber,
      workQueue = taskQueue.asInstanceOf[BlockingQueue[Runnable]],
      threadFactory = threadFactory)
  }
}
