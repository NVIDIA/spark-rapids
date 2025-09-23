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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.TaskCompletionListener

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

  override def run(): Unit = {
    runner.state match {
      case Running =>
        // Pass the schedule time to the task metrics builder
        runner.metricsBuilder.setScheduleTimeMs(scheduleTime)
        scheduleTime = 0L
        super.run()
      // Throw the ScheduleFailed exception within the scope of `FutureTask.run`, so that
      // the exception can be properly recorded and propagated to the caller of `get()`.
      case ScheduleFailed(ex: Throwable) =>
        // Trick: register a pre-hook to let `AsyncRunner.call` throw the exception
        runner.addPreHook(() => throw ex)
        super.run()
      // Expected states to bypass the execution
      case Pending =>
      case _ =>
        throw new IllegalStateException(s"should NOT reach this line: $runner")
    }
  }

  override def setException(e: Throwable): Unit = {
    runner.state = ExecFailed(e)
    super.setException(e)
  }

  override def toString: String = {
    s"RapidsFutureTask(runner=$runner, scheduleTime=${scheduleTime / 1e6}ms)"
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
    o2.runner.priority.compareTo(o1.runner.priority)
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
      case runner: AsyncRunner[_] =>
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
      case ft: RapidsFutureTask[_] =>
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
    // Only RapidsFutureTask is expected here
    val futTask: RapidsFutureTask[_] = r match {
      case fut: RapidsFutureTask[_] => fut
      case _ => throw new RuntimeException(s"Unexpected runnable: ${r.getClass.getName}")
    }
    val rr = futTask.runner

    // Update the runner state
    rr.state match {
      // Normally the runner should be in Init state before being executed:
      // either the first time or re-scheduled after a timeout
      case init: Init =>
        rr.state = Pending
        // register all the callbacks for the first time
        if (init.firstTime) {
          setupReleaseCallback(futTask)
        }
      // It is possible that the runner has already been cancelled from the Spark task side
      // before being polled from the work queue. In this case, we just mark the state
      // as ScheduleFailed and skip the execution.
      case Cancelled =>
        rr.state = ScheduleFailed(new IllegalStateException("cancelled"))
        logWarning(s"Cancelled before schedule: $rr")
      // Update the state to ScheduleFailed, but do not throw exception here to avoid
      // interrupting the ThreadWorker. Since the exception thrown here will not be
      // caught by the main loop of ThreadWorker. (same as below)
      case _ =>
        rr.state = ScheduleFailed(new IllegalStateException("Unexpected state"))
        logError(s"Unexpected state before schedule: $rr")
    }

    // Resource acquisition
    if (rr.state == Pending) {
      // Talk to the ResourcePool to acquire resource for the runner
      val acqStatus = mgr.acquireResource(rr, timeoutMs)
      // Check if the runner has been cancelled during the resource acquisition
      if (rr.state == Cancelled) {
        rr.state = ScheduleFailed(new IllegalStateException("cancelled"))
        logWarning(s"Cancelled during resource acquisition: $rr")
      } else {
        // The runner should still be in Pending state if not being cancelled
        require(rr.state == Pending, s"Runner $rr is expected to be in Pending state")
        // Update the runner state based on the acquisition result
        acqStatus match {
          // Proceed to execution: Pending -> Running
          case s: AcquireSuccessful =>
            rr.state = Running
            futTask.scheduleTime += s.elapsedTime
          // Fail the scheduling: Pending -> ScheduleFailed
          case AcquireExcepted(ex) =>
            rr.state = ScheduleFailed(ex)
            logError(s"$ex [$rr]")
          // Bypass the execution: Pending -> Pending
          case AcquireFailed =>
        }
      }
    }
  }

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    // Only RapidsFutureTask is expected here
    val futTask: RapidsFutureTask[_] = r match {
      case fut: RapidsFutureTask[_] => fut
      case _ => throw new RuntimeException(s"Unexpected runnable: ${r.getClass.getName}")
    }

    // Throw the unexpected exception if exists, since the FutureTask should have caught
    // and recorded the exception internally.
    if (t != null) {
      futTask.runner.state = ExecFailed(t)
      // Also try to fail the Spark task which launched this runner.
      futTask.runner.sparkTaskContext.foreach { ctx =>
        TrampolineUtil.markTaskFailed(ctx, t)
      }
      logError(s"Uncaught exception from $futTask: ${t.getMessage}", t)
      throw new RuntimeException(t)
    }

    val rr = futTask.runner
    // Update the runner state
    rr.state match {
      // Running -> Completed
      case Running => rr.state = Completed
      // ScheduleFailed -> remains ScheduleFailed
      // ExecFailed -> remains ExecFailed
      // Cancelled -> remains Cancelled
      case ExecFailed(_) | ScheduleFailed(_) | Pending =>
      case _ =>
        throw new IllegalStateException(s"Unexpected state: $rr")
    }
    // Handle eager release cases
    if (rr.isHoldingResource) {
      val needReleaseNow: Boolean = rr.state match {
        case ExecFailed(_) => true
        case Completed =>
          rr.result match {
            case None =>
              throw new IllegalStateException(s"In Completed State but NO Result: $rr")
            case Some(_: FastReleaseResult[_]) => true
            case _ => false
          }
        // Only runners in `Completed` and `ExecFailed` may hold resource
        case state =>
          throw new IllegalStateException(s"State($state) should NOT hold Resource: $rr")
      }
      if (needReleaseNow) {
        rr.releaseResourceCallback()
      }
    } else if (rr.state == Pending) {
      // Requeue runners which failed to acquire resource and bypassed the execution.
      futTask.scheduleTime += timeoutMs * 1000000L
      rr.state = Init(firstTime = false) // reset to Init state for re-scheduling
      val reAddSuccess = workQueue.add(futTask)
      require(reAddSuccess, s"Failed to re-add $futTask to the work queue after execution")
      logDebug(s"Re-add timeout runner: $futTask")
    }
  }

  private def setupReleaseCallback[T](fut: RapidsFutureTask[T]): Unit = {
    val runner = fut.runner
    // Bind the resource release callback to the AsyncRunner
    runner.releaseResourceCallback = () => {
      require(runner.isHoldingResource, s"call relCallback without holding resource: $runner")
      require(runner.state == Cancelled ||
          runner.state == Completed ||
          runner.state.isInstanceOf[ExecFailed],
        s"Runner $runner is expected to be: Cancelled | Completed | ExecFailed")
      // Modify the states of HostMemoryPool
      mgr.releaseResource(runner)
      // Finalize the runner state
      runner.state match {
        case Completed => // Completed -> Closed
          runner.state = Closed(None)
        case ExecFailed(ex) => // ExecFailed -> Closed
          runner.state = Closed(Some(ex))
        case Cancelled => // Cancelled -> Closed
          runner.state = Closed(Some(new IllegalStateException("cancelled")))
        case _ =>
          throw new IllegalStateException(s"Should NOT reach here: $runner")
      }
    }
    // Register a callback to ensure the AsyncRunner being fully closed when the Spark task
    // completes, regardless of whether the task succeeds or fails. This is a safety net to
    // prevent resource leaks in case that Spark task is killed or fails unexpectedly.
    runner.sparkTaskContext.foreach { ctx =>
      ctx.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          // 1. If the runner is still running, we try to cancel it first
          if (fut.runner.state == Running) {
            fut.cancel(true)
          }
          // 2. Mark the runner as Cancelled
          fut.runner.state = Cancelled
          // 3. If the runner is still holding resource, we release it
          if (fut.runner.isHoldingResource) {
            fut.runner.releaseResourceCallback()
          }
        }
      })
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
