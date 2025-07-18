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

class RapidsFutureTask[T](val task: AsyncTask[T]) extends FutureTask[AsyncResult[T]](task) {
  private[async] var priority: Float = task.priority
  private var heldResource: Boolean = false
  private var completed: Boolean = false
  private var caughtException: Boolean = false

  override def run(): Unit = if (!caughtException) {
    require(!completed, "Task has already been completed")
    if (heldResource) {
      super.run()
      completed = true
    }
  }

  def holdResource(): Unit = {
    require(!completed, "Task has already been completed")
    require(!heldResource, "Cannot hold resource that is already held")
    heldResource = true
  }

  def releaseResource(): Unit = {
    require(heldResource, "Cannot release resource that was not held")
    heldResource = false
  }

  def adjustPriority(delta: Float): Float = {
    require(!completed, "Task has already been completed")
    priority += delta
    priority
  }

  override def setException(e: Throwable): Unit = {
    caughtException = true
    completed = true
    super.setException(e)
  }

  def isHeldResource: Boolean = heldResource

  def isCompleted: Boolean = completed
}

class RapidsFutureTaskComparator[T] extends java.util.Comparator[RapidsFutureTask[T]] {
  override def compare(o1: RapidsFutureTask[T], o2: RapidsFutureTask[T]): Int = {
    (-o1.priority).compareTo(-o2.priority)
  }
}

class ResourceBoundedThreadExecutor(mgr: ResourcePool,
    waitResourceTimeoutMs: Long,
    retryPriorAdjust: Float,
    corePoolSize: Int,
    maximumPoolSize: Int,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    keepAliveTime: Long = 100L) extends ThreadPoolExecutor(corePoolSize,
  maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, workQueue, threadFactory) with Logging {

  logWarning(s"Creating ResourceBoundedThreadExecutor with resourcePool: ${mgr.toString}, " +
    s"corePoolSize: $corePoolSize, maximumPoolSize: $maximumPoolSize, " +
    s"waitResourceTimeoutMs: $waitResourceTimeoutMs, retryPriorityAdjustment: $retryPriorAdjust")

  override def submit[T](fn: Callable[T]): Future[T] = {
    fn match {
      case task: AsyncTask[_] =>
        //register the resource release callback
        task.releaseResourceCallback = () => mgr.releaseResource(task)
        super.submit(task)
      case f =>
        throw new IllegalArgumentException(
          s"ResourceBoundedThreadExecutor only accepts AsyncTask, but got: ${f.getClass.getName}")
    }
  }

  // This method is only for the extensions of RapidsFutureTask.
  override def submit[T](r: Runnable, result: T): Future[T] = {
    r match {
      case futTask: RapidsFutureTask[_] =>
        //register the resource release callback
        futTask.task.releaseResourceCallback = () => mgr.releaseResource(futTask.task)
        super.submit(futTask, null.asInstanceOf[T])
      case _ =>
        throw new UnsupportedOperationException("only accepts AsyncTask or RapidsFutureTask")
    }
  }

  override def submit(r: Runnable): Future[_] = {
    throw new UnsupportedOperationException("only accepts AsyncTask or RapidsFutureTask")
  }

  override protected def newTaskFor[T](fn: Callable[T]): RunnableFuture[T] = {
    fn match {
      case task: AsyncTask[_] =>
        new RapidsFutureTask(task)
      case f =>
        throw new RuntimeException(s"Unexpected functor: ${f.getClass.getName}")
    }
  }

  override protected def newTaskFor[T](r: Runnable, result: T): RunnableFuture[T] = {
    r match {
      case futTask: RapidsFutureTask[_] =>
        futTask.asInstanceOf[RunnableFuture[T]]
      case task: AsyncTask[_] =>
        new RapidsFutureTask(task).asInstanceOf[RunnableFuture[T]]
      case f =>
        throw new RuntimeException(s"Unexpected runnable: ${f.getClass.getName}")
    }
  }

  override def beforeExecute(t: Thread, r: Runnable): Unit = {
    r match {
      case fut: RapidsFutureTask[_] =>
        mgr.acquireResource(fut.task, waitResourceTimeoutMs) match {
          case AcquireSuccessful =>
            fut.holdResource()
          case AcquireFailed =>
            // bypass the execution via not holding the resource
          case AcquireExcepted(exception) =>
            logError(s"Invalid resource request for task ${fut.task}: ${exception.getMessage}")
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
        if (fut.isHeldResource) {
          // Release intermediately if the task supports that or if an exception occurred.
          if (!fut.task.holdResourceAfterCompletion || t != null) {
            fut.task.releaseResourceCallback()
            fut.releaseResource()
          } else {
            fut.task.holdResource = true
          }
        }
        // If the task failed to acquire enough resource, we bypass the execution and re-add it to
        // the task queue with a priority penalty to avoid starvation.
        if (t == null && !fut.isCompleted) {
          fut.adjustPriority(retryPriorAdjust)
          require(workQueue.add(fut),
            s"Failed to re-add task ${fut.task} to the work queue after execution")
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
      retryPriorityAdjust: Float = 0.0f): ResourceBoundedThreadExecutor = {
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
