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

import java.util.concurrent.{Callable, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.function.LongUnaryOperator

import scala.collection.mutable

import ai.rapids.cudf.{HostMemoryAllocator, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{HostAlloc, RapidsConf}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.jni.TaskPriority

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.{bytesToString => bToStr}

/**
 * AsyncRunResource represents the resource requirement for an AsyncRunner.
 */
trait AsyncRunResource {
  /**
   * Interface to get the size of the resource in bytes. We assume all resources can be
   * quantified in bytes for simplicity: host memory and potentially GPU memory.
   */
  def sizeInBytes: Long
}

/**
 * HostResource represents host memory requirement for tasks scheduled by HostMemoryPool.
 */
case class HostResource(sizeInBytes: Long) extends AsyncRunResource {
  override def toString: String = s"HostResource(${bToStr(sizeInBytes)})"
}

object HostResource {
  lazy val Empty: HostResource = HostResource(0L) // singleton for empty host resource
}

/**
 * Result wrapper for AsyncRunner execution that carries the output data and optional resource
 * release callback for deferred resource management.
 */
trait AsyncResult[T] {
  def data: T

  /**
   * Metrics associated with the task execution, such as scheduling and execution time.
   */
  def metrics: AsyncMetrics
}

case class AsyncResultImpl[T](data: T, metrics: AsyncMetrics) extends AsyncResult[T]

object AsyncResult {
  def apply[T](data: T, metrics: AsyncMetrics): AsyncResult[T] = {
    AsyncResultImpl(data, metrics)
  }
}

case class AsyncMetrics(scheduleTimeMs: Long, executionTimeMs: Long)

class AsyncMetricsBuilder {
  private var scheduleTimeMs: Long = 0L
  private var executionTimeMs: Long = 0L

  def setScheduleTimeMs(time: Long): AsyncMetricsBuilder = {
    this.scheduleTimeMs = time
    this
  }

  def setExecutionTimeMs(time: Long): AsyncMetricsBuilder = {
    this.executionTimeMs = time
    this
  }

  def build(): AsyncMetrics = {
    AsyncMetrics(scheduleTimeMs, executionTimeMs)
  }
}

/**
 * States of AsyncRunner during its lifecycle
 *
 * - Init: The initial state when the runner is polled from the worker queue. The firstTime flag
 * indicates if it's the first time the runner is being scheduled.
 * - Pending: The runner is waiting for the required resource.
 * - ScheduleFailed: The runner failed to be scheduled due to resource acquisition failure.
 * - Running: The runner is currently executing.
 * - Completed: The runner has completed execution successfully, but still holds the resource.
 * - ExecFailed: The runner execution failed with an exception, but still holds the resource.
 * - Closed: The runner has been closed and released the resource. If an exception is provided,
 * it indicates a failed workload.
 * - Cancelled: The runner has been cancelled from outside (usually due to Spark Task failure,
 * interruption)
 */
sealed trait AsyncRunnerState

case class Init(firstTime: Boolean) extends AsyncRunnerState

case object Pending extends AsyncRunnerState

case class ScheduleFailed(exception: Throwable) extends AsyncRunnerState

case object Running extends AsyncRunnerState

case object Completed extends AsyncRunnerState

case class ExecFailed(exception: Throwable) extends AsyncRunnerState

case class Closed(exception: Option[Throwable]) extends AsyncRunnerState

case object Cancelled extends AsyncRunnerState

/**
 * The AsyncRunner interface represents a resource-aware runner that can be scheduled by
 * ResourceBoundedThreadExecutor. Runners define their resource requirements, execution priority,
 * and lifecycle management through state transitions.
 *
 * AsyncRunner provides hooks for resource lifecycle management including:
 * - Resource acquisition through the ResourcePool before execution
 * - Optional piece-by-piece resource recycling during execution via tryFree
 * - Automatic resource release upon completion (or failure) through `ResourcePool.release`
 *
 * The runner transitions through several states (Init -> Pending -> Running -> Completed/Failed
 * -> Closed) with corresponding resource allocation and deallocation at each stage.
 */
trait AsyncRunner[T] extends Callable[AsyncResult[T]] with AutoCloseable {
  /**
   * Resource required by the runner, such as host memory or GPU semaphore.
   */
  def resource: AsyncRunResource

  /**
   * Priority of the async runner, higher value means higher priority.
   */
  def priority: Long

  /**
   * Optional method which attempts to free up resources that have finished being used.
   * This method is for recycling resources piece by piece, as soon as the piece ends its usage.
   * In addition, it is a [mutable] operation, the returned resource should be deducted from the
   * current resource requirement of the runner. If no resources can be freed at the moment,
   * return None.
   */
  protected[async] def tryFree(byForce: Boolean): (Long, Long)

  /**
   * The abstract method defines the actual execution logic, which should be implemented by the
   * subclass.
   */
  protected def callImpl(): T

  /**
   * Builds the result of the runner execution. This method can be overridden by subclasses to
   * customize the result format other than AsyncResultImpl to carry additional information.
   */
  protected def buildResult(resultData: T, metrics: AsyncMetrics): AsyncResult[T] = {
    AsyncResult(resultData, metrics)
  }

  // Close method of AsyncRunner should be idempotent
  override def close(): Unit = {
    // Ensure we do NOT block multiple closings for the same runner
    if (!closeStarted.compareAndSet(false, true)) {
      return
    }
    withStateLock(releaseAnyway = true) { rr =>
      rr.getState match {
        case Cancelled | Closed(_) | Completed | ExecFailed(_) =>
        // terminated states, safe to close
        case state =>
          throw new IllegalStateException(s"Unexpected runner state: $state of $this")
      }
      Option(poolPtr).foreach(_.finishUpRunner(this))
    }
  }

  def call(): AsyncResult[T] = {
    require(result == null, s"AsyncRunner.call() should only be called once: $this")

    val startTime = System.nanoTime()
    val resultData = try {
      beforeExecuteHooks.foreach { hook => hook() }
      callImpl()
    } finally {
      afterExecuteHooks.foreach { hook => hook() }
    }
    metricsBuilder.setExecutionTimeMs(System.nanoTime() - startTime)

    result = buildResult(resultData, metricsBuilder.build())
    result
  }

  private val beforeExecuteHooks = mutable.ArrayBuffer.empty[() => Unit]
  private val afterExecuteHooks = mutable.ArrayBuffer.empty[() => Unit]

  // Add hook to be executed right before the task execution.
  def addPreHook(hook: () => Unit): Unit = beforeExecuteHooks += hook

  // Add hook to be executed right after the task execution.
  def addPostHook(hook: () => Unit): Unit = afterExecuteHooks += hook

  /**
   * This method is called when the required resource has been just acquired from pool.
   * It can be overridden by subclasses to perform actions right after the acquisition.
   */
  protected[async] def onStart(pool: ResourcePool): Unit = {
    require(Option(poolPtr).isEmpty, "The resource has already been acquired")
    poolPtr = pool
  }

  /**
   * This method is called when the runner is being closed and its acquired resource is about to be
   * released back into the pool. It can be overridden by subclasses to perform cleanup actions.
   */
  protected[async] def onClose(): Unit = {
    poolPtr = null
  }

  // Cache the result for 2 purposes:
  // 1. Ensure call() is only called once
  // 2. Facilitate accessing the status of AsyncRunner inside ThreadExecutor after execution
  @volatile private[async] var result: AsyncResult[T] = _

  private[async] lazy val metricsBuilder = new AsyncMetricsBuilder

  /**
   * AsyncRunner is not necessarily tied to a Spark task, despite it is usually the case.
   * sparkTaskId is None if the runner is not associated with any Spark task. Otherwise, it
   * carries the corresponding Spark task ID.
   */
  def sparkTaskContext: Option[TaskContext] = None

  // Pointer to the resource pool from which the resource is acquired.
  @volatile protected var poolPtr: ResourcePool = _

  // Atomic flag to ensure close() is only executed once.
  private[async] val closeStarted: AtomicBoolean = new AtomicBoolean(false)

  /**
   * Get the current state of the AsyncRunner, nonblocking method.
   */
  private[async] def getState: AsyncRunnerState = state

  /**
   * Set the state of the AsyncRunner, blocking method.
   * The state transition is only recommended to be done inside ResourcePool, in order to
   * maintain the consistency between the state and resource allocation.
   */
  private[async] def setState(newState: AsyncRunnerState): Unit = {
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")
    if (newState != state) {
      state = newState
    }
  }

  @volatile private var state: AsyncRunnerState = Init(firstTime = true)

  def isHoldingStateLock: Boolean = stateLock.isHeldByCurrentThread

  /**
   * Execute a function while holding the runner's state lock to prevent concurrent state changes.
   * By default, the lock is acquired before executing fn and released after fn returns if it is
   * not already held by the current thread.
   *
   * Options:
   * - holdAnyway: if true, the lock will be kept when fn returns (caller remains holding the
   *   lock), which is useful for multi-steps state transitions where follow-up work must run
   *   under the same critical section (e.g., acquire -> setState -> onStart -> allocate).
   * - releaseAnyway: when the lock is already held by the current thread (nested call), forces
   *   unlocking after fn returns. This method works as the counterpart of holdAnyway to ensure
   *   the lock is released in cleanup flows.
   */
  def withStateLock[R](
      holdAnyway: Boolean = false,
      releaseAnyway: Boolean = false)(fn: AsyncRunner[T] => R): R = {
    if (stateLock.isHeldByCurrentThread) {
      try {
        fn(this)
      } finally {
        if (releaseAnyway) {
          stateLock.unlock()
        }
      }
    } else {
      stateLock.lock()
      var holdStateLock = holdAnyway
      try {
        fn(this)
      } catch {
        case t: Throwable =>
          holdStateLock = false
          throw t
      } finally {
        if (!holdStateLock) {
          stateLock.unlock()
        }
      }
    }
  }

  protected val stateLock = new ReentrantLock()

  // Unique ID for the runner, mainly for logging and tracking purpose.
  private[async] val runnerId: Long = AsyncRunner.nextRunnerId()

  override def toString: String = {
    val tid = sparkTaskContext.map(_.taskAttemptId()).getOrElse(-1L)
    s"AsyncRunner(state=$state, runnerId=$runnerId, " +
        s"taskId=$tid, resource=$resource, priority=$priority)"
  }
}

/**
 * An AsyncRunner that requires no resource limits and has the highest scheduling priority.
 * Useful for lightweight jobs that should execute immediately without resource constraints.
 */
abstract class UnboundedAsyncRunner[T] extends AsyncRunner[T] {
  // Unbounded runners do not have a resource limit.
  override val resource: AsyncRunResource = HostResource.Empty

  // Unbounded runners have the highest priority.
  override val priority: Long = Long.MaxValue

  override protected[async] def tryFree(byForce: Boolean): (Long, Long) = {
    (0L, 0L)
  }
}

/**
 * The base runner class for memory-bound runners that require a specific amount of host memory.
 * This type of runner is supposed to be scheduled by the ResourceBoundedThreadPoolExecutor
 * with HostMemoryPool.
 *
 * The runner priority is derived from the TaskPriority if it is associated with a Spark task,
 * which makes runners from the same Spark task to be scheduled one after another. It is important
 * to avoid starvation of runners of the same Spark task which might depend on each other.
 */
abstract class MemoryBoundedAsyncRunner[T] extends AsyncRunner[T]
    with HostMemoryAllocator with Logging {

  // The memory requirement in bytes for the memory-bound runner.
  val requiredMemoryBytes: Long

  // The base memory allocator from which the runner actually allocates memory.
  val baseMemoryAllocator: HostMemoryAllocator

  // The maximum retry attempts waiting for all allocated host memory buffers to be closed
  protected val maxRetriesOnClose: Int

  final override def resource: AsyncRunResource = {
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")
    if (localPool < 0) {
      require(requiredMemoryBytes > 0, s"requiredMemoryBytes($requiredMemoryBytes) should > 0")
      localPool = requiredMemoryBytes
    }
    HostResource(localPool)
  }

  override protected[async] def tryFree(byForce: Boolean): (Long, Long) = {
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")
    if (getState == Running) {
      require(!byForce, s"Cannot force free memory when the runner is running: $this")
      (0L, localPool)
    } else {
      val freeable = if (byForce) {
        localPool
      } else {
        localPool - usedMem.get()
      }
      localPool -= freeable
      (freeable, localPool)
    }
  }

  override def priority: Long = {
    sparkTaskContext match {
      case Some(ctx) => TaskPriority.getTaskPriority(ctx.taskAttemptId())
      case None => 0L
    }
  }

  override protected[async] def onStart(pool: ResourcePool): Unit = {
    super.onStart(pool)
    // Initialize the local memory pool when the resource is acquired
    require(localPool == requiredMemoryBytes,
      s"localPool(${bToStr(localPool)}) != initial value(${bToStr(requiredMemoryBytes)}")
    usedMem.set(0L)
    peakUsedMem = 0L
  }

  // AsyncRunner.close() operates on the virtual memory, while MemoryBoundedAsyncRunner is backed
  // by actual host memory allocations, so the close operation cannot be achieved until all host
  // buffers are actually closed.
  //
  // Also, we cannot close host buffers explicitly here as this would cause double-free errors,
  // so we have no choice but waiting for all allocated buffers being closed.
  override protected[async] def onClose(): Unit = {
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")
    // Get the maximum retry attempts from configuration, default to 4 (4 * 30s = 120s)
    val waitIntervalSeconds = 30L // fixed wait interval
    var retryCount = 0

    // wait until all allocated host memory buffers are actually closed
    while (usedMem.get() > 0L) {
      // Wait for buffer close signal. Here we use a timeout wait to enable logging if the
      // runner is blocked for too long.
      bufCloseCond.await(waitIntervalSeconds, TimeUnit.SECONDS)
      retryCount += 1
      val maxRetries = maxRetriesOnClose
      if (usedMem.get() > 0L) {
        if (retryCount >= maxRetries) {
          val usedBytes = usedMem.get()
          throw new IllegalStateException(
            s"Exceeded max retry attempts ($maxRetries) waiting for host memory buffers " +
                s"to be closed for $this. Still holding ${bToStr(usedBytes)} of unreleased " +
                s"memory. This likely indicates leaked buffer references. " +
                s"Adjust ${RapidsConf.MULTITHREAD_READ_MAX_BUFFER_CLOSE_WAIT_RETRIES.key} to " +
                s"change the retry threshold.")
        }
        logWarning(s"$this is blocking on waiting buffers to be closed, " +
            s"retry $retryCount/$maxRetries (interval: ${waitIntervalSeconds}s)")
      }
    }
    super.onClose()
  }

  override def toString: String = {
    val tid = sparkTaskContext.map(_.taskAttemptId()).getOrElse(-1L)
    val initVal = bToStr(requiredMemoryBytes)
    lazy val usedVal = usedMem.get()
    val s = getState
    s match {
      case Running =>
        val used = bToStr(usedVal)
        s"MemBndRnnr(state=$s, runnerId=$runnerId, taskId=$tid, Budget($initVal/used $used))"
      case Pending | Init(_) =>
        s"MemBndRnnr(state=$s, runnerId=$runnerId, taskId=$tid, Budget($initVal))"
      case _ if localPool > 0 =>
        val used = bToStr(usedVal)
        s"MemBndRnnr(state=$s, runnerId=$runnerId, taskId=$tid, " +
            s"LocalPool(${bToStr(localPool)}/used $used/init $initVal))"
      case _ if usedVal > 0 =>
        val used = bToStr(usedVal)
        s"MemBndRnnr(state=$s, runnerId=$runnerId, taskId=$tid, Budget($initVal/used $used))"
      case _ =>
        val pk = bToStr(peakUsedMem)
        s"MemBndRnnr(state=$s, runnerId=$runnerId, taskId=$tid, Budget($initVal/peak $pk))"
    }
  }

  override def allocate(size: Long, preferPinned: Boolean): HostMemoryBuffer = {
    require(getState == Running, s"Memory allocation is only allowed in Running state: $this")
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")

    // Check and update the used memory atomically
    var memToBrw = 0L
    var newUsed = usedMem.updateAndGet { curUsed: Long =>
      val newUsed = curUsed + size
      memToBrw = newUsed - localPool
      newUsed min localPool
    }
    // If the local pool is insufficient, try to borrow from the global pool
    if (memToBrw > 0) {
      logWarning(
        s"[runnerID=$runnerId] LocalMemPool ${bToStr(localPool)}(used ${bToStr(newUsed)}) " +
            s"is NOT enough for the ALLOC(${bToStr(size)}): try to borrow ${bToStr(memToBrw)}")
      // Blocking call to borrow memory from the global pool
      poolPtr.asInstanceOf[HostMemoryPool].borrowMemory(memToBrw)
      localPool += memToBrw
      newUsed = usedMem.addAndGet(memToBrw)
    }
    if (newUsed > peakUsedMem) {
      peakUsedMem = newUsed
    }
    // Call the base allocator to allocate the actual buffer
    val buf = withRetryNoSplit[HostMemoryBuffer] {
      baseMemoryAllocator.allocate(size, preferPinned)
    }
    // Register a close handler to return the memory back either to the local or global pool
    HostAlloc.addEventHandler(buf, new OnCloseHandler(size, this))
    buf
  }

  override def allocate(size: Long): HostMemoryBuffer = {
    allocate(size, preferPinned = true)
  }

  private class OnCloseHandler(
      bufferSize: Long,
      r: AsyncRunner[T]) extends MemoryBuffer.EventHandler {

    logDebug(s"[OnCloseHandler Created] bufferSize=${bToStr(bufferSize)} for $r")

    def onClosed(refCount: Int): Unit = if (refCount == 0) {
      // Return the memory back to the local pool and signal waiting onClose thread if exists
      r.withStateLock[Unit]() { _ =>
        // TODO: check if the returned memory satisfies the ongoing borrow requests
        usedMem.addAndGet(-bufferSize)
        bufCloseCond.signal() // awaken onClose waiting thread if exists
        logDebug(s"[OnCloseHandler Closed] bufferSize=${bToStr(bufferSize)} for $r")
        // Instantly release memory back to the global pool unless:
        // 1. the runner is still running
        // 2. there is a close operation ongoing (highly likely waiting for the bufCloseCond)
        if (getState != Running && !closeStarted.get()) {
          poolPtr.release(r, forcefully = false)
        }
      }
    }
  }

  // The capacity of the local memory pool for the runner, which can grow and shrink over time.
  // 1) -1 means uninitialized.
  // 2) should be accessed and modified only when holding the state lock.
  private var localPool: Long = -1
  // Memory currently allocated from the runner, localPool - usedMem = available memory.
  // Wrapped in AtomicLong because it is modified by the OnCloseHandler as well.
  private val usedMem = new AtomicLong(0L)
  // peak memory allocated from the runner
  private var peakUsedMem: Long = 0L

  // condition variable to signal HostMemoryBuffer close events
  private val bufCloseCond = stateLock.newCondition()
}

/**
 * A simple AsyncRunner implementation that wraps a function to be executed asynchronously.
 * NOTE: This class is only used in tests currently, removing the forTest suffix if it is used
 * in production code in the future.
 */
private[async] class SimpleAsyncFunctor[T](
    var res: AsyncRunResource,
    override val priority: Long,
    functor: () => T) extends AsyncRunner[T] {

  require(res.sizeInBytes >= 0, s"Resource size must be non-negative, got: ${res.sizeInBytes}")

  override def callImpl(): T = functor()

  override def resource: AsyncRunResource = res

  override protected[async] def tryFree(byForce: Boolean): (Long, Long) = {
    require(isHoldingStateLock, s"The caller must hold the state lock: $this")
    val freed = res.sizeInBytes
    res = HostResource.Empty
    (freed, 0L)
  }
}

private[async] class UnboundedAsyncFunctor[T](fn: () => T) extends UnboundedAsyncRunner[T] {
  override protected def callImpl(): T = fn()
}

object AsyncRunner {

  // Create a CPU-bound AsyncRunner with specified memory requirement and priority.
  // NOTE: The API is only used in tests currently
  def newCpuTask[T](fn: () => T,
      memoryBytes: Long,
      priority: Long = 0L): AsyncRunner[T] = {
    require(priority >= 0, s"Priority must be non-negative, got: $priority")
    val p = hostMemoryPenalty(memoryBytes, priority)
    new SimpleAsyncFunctor[T](HostResource(memoryBytes), p, fn)
  }

  // Create a light-weight unbounded AsyncRunner with the highest priority.
  // NOTE: The API is only used in tests currently
  def newUnboundedTask[T](fn: () => T): AsyncRunner[T] = {
    new UnboundedAsyncFunctor(fn)
  }

  // Atomically increase the global runner ID with overflow protection
  private def nextRunnerId(): Long = globalRunnerId.getAndUpdate(idUpdateFn)

  private lazy val idUpdateFn = new LongUnaryOperator {
    override def applyAsLong(operand: Long): Long = {
      if (operand == Long.MaxValue) 0L else operand + 1L
    }
  }

  private lazy val globalRunnerId = new AtomicLong(0)

  // Adjust the priority based on the memory overhead to minimal the potential clogging:
  // lightweight tasks should have higher priority.
  private def hostMemoryPenalty(memoryBytes: Long, priority: Long): Long = {
    require(memoryBytes >= 0, s"Memory bytes must be non-negative, got: $memoryBytes")
    priority - (memoryBytes >> 10)
  }
}
