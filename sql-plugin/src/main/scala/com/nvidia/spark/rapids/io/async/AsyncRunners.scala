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

import java.util.concurrent.Callable
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.function.LongUnaryOperator

import org.apache.spark.TaskContext

/**
 * Marker trait for resources required by AsyncRunners.
 */
sealed trait AsyncRunResource

/**
 * HostResource represents host memory resource requirement for CPU-bound tasks.
 */
case class HostResource(
    hostMemoryBytes: Long,
    groupedHostMemoryBytes: Option[Long] = None) extends AsyncRunResource

/**
 * DeviceResource is a marker object for GPU resources, no additional fields needed.
 */
object DeviceResource extends AsyncRunResource

object AsyncRunResource {
  def newCpuResource(hostMemoryBytes: Long,
      groupedMemoryBytes: Option[Long] = None): AsyncRunResource = {
    HostResource(hostMemoryBytes, groupedHostMemoryBytes = groupedMemoryBytes)
  }

  def newGpuResource(): AsyncRunResource = DeviceResource
}

/**
 * Result wrapper for AsyncRunner execution that carries the output data and optional resource
 * release callback for deferred resource management.
 */
trait AsyncResult[T] extends AutoCloseable {
  val data: T

  /**
   * Metrics associated with the task execution, such as scheduling and execution time.
   */
  val metrics: AsyncMetrics

  /**
   * Indicates whether the result holds a release hook to be executed upon closure.
   */
  val releaseHook: Option[() => Unit] = None

  /**
   * Trigger release hook if it exists. Do not try to close the data itself, because
   * the lifecycle of the data is managed by the caller.
   */
  override def close(): Unit = {
    releaseHook.foreach(callback => callback())
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
 * Basic implementation of AsyncResult for runners that release resources immediately upon
 * completion. This result type is suitable for short-lived operations that don't need to retain
 * resources after execution. FastReleaseResult has no release hook and resources are freed
 * automatically as soon as the runner finishes.
 */
class FastReleaseResult[T](override val data: T,
    override val metrics: AsyncMetrics) extends AsyncResult[T]

/**
 * Basic implementation of AsyncResult that allows resources to be held after runner completion
 * and released later via an explicit callback. This result type is suitable for operations that
 * need to retain resources after execution until the consumer explicitly releases them.
 * DecayReleaseResult provides a release hook that must be triggered by calling close() when
 * resources are no longer needed.
 */
class DecayReleaseResult[T](override val data: T,
    override val metrics: AsyncMetrics,
    releaseCallback: () => Unit) extends AsyncResult[T] {
  override val releaseHook: Option[() => Unit] = Some(releaseCallback)
}

/**
 * The AsyncRunner interface represents a resource-aware runner that can be scheduled by
 * ResourceBoundedThreadExecutor. Runners define their resource requirements, execution priority,
 * and can optionally hold resources after completion for deferred release.
 *
 * AsyncRunner provides hooks for resource lifecycle management and supports both immediate
 * and deferred resource release patterns based on the corresponding AsyncResult implementation:
 * - FastReleaseResult: resources are released immediately after the runner completion.
 * - DecayReleaseResult: resources are held after runner completion and released when the caller
 *   explicitly invokes the release callback. This is useful for runners that need to retain
 *   resources for a certain period after execution.
 */
trait AsyncRunner[T] extends Callable[AsyncResult[T]] {
  /**
   * Resource required by the runner, such as host memory or GPU semaphore.
   */
  def resource: AsyncRunResource

  /**
   * Priority of the async runner, higher value means higher priority.
   *
   * NOTE: The priority may be the same as the universal priority generated by TaskPriority under
   * most circumstances, such as GpuMultiFileReader, but it might also be adjusted based on the
   * resource requirements or other factors.
   */
  def priority: Double

  /**
   * The abstract method defines the actual execution logic, which should be implemented by the
   * subclass.
   */
  protected def callImpl(): T

  /**
   * Builds the result of the runner execution, which includes the data and a callback to release
   * resources if needed. This method can be overridden by subclasses to customize the result
   * format to carry additional metadata or state.
   */
  protected def buildResult(resultData: T, metrics: AsyncMetrics): AsyncResult[T]

  def call(): AsyncResult[T] = {
    require(result.isEmpty, "AsyncRunner.call() should only be called once")

    val startTime = System.nanoTime()
    val resultData = try {
      beforeExecuteHook.foreach { hook => hook() }
      callImpl()
    } finally {
      afterExecuteHook.foreach { hook => hook() }
    }
    metricsBuilder.setExecutionTimeMs(System.nanoTime() - startTime)

    result = Some(buildResult(resultData, metricsBuilder.build()))
    result.get
  }

  protected var beforeExecuteHook: Option[() => Unit] = None
  protected var afterExecuteHook: Option[() => Unit] = None

  // Set hooks to be executed right before the task execution.
  def setBeforeHook(hook: () => Unit): Unit = beforeExecuteHook = Some(hook)

  // Set hooks to be executed right after the task execution.
  def setAfterHook(hook: () => Unit): Unit = afterExecuteHook = Some(hook)

  /**
   * This method is called when the required resource has been just acquired from pool.
   * It can be overridden by subclasses to perform actions right after the acquisition.
   */
  def onAcquire(): Unit = {
  }

  /**
   * This method is called when the acquired resource has been just released back into pool.
   * It can be overridden by subclasses to perform actions right after the release.
   */
  def onRelease(): Unit = {
  }

  /**
   * Guarantees the release callback will NOT be called unless the task holds the resource.
   * This method is protected not private, so it can be called by subclasses to build own result.
   */
  protected def callDecayReleaseCallback(): Unit = {
    require(execException.isEmpty, "The resource was somehow released forcefully, " +
        s"caught runtime exception: ${execException.get}")
    require(!hasDecayReleased, "callDecayReleaseCallback has been already called for once")
    releaseResourceCallback()
    hasDecayReleased = true
  }

  override def toString: String = {
    s"AsyncRunner(id=$runnerID, resource=$resource, priority=$priority)"
  }

  // Cache the result for 2 purposes:
  // 1. Ensure call() is only called once
  // 2. Facilitate accessing the status of AsyncRunner inside ThreadExecutor after execution,
  //    specifically, to help with handling resource release in `RapidsFutureTask.releaseResource`.
  private[async] var result: Option[AsyncResult[T]] = None

  private[async] var releaseResourceCallback: () => Unit = () => {}

  private[async] lazy val metricsBuilder = new AsyncMetricsBuilder

  private[async] var execException: Option[Throwable] = None

  // Unique ID for the runner, mainly for logging and tracking purpose.
  private[async] val runnerID: String = AsyncRunner.nextRunnerId()

  private var hasDecayReleased = false
}

/**
 * An AsyncRunner that requires no resource limits and has the highest scheduling priority.
 * Useful for lightweight tasks that should execute immediately without resource constraints.
 */
abstract class UnboundedAsyncRunner[T] extends AsyncRunner[T] {
  // Unbounded tasks do not have a resource limit.
  override val resource: AsyncRunResource = AsyncRunResource.newCpuResource(0L)

  // Unbounded tasks have the highest priority.
  override val priority: Double = Double.MaxValue

  // Unbounded tasks use FastReleaseResult as the placeholder.
  override protected def buildResult(resultData: T, metrics: AsyncMetrics): AsyncResult[T] = {
    new FastReleaseResult(resultData, metrics)
  }
}

/**
 * A simple AsyncRunner implementation that wraps a function to be executed asynchronously.
 * NOTE: This class is only used in tests currently, removing the forTest suffix if it is used
 * in production code in the future.
 */
class AsyncFunctorForTest[T](
    override val resource: AsyncRunResource,
    override val priority: Double,
    functor: () => T) extends AsyncRunner[T] {

  override protected def buildResult(resultData: T, metrics: AsyncMetrics): AsyncResult[T] = {
    new FastReleaseResult(resultData, metrics)
  }

  override def callImpl(): T = functor()
}

object AsyncRunner {

  // Create a CPU-bound AsyncRunner with specified memory requirement and priority.
  // NOTE: The API is only used in tests currently
  def newCpuTask[T](fn: () => T,
      memoryBytes: Long,
      priority: Float = 0.0f): AsyncRunner[T] = {
    val adjustedPriority = hostMemoryPenalty(memoryBytes, priority)
    new AsyncFunctorForTest[T](AsyncRunResource.newCpuResource(memoryBytes), adjustedPriority, fn)
  }

  // Create a light-weight unbounded AsyncRunner with the highest priority.
  // NOTE: The API is only used in tests currently
  def newUnboundedTask[T](fn: () => T): AsyncRunner[T] = {
    new AsyncFunctorForTest[T](AsyncRunResource.newCpuResource(0L), Float.MaxValue, fn)
  }

  private def nextRunnerId(): String = {
    val sparkTaskId: Long = Option(TaskContext.get()) match {
      case Some(tc) => tc.taskAttemptId()
      case None => 0L
    }
    // Atomically increase the global runner ID with overflow protection
    val runnerId = globalRunnerId.getAndUpdate(idUpdateFn)
    // Format: T<taskId>_R<runnerId>
    s"T${sparkTaskId}_R$runnerId"
  }

  private lazy val idUpdateFn = new LongUnaryOperator {
    override def applyAsLong(operand: Long): Long = {
      if (operand == Long.MaxValue) 0L else operand + 1L
    }
  }

  private lazy val globalRunnerId = new AtomicLong(0)

  // Adjust the priority based on the memory overhead to minimal the potential clogging:
  // lightweight tasks should have higher priority
  private def hostMemoryPenalty(memoryBytes: Long, priority: Double): Double = {
    require(memoryBytes >= 0, s"Memory bytes must be non-negative, got: $memoryBytes")
    priority - math.log10(memoryBytes)
  }
}

/**
 * An AsyncRunner that shares resource allocation with other tasks in the same group.
 * When any task in the group requests resources, it requests the total amount needed for the
 * entire group. Once one task successfully acquires the group's resources, other tasks in the
 * same group can proceed without additional resource allocation. This pattern is useful for
 * coordinated async tasks which are consumed by the same consumer, such as multithreaded reader.
 */
abstract class GroupedAsyncRunner[T] extends AsyncRunner[T] {
  // The shared state for the group, which should be defined by the subclass.
  protected val sharedState: GroupSharedState

  override def onAcquire(): Unit = {
    sharedState.holdingResource.set(true)
  }

  override def onRelease(): Unit = {
    val numUnreleased = sharedState.taskToBeReleased.decrementAndGet()
    // If all tasks in the group have released their resources, we can reset the holding state.
    if (numUnreleased == 0) {
      sharedState.holdingResource.set(false)
    }
  }

  // The core method to query if the group behind the task is holding the resource.
  def holdSharedResource: Boolean = sharedState.holdingResource.get()
}

case class GroupSharedState(taskToBeReleased: AtomicInteger, holdingResource: AtomicBoolean)

object GroupTaskHelpers {
  def newSharedState(groupSize: Int): GroupSharedState = {
    GroupSharedState(
      taskToBeReleased = new AtomicInteger(groupSize),
      holdingResource = new AtomicBoolean(false))
  }
}
