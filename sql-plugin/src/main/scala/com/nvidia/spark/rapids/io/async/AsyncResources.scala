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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.bytesToString

sealed trait TaskResource

case class HostResource(
    hostMemoryBytes: Long,
    groupedHostMemoryBytes: Option[Long] = None) extends TaskResource

// DeviceResource is a marker object for GPU resources, no additional fields needed.
object DeviceResource extends TaskResource

object TaskResource {
  def newCpuResource(hostMemoryBytes: Long,
      groupedMemoryBytes: Option[Long] = None): TaskResource = {
    HostResource(hostMemoryBytes, groupedHostMemoryBytes = groupedMemoryBytes)
  }

  def newGpuResource(): TaskResource = DeviceResource
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
 * Result wrapper for AsyncTask execution that carries the task's output data and optional
 * resource release callback for deferred resource management.
 */
trait AsyncResult[T] extends AutoCloseable {
  def data: T

  def releaseResourceCallback(): Unit = {}

  def hasReleaseCallback: Boolean = false

  def metrics: AsyncMetrics

  /**
   * Trigger release callback if it exists. Do not try to close the data itself, because
   * the lifecycle of the data is managed by the caller.
   */
  override def close(): Unit = {
    if (hasReleaseCallback) {
      releaseResourceCallback()
    }
  }
}

class SimpleAsyncResult[T](override val data: T,
    override val metrics: AsyncMetrics) extends AsyncResult[T]

class AsyncResultDecayRelease[T](override val data: T,
    override val metrics: AsyncMetrics,
    private val releaseCallback: () => Unit) extends AsyncResult[T] {
  override val hasReleaseCallback: Boolean = true

  override def releaseResourceCallback(): Unit = releaseCallback()
}

/**
 * The AsyncTask interface represents a resource-aware task that can be scheduled by
 * ResourceBoundedThreadExecutor. Tasks define their resource requirements, execution priority,
 * and can optionally hold resources after completion for deferred release.
 *
 * AsyncTask provides hooks for resource lifecycle management and supports both immediate
 * and deferred resource release patterns based on the task's holdResourceAfterCompletion flag.
 */
trait AsyncTask[T] extends Callable[AsyncResult[T]] {
  /**
   * Resource required by the task, such as host memory or GPU semaphore.
   */
  def resource: TaskResource

  /**
   * Priority of the task, higher value means higher priority.
   */
  def priority: Double

  /**
   * The abstract method defines the actual task logic, which should be implemented by the
   * subclass.
   */
  protected def callImpl(): T

  /**
   * Builds the result of the task execution, which includes the data and a callback to release
   * resources if needed. This method can be overridden by subclasses to customize the result
   * format to carry additional metadata or state.
   */
  protected def buildResult(resultData: T, metrics: AsyncMetrics): AsyncResult[T] = {
    if (!holdResourceAfterCompletion) {
      new SimpleAsyncResult(resultData, metrics)
    } else {
      new AsyncResultDecayRelease(resultData, metrics, releaseResource)
    }
  }

  def call(): AsyncResult[T] = {
    val startTime = System.nanoTime()
    val resultData = try {
      beforeExecuteHook.foreach { hook => hook() }
      callImpl()
    } finally {
      afterExecuteHook.foreach { hook => hook() }
    }
    metricsBuilder.setExecutionTimeMs(System.nanoTime() - startTime)
    buildResult(resultData, metricsBuilder.build())
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
   * Returns true if the task should release its resources after completion.
   * This is useful for tasks that are not long-lived and can release resources
   * immediately after they finish.
   */
  def holdResourceAfterCompletion: Boolean = false

  /**
   * Guarantees the release callback will NOT be called unless the task holds the resource.
   * This method is protected not private, so it can be called by subclasses to build own result.
   */
  protected def releaseResource(): Unit = {
    require(decayRelease, "Task does NOT hold resource after completion")
    require(releaseResourceCallback != null, "releaseResourceCallback is not registered")
    releaseResourceCallback()
    decayRelease = false
  }

  private[async] var releaseResourceCallback: () => Unit = _

  private[async] var decayRelease = false

  private[async] lazy val metricsBuilder = new AsyncMetricsBuilder
}

/**
 * An AsyncTask that requires no resource limits and has the highest scheduling priority.
 * Useful for lightweight tasks that should execute immediately without resource constraints.
 */
abstract class UnboundedAsyncTask[T] extends AsyncTask[T] {
  // Unbounded tasks do not have a resource limit.
  override val resource: TaskResource = TaskResource.newCpuResource(0L)

  // Unbounded tasks have the highest priority.
  override val priority: Double = Double.MaxValue
}

case class GroupSharedState(groupSize: Int,
    launchedTasks: AtomicInteger,
    releasedTasks: AtomicInteger,
    holdingResource: AtomicBoolean)

object GroupTaskHelpers {
  def newSharedState(groupSize: Int): GroupSharedState = {
    GroupSharedState(groupSize,
      new AtomicInteger(0), new AtomicInteger(0), new AtomicBoolean(false))
  }

  /*
   * Returns a nearly unique priority value for GroupedAsyncTask instance.
   * With group-based priorities, we can ensure that tasks in the same group are scheduled in the
   * same time window as possible, in case that only a part of tasks in the group get scheduled
   * while the rest are blocked because of the schedule order.
   */
  def generateGroupPriority: Double = {
    groupwisePriorityCounter.compareAndSet(0L, 1000000000L)
    // As for cpu tasks, 100 is big enough difference to divide different groups, considering that
    // the default memory penality is floor(log2(MemoryBytes)).
    groupwisePriorityCounter.getAndAdd(-100L).toDouble
  }

  private lazy val groupwisePriorityCounter = new AtomicLong(1000000000L)
}

/**
 * An AsyncTask that shares resource allocation with other tasks in the same group.
 * When any task in the group requests resources, it requests the total amount needed for the
 * entire group. Once one task successfully acquires the group's resources, other tasks in the
 * same group can proceed without additional resource allocation. This pattern is useful for
 * coordinated async tasks which are consumed by the same consumer, such as multithreaded reader.
 */
abstract class GroupedAsyncTask[T] extends AsyncTask[T] {
  // The shared state for the group, which should be defined by the subclass.
  protected val sharedState: GroupSharedState

  override def call(): AsyncResult[T] = {
    sharedState.launchedTasks.incrementAndGet()
    super.call()
  }

  override def onAcquire(): Unit = {
    sharedState.holdingResource.set(true)
  }

  override def onRelease(): Unit = {
    val numReleased = sharedState.releasedTasks.incrementAndGet()
    // If all tasks in the group have released their resources, we can reset the holding state.
    if (numReleased == sharedState.groupSize) {
      sharedState.holdingResource.set(false)
    }
  }

  // The core method to query if the group behind the task is holding the resource.
  def holdSharedResource: Boolean = sharedState.holdingResource.get()
}

class AsyncFunctor[T](
    override val resource: TaskResource,
    override val priority: Double,
    functor: () => T) extends AsyncTask[T] {
  override def callImpl(): T = functor()
}

object AsyncTask {
  // Adjust the priority based on the memory overhead to minimal the potential clogging:
  // lightweight tasks should have higher priority
  def hostMemoryPenalty(memoryBytes: Long, priority: Double = 0.0): Double = {
    require(memoryBytes >= 0, s"Memory bytes must be non-negative, got: $memoryBytes")
    priority - math.log10(memoryBytes)
  }

  def newCpuTask[T](fn: () => T,
      memoryBytes: Long,
      priority: Float = 0.0f): AsyncTask[T] = {
    val adjustedPriority = hostMemoryPenalty(memoryBytes, priority)
    new AsyncFunctor[T](TaskResource.newCpuResource(memoryBytes), adjustedPriority, fn)
  }

  def newUnboundedTask[T](fn: () => T): AsyncTask[T] = {
    new AsyncFunctor[T](TaskResource.newCpuResource(0L), Float.MaxValue, fn)
  }
}

// Being thrown when a task requests resources that are not valid or exceed the limits
class InvalidResourceRequest(msg: String) extends RuntimeException(
  s"Invalid resource request: $msg")

// Represents the status of acquiring resources for a task
sealed trait AcquireStatus
case class AcquireSuccessful(elapsedTime: Long) extends AcquireStatus
// AcquireFailed indicates that the task could not be scheduled due to resource constraints
case object AcquireFailed extends AcquireStatus
// AcquireExcepted indicates that an exception occurred while trying to acquire resources
case class AcquireExcepted(exception: Throwable) extends AcquireStatus

/**
 * ResourceManager interface to be implemented for AsyncTasks requiring different kinds of
 * resources.
 *
 * Currently, only HostMemoryManager is implemented, which limits the maximum in-flight host
 * memory bytes. In the future, we can add more.
 */
trait ResourcePool {
  /**
   * Returns true if the task can be accepted, false otherwise.
   * TrafficController will block the task from being scheduled until this method returns true.
   */
  def acquireResource[T](task: AsyncTask[T], timeout: Long): AcquireStatus

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def releaseResource[T](task: AsyncTask[T]): Unit
}

/**
 * HostMemoryPool enforces a maximum limit on total host memory bytes that can be held
 * by in-flight tasks simultaneously. It provides blocking resource acquisition with
 * configurable timeout and supports both individual and grouped task allocation patterns.
 *
 * For grouped tasks, the pool recognizes when tasks share resource allocation and avoids
 * double-counting memory usage within the same group.
 *
 * The implementation uses condition variables to efficiently block and wake up waiting
 * tasks when resources become available through task completion and resource release.
 */
class HostMemoryPool(val maxHostMemoryBytes: Long) extends ResourcePool with Logging {

  private val lock = new ReentrantLock()

  private val condition = lock.newCondition()

  private val remaining: AtomicLong = new AtomicLong(maxHostMemoryBytes)

  private val holdingBuffers: AtomicLong = new AtomicLong(0L)

  private val holdingGroups: AtomicInteger = new AtomicInteger(0)

  override def acquireResource[T](task: AsyncTask[T], timeoutMs: Long): AcquireStatus = {
    val resource = extractResource(task)
    val requiredAmount: Long = task match {
      case _: GroupedAsyncTask[T] => resource.groupedHostMemoryBytes.getOrElse(
          throw new InvalidResourceRequest(
            s"GroupedAsyncTask must have groupedHostMemoryBytes defined, but got $resource"))
      case _ => resource.hostMemoryBytes
    }
    requiredAmount match {
      case 0 =>
        AcquireSuccessful(elapsedTime = 0L)
      case required if required > maxHostMemoryBytes =>
        // Call the failure callback to notify the caller about the failure
        AcquireExcepted(
          new InvalidResourceRequest(
            s"Task requires more host memory($required) than pool size($maxHostMemoryBytes)"))
      case required: Long =>
        var isDone = false
        var isTimeout = false
        val timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        var waitTimeNs = timeoutNs
        lock.lockInterruptibly()
        try {
          while (!isDone && !isTimeout) {
            task match {
              case t: GroupedAsyncTask[T] if t.holdSharedResource => isDone = true
              case _ =>
            }
            if (isDone) {
            } else if (remaining.get() >= required) {
              remaining.getAndAdd(-required)
              isDone = true
            } else if (waitTimeNs > 0) {
              waitTimeNs = condition.awaitNanos(waitTimeNs)
            }  else {
              isTimeout = true
              logWarning(s"Failed to acquire ${bytesToString(required)}, " +
                  s"remaining=${bytesToString(remaining.get())}, " +
                  s"pendingTasks=${holdingBuffers.get()}, pendingGroups=${holdingGroups.get()}")
            }
          }
          if (isDone) {
            holdingBuffers.incrementAndGet()
            task match {
              case grouped: GroupedAsyncTask[_] if !grouped.holdSharedResource =>
                val numGroups = holdingGroups.incrementAndGet()
                logDebug(s"Acquire a SharedGroup(${bytesToString(required)}), " +
                    s"remaining=${bytesToString(remaining.get())}, pendingGroups=$numGroups")
              case _ =>
              // No action needed for non-grouped tasks
            }
            task.onAcquire()
            AcquireSuccessful(elapsedTime = timeoutNs - waitTimeNs)
          } else {
            AcquireFailed
          }
        } catch {
          case ex: Throwable => AcquireExcepted(ex)
        } finally {
          lock.unlock()
        }
    }
  }

  override def releaseResource[T](task: AsyncTask[T]): Unit = {
    // Even for grouped tasks, we release the resource separately,
    val toRelease = extractResource(task).hostMemoryBytes
    if (toRelease > 0) {
      val newVal = remaining.addAndGet(toRelease)
      task.onRelease()
      val pendingTaskNum = holdingBuffers.decrementAndGet()
      task match {
        case g: GroupedAsyncTask[T] if !g.holdSharedResource =>
          val numGroup = holdingGroups.decrementAndGet()
          val groupSizeBytes = g.resource.asInstanceOf[HostResource].groupedHostMemoryBytes.get
          logDebug(s"Release a SharedGroup(${bytesToString(groupSizeBytes)}), " +
              s"remaining=${bytesToString(newVal)}, " +
              s"pendingTasks=$pendingTaskNum, pendingGroups=$numGroup)")
        case _ =>
      }
      lock.lock()
      condition.signalAll()
      lock.unlock()
    }
  }

  override def toString: String = {
    s"HostMemoryPool(maxHostMemoryBytes=${maxHostMemoryBytes >> 20}MB)"
  }

  private def extractResource(task: AsyncTask[_]): HostResource = {
    task.resource match {
      case r: HostResource => r
      case r => throw new InvalidResourceRequest(
        s"Task ${task.getClass.getName} does not require HostResource, but got $r")
    }
  }
}
