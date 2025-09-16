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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.bytesToString

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
 * ResourceManager interface to be implemented for AsyncRunners requiring different kinds of
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
  def acquireResource[T](task: AsyncRunner[T], timeout: Long): AcquireStatus

  /**
   * Callback to be called when a task is completed, either successfully or with an exception.
   */
  def releaseResource[T](task: AsyncRunner[T]): Unit
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

  override def acquireResource[T](runner: AsyncRunner[T], timeoutMs: Long): AcquireStatus = {
    // step 1: extract the resource requirements and runner info
    val (selfMemReq, groupMemReq, isGroupRunner) = runner match {
      // For grouped runners, if the group is already holding the resource, no additional
      // allocation is needed.
      case r: GroupedAsyncRunner[T] if r.holdSharedResource =>
        (0L, Some(0L), true)
      // For grouped runners that are not yet holding the resource, allocate the full group size.
      case _: GroupedAsyncRunner[T] =>
        val resource = extractResource(runner)
        val groupMem = resource.groupedHostMemoryBytes.getOrElse(
          throw new InvalidResourceRequest(
            s"GroupedAsyncRunner must have groupedHostMemoryBytes defined, but got $resource"))
        (resource.hostMemoryBytes, Some(groupMem), true)
      // For non-grouped runners, allocate the individual task size.
      case _ =>
        (extractResource(runner).hostMemoryBytes, None, false)
    }

    // step 2: try to acquire the resource with blocking and timeout
    // 2.1 If no resource needed, acquire immediately
    if (selfMemReq == 0L && groupMemReq.getOrElse(0L) == 0L) {
      holdingBuffers.incrementAndGet()
      runner.onAcquire()
      return AcquireSuccessful(elapsedTime = 0L)
    }
    // 2.2 If the request runner itself exceeds the maximum pool size, fail immediately by
    // returning a failure signal
    if (selfMemReq > maxHostMemoryBytes) {
      val invalidReq = new InvalidResourceRequest("Task requires more host memory(" +
          s"${bytesToString(selfMemReq)}) than pool size(${bytesToString(maxHostMemoryBytes)})")
      return AcquireExcepted(invalidReq)
    }
    // 2.3 The main path for acquiring resource with blocking and timeout
    var isDone = false
    var isTimeout = false
    val timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    var waitTimeNs = timeoutNs
    // Enter into the critical section which is guarded by the lock from concurrent access
    lock.lockInterruptibly()
    try {
      while (!isDone && !isTimeout) {
        // Check again for GroupedAsyncRunner after entering the critical section
        if (isGroupRunner && runner.asInstanceOf[GroupedAsyncRunner[T]].holdSharedResource) {
          isDone = true
        } else {
          // If enough resource is available, acquire it and exit the loop.
          // IMPORTANT: For grouped runners, we will always allocate the full group size once the
          // remaining resource is enough for the current runner itself. Even if there is not
          // enough remaining resource for the entire group, in this case the forceful allocation
          // will cause the remaining resource to go negative. This is to avoid deadlock when
          // there exists huge group which exceeds the pool size or occupies most of the pool,
          // although this may temporarily exceed the pool limit.
          if (remaining.get() >= selfMemReq) {
            remaining.getAndAdd(-groupMemReq.getOrElse(selfMemReq))
            isDone = true
          } else if (waitTimeNs > 0) {
            waitTimeNs = condition.awaitNanos(waitTimeNs)
          } else {
            isTimeout = true
            logWarning(s"Failed to acquire ${bytesToString(selfMemReq)}, " +
                s"remaining=${bytesToString(remaining.get())}, " +
                s"pendingTasks=${holdingBuffers.get()}, pendingGroups=${holdingGroups.get()}")
          }
        }
      }
      if (isDone) {
        holdingBuffers.incrementAndGet()
        // Update holdingGroups only for the first runner in a group that acquired the resource
        if (isGroupRunner && !runner.asInstanceOf[GroupedAsyncRunner[T]].holdSharedResource) {
          val numGroups = holdingGroups.incrementAndGet()
          logDebug(s"Acquire a SharedGroup(${bytesToString(groupMemReq.get)}), " +
              s"remaining=${bytesToString(remaining.get())}, pendingGroups=$numGroups")
        }
        runner.onAcquire()
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

  override def releaseResource[T](task: AsyncRunner[T]): Unit = {
    // Even for grouped tasks, we release the resource separately,
    val toRelease = extractResource(task).hostMemoryBytes
    if (toRelease > 0) {
      val newVal = remaining.addAndGet(toRelease)
      task.onRelease()
      val pendingTaskNum = holdingBuffers.decrementAndGet()
      task match {
        case g: GroupedAsyncRunner[T] if !g.holdSharedResource =>
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
    s"HostMemoryPool(maxHostMemoryBytes=${bytesToString(maxHostMemoryBytes)})"
  }

  private def extractResource(task: AsyncRunner[_]): HostResource = {
    task.resource match {
      case r: HostResource => r
      case r => throw new InvalidResourceRequest(
        s"Task ${task.getClass.getName} does not require HostResource, but got $r")
    }
  }
}
