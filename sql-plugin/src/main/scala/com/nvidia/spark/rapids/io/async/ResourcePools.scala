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

import java.lang.{Long => JLong}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil.{bytesToString => bToStr}

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
  protected[async] def acquire[T](runner: AsyncRunner[T], timeout: Long): AcquireStatus

  /**
   * Closed a completed AsyncRunner (either successfully or failed) and cleanup its resources.
   *
   * This method is typically called as a callback hooked to various completion events to handle
   * scenarios such as cancellation or end of lifecycle.
   */
  protected[async] def finishUpRunner[T](runner: AsyncRunner[T]): Unit

  /**
   * Release resources held by the runner. If forcefully is true, release all resources regardless
   * of what the runner actually holds. Otherwise, only release what the runner can free.
   *
   * NOTE: Although this method is NOT responsible for closing, it is recommended to close the
   * runner if it is no longer holding any resources after the release, such as HostMemoryPool.
   */
  protected[async] def release[T](runner: AsyncRunner[T], forcefully: Boolean): Unit

  /**
   * Performed the detection after the runner has been fully closed, serving as a final safety
   * check to catch any resource management bugs. If a leak is detected, this method should
   * throw an IllegalStateException with details about the leak.
   *
   * Currently, This method is only called at the end of Spark task completion callback to verify
   * two aspects:
   * - Runner-side: verify the runner has zero resource usage after being closed
   * - Pool-side: when runner count reaches zero, verify all resources have been returned
   */
  protected[async] def detectResourceLeak[T](runner: AsyncRunner[T]): Unit = {}
}

/**
 * HostMemoryPool enforces a maximum limit on total host memory bytes that can be held
 * by in-flight runner simultaneously. It provides blocking resource acquisition with
 * configurable timeout.
 *
 * The implementation uses condition variables to efficiently block and wake up waiting
 * tasks when resources become available through task completion and resource release.
 */
class HostMemoryPool(val maxHostMemoryBytes: Long) extends ResourcePool with Logging {

  private val poolLock = new ReentrantLock()

  private val acquireCondition = poolLock.newCondition()
  private val borrowCondition = poolLock.newCondition()
  @volatile private var numOfBorrowWaiters: Int = 0

  // Tracking running AsyncRunners which actually acquires host memory, which is mainly for deadlock
  // prevention for now.
  private val numRunnerInFlight: AtomicLong = new AtomicLong(0L)

  private var remaining: Long = maxHostMemoryBytes

  // Only counts for the AsyncRunners which actually acquired host memory.
  private val numRunnerInPool: AtomicLong = new AtomicLong(0L)

  // Map of TaskAttemptId -> number of active runners, use Java Long for nullability
  private val tasksInPool = new ConcurrentHashMap[Long, JLong](64)

  override protected[async] def acquire[T](runner: AsyncRunner[T],
      timeoutMs: Long): AcquireStatus = {
    // step 1: extract the resource requirements and runner info
    val memoryRequire: Long = extractResource(runner).sizeInBytes

    // step 2: try to acquire the resource with blocking and timeout
    // 2.1 If no resource needed, acquire immediately
    if (memoryRequire == 0L) {
      AcquireSuccessful(elapsedTime = 0L)
    }
    // 2.2 The main path for acquiring resource with blocking and timeout
    else {
      var isDone = false
      var isTimeout = false
      val timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeoutMs)
      var waitTimeNs = timeoutNs
      // Enter into the critical section which is guarded by the lock from concurrent access
      poolLock.lockInterruptibly()
      try {
        // The main loop to try acquiring the resource with blocking and timeout
        do {
          if (remaining >= memoryRequire || {
            // [Deadlock Prevention]
            // Due to the decay release, the virtual memory limit may interact with other dependency
            // mechanisms, such as in a local join. In this scenario, both sides of the join may
            // perform multithreaded scans limited by the HostMemoryPool. The join operator requires
            // outputs from both sides, but one side may occupy all the memory budget, leaving the
            // other side blocked and waiting for memory to be released.
            //
            // [Solution]
            // If there is no runner in flight, run the request runner immediately regardless of
            // the current available resource.
            numRunnerInFlight.compareAndSet(0L, 1L)
          }) {
            remaining -= memoryRequire
            isDone = true
            // When remaining < 0, the increment was done by the CAS above
            if (remaining >= 0L) {
              numRunnerInFlight.incrementAndGet()
            }
            // register a post-hook to decrement it as soon as the runner is done
            runner.addPostHook(() => {
              numRunnerInFlight.decrementAndGet()
              // wake up potential borrowers cuz numInFlight reduced
              poolLock.lockInterruptibly()
              try {
                borrowCondition.signal()
              } finally {
                poolLock.unlock()
              }
            })
          } else if (waitTimeNs > 0L) {
            waitTimeNs = acquireCondition.awaitNanos(waitTimeNs)
          } else {
            isTimeout = true
            logWarning(s"Failed to acquire ${bToStr(memoryRequire)}: ${printStatus()}")
          }
        }
        while (!isDone && !isTimeout)

        if (!isDone) {
          AcquireFailed
        } else {
          // Update nonAtomic states if the resource is acquired successfully
          runner.sparkTaskContext.foreach { ctx =>
            registerRunner(ctx)
          }
          // Log a warning when the resource is over-committed
          if (remaining < 0) {
            logWarning(
              s"Over-committed HostMemoryPool: ${printStatus()}")
          }
          AcquireSuccessful(elapsedTime = timeoutNs - waitTimeNs)
        }
      } catch {
        case ex: Throwable => AcquireExcepted(ex)
      } finally {
        poolLock.unlock()
      }
    }
  }

  // NOTE: this method should be called within the state lock of the runner
  override protected[async] def release[T](rr: AsyncRunner[T], forcefully: Boolean): Unit = {
    if (rr.getState == Running) {
      throw new IllegalStateException(s"Cannot release resources from a running runner: $rr")
    }

    // Try to free the resource from the runner as much as possible
    val (free, remain) = rr.tryFree(forcefully)
    if (forcefully) {
      if (remain > 0L) {
        throw new IllegalStateException(
          s"Forceful release failed to free all resources from $rr, remain=${bToStr(remain)}")
      }
      if (!rr.closeStarted.get()) {
        throw new IllegalStateException(
          s"Runner must have started close process when forcefully releasing: $rr")
      }
    }

    // Return the freed resource back to the pool
    if (free > 0L) {
      poolLock.lockInterruptibly()
      try {
        // Return the budget and wake up waiters
        remaining += free
        // BorrowCondition has higher priority than acquireCondition
        if (poolLock.hasWaiters(borrowCondition)) {
          borrowCondition.signalAll()
        } else if (poolLock.hasWaiters(acquireCondition)) {
          acquireCondition.signalAll()
        }
      } finally {
        poolLock.unlock()
      }
    }

    // forcefully=true implies current thread started the close process, so no need to check if
    // the close process has already started by other threads.
    if (forcefully || (
        remain == 0L && rr.closeStarted.compareAndSet(false, true))) {
      closeRunner(rr)
    }
  }

  override protected[async] def finishUpRunner[T](runner: AsyncRunner[T]): Unit = {
    if (runner.resource.sizeInBytes > 0) {
      release(runner, forcefully = true)
    } else {
      closeRunner(runner)
    }
  }

  /**
   * Borrows memory from the pool with blocking and priority semantics.
   *
   * This method is used when a runner needs to temporarily acquire additional memory beyond its
   * initial allocation. It blocks until sufficient memory becomes available. If all in-flight
   * runners are waiting to borrow, deadlock prevention allows the borrower to proceed immediately,
   * which can over-commit the pool (remaining may become negative).
   *
   * Borrowers have higher priority than regular acquire operations. When memory is released,
   * waiting borrowers are awakened first. If all borrowers are satisfied and memory remains
   * available, acquire waiters are then signaled.
   */
  private[async] def borrowMemory(sizeInBytes: Long): Unit = {
    poolLock.lockInterruptibly()
    var beAwakened = false
    try {
      var noWaiting = false
      while (!noWaiting && remaining < sizeInBytes) {
        if (numOfBorrowWaiters + 1 >= numRunnerInFlight.get()) {
          // [Deadlock Prevention] if all in-flight runners are waiting to borrow memory,
          // allow this borrower to proceed to avoid circular wait.
          noWaiting = true
        } else {
          numOfBorrowWaiters += 1
          borrowCondition.await()
          beAwakened = true
          numOfBorrowWaiters -= 1
        }
      }
      remaining -= sizeInBytes

      if (noWaiting) {
        logWarning(s"Deadlock prevention triggered in borrowMemory: ${printStatus()}")
      }

      // Try to trigger acquireCondition as well if all borrowers are satisfied:
      // 1. beAwakened guarantees current thread was awakened by a release action
      // 2. numOfBorrowWaiters == 0 ensures no other high-priority waiters are pending
      // 3. remaining > 0L means there is available resource remained
      // 4. lock.hasWaiters(acquireCondition) checks if there are acquireCondition waiters
      if (beAwakened && numOfBorrowWaiters == 0 &&
          remaining > 0L && poolLock.hasWaiters(acquireCondition)) {
        acquireCondition.signalAll()
      }
    } finally {
      poolLock.unlock()
    }
  }

  // Close the runner, requires the state lock of the runner
  private def closeRunner[T](runner: AsyncRunner[T]): Unit = {
    require(runner.isHoldingStateLock, s"The caller must hold the state lock: $this")

    // Callback for onClose actions
    runner.onClose()
    // Finalize the runner state
    runner.getState match {
      case Completed => // Completed -> Closed
        runner.setState(Closed(None))
      case ExecFailed(ex) => // ExecFailed -> Closed
        runner.setState(Closed(Some(ex)))
      case Cancelled => // Cancelled -> Closed
        runner.setState(Closed(Some(new IllegalStateException("cancelled"))))
      case _ =>
        throw new IllegalStateException(s"Should NOT reach here: $this")
    }
    // Unregister the runner from the Pool, within the lock
    runner.sparkTaskContext.foreach { ctx =>
      unregisterRunner(ctx)
    }
    logInfo(s"Closed $runner, ${printStatus()}")
  }

  private def registerRunner(ctx: TaskContext): Unit = {
    numRunnerInPool.incrementAndGet()
    tasksInPool.compute(ctx.taskAttemptId(), (_, v: JLong) => {
      if (v == null) new JLong(1) else v + 1L
    })
  }

  private def unregisterRunner(ctx: TaskContext): Unit = {
    numRunnerInPool.decrementAndGet()
    val tid = ctx.taskAttemptId()
    // Decrement the runner count for the task, remove the entry if it reaches zero
    tasksInPool.computeIfPresent(tid, (_, v: JLong) => {
      // It is possible runnersForTask == 0, if some runners were cancelled from caller side
      if (v <= 1L) {
        null
      } else {
        v - 1L
      }
    })
  }

  override protected[async] def detectResourceLeak[T](runner: AsyncRunner[T]): Unit = {
    // Step 1. check the potential leak from the closed runner
    if (!runner.getState.isInstanceOf[Closed]) {
      throw new IllegalStateException(s"Detected other State than Closed: $runner")
    }
    if (runner.resource.sizeInBytes > 0L) {
      throw new IllegalStateException(s"LocalPool is NOT cleaned up: $runner")
    }

    // Step 2. check the leak from the Pool side, which only happens when there is no runner
    // in the pool
    if (numRunnerInPool.get() == 0L) {
      poolLock.lockInterruptibly()
      try {
        if (numRunnerInPool.get() == 0L) {
          if (remaining < maxHostMemoryBytes) {
            throw new IllegalStateException(s"Detected resource leak: $toString")
          }
          if (!tasksInPool.isEmpty) {
            throw new IllegalStateException(
              s"Detected Spark Task tracking leak, tasksInPool=${tasksInPool.toString}")
          }
        }
      } finally {
        poolLock.unlock()
      }
    }
  }

  override def toString: String = s"HostMemoryPool(${printStatus()})"

  private def printStatus(): String = {
    val sb = mutable.StringBuilder.newBuilder
    sb.append("remaining=")
        .append(bToStr(remaining))
        .append('/')
        .append(bToStr(maxHostMemoryBytes))
        .append(", AsyncRunners=")
        .append(numRunnerInFlight.get())
        .append("/")
        .append(numRunnerInPool.get())
        .append(", waitingBorrowers=")
        .append(numOfBorrowWaiters)
        .append(", SparkTasks=")
        .append(tasksInPool.size())
    sb.toString()
  }

  private def extractResource(rr: AsyncRunner[_]): HostResource = {
    rr.resource match {
      case r: HostResource => r
      case r => throw new IllegalStateException(
        s"Unexpected resource type ${r.getClass} in $this")
    }
  }
}
