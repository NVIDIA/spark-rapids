/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, NvtxColor, NvtxRange, Rmm}
import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

trait MemoryLease extends AutoCloseable {
  /**
   * Get the amount of memory assigned to this lease
   */
  def leaseAmount: Long
}

/**
 * Allows tasks to coordinate between each other to avoid
 * oversubscribing GPU memory.
 */
object GpuMemoryLeaseManager extends Logging {
  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile private var instance: GpuMemoryLeaseManager = _

  private def getInstance: GpuMemoryLeaseManager = {
    if (instance == null) {
      GpuMemoryLeaseManager.synchronized {
        // The instance is trying to be used before it is initialized.
        // Since we don't have access to a configuration object here,
        // default to only one task per GPU behavior.
        if (instance == null) {
          initialize(0)
        }
      }
    }
    instance
  }

  /**
   * Initializes the GPU memory lease manager
   * @param savedForShuffle the amount of memory that is saved for suffle
   */
  def initialize(savedForShuffle: Long): Unit = synchronized {
    if (instance != null) {
      throw new IllegalStateException("already initialized")
    }
    val pool = if (Rmm.isInitialized) {
      Rmm.getPoolSize
    } else {
      // This should not really ever happen
      val mem = Cuda.memGetInfo()
      mem.free
    }
    instance = new GpuMemoryLeaseManager(pool - savedForShuffle)
  }

  /**
   * Initialization intended to only be used for testing
   * @param poolSize the size of the pool
   */
  def initializeForTest(poolSize: Long): Unit = synchronized {
    if (instance != null) {
      shutdown()
    }
    instance = new GpuMemoryLeaseManager(poolSize)
  }

  /**
   * Request a lease to access more GPU memory. The thread may block until enough memory can
   * be made available. All memory for this task must be spillable before this is called,
   * because if the task blocks that memory can be handed out to other tasks to make progress.
   *
   * @param tc the task that is making the request.
   * @param optionalAmount the amount of memory that is desired. If this amount of memory is not
   *                       available because the GPU does not have that much memory the request may
   *                       be lowered. This is on top of any required amount of memory.
   * @param requiredAmount the amount of memory that is required. This is on top of any optional
   *                       amount of memory that is requested. If the GPU does not have enough
   *                       memory to fulfill this request an exception will be thrown.
   * @return a MemoryLease that allows the task to allocate more memory.
   */
  def requestLease(tc: TaskContext, optionalAmount: Long, requiredAmount: Long = 0): MemoryLease =
    getInstance.requestLease(tc, optionalAmount, requiredAmount, nonBlocking = false)

  /**
   * Request a least to access more GPU memory, but don't block to get more memory. Because this is
   * non-blocking existing data does not need to be made spillable.
   *
   * @param tc the task that is making the request.
   * @param optionalAmount the amount of memory that is desired. If there is not enough memory
   *                       available the amount may be reduced, all the way to 0 bytes if nothing
   *                       is available.
   * @return a MemoryLease that allows the task to allocate more memory.
   */
  def requestNonBlockingLease(tc: TaskContext, optionalAmount: Long): MemoryLease =
    getInstance.requestLease(tc, optionalAmount, requiredAmount = 0, nonBlocking = true)

  /**
   * Get the total amount of memory currently leased to this task
   */
  def getTotalLease(tc: TaskContext): Long =
    getInstance.getTotalLease(tc)

  /**
   * When a task has finished running this should be called by the GpuSemaphore to clean
   * up any state associated with the task. No one else should call this outside of tests.
   */
  def releaseAllForTask(tc: TaskContext): Unit =
    getInstance.releaseAllForTask(tc)


  /**
   * Uninitialize the GPU Memory Lease Manager.
   * NOTE: This does not wait for active tasks to release!
   */
  def shutdown(): Unit = synchronized {
    if (instance != null) {
      try {
        val tmp = instance
        // Just in case we fail to fully shut down, make sure that we can re-initialize again
        // this is really just for stability of the tests.
        instance = null
        tmp.shutdown()
      } catch {
        case e: Exception =>
          // Keeping this anti-pattern around until
          // https://github.com/NVIDIA/spark-rapids/issues/7359
          // is fixed
          logError("Exception during shutdown of GpuMemoryLeaseManager", e)
          throw e
      }
    }
  }
}


private final class GpuMemoryLeaseManager(memoryForLease: Long) extends Logging with Arm {
  // We are trying to keep the synchronization in this class simple. This is because
  // we don't expect to have a large number of threads active on the GPU at any
  // point in time, so we don't need to try and optimize it too much. All state can only
  // be manipulated while holding the GpuMemoryLeaseManager lock. (so essentially just always
  // grab the lock before doing anything. This includes state in TrackingForTask. To keep
  // things simple all threads that are waiting for more memory will be notified when enough
  // memory is available to handle the next thread, but only the threads associated with that
  // task will stay awake and continue processing. All of the others will wait/block again.
  class TrackingForTask {
    var totalUsed: Long = 0
    var waitingThreadCount: Long = 0
    var canWakeUp: Boolean = false
  }

  // The total amount of memory available to be leased
  private var memoryAvailable: Long = memoryForLease
  // The current active tasks along with their state
  private val activeTasks = new ConcurrentHashMap[Long, TrackingForTask]
  // The list of tasks in priority order to be fulfilled. Currently this is FIFO order
  private val taskPriorityQueue = new util.LinkedList[TrackingForTask]()
  private var shuttingDown: Boolean = false

  def shutdown(): Unit = synchronized {
    shuttingDown = true
    if (!activeTasks.isEmpty) {
      logDebug(s"shutting down with ${activeTasks.size} tasks still registered")
      notifyAll()
    }
  }

  /**
   * Wake up any tasks that can now run. This should be called any time that memory is returned
   * and memoryAvailable increases. It must be called while the GpuMemoryLeaseManager lock is
   * held. That is why it is a private method and should never be made public.
   */
  private def wakeTasksThatCanRun(): Unit = {
    var anyWoke = false
    var keepGoing = !taskPriorityQueue.isEmpty
    while (keepGoing) {
      val head = taskPriorityQueue.peek()
      if (head.totalUsed <= memoryAvailable) {
        taskPriorityQueue.pop()
        memoryAvailable -= head.totalUsed
        head.canWakeUp = true
        keepGoing = !taskPriorityQueue.isEmpty
        anyWoke = true
      } else {
        keepGoing = false
      }
    }
    if (anyWoke) {
      notifyAll()
    }
  }

  /**
   * Request a new lease. This can throw if the requiredAmount cannot be fulfilled.
   * @param tc the task the lease is for
   * @param optionalAmount the amount optionally needed (amount might be lowered if needed)
   * @param requiredAmount the amount that is required to work (not compatible with
   *                       nonBlocking = true)
   * @param nonBlocking can the request block to get the requested resources, or not.
   * @return the lease.
   */
  def requestLease(tc: TaskContext,
      optionalAmount: Long,
      requiredAmount: Long,
      nonBlocking: Boolean): MemoryLease = synchronized {
    val taskAttemptId = tc.taskAttemptId()
    if (nonBlocking && requiredAmount > 0) {
      throw new IllegalArgumentException("Non-blocking requests with a required minimum amount " +
          "of memory are not supported, because they are too likely to fail randomly")
    }
    val alreadyRequested = getTotalLease(tc)
    if (requiredAmount + alreadyRequested > memoryForLease) {
      throw new IllegalArgumentException(s"Task: $taskAttemptId requested at least " +
          s"$requiredAmount more bytes, but already has leased $alreadyRequested bytes which " +
          s"would got over the total for the worker of $memoryForLease bytes.")
    }

    if (nonBlocking && (!taskPriorityQueue.isEmpty || memoryAvailable <= 0)) {
      // This would block so just indicate it right now.
      return new MemoryLease {
        override def leaseAmount: Long = 0

        override def close(): Unit = {}
      }
    }

    val totalAmountRequested = requiredAmount + optionalAmount
    val amountToRequest = if (nonBlocking && totalAmountRequested > memoryAvailable) {
      logDebug(s"Task: $taskAttemptId requested $totalAmountRequested bytes in a " +
          s"non-blocking request, but only $memoryAvailable is available. The request will " +
          s"be reduced to $memoryAvailable.")
      memoryAvailable
    } else if (totalAmountRequested + alreadyRequested > memoryForLease) {
      logWarning(s"Task: $taskAttemptId requested $totalAmountRequested bytes, but has " +
          s"already requested $alreadyRequested bytes. This would go over the total for the " +
          s"worker $memoryForLease reducing the request on the hope that this was an " +
          s"overestimate.")
      memoryForLease - alreadyRequested
    } else {
      totalAmountRequested
    }

    val task = activeTasks.computeIfAbsent(taskAttemptId, _ => new TrackingForTask)

    if (!taskPriorityQueue.isEmpty || memoryAvailable < amountToRequest) {
      // Need to block until there is memory available
      if (task.waitingThreadCount > 0 || task.canWakeUp) {
        throw new IllegalStateException("MULTIPLE THREADS TRYING TO REQUEST RESOURCES FOR " +
            "A SINGLE TASK")
      }
      // We should never hit this, because the checks before should avoid blocking, but just to be
      // safe we are going to check.
      assert(!nonBlocking)

      // Release all of the currently used memory so it can be reused by another task if needed.
      memoryAvailable += task.totalUsed
      wakeTasksThatCanRun()

      // Update the task request state for what is now needed
      task.waitingThreadCount = 1
      task.totalUsed += amountToRequest
      taskPriorityQueue.offer(task)
      withResource(new NvtxRange("Acquire GPU Memory", NvtxColor.YELLOW)) { _ =>
        while (!task.canWakeUp) {
          try {
            wait()
          } catch {
            case _: InterruptedException =>
          }
          if (shuttingDown) {
            throw new IllegalStateException("GpuMemoryLeaseManager shut down")
          }
        }
      }

      // Memory available was already updated when the task was marked for wakeup.
      task.canWakeUp = false
      task.waitingThreadCount -= 1
    } else {
      // No need to block we can just do this...
      memoryAvailable -= amountToRequest
      task.totalUsed += amountToRequest
    }
    new MemoryLease {
      override def leaseAmount: Long = amountToRequest

      override def close(): Unit = {
        releaseLease(tc, amountToRequest)
      }
    }
  }

  def getTotalLease(tc: TaskContext): Long = synchronized {
    val taskAttemptId = tc.taskAttemptId()
    val data = activeTasks.get(taskAttemptId)
    if (data == null) {
      0
    } else {
      data.totalUsed
    }
  }

  private def releaseLease(tc: TaskContext, amount: Long): Unit = synchronized {
    val taskAttemptId = tc.taskAttemptId()
    val data = activeTasks.get(taskAttemptId)
    if (data != null) {
      // For now we are just going to ignore that it is gone. Could be a race with releaseAllForTask
      if (data.totalUsed >= amount) {
        memoryAvailable += amount
        data.totalUsed -= amount
      } else {
        throw new IllegalStateException(s"Looks like a double free happened returning $amount" +
            s" bytes, but only ${data.totalUsed} available to return")
      }
      wakeTasksThatCanRun()
    }
  }

  def releaseAllForTask(tc: TaskContext): Unit = synchronized {
    val taskAttemptId = tc.taskAttemptId()
    val task = activeTasks.get(taskAttemptId)
    if (task != null) {
      if (taskPriorityQueue.contains(task)) {
        throw new IllegalStateException("Attempting to release all resources when a task still " +
            "has a request pending")
      }
      activeTasks.remove(taskAttemptId)
      memoryAvailable += task.totalUsed
      task.totalUsed = 0
      wakeTasksThatCanRun()
    }
  }
}