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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

/**
 * The result of requesting a lease for more memory. Closing this will return the lease so
 * other tasks can also use it.
 */
trait MemoryLease extends AutoCloseable {
  /**
   * Get the amount of memory assigned to this lease
   */
  def leaseAmount: Long
}

/**
 * Allows tasks to coordinate between each other to avoid oversubscribing GPU memory. This class
 * provides a set of APIs that allow SparkPlan nodes that use GPU memory to request more memory
 * if they think they will need it. By default each task at startup will be given a base amount
 * of memory to use. This base amount is typically based on a multiple of the target batch size.
 * Each node in the plan should look at the input batch(s) that they get and the current lease
 * that is assigned to that task to decide if it needs more memory to complete the processing.
 * If the node decides that it needs more memory to complete an operation it can ask the
 * GpuMemoryLeaseManager for a lease. There are several different ways to request this lease
 * depending on the situation, so lets use the an ORC write as an example on what to do.
 * Be aware that the numbers here are just made up, so only take this as an example of what
 * could be done, and not exactly what will be done.
 * <br/>
 * An ORC write can take a number of different forms. We are going to start with the simplest
 * one where we take a batch as input and write it out to a file. In this situation we can
 * look at the input batch size, along with the types of the columns and the compression algorithm
 * being used and guess, using a heuristic, that this 1 GiB input batch is going to require an
 * additional 8 GiB of GPU memory to do all of the compression needed. We can call into
 * `getTotalLease(taskContext)` to know how much memory our task has already been allocated.
 * In this case we are going to say 4 GiB. 1 GiB already used + 8 GiB more needed to complete
 * the operation - 4 GiB available to use = 5 GiB more needed to complete the operation.
 * Because we need more memory to finish the task we can make the input batch is spillable
 * and call `requestLease(taskContext, 0, moreNeeded)` where the `moreNeeded` holds 5 GiB, the
 * result of the math, and is the `requiredAmount` parameter to `requestLease`. The method may
 * block until it can get access to the full 9 GiB of memory for this task are available to it.
 * Because it may block we have to make the input batch spillable. This is so other tasks can use
 * that memory if needed. This is to avoid deadlocks. Once the method returns we know that we have
 * everything needed to use those 9 GiB of memory. We can then write out the result and be done.
 * <br/>
 * But this has a number of problems with it that need to be addressed. First off, what happens
 * if the GPU does not have 9 GiB of physical memory available in the RMM memory pool? What if we
 * are running on say an old 8 GiB GPU? If we say that the amount needed is the `requiredAmount`
 * then the task will fail without even trying. This is not ideal, especially because the 8 GiB
 * was just an estimate. We might be wrong. If we switch the call to
 * `requestLease(taskContext, moreNeeded)`, now the amount requested is the `optionalAmount`
 * parameter. Now if the GPU does not have enough memory it will make all of that memory available
 * to the task and we can hope that it is enough, but at least it gives us a chance to see.
 * Generally `requiredAmount` should only be used if the estimate is very accurate, like a concat
 * operation.
 * <br/>
 * The next problem is blocking and spilling. Making the input batch spillable is not ideal.
 * It is not that expensive, but it is not free. And if we are on a large 80 GiB GPU there is
 * a real possibility that we would never need to block. But if the task does block it is
 * probable that the input batch will spill to host memory or even disk, which will need
 * to be read back into GPU memory before we can finish doing the write. We can instead see how
 * much of the memory we can get access to without blocking, and then decide what to do next. We
 * can do this by calling `requestNonBlockingLease(taskContext, moreNeeded)`. This API is
 * guaranteed to not block and as such the caller does not need to make sure all of the current
 * working memory is spillable. The downside is that the `MemoryLease` that is returned might
 * have anywhere between 0 bytes and the full amount requested. Because of this we are going to
 * have to dynamically adjust based on what happens. If the amount of memory returned is the full
 * amount, then we saved making the input spillable and can just do the processing as usual. If
 * the amount returned is not quite enough to do the entire batch, but it looks like we could split
 * the batch into 2 sub-batches, then depending on benchmarking, it might be worth splitting the
 * input batch into two. Making half of it spillable, and out processing the other half of the data.
 * If there isn't enough memory to even do that, then we can fall back to making that entire input
 * batch spillable and calling the blocking API to get everything that we think we need to complete
 * the operation.
 */
object GpuMemoryLeaseManager extends Logging {
  private val enabled = {
    val propstr = System.getProperty("com.nvidia.spark.rapids.gmlm.enabled", "true")
    java.lang.Boolean.parseBoolean(propstr)
  }

  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile private var instance: GpuMemoryLeaseManager = _
  @volatile private var concurrentGpuTasks = -1
  @volatile private var hasWarnedAboutTargetSize = false

  private def getInstance: GpuMemoryLeaseManager = {
    if (instance == null) {
      GpuMemoryLeaseManager.synchronized {
        // The instance is trying to be used before it is initialized.
        // Since we don't have access to a configuration object here,
        // default to only one task per GPU behavior.
        if (instance == null) {
          throw new IllegalStateException("GpuMemoryLeaseManager was never initialized")
        }
      }
    }
    instance
  }

  def getAdjustedTargetBatchSize(targetSize: Option[Long]): Long = {
    if (!enabled) {
      // The default for what we did originally
      targetSize.getOrElse(Int.MaxValue.toLong)
    } else {
      val poolSize = getInstance.memoryForLease
      val setTargetBatchSize = targetSize.getOrElse(Int.MaxValue.toLong)
      val isBatchSizeSet = targetSize.isDefined
      val idealMinBatchSize = 1024L * 1024 * 1024 // 1 GiB
      val perTaskMultFactor = 4L

      if (isBatchSizeSet) {
        // We cannot warn if the target size is too large, because this might be set
        // by something that is not the RapidsConf, such as for a RequireSingleBatch for a
        // coalesce batch...
        setTargetBatchSize
      } else {
        // We should pick a batch size that is reasonable, and warn if we cannot pick a good one
        val batchSize = Math.min(poolSize / perTaskMultFactor / concurrentGpuTasks,
          Int.MaxValue)
        if (!hasWarnedAboutTargetSize && batchSize < idealMinBatchSize) {
          logWarning(s"By setting ${RapidsConf.CONCURRENT_GPU_TASKS} to " +
              s"$concurrentGpuTasks on a GPU with only $poolSize bytes available " +
              s"the target batch size was adjusted to $batchSize to avoid running out of " +
              s"memory. This is small enough it might cause performance issues.")
          hasWarnedAboutTargetSize = true
        }
        batchSize
      }
    }
  }

  /**
   * Initializes the GPU memory lease manager
   *
   * @param poolSize the size of the pool to hand out leases from.
   * @param concurrentGpuTasks the number of concurrent GPU tasks, to help dynamically adjust
   *                           the target batch size, if needed.
   */
  def initialize(poolSize: Long, concurrentGpuTasks: Int): Unit = synchronized {
    if (enabled) {
      if (instance != null) {
        throw new IllegalStateException("already initialized")
      }
      instance = new GpuMemoryLeaseManager(poolSize)
      GpuMemoryLeaseManager.concurrentGpuTasks = concurrentGpuTasks
    }
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
  def requestLease(tc: TaskContext, optionalAmount: Long, requiredAmount: Long = 0): MemoryLease = {
    if (enabled) {
      getInstance.requestLease(tc, optionalAmount, requiredAmount, nonBlocking = false)
    } else {
      new MemoryLease {
        override def leaseAmount: Long = optionalAmount + requiredAmount

        override def close(): Unit = ()
      }
    }
  }

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
  def requestNonBlockingLease(tc: TaskContext, optionalAmount: Long): MemoryLease = {
    if (enabled) {
      getInstance.requestLease(tc, optionalAmount, requiredAmount = 0, nonBlocking = true)
    } else {
      new MemoryLease {
        override def leaseAmount: Long = optionalAmount

        override def close(): Unit = ()
      }
    }
  }

  /**
   * Get the total amount of memory currently leased to this task, or -1 if the lease manager
   * is disabled.
   */
  def getTotalLease(tc: TaskContext): Long = {
    if (enabled) {
      getInstance.getTotalLease(tc)
    } else {
      -1L
    }
  }

  /**
   * When a task has finished running this should be called by the GpuSemaphore to clean
   * up any state associated with the task. No one else should call this outside of tests.
   */
  def releaseAllForTask(tc: TaskContext): Unit =
    if (enabled) {
      getInstance.releaseAllForTask(tc)
    }


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


private final class GpuMemoryLeaseManager(val memoryForLease: Long) extends Logging with Arm {
  // We are trying to keep the synchronization in this class simple. This is because
  // we don't expect to have a large number of threads active on the GPU at any
  // point in time, so we don't need to try and optimize it too much. All state can only
  // be manipulated while holding the GpuMemoryLeaseManager lock, so essentially just always
  // grab the lock before doing anything. This includes state in TrackingForTask. To keep
  // things simple all threads that are waiting for more memory will be notified when enough
  // memory is available to handle the next thread, but only the threads associated with that
  // task will stay awake and continue processing. All of the others will wait/block again.
  class TrackingForTask {
    var totalUsed: Long = 0
    var isThreadWaiting: Boolean = false
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
    require(!nonBlocking || requiredAmount <= 0,
      "Non-blocking requests with a required minimum amount of memory are not supported, because " +
          "they are too likely to fail randomly")
    val alreadyRequested = getTotalLease(tc)
    require(requiredAmount + alreadyRequested <= memoryForLease,
      s"Task: $taskAttemptId requested at least $requiredAmount more bytes, but already has " +
          s"leased $alreadyRequested bytes which would got over the total for the worker " +
          s"of $memoryForLease bytes.")

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
      if (task.isThreadWaiting || task.canWakeUp) {
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
      task.isThreadWaiting = true
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
      task.isThreadWaiting = false
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