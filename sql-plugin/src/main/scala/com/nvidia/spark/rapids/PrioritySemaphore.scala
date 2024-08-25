/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.mutable.PriorityQueue

object PrioritySemaphore {
  private val DEFAULT_MAX_PERMITS = 1000
}

class PrioritySemaphore[T](val maxPermits: Int)(implicit ordering: Ordering[T]) {
  // This lock is used to generate condition variables, which affords us the flexibility to notify
  // specific threads at a time. If we use the regular synchronized pattern, we have to either
  // notify randomly, or if we try creating condition variables not tied to a shared lock, they
  // won't work together properly, and we see things like deadlocks.
  private val lock = new ReentrantLock()
  private var occupiedSlots: Int = 0

  private case class ThreadInfo(priority: T, condition: Condition, numPermits: Int) {
    var signaled: Boolean = false
    var interrupted: Boolean = false
  }

  // We expect a relatively small number of threads to be contending for this lock at any given
  // time, therefore we are not concerned with the insertion/removal time complexity.
  private val waitingQueue: PriorityQueue[ThreadInfo] = PriorityQueue()(Ordering.by(_.priority))

  def this()(implicit ordering: Ordering[T]) = this(PrioritySemaphore.DEFAULT_MAX_PERMITS)(ordering)

  def tryAcquire(numPermits: Int, priority: T): Boolean = {
    lock.lock()
    try {
      if (waitingQueue.nonEmpty && ordering.gt(waitingQueue.head.priority, priority)) {
        false
      } else if (!canAcquire(numPermits)) {
        false
      } else {
        commitAcquire(numPermits)
        true
      }
    } finally {
      lock.unlock()
    }
  }

  def acquire(numPermits: Int, priority: T): Unit = {
    lock.lock()
    val condition = lock.newCondition()
    val info = ThreadInfo(priority, condition, numPermits)
    try {
      if (canAcquire(numPermits)) {
        commitAcquire(numPermits)
      } else {
        waitingQueue.enqueue(info)
        while (!info.signaled) {
          info.condition.await()
        }
      }
    } catch {
      case _: InterruptedException =>
        info.interrupted = true
    } finally {
      lock.unlock()
    }
  }

  private def commitAcquire(numPermits: Int): Unit = {
    occupiedSlots += numPermits
  }

  def release(numPermits: Int): Unit = {
    lock.lock()
    try {
      occupiedSlots -= numPermits
      // acquire and wakeup for all threads that now have enough permits
      var done = false
      while (!done && waitingQueue.nonEmpty) {
        val nextThread = waitingQueue.head
        if (nextThread.interrupted) {
          waitingQueue.dequeue()
        } else if (canAcquire(nextThread.numPermits)) {
          val popped = waitingQueue.dequeue()
          assert(popped eq nextThread)
          commitAcquire(nextThread.numPermits)
          nextThread.signaled = true
          nextThread.condition.signal()
        } else {
          done = true
        }
      }
    } finally {
      lock.unlock()
    }
  }

  private def canAcquire(numPermits: Int): Boolean = {
    occupiedSlots + numPermits <= maxPermits
  }

}
