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
  private val lock = new ReentrantLock()
  private var occupiedSlots: Int = 0

  private case class ThreadInfo(priority: T, condition: Condition)

  private val waitingQueue: PriorityQueue[ThreadInfo] = PriorityQueue()(Ordering.by(_.priority))

  def this()(implicit ordering: Ordering[T]) = this(PrioritySemaphore.DEFAULT_MAX_PERMITS)(ordering)

  def tryAcquire(numPermits: Int): Boolean = {
    lock.lock()
    try {
    if (canAcquire(numPermits)) {
      commitAcquire(numPermits)
      true
    } else {
      false
    }
    } finally {
      lock.unlock()
    }
  }

  def acquire(numPermits: Int, priority: T): Unit = {
    lock.lock()
    try {
      val condition = lock.newCondition()
      while (!canAcquire(numPermits)) {
        waitingQueue.enqueue(ThreadInfo(priority, condition))
        condition.await()
      }
      commitAcquire(numPermits)

    } finally {
      lock.unlock()
    }}

  private def commitAcquire(numPermits: Int): Unit = {
    occupiedSlots += numPermits
  }

  def release(numPermits: Int): Unit = {
    lock.lock()
    try {
      occupiedSlots -= numPermits
      if (waitingQueue.nonEmpty) {
        val nextThread = waitingQueue.dequeue()
        nextThread.condition.signal()
      }
    }
    finally {
      lock.unlock()
    }
  }

  private def canAcquire(numPermits: Int): Boolean = {
    occupiedSlots + numPermits <= maxPermits
  }

}
