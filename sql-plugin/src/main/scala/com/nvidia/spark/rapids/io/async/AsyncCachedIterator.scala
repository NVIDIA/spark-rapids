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

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

import com.nvidia.spark.rapids.{GpuMetric, NoopMetric}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * The interface of AsyncCachedIterator. Comparing to the iterator, it breaks down the `next`
 * method into two methods: `waitForNext` and `takeNext`. With these two methods, the interface
 * can control the internal buffer more precisely than normal Iterators, to control the lifecycle
 * of `T` more precisely.
 *
 * AsyncCachedIterator is specialized for the scenario of single producer <-> single consumer. And
 * all of its methods should be only called by the consumer.
 */
trait AsyncCachedIterator[T] extends Iterator[T] {
  // Wait until the producer completes the production of the element for the next consumption.
  // It means that the waiting would actually take place when the producer buffers nothing for now.
  // The method works based on the assumption there is only one consumer in current.
  def waitForNext(): Unit

  // Consume the next element which should be already cached in the internal buffer.
  def takeNext(): T

  override def next(): T = {
    waitForNext()
    takeNext()
  }
}

/**
 * The dummy implementation simply bypasses the base iterator.
 */
class DummySyncProducer[T](base: Iterator[T]) extends AsyncCachedIterator[T] {

  private lazy val deck = mutable.Queue.empty[T]

  override def hasNext: Boolean = base.hasNext || deck.nonEmpty

  override def waitForNext(): Unit = if (deck.isEmpty) {
    deck.enqueue(base.next())
  }

  override def takeNext(): T = {
    require(deck.nonEmpty, "The next element is not ready")
    deck.dequeue()
  }
}

/**
 * The implementation of AsyncCacheIterator which wraps base iterator as async producer and
 * caches the result with a simple RingBuffer whose capacity is strictly limited.
 *
 * We do not use BlockingQueue as RingBuffer, because it may stack N + 1 elements for the queue of
 * capacity(N), since the blocking occurs at the end of the production (when we put the element to
 * the queue) rather than at the beginning of the production. Considering that each element is
 * usually very large, we would like to minimize the capacity of buffer for the most time. In
 * order to support the real size=1 buffer, we implement a very simple RingBuffer.
 */
class AsyncCacheIteratorImpl[T](
    base: Iterator[T],
    capacity: Int = 1,
    waitForUpstream: Option[GpuMetric] = None) extends AsyncCachedIterator[T] with Logging {

  // Mark if there is in-progress element being produced in producerThread
  @volatile private var baseHasNext: Boolean = _

  // The two pointers of the RingBuffer. For the simplicity, indices themselves increment
  // monotonously. And the actual index of ring buffer is `index % capacity`.
  @volatile private var readIndex: Int = 0
  @volatile private var writeIndex: Int = 0
  // Record the total batches. Also serve as the end mask of both producer and consumer.
  @volatile private var totalElements: Int = 0

  // This lock guarantees that the status change of baseHasNext and the check of baseHasNext
  // can NOT be executed currently.
  private lazy val isInProgressLock = new ReentrantLock()
  // The lock synchronizes the critical status of buffer (EMPTY | FULL)
  private lazy val bufferLock = new ReentrantLock()
  // Asleep the consumerThread if the buffer is empty.
  // Awake the consumerThread as soon as the next element being ready.
  private lazy val emptyCond = bufferLock.newCondition()
  // Asleep the producerThread if the buffer is full.
  // Awake the producerThread as soon as elements being consumed.
  private lazy val fullCond = bufferLock.newCondition()

  private lazy val buffer = Array.ofDim[Either[Throwable, T]](capacity)

  private def isBufferEmpty: Boolean = writeIndex == readIndex
  private def isBufferFull: Boolean = writeIndex - readIndex == capacity

  private var producerThread: Thread = _
  private val taskAttId = TaskContext.get().taskAttemptId()

  private val upstreamNs = waitForUpstream.getOrElse(NoopMetric)

  private lazy val producer: Runnable = new Runnable {
    // This context will be got in the main Thread during the initialization of `producer`
    private val taskContext: TaskContext = TaskContext.get()
    require(taskContext != null, "must work inside Spark Executor")

    override def run(): Unit = {
      require(baseHasNext, "The base iterator should be guaranteed as nonEmpty one")
      TrampolineUtil.setTaskContext(taskContext)
      try {
        do {
          // Wait for empty slot of RingBuffer
          if (isBufferFull) {
            lockGuard(bufferLock) {
              if (isBufferFull) fullCond.await()
            }
          }
          // Trigger the base iterator
          buffer(writeIndex % capacity) = Right(base.next())
          // Update the writeIndex and try to awake the consumer thread which calls waitForNext
          lockGuard(isInProgressLock) {
            baseHasNext = false
            writeIndex += 1
          }
          lockGuard(bufferLock) {
            emptyCond.signal()
          }
        } while (
          // Check hasNext at the end of the loop, since the first check was done in main thread
          lockGuard(isInProgressLock) {
            baseHasNext = base.hasNext
            baseHasNext
          }
        )
      } catch {
        case ex: Throwable =>
          // transfer the exception info to the main thread as an interrupted signal
          buffer(writeIndex % capacity) = Left(ex)
          lockGuard(isInProgressLock) {
            baseHasNext = false
            writeIndex += 1
          }
          throw new RuntimeException(ex)
      } finally {
        totalElements = writeIndex
        logInfo(s"[$taskAttId] finished all jobs($totalElements elements)")
        TrampolineUtil.unsetTaskContext()
      }
    }
  }

  override def hasNext: Boolean = {
    if (producerThread == null) {
      // Lazy Init of the async producer. Only swamp it if the base iterator is nonEmpty.
      upstreamNs.ns {
        baseHasNext = base.hasNext
      }
      if (baseHasNext) {
        producerThread = new Thread(producer, s"AsyncCacheProducer_$taskAttId")
        producerThread.start()
      }
      baseHasNext
    } else {
      // Firstly, check the buffer. If empty, then check if there is any in-progress element.
      !isBufferEmpty || {
        lockGuard(isInProgressLock) {
          // Need to check the buffer again.
          !isBufferEmpty || baseHasNext
        }
      }
    }
  }

  override def waitForNext(): Unit = {
    // Only wait if the buffer is empty now
    if (isBufferEmpty) {
      upstreamNs.ns {
        lockGuard(bufferLock) {
          if (isBufferEmpty) emptyCond.await()
        }
      }
    }
  }

  override def takeNext(): T = {
    require(!isBufferEmpty, "The next element is not ready")
    // Get the element from buffer directly, since the hasNext should be checked before
    buffer(readIndex % capacity) match {
      case Left(ex: Throwable) =>
        logError(s"[$taskAttId] AsyncCacheProducerImpl failed with exceptions")
        throw new RuntimeException(s"[$taskAttId] AsyncCacheProducerImpl", ex)
      case Right(ret) =>
        // Update the readIndex and try to awake the producer thread
        lockGuard(bufferLock) {
          readIndex += 1
          fullCond.signal()
        }
        logDebug(s"[$taskAttId] readIndex: $readIndex; writeIndex: $writeIndex")
        if (totalElements == readIndex) {
          logInfo(s"[$taskAttId] AsyncCacheProducerImpl exited")
        }
        ret
    }
  }

  private def lockGuard[R](lock: ReentrantLock)(f: => R): R = {
    try {
      lock.lock()
      f
    } finally {
      lock.unlock()
    }
  }
}
