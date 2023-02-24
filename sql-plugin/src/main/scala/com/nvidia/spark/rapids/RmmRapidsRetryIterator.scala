/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{RetryOOM, RmmSpark, SplitAndRetryOOM}

object RmmRapidsRetryIterator extends Arm {

  /**
   * withRetry for Iterator[T]. This helper calls a function `fn` as it takes
   * elements from the iterator given in `input`, and it can retry the work in `fn`,
   * and optionally split items into smaller chunks. The `splitPolicy` function must
   * close the item passed to it. The resulting iterator may or
   * may not have the same number of elements as the source iterator.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetry` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. Elements of `input` not manifested are the responsibility of the caller to
   * close!
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input an iterator of T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: Iterator[T],
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    new RmmRapidsRetryAutoCloseableIterator(input, fn, splitPolicy)
  }

  /**
   * withRetry for T. This helper calls a function `fn` with the single input `T`,
   * and it can retry the work in `fn` and optionally split `input` into smaller chunks.
   * The resulting iterator may be 1 element, if successful on the first attempt or retry,
   * or it could be multiple if splits were required.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetry` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception `input` is also closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input a single item T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: T,
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    new RmmRapidsRetryAutoCloseableIterator(
      SingleItemAutoCloseableIteratorInternal(input),
      fn,
      splitPolicy)
  }

  /**
   * withRetryNoSplit for T. This helper calls a function `fn` with the `input`, and it will
   * retry the call to `fn` if needed. This does not split the
   * input into multiple chunks. The result is a single item of type K.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetryNoSplit` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception `input` is also closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input       a single item T
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: T)
      (fn: T => K): K = {
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(
        SingleItemAutoCloseableIteratorInternal(input),
        fn))
  }

  /**
   * withRetryNoSplit for Seq[T]. This helper calls a function `fn` with the whole sequence
   * given in `input`, and it will retry the call to `fn` if needed. This does not split the
   * input into multiple chunks. The result is a single item of type K.
   *
   * While T is a generic `AutoCloseable` subclass most of the time we expect it to be
   * `SpillableColumnarBatch`. The expectation when code enters `withRetryNoSplit` is that
   * all of the caller's data is spillable already, allowing the thread to be blocked, and
   * its data eventually spilled because of other higher priority work.
   *
   * This function will close the elements of `input` as `fn` is successfully
   * invoked. In the event of an unhandled exception, all elements of `input` are closed.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param input       a single item T
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: Seq[T])
      (fn: Seq[T] => K): K = {
    val wrapped = AutoCloseableSeqInternal(input)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(
        SingleItemAutoCloseableIteratorInternal(wrapped),
        fn))
  }

  /**
   * Helper method to drain an iterator and ensuring that it was non-empty
   * and it had a single item in it.
   */
  private def drainSingleWithVerification[K](it: Iterator[K]): K = {
    require(it.hasNext,
      "Couldn't drain a single item with a closed iterator!")
    val item = it.next()
    require(!it.hasNext,
      "Multiple items found in the source iterator but one expected!")
    item
  }

  /**
   * AutoCloseable wrapper on Seq[T], returning a Seq[T] that can be closed.
   * @param ts the Seq to wrap
   * @tparam T the type of the items in `ts`
   */
  private case class AutoCloseableSeqInternal[T <: AutoCloseable](ts: Seq[T])
      extends Seq[T] with AutoCloseable{
    override def close(): Unit = {
      ts.foreach(_.safeClose())
    }

    override def length: Int = ts.length

    override def iterator: Iterator[T] = ts.iterator

    override def apply(idx: Int): T = ts.apply(idx)
  }

  /**
   * An iterator of a single item that is able to close if .next
   * has not been called on it.
   * @param ts the AutoCloseable item to close if this iterator hasn't been drained
   * @tparam T the type of `ts`, must be AutoCloseable
   */
  private case class SingleItemAutoCloseableIteratorInternal[T <: AutoCloseable](ts: T)
    extends Iterator[T] with AutoCloseable {

    private var wasCalled = false
    override def hasNext: Boolean = !wasCalled
    override def next(): T = {
      wasCalled = true
      ts
    }
    override def close(): Unit = {
      if (!wasCalled) {
        ts.close()
      }
    }
  }

  /**
   * RmmRapidsRetryAutoCloseableIterator exposes an iterator that can retry work,
   * specified by `fn`, abstracting away the retry specifics. Elements passed to this iterator
   * must be AutoCloseable.
   *
   * It assumes the type K is AutoCloseable, and that if a split policy is specified, that it
   * is capable of handling splitting one K into a sequence of them.
   *
   * When an attempt to invoke function `fn` is successful, the item K in `input` will be
   * closed. In the case of a failure, all attempts will be closed. It is the responsibility
   * of the caller to close any remaining items in `input` that have not been attempted.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @tparam T element type that must be AutoCloseable
   * @tparam K `fn` result type
   * @param input an iterator of T
   * @param fn a function that takes T and produces K
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   */
  class RmmRapidsRetryAutoCloseableIterator[T <: AutoCloseable, K](
      input: Iterator[T],
      fn: T => K,
      splitPolicy: T => Seq[T])
      extends RmmRapidsRetryIterator(input, fn, splitPolicy)
        with Arm {

    def this(
        input: Iterator[T],
        fn: T => K) = {
      this(input, fn, null)
    }

    override def invokeFn(k: T): K = {
      val res = super.invokeFn(k)
      k.close() // close `k` only if we didn't throw from `invokeFn`
      res
    }

    override def hasNext: Boolean = super.hasNext

    override def next(): K = {
      if (!hasNext) {
        throw new NoSuchElementException("Closed called on an empty iterator.")
      }
      try {
        super.next()
      } catch {
        case t: Throwable =>
          // exception occurred while trying to handle this retry
          // we close our attempts (which includes the item we last attempted)
          attemptStack.safeClose(t)
          throw t
      }
    }
  }

  /**
   * RmmRapidsRetryIterator exposes an iterator that can retry work,
   * specified by `fn`, abstracting away the retry specifics.
   *
   * @tparam T element type
   * @tparam K `fn` result type
   * @param input an iterator of T
   * @param fn a function that takes T and produces K
   * @param splitPolicy an optional function that can split T into a Seq[T], if provided
   *                    `splitPolicy` must take ownership of items passed to it.
   */
  class RmmRapidsRetryIterator[T, K](
      input: Iterator[T],
      fn: T => K,
      splitPolicy: T => Seq[T]) extends Iterator[K] with Arm {
    def this(input: Iterator[T], fn: T => K) =
      this(input, fn, null)

    protected val attemptStack = new mutable.ArrayStack[T]()

    override def hasNext: Boolean = input.hasNext || attemptStack.nonEmpty

    protected def invokeFn(k: T): K = {
      fn(k)
    }

    override def next(): K = {
      // this is set on the first exception, and we add suppressed if there are others
      // during the retry attempts
      var lastException: Throwable = null
      var firstAttempt: Boolean = true
      var result: Option[K] = None
      var doSplit = false
      while (result.isEmpty && (attemptStack.nonEmpty || input.hasNext)) {
        if (attemptStack.isEmpty && input.hasNext) {
          attemptStack.push(input.next())
        }
        if (!firstAttempt) {
          // call thread block API
          RmmSpark.blockThreadUntilReady()
        }
        firstAttempt = false
        val popped = attemptStack.pop()
        val attempt =
          if (doSplit) {
            // If `split` OOMs, we are already the last thread standing
            // there is likely not much we can do, and for now we don't handle
            // this OOM
            val splitted = splitAndClose(popped)
            // the splitted sequence needs to be inserted in reverse order
            // so we try the first item first.
            splitted.reverse.foreach(attemptStack.push)
            attemptStack.pop()
          } else {
            popped
          }
        doSplit = false
        try {
          // call the user's function
          result = Some(invokeFn(attempt))
        } catch {
          case retryOOM: RetryOOM =>
            if (lastException != null) {
              retryOOM.addSuppressed(lastException)
            }
            lastException = retryOOM
            // put it back
            attemptStack.push(attempt)
          case splitAndRetryOOM: SplitAndRetryOOM => // we are the only thread
            if (lastException != null) {
              splitAndRetryOOM.addSuppressed(lastException)
            }
            lastException = splitAndRetryOOM
            // put it back
            attemptStack.push(attempt)
            doSplit = true
          case other: Throwable =>
            if (lastException != null) {
              other.addSuppressed(lastException)
            }
            lastException = other
            // put this attempt back on our stack, so that it will be closed
            attemptStack.push(attempt)

            // we want to throw early here, since we got an exception
            // we were not prepared to handle
            throw lastException
        }
      }
      result.get
    }

    // It is assumed that OOM in this function is not handled.
    private def splitAndClose(item:T): Seq[T] = {
      if (splitPolicy == null) {
        // put item into the attempt stack, to be closed
        attemptStack.push(item)
        throw new OutOfMemoryError(
          "Attempted to handle a split, but was not initialized with a splitPolicy.")
      }
      // splitPolicy must close `item`
      splitPolicy(item)
    }
  }

  /**
   * Common split function from a single SpillableColumnarBatch to a sequence of them,
   * that tries to split the input into two chunks. If the input cannot be split in two,
   * because we are down to 1 row, this function throws `OutOfMemoryError`.
   *
   * Note how this function closes the input `spillable` that is passed in.
   *
   * @return a Seq[SpillableColumnarBatch] with 2 elements.
   */
  def splitSpillableInHalfByRows: SpillableColumnarBatch => Seq[SpillableColumnarBatch] = {
    (spillable: SpillableColumnarBatch) => {
      val spillCallback = spillable.getSpillCallback
      withResource(spillable) { _ =>
        val toSplitRows = spillable.numRows()
        if (toSplitRows <= 1) {
          throw new OutOfMemoryError(s"A batch of $toSplitRows cannot be split!")
        }
        val (firstHalf, secondHalf) = withResource(spillable.getColumnarBatch()) { src =>
          withResource(GpuColumnVector.from(src)) { tbl =>
            val splitIx = (tbl.getRowCount / 2).toInt
            withResource(tbl.contiguousSplit(splitIx)) { cts =>
              val tables = cts.map(_.getTable)
              val batches = tables.safeMap(GpuColumnVector.from(_, spillable.dataTypes))
              val spillables = batches.safeMap { b =>
                SpillableColumnarBatch(
                  b,
                  SpillPriorities.ACTIVE_BATCHING_PRIORITY,
                  spillCallback)
              }
              closeOnExcept(spillables) { _ =>
                require(spillables.length == 2,
                  s"Contiguous split returned ${spillables.length} tables but two were " +
                      s"expected!")
              }
              (spillables.head, spillables.last)
            }
          }
        }
        Seq(firstHalf, secondHalf)
      }
    }
  }
}
