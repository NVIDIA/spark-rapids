/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

import scala.annotation.tailrec
import scala.collection.mutable

import ai.rapids.cudf.CudfColumnSizeOverflowException
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.{CpuRetryOOM, CpuSplitAndRetryOOM, GpuRetryOOM, GpuSplitAndRetryOOM, RmmSpark, RmmSparkThreadState}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

object RmmRapidsRetryIterator extends Logging {

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
   * @param input       an iterator of T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: Iterator[T],
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    val attemptIter = new AutoCloseableAttemptSpliterator(input, fn, splitPolicy)
    new RmmRapidsRetryAutoCloseableIterator(attemptIter)
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
   * @param input       a single item T
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   * @param fn          the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return an iterator of K
   */
  def withRetry[T <: AutoCloseable, K](
      input: T,
      splitPolicy: T => Seq[T])
      (fn: T => K): Iterator[K] = {
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(input), fn, splitPolicy)
    new RmmRapidsRetryAutoCloseableIterator(attemptIter)
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
   * @param input a single item T
   * @param fn    the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: T)
      (fn: T => K): K = {
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(input), fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
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
   * @param input a single item T
   * @param fn    the work to perform. Takes T and produces an output K
   * @tparam T element type that must be AutoCloseable (likely `SpillableColumnarBatch`)
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[T <: AutoCloseable, K](
      input: Seq[T])
      (fn: Seq[T] => K): K = {
    val wrapped = AutoCloseableSeqInternal(input)
    val attemptIter = new AutoCloseableAttemptSpliterator(
      SingleItemAutoCloseableIteratorInternal(wrapped), fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
  }

  /**
   * no-input withRetryNoSplit. This helper calls a function `fn` retrying the call if needed.
   * The result is a single item of type K.
   *
   * The expectation when code enters `withRetryNoSplit` is that all of the caller's data is
   * spillable already, allowing the thread to be blocked, and its data eventually spilled
   * because of other higher priority work.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @param fn the work to perform. It is a function that takes nothing and produces K
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRetryNoSplit[K](fn: => K): K = {
    val attemptIter = new NoInputSpliterator(fn)
    drainSingleWithVerification(
      new RmmRapidsRetryAutoCloseableIterator(attemptIter))
  }

  /**
   * Returns a tuple of (shouldRetry, shouldSplit, isFromGpuOom) depending the exception
   * passed
   */
  private def isRetryOrSplitAndRetry(ex: Throwable): (Boolean, Boolean, Boolean) = {
    ex match {
      case _: GpuRetryOOM => (true, false, true)
      case _: CpuRetryOOM => (true, false, false)
      case _: GpuSplitAndRetryOOM => (true, true, true)
      case _: CpuSplitAndRetryOOM => (true, true, false)
      case _ => (false, false, false)
    }
  }

  /**
   * Returns a tuple of (causedByRetry, causedBySplit, ifFromGpuoom) depending the exception
   * passed
   */
  private def causedByRetryOrSplit(ex: Throwable): (Boolean, Boolean, Boolean) = {
    var current = ex
    var causedByRetry = false
    var causedBySplit = false
    var isFromGpuOom = false
    // check if there is a hidden retry or split OOM
    while (current != null && !causedByRetry) {
      current = current.getCause()
      val (isRetry, isSplit, isGpuOom) = isRetryOrSplitAndRetry(current)
      causedByRetry = isRetry
      causedBySplit = causedBySplit || isSplit
      isFromGpuOom = isGpuOom
    }
    (causedByRetry, causedBySplit, isFromGpuOom)
  }

  private def isColumnSizeOverflow(ex: Throwable): Boolean =
    ex.isInstanceOf[CudfColumnSizeOverflowException]

  @tailrec
  private def isOrCausedByColumnSizeOverflow(ex: Throwable): Boolean = {
    ex != null && (isColumnSizeOverflow(ex) || isOrCausedByColumnSizeOverflow(ex.getCause))
  }

  /**
   * withRestoreOnRetry for Retryable. This helper function calls `fn` with no input and
   * returns the result. In the event of an OOM Retry exception, it calls the restore() method
   * of the input and then throws the oom exception.  This is intended to be used within the `fn`
   * of one of the withRetry* functions.  It provides an opportunity to reset state in the case
   * of a retry.
   *
   * @param r  a single item T
   * @param fn the work to perform. Takes no input and produces K
   * @tparam T element type that must be a `Retryable` subclass
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRestoreOnRetry[T <: Retryable, K](r: T)(fn: => K): K = {
    try {
      fn
    } catch {
      case ex: Throwable =>
        // Only restore on retry exceptions
        val (topLevelIsRetry, _, _) = isRetryOrSplitAndRetry(ex)
        if (topLevelIsRetry || causedByRetryOrSplit(ex)._1 || isOrCausedByColumnSizeOverflow(ex)) {
          r.restore()
        }
        throw ex
    }
  }

  /**
   * withRestoreOnRetry for Retryable. This helper function calls `fn` with no input and
   * returns the result. In the event of an OOM Retry exception, it calls the restore() method
   * of the input and then throws the oom exception.  This is intended to be used within the `fn`
   * of one of the withRetry* functions.  It provides an opportunity to reset state in the case
   * of a retry.
   *
   * @param r  a Seq of item T
   * @param fn the work to perform. Takes no input and produces K
   * @tparam T element type that must be a `Retryable` subclass
   * @tparam K `fn` result type
   * @return a single item of type K
   */
  def withRestoreOnRetry[T <: Retryable, K](r: Seq[T])(fn: => K): K = {
    try {
      fn
    } catch {
      case ex: Throwable =>
        // Only restore on retry exceptions
        val (topLevelIsRetry, _, _) = isRetryOrSplitAndRetry(ex)
        if (topLevelIsRetry || causedByRetryOrSplit(ex)._1 || isOrCausedByColumnSizeOverflow(ex)) {
          r.foreach(_.restore())
        }
        throw ex
    }
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
   *
   * @param ts the Seq to wrap
   * @tparam T the type of the items in `ts`
   */
  private case class AutoCloseableSeqInternal[T <: AutoCloseable](ts: Seq[T])
      extends Seq[T] with AutoCloseable {
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
   *
   * @param ts the AutoCloseable item to close if this iterator hasn't been drained
   * @tparam T the type of `ts`, must be AutoCloseable
   */
  private case class SingleItemAutoCloseableIteratorInternal[T <: AutoCloseable](ts: T)
      extends Iterator[T] with AutoCloseable {

    private var wasCalledSuccessfully = false

    override def hasNext: Boolean = !wasCalledSuccessfully

    override def next(): T = {
      wasCalledSuccessfully = true
      ts
    }

    override def close(): Unit = {
      if (!wasCalledSuccessfully) {
        ts.close()
      }
    }
  }

  /**
   * A trait that defines an iterator of type K that supports two extra things:
   * the ability to split its input, and the ability to close itself.
   *
   * Note that the input's type is not defined and is not relevant to this trait.
   *
   * @tparam K the resulting type
   */
  trait Spliterator[K] extends Iterator[K] with AutoCloseable {
    override def hasNext: Boolean

    /**
     * Split is a function that is invoked by `RmmRapidsRetryIterator` when `GpuSplitAndRetryOOM`
     * or `CpuSplitAndRetryOOM`
     * is thrown. This function is implemented by `Spliterator` classes to attempt to handle
     * this exception by reducing the size of attempts (the thing that `.next` is
     * using as an input), usually by splitting a batch in half by number of rows, or
     * splitting a collection of batches into smaller collections to be attempted separately,
     * likely reducing GPU memory that needs to be manifested while calling `.next`.
     *
     * @param isFromGpuOom true if the split happened because of a GPU OOM. Otherwise it was a
     *                     CPU off heap OOM.
     */
    def split(isFromGpuOom: Boolean): Unit

    override def next(): K

    override def close(): Unit
  }

  /**
   * A spliterator that doesn't take any inputs, hence it is "empty", and it doesn't know
   * how to split. It allows the caller to call the function `fn` once on `next`.
   *
   * @param fn the work to perform. It is a function that takes nothing and produces K
   * @tparam K the resulting type
   */
  class NoInputSpliterator[K](fn: => K) extends Spliterator[K] {
    private var wasCalledSuccessfully: Boolean = false

    override def hasNext: Boolean = !wasCalledSuccessfully

    override def split(isFromGpuOom: Boolean): Unit = {
      if (isFromGpuOom) {
        throw new GpuSplitAndRetryOOM("GPU OutOfMemory: could not split inputs and retry")
      } else {
        throw new CpuSplitAndRetryOOM("CPU OutOfMemory: could not split inputs and retry")
      }
    }

    override def next(): K = {
      RmmSpark.currentThreadStartRetryBlock()
      val res = try {
        fn
      } finally {
        RmmSpark.currentThreadEndRetryBlock()
      }
      wasCalledSuccessfully = true
      res
    }

    override def close(): Unit = {}
  }

  /**
   * A spliterator that takes an input iterator of auto closeable T, and a function `fn`
   * that can map `T` to `K`, with an additional `splitPolicy` that can split `T` into a
   * `Seq[T]`
   *
   * It assumes the type T is AutoCloseable, and that if a split policy is specified, that it
   * is capable of handling splitting one T into a sequence of them.
   *
   * When an attempt to invoke function `fn` is successful, the item T in `input` will be
   * closed. In the case of a failure, all attempts will be closed. It is the responsibility
   * of the caller to close any remaining items in `input` that have not been attempted.
   *
   * `fn` must be idempotent: this is a requirement because we may call `fn` multiple times
   * while handling retries.
   *
   * @tparam T element type that must be AutoCloseable
   * @tparam K `fn` result type
   * @param input       an iterator of T
   * @param fn          a function that takes T and produces K
   * @param splitPolicy a function that can split an item of type T into a Seq[T]. The split
   *                    function must close the item passed to it.
   */
  class AutoCloseableAttemptSpliterator[T <: AutoCloseable, K](
      input: Iterator[T],
      fn: T => K,
      splitPolicy: T => Seq[T])
      extends Spliterator[K] {
    def this(input: Iterator[T], fn: T => K) =
      this(input, fn, null)

    private def closeInternal(): Unit = {
      attemptStack.safeClose()
      attemptStack.clear()
    }

    // Don't install the callback if in a unit test
    private val onClose = Option(TaskContext.get()).map { tc =>
      onTaskCompletion(tc) {
        closeInternal()
      }
    }

    protected val attemptStack = new mutable.ArrayStack[T]()

    override def hasNext: Boolean = input.hasNext || attemptStack.nonEmpty

    override def split(isFromGpuOom: Boolean): Unit = {
      // If `split` OOMs, we are already the last thread standing
      // there is likely not much we can do, and for now we don't handle
      // this OOM
      if (splitPolicy == null) {
        if (isFromGpuOom) {
          throw new GpuSplitAndRetryOOM("GPU OutOfMemory: could not split inputs and retry")
        } else {
          throw new CpuSplitAndRetryOOM("CPU OutOfMemory: could not split inputs and retry")
        }
      }
      // splitPolicy must take ownership of the argument
      val splitted = splitPolicy(attemptStack.pop())
      // the splitted sequence needs to be inserted in reverse order
      // so we try the first item first.
      splitted.reverse.foreach(attemptStack.push)
    }

    override def next(): K = {
      if (attemptStack.isEmpty && input.hasNext) {
        attemptStack.push(input.next())
      }
      val popped = attemptStack.head
      RmmSpark.currentThreadStartRetryBlock()
      val res = try {
        fn(popped)
      } finally {
        RmmSpark.currentThreadEndRetryBlock()
      }
      attemptStack.pop().close()
      if (attemptStack.isEmpty && !input.hasNext) {
        // No need to call the onClose because the attemptStack is empty
        onClose.foreach(_.removeCallback())
      }
      res
    }

    override def close(): Unit = {
      onClose.map(_.removeAndCall()).getOrElse(closeInternal())
    }
  }

  /**
   * RmmRapidsRetryAutoCloseableIterator exposes an iterator that can retry work,
   * specified by `fn`, abstracting away the retry specifics. Elements passed to this iterator
   * must be AutoCloseable.
   *
   * It assumes the type T is AutoCloseable, and that if a split policy is specified, that it
   * is capable of handling splitting one T into a sequence of them.
   *
   * @tparam T element type that must be AutoCloseable
   * @tparam K result type
   * @param attemptIter an iterator of T
   */
  class RmmRapidsRetryAutoCloseableIterator[T <: AutoCloseable, K](
      attemptIter: Spliterator[K])
      extends RmmRapidsRetryIterator[T, K](attemptIter) {

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
          attemptIter.close()
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
   * @param attemptIter an iterator of T
   */
  class RmmRapidsRetryIterator[T, K](attemptIter: Spliterator[K])
      extends Iterator[K] {
    // We want to be sure that retry will work in all cases
    TaskRegistryTracker.registerThreadForRetry()
    // used to figure out if we should inject an OOM (only for tests)
    private val config = Option(SQLConf.get).map(new RapidsConf(_))

    // this is true if an OOM was injected (only for tests)
    private var injectedOOM = false
    // this is true if the OOM was cleared after it was injected (only for tests)
    private var injectedOOMCleared = false

    override def hasNext: Boolean = attemptIter.hasNext

    private def clearInjectedOOMIfNeeded(): Unit = {
      if (injectedOOM && !injectedOOMCleared) {
        val threadId = RmmSpark.getCurrentThreadId
        // if for some reason we don't throw, or we throw something that isn't a GpuRetryOOM
        // or CpuRetryOOM
        // we want to remove the retry we registered before we leave the withRetry block.
        // If the thread is in an UNKNOWN state, then it is already cleared.
        if (RmmSpark.getStateOf(threadId) != RmmSparkThreadState.UNKNOWN) {
          RmmSpark.forceRetryOOM(threadId, 0)
        }
        injectedOOMCleared = true
      }
    }

    override def next(): K = {
      // this is set on the first exception, and we add suppressed if there are others
      // during the retry attempts
      var lastException: Throwable = null
      var firstAttempt: Boolean = true
      var result: Option[K] = None
      var doSplit = false
      var isFromGpuOom = true
      while (result.isEmpty && attemptIter.hasNext) {
        if (!firstAttempt) {
          // call thread block API
          try {
            RmmSpark.blockThreadUntilReady()
          } catch {
            case _: GpuSplitAndRetryOOM =>
              doSplit = true
              isFromGpuOom = true
            case _: CpuSplitAndRetryOOM =>
              doSplit = true
              isFromGpuOom = false
          }
        }
        firstAttempt = false
        if (doSplit) {
          attemptIter.split(isFromGpuOom)
        }
        doSplit = false
        try {
          // call the user's function
          config.foreach {
            case rapidsConf if !injectedOOM && rapidsConf.testRetryOOMInjectionMode.numOoms > 0 =>
              injectedOOM = true
              // ensure we have associated our thread with the running task, as
              // `forceRetryOOM` requires a prior association.
              RmmSpark.currentThreadIsDedicatedToTask(TaskContext.get().taskAttemptId())
              val injectConf = rapidsConf.testRetryOOMInjectionMode
              if (injectConf.withSplit) {
                RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId,
                          injectConf.numOoms,
                          injectConf.oomInjectionFilter.ordinal,
                          injectConf.skipCount)
              } else {
                RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId,
                  injectConf.numOoms,
                  injectConf.oomInjectionFilter.ordinal,
                  injectConf.skipCount)
              }
            case _ => ()
          }
          result = Some(attemptIter.next())
          clearInjectedOOMIfNeeded()
        } catch {
          case ex: Throwable =>
            // handle a retry as the top-level exception
            val (topLevelIsRetry, topLevelIsSplit, isGpuOom) = isRetryOrSplitAndRetry(ex)
            doSplit = topLevelIsSplit
            isFromGpuOom = isGpuOom

            // handle any retries that are wrapped in a different top-level exception
            var causedByRetry = false
            if (!topLevelIsRetry) {
              val (cbRetry, cbSplit, isGpuOom) = causedByRetryOrSplit(ex)
              causedByRetry = cbRetry
              doSplit = doSplit || cbSplit
              isFromGpuOom = isGpuOom
            }

            clearInjectedOOMIfNeeded()

            // make sure we add any prior exceptions to this one as causes
            if (lastException != null && lastException != ex) {
              ex.addSuppressed(lastException)
            }
            lastException = ex

            if (!topLevelIsRetry && !causedByRetry) {
              if (isOrCausedByColumnSizeOverflow(ex)) {
                // CUDF column size overflow? Attempt split-retry.
                doSplit = true
              } else {
                // we want to throw early here, since we got an exception
                // we were not prepared to handle
                throw lastException
              }
            }
          // else another exception wrapped a retry. So we are going to try again
        }
      }
      if (result.isEmpty) {
        // then lastException must be set, throw it.
        throw lastException
      }
      result.get
    }
  }

  /**
   * Common split function from a single SpillableColumnarBatch to a sequence of them,
   * that tries to split the input into two chunks. If the input cannot be split in two,
   * because we are down to 1 row, this function throws `GpuSplitAndRetryOOM` or
   * `CpuSplitAndRetryOOM`.
   *
   * Note how this function closes the input `spillable` that is passed in.
   *
   * @return a Seq[SpillableColumnarBatch] with 2 elements.
   */
  def splitSpillableInHalfByRows: SpillableColumnarBatch => Seq[SpillableColumnarBatch] = {
    (spillable: SpillableColumnarBatch) => {
      withResource(spillable) { _ =>
        val toSplitRows = spillable.numRows()
        if (toSplitRows <= 1) {
          throw new GpuSplitAndRetryOOM(
            s"GPU OutOfMemory: a batch of $toSplitRows cannot be split!")
        }
        val (firstHalf, secondHalf) = withResource(spillable.getColumnarBatch()) { src =>
          withResource(GpuColumnVector.from(src)) { tbl =>
            val splitIx = (tbl.getRowCount / 2).toInt
            withResource(tbl.contiguousSplit(splitIx)) { cts =>
              val tables = cts.map(_.getTable)
              withResource(tables.safeMap(GpuColumnVector.from(_, spillable.dataTypes))) {
                batches =>
                  val spillables = batches.safeMap { b =>
                    SpillableColumnarBatch(
                      GpuColumnVector.incRefCounts(b),
                      SpillPriorities.ACTIVE_BATCHING_PRIORITY)
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
        }
        Seq(firstHalf, secondHalf)
      }
    }
  }

  private def splitTargetSizeInHalfInternal(
      target: AutoCloseableTargetSize, isGpu: Boolean): Seq[AutoCloseableTargetSize] = {
    withResource(target) { _ =>
      val newTarget = target.targetSize / 2
      if (newTarget < target.minSize) {
        if (isGpu) {
          throw new GpuSplitAndRetryOOM(
            s"GPU OutOfMemory: targetSize: ${target.targetSize} cannot be split further!" +
                s" minimum: ${target.minSize}")
        } else {
          throw new CpuSplitAndRetryOOM(
            s"CPU OutOfMemory: targetSize: ${target.targetSize} cannot be split further!" +
                s" minimum: ${target.minSize}")
        }
      }
      Seq(AutoCloseableTargetSize(newTarget, target.minSize))
    }
  }

  /**
   * A common split function for an AutoCloseableTargetSize, which just divides the target size
   * in half, and creates a seq with just one element representing the new target size.
   * @return a Seq[AutoCloseableTargetSize] with 1 element.
   * @throws GpuSplitAndRetryOOM if it reaches the split limit.
   */
  def splitTargetSizeInHalfGpu: AutoCloseableTargetSize => Seq[AutoCloseableTargetSize] =
    (target: AutoCloseableTargetSize) => {
      splitTargetSizeInHalfInternal(target, true)
  }

  /**
   * A common split function for an AutoCloseableTargetSize, which just divides the target size
   * in half, and creates a seq with just one element representing the new target size.
   *
   * @return a Seq[AutoCloseableTargetSize] with 1 element.
   * @throws CpuSplitAndRetryOOM if it reaches the split limit.
   */
  def splitTargetSizeInHalfCpu: AutoCloseableTargetSize => Seq[AutoCloseableTargetSize] =
    (target: AutoCloseableTargetSize) => {
      splitTargetSizeInHalfInternal(target, false)
  }
}

/**
 * This is a wrapper that turns a target size into an autocloseable to allow it to be used
 * in withRetry blocks.  It is intended to be used to help with cases where the split calculation
 * happens inside the retry block, and depends on the target size.  On a `GpuSplitAndRetryOOM` or
 * `CpuSplitAndRetryOOM`, a split policy like `splitTargetSizeInHalfGpu` or
 * `splitTargetSizeInHalfCpu` can be used to retry the block with a smaller target size.
 */
case class AutoCloseableTargetSize(targetSize: Long, minSize: Long) extends AutoCloseable {
  override def close(): Unit = ()
}
