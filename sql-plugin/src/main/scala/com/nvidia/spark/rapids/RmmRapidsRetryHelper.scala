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
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.{RetryOOM, RmmSpark, SplitAndRetryOOM}

import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * RmmRapidsRetryHelper is used to attempt work on the GPU with the ability to retry it,
 * abstracting the retry logic from the calling code.
 * @param input a SpillableColumnarBatch that we would like to attempt work with
 * @param splitPolicy a function that can split a SpillableColumnarBatch into multiple
 *                    spillable batches, can be set to null in case splits are not handled.
 * @param mergePolicy a function that can be used to merge SpillableColumnarBatches that had
 *                    been previously split, can be set to null in case merging is not handled
 *                    or not required.
 */
class RmmRapidsRetryHelper(
    input: SpillableColumnarBatch,
    splitPolicy: SpillableColumnarBatch => Seq[SpillableColumnarBatch],
    mergePolicy: Seq[SpillableColumnarBatch] => SpillableColumnarBatch)
    extends Arm {
  // TODO: in the future we probably want to take this from the SpillableColumnarBatch
  private val inputSpillCallback = RapidsBuffer.defaultSpillCallback
  private var doSplit = false
  private val attemptStack = new mutable.ArrayStack[SpillableColumnarBatch]()

  /**
   * withRetry takes a function `fn` that takes as input a `ColumnarBatch` and outputs
   * a `ColumnarBatch`, in other words, the caller doesn't own the spillables in `input`.
   * The ownership allows `withRetry` to retry without worrying about OOM exceptions
   * in `fn`, but it means we should use `withRetry` at fine granularity, around a cuDF
   * function call.
   *
   * As `fn` succeeds, the results are made spillable and queued. This, currently,
   * requires calling `contiguousSplit` and doubling the memory usage, and so we could OOM
   * while making the batch spillable, which is OK since we would get a RetryOOM or
   * SplitOrRetryOOM again.
   *
   * @param fn function to process a batch in an OOM-safe way ColumnarBatch => ColumnarBatch
   * @return Seq[SpillableColumnarBatch] for successful results
   */
  def withRetry(fn: ColumnarBatch => ColumnarBatch): Seq[SpillableColumnarBatch] = {
    val results = new ArrayBuffer[SpillableColumnarBatch]()
    val resultAppender = (result: ColumnarBatch) => {
      results.append(SpillableColumnarBatch(
        result,
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
        inputSpillCallback))
    }
    try {
      doRetry[ColumnarBatch](fn, Some(resultAppender))
    } catch {
      case t: Throwable =>
        results.safeClose(t)
        throw t
    }
    if (results.size == 1) {
      results
    } else {
      // Like with `split`, we just OOM if `merge` fails for now.
      // In the future we may want to have a "dontMergeOnOOM" policy
      // where it could be best effort => it may be acceptable for
      // some of the operators to return several batches...
      val merged = merge(results)
      if (merged == null) {
        results
      } else {
        Seq(merged)
      }
    }
  }

  private def doRetry[T](
      fn: ColumnarBatch => T,
      resultAppender: Option[T => Unit] = None): Unit = {
    attemptStack.push(input)
    // this is set on the first exception, and we add suppressed if there are others
    // during the retry attempts
    var oom: Throwable = null
    var firstAttempt: Boolean = true
    while (attemptStack.nonEmpty) {
      if (!firstAttempt) {
        // call thread block API
        RmmSpark.blockThreadUntilReady()
      }
      firstAttempt = false
      val popped = attemptStack.pop()
      val attempt = try {
        if (doSplit) {
          // If `split` OOMs, we are already the last thread standing
          // there is likely not much we can do, and for now we don't handle
          // this OOM
          val splitted = split(popped)
          // the splitted sequence needs to be inserted in reverse order
          // so we try the first item first.
          splitted.reverse.foreach(attemptStack.push)
          attemptStack.pop()
        } else {
          popped
        }
      } catch {
        case t: Throwable =>
          // exception occurred while trying to split
          // we close our attempts and rethrow
          attemptStack.safeClose(t)
          throw t
      }
      doSplit = false
      try {
        withResource(attempt.getColumnarBatch()) { cb =>
          val result = fn(cb)
          resultAppender.foreach(appender => appender(result))
        }
      } catch {
        case retryOOM: RetryOOM =>
          if (oom == null) {
            oom = retryOOM
          } else {
            oom.addSuppressed(retryOOM)
          }

          // put it back
          attemptStack.push(attempt)
        case splitAndRetryOOM: SplitAndRetryOOM => // we are the only thread
          if (oom == null) {
            oom = splitAndRetryOOM
          } else {
            oom.addSuppressed(splitAndRetryOOM)
          }
          // put it back
          attemptStack.push(attempt)
          doSplit = true
        case other: Throwable =>
          if (oom == null) {
            oom = other
          } else {
            oom.addSuppressed(other)
          }
          // close any buffers we were trying to work with
          attemptStack.push(attempt)
          attemptStack.safeClose(oom)

          // we want to throw early here, since we got an exception
          // we were not prepared to handle
          throw oom
      }
    }
  }

  // It is assumed that OOM in this function is not handled.
  def split(item: SpillableColumnarBatch): Seq[SpillableColumnarBatch] = {
    if (splitPolicy == null) {
      throw new OutOfMemoryError(
        "Attempted to handle a split, but was not initialized with a splitPolicy.")
    }
    withResource(item) { _ =>
      val splitted = splitPolicy(item)
      require(splitted.size >= 2,
        "While attempting to split to handle OOM, a single batch was returned")
      splitted
    }
  }

  // It is assumed that OOM in this function is not handled.
  def merge(items: Seq[SpillableColumnarBatch]): SpillableColumnarBatch = {
    if (mergePolicy == null) {
      null
    } else {
      withResource(items) { _ =>
        mergePolicy(items)
      }
    }
  }
}
