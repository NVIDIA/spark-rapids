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

import org.apache.spark.TaskContext

/**
 * Helper iterator that wraps a BufferedIterator of AutoCloseable subclasses.
 * This iterator also implements AutoCloseable, so it can be closed in case
 * of exceptions.
 *
 * @param wrapped the buffered iterator
 * @tparam T an AutoCloseable subclass
 */
class CloseableBufferedIterator[T <: AutoCloseable](wrapped: BufferedIterator[T])
  extends BufferedIterator[T] with AutoCloseable {
  // register against task completion to close any leaked buffered items
  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  private[this] var isClosed = false
  override def head: T = wrapped.head
  override def headOption: Option[T] = wrapped.headOption
  override def next: T = wrapped.next
  override def hasNext: Boolean = wrapped.hasNext
  override def close(): Unit = {
    if (!isClosed) {
      headOption.foreach(_.close())
      isClosed = true
    }
  }
}
