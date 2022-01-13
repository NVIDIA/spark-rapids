/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import java.util.concurrent.TimeUnit

/**
 * Utility methods for managing spillable buffer priorities.
 * The spill priority numerical space is divided into potentially overlapping
 * ranges based on the type of buffer.
 */
object SpillPriorities {
  private val startTimestamp = System.nanoTime

  /**
   * Priorities for task output buffers intended for shuffle.
   */
  val OUTPUT_FOR_SHUFFLE_INITIAL_PRIORITY: Long = 0

  def getShuffleOutputBufferReadPriority: Long = {
    // shuffle output buffers that have been read are likely to never be read again,
    // so use a low value to prioritize spilling.
    // Always prefer spilling read buffers before unread buffers, and prefer spilling
    // buffers read a while ago to buffers more recently read.
    val millisFromStart = TimeUnit.NANOSECONDS.toMillis(System.nanoTime - startTimestamp)
    math.min(Long.MinValue + millisFromStart, OUTPUT_FOR_SHUFFLE_INITIAL_PRIORITY - 1)
  }

  /**
   * Priorities for buffers received from shuffle.
   * Shuffle input buffers are about to be read by a task, so spill
   * them if there's no other choice, but leave some space at the end of the priority range
   * so there can be some things after it.
   */
  val INPUT_FROM_SHUFFLE_PRIORITY: Long = Long.MaxValue - 1000

  /**
   * Priority for buffers that are waiting for next to be called.  i.e. data held between
   * calls to `hasNext` and `next` or between different calls to `next`.
   */
  val ACTIVE_ON_DECK_PRIORITY: Long = INPUT_FROM_SHUFFLE_PRIORITY + 1

  /**
   * Priority for multiple buffers being buffered within a call to next.
   */
  val ACTIVE_BATCHING_PRIORITY: Long = ACTIVE_ON_DECK_PRIORITY + 100

  /**
   * Priority offset for host memory buffers allocated from the pinned memory pool. They are at
   * lower priorities, so will be spilled first, making more pinned memory available.
   */
  val HOST_MEMORY_BUFFER_PINNED_OFFSET: Long = -100

  /**
   * Priority offset for host memory buffers allocated from the internal pageable memory pool. They
   * are at higher priorities than pinned memory buffers, thus making more pinned memory available;
   * but at lower priorities than directly allocated buffers, thus freeing up the internal pageable
   * memory pool.
   */
  val HOST_MEMORY_BUFFER_PAGEABLE_OFFSET: Long = 0

  /**
   * Priority offset for host memory buffers directly allocated from the OS. They are at higher
   * priorities, thus freeing up memory pools first.
   */
  val HOST_MEMORY_BUFFER_DIRECT_OFFSET: Long = 100
}
