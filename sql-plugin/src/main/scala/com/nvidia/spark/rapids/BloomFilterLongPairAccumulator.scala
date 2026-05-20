/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.util.AccumulatorV2

/**
 * Shared base for the CuBF observability accumulators that aggregate a `(Long, Long)` pair per
 * `bfId`. Owns the AccumulatorV2 boilerplate (state, isZero/copy/reset/add/merge/value) so each
 * concrete subclass only has to declare which updater trait it implements and how to spawn an
 * empty instance for `copy()`.
 *
 * Concrete subclasses ship executor-side `update(a, b)` deltas back to the driver via Spark's
 * accumulator serialization protocol; the driver-merged value is the per-bfId pair sum.
 */
abstract class BloomFilterLongPairAccumulator
    extends AccumulatorV2[(Long, Long), (Long, Long)] {

  /** Spawn a fresh, zero-state instance of the same concrete type. Used by `copy()`. */
  protected def newEmpty(): BloomFilterLongPairAccumulator

  private var first: Long = 0L
  private var second: Long = 0L

  override def isZero: Boolean = first == 0L && second == 0L

  override def copy(): AccumulatorV2[(Long, Long), (Long, Long)] = {
    val acc = newEmpty()
    acc.first = first
    acc.second = second
    acc
  }

  override def reset(): Unit = {
    first = 0L
    second = 0L
  }

  override def add(v: (Long, Long)): Unit = synchronized {
    first += v._1
    second += v._2
  }

  override def merge(other: AccumulatorV2[(Long, Long), (Long, Long)]): Unit = synchronized {
    val o = other.asInstanceOf[BloomFilterLongPairAccumulator]
    first += o.first
    second += o.second
  }

  override def value: (Long, Long) = (first, second)
}

object BloomFilterLongPairAccumulator {

  /**
   * Idempotent driver-side registration helper. On first access for a given `bfId`, builds a
   * fresh accumulator via `factory`, registers it with `sc` under the name `<namePrefix>_<bfId>`,
   * and caches the reference. Subsequent calls return the cached instance.
   *
   * The returned reference is safe to capture in a closure shipped to executors; AccumulatorV2's
   * serialization protocol carries executor-side `update` deltas back to the driver.
   */
  def getOrCreateCached[A <: BloomFilterLongPairAccumulator](
      cache: ConcurrentHashMap[String, A],
      sc: SparkContext,
      bfId: String,
      namePrefix: String,
      factory: () => A): A = {
    cache.computeIfAbsent(bfId, _ => {
      val acc = factory()
      sc.register(acc, s"${namePrefix}_$bfId")
      acc
    })
  }
}
