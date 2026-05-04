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
 * Driver-side aggregator of probe-side `(rowsIn, rowsPassed)` per
 * bfId:
 *  - registered once per bfId via `driverGetOrCreate`
 *  - executor-side `add` updates ship through Spark's accumulator
 *    serialization protocol back to the driver
 *  - merged into a single `(rowsIn, rowsPassed)` aggregate
 *
 * Implements `BloomFilterPredicateUpdater` so call sites in
 * `GpuBloomFilterMightContain.columnarEval` can hold a
 * trait-typed reference and a counting spy can substitute for it
 * under unit test.
 */
class BloomFilterProbeAccumulator
    extends AccumulatorV2[(Long, Long), (Long, Long)]
    with BloomFilterPredicateUpdater {

  private var rowsIn: Long = 0L
  private var rowsPassed: Long = 0L

  override def isZero: Boolean = rowsIn == 0L && rowsPassed == 0L

  override def copy(): AccumulatorV2[(Long, Long), (Long, Long)] = {
    val acc = new BloomFilterProbeAccumulator
    acc.rowsIn = rowsIn
    acc.rowsPassed = rowsPassed
    acc
  }

  override def reset(): Unit = {
    rowsIn = 0L
    rowsPassed = 0L
  }

  override def add(v: (Long, Long)): Unit = synchronized {
    rowsIn += v._1
    rowsPassed += v._2
  }

  override def merge(other: AccumulatorV2[(Long, Long), (Long, Long)]): Unit = synchronized {
    val o = other.asInstanceOf[BloomFilterProbeAccumulator]
    rowsIn += o.rowsIn
    rowsPassed += o.rowsPassed
  }

  override def value: (Long, Long) = (rowsIn, rowsPassed)

  override def update(rowsIn: Long, rowsPassed: Long): Unit = {
    add((rowsIn, rowsPassed))
  }
}

object BloomFilterProbeAccumulator {

  private val cache = new ConcurrentHashMap[String, BloomFilterProbeAccumulator]()

  /**
   * Driver-only call. Registers a named accumulator
   * `cubf_probe_<bfId>` with the active SparkContext on first
   * access; returns the cached reference on subsequent calls.
   * The returned reference can be captured in a closure shipped
   * to executors; AccumulatorV2's serialization protocol carries
   * executor-side `update` deltas back to the driver.
   */
  def driverGetOrCreate(sc: SparkContext, bfId: String): BloomFilterProbeAccumulator = {
    cache.computeIfAbsent(bfId, _ => {
      val acc = new BloomFilterProbeAccumulator
      sc.register(acc, s"cubf_probe_$bfId")
      acc
    })
  }
}
