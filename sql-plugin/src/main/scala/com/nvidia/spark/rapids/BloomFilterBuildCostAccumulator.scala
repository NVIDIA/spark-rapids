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
 * Driver-side aggregator of build-side `(buildWallNanos, bfBytes)`
 * per bfId. Companion to `BloomFilterProbeAccumulator` (the
 * probe-side counterpart):
 *  - registered once per bfId via `driverGetOrCreate`
 *  - executor-side `add` updates ship through Spark's accumulator
 *    serialization protocol back to the driver
 *  - merged into a single `(buildWallNanos, bfBytes)` aggregate
 *    summed across partitions
 *
 * Implements `BloomFilterBuildCostUpdater` so call sites in
 * `GpuGenerateBloomFilterExec`'s finalize path can hold a
 * trait-typed reference and a counting spy can substitute for it
 * under unit test. `update` fires once per BF build at finalize,
 * never per input batch.
 */
class BloomFilterBuildCostAccumulator
    extends AccumulatorV2[(Long, Long), (Long, Long)]
    with BloomFilterBuildCostUpdater {

  private var buildWallNanos: Long = 0L
  private var bfBytes: Long = 0L

  override def isZero: Boolean = buildWallNanos == 0L && bfBytes == 0L

  override def copy(): AccumulatorV2[(Long, Long), (Long, Long)] = {
    val acc = new BloomFilterBuildCostAccumulator
    acc.buildWallNanos = buildWallNanos
    acc.bfBytes = bfBytes
    acc
  }

  override def reset(): Unit = {
    buildWallNanos = 0L
    bfBytes = 0L
  }

  override def add(v: (Long, Long)): Unit = synchronized {
    buildWallNanos += v._1
    bfBytes += v._2
  }

  override def merge(other: AccumulatorV2[(Long, Long), (Long, Long)]): Unit = synchronized {
    val o = other.asInstanceOf[BloomFilterBuildCostAccumulator]
    buildWallNanos += o.buildWallNanos
    bfBytes += o.bfBytes
  }

  override def value: (Long, Long) = (buildWallNanos, bfBytes)

  override def update(buildWallNanos: Long, bfBytes: Long): Unit = {
    add((buildWallNanos, bfBytes))
  }
}

object BloomFilterBuildCostAccumulator {

  private val cache = new ConcurrentHashMap[String, BloomFilterBuildCostAccumulator]()

  /**
   * Driver-only call. Registers a named accumulator
   * `cubf_build_<bfId>` with the active SparkContext on first
   * access; returns the cached reference on subsequent calls.
   * The returned reference can be captured in a closure shipped
   * to executors; AccumulatorV2's serialization protocol carries
   * executor-side `update` deltas back to the driver. Naming
   * mirrors `BloomFilterProbeAccumulator`'s `cubf_probe_<bfId>`.
   */
  def driverGetOrCreate(sc: SparkContext, bfId: String): BloomFilterBuildCostAccumulator = {
    cache.computeIfAbsent(bfId, _ => {
      val acc = new BloomFilterBuildCostAccumulator
      sc.register(acc, s"cubf_build_$bfId")
      acc
    })
  }
}
