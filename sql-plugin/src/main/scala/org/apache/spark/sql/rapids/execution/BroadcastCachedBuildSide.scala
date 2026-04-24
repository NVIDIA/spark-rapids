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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{DistinctHashJoin => CudfDistinctHashJoin, HashJoin => CudfHashJoin, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuProjectExec, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.spill.SharedRecomputableDeviceHandle

import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait CachedBuildSide extends AutoCloseable {
  def buildStats: JoinBuildSideStats
}

final class CachedHashJoin(
    override val buildStats: JoinBuildSideStats,
    val handle: SharedRecomputableDeviceHandle[CudfHashJoin]) extends CachedBuildSide {
  override def close(): Unit = handle.close()
}

final class CachedDistinctHashJoin(
    override val buildStats: JoinBuildSideStats,
    val handle: SharedRecomputableDeviceHandle[CudfDistinctHashJoin]) extends CachedBuildSide {
  override def close(): Unit = handle.close()
}

case class BroadcastCachedBuildSideKey(
    projectedBuildKeys: Seq[String],
    compareNullsEqual: Boolean,
    filterOutNulls: Boolean)

object BroadcastCachedBuildSide {
  def key(
      boundBuiltKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean): BroadcastCachedBuildSideKey = {
    BroadcastCachedBuildSideKey(
      boundBuiltKeys.map(_.canonicalized.sql),
      compareNullsEqual,
      filterOutNulls)
  }

  private def withBuildKeys[T](
      broadcastBatch: SpillableColumnarBatch,
      boundBuiltKeys: Seq[GpuExpression],
      filterOutNulls: Boolean)(f: Table => T): T = {
    def projectAndApply(cb: ColumnarBatch): T = {
      withResource(GpuProjectExec.project(cb, boundBuiltKeys)) { buildKeys =>
        withResource(GpuColumnVector.from(buildKeys)) { buildKeysTable =>
          f(buildKeysTable)
        }
      }
    }
    if (filterOutNulls) {
      val retainedBatch = broadcastBatch.incRefCount()
      withResource(GpuHashJoin.filterNullsWithRetryAndClose(retainedBatch, boundBuiltKeys)) {
        projectAndApply
      }
    } else {
      val retainedBatch = broadcastBatch.incRefCount()
      withRetryNoSplit(retainedBatch) { _ =>
        withResource(retainedBatch.getColumnarBatch()) { projectAndApply }
      }
    }
  }

  /**
   * cuDF's reusable hash join handles are safe for concurrent probes. The executor-wide cache
   * therefore pins the live handle while a task is probing it and relies on
   * `SharedRecomputableDeviceHandle` to track spillability with an application-level pin count.
   */
  def create(
      broadcastBatch: SpillableColumnarBatch,
      boundBuiltKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean): CachedBuildSide = {
    def buildHashJoin(): CudfHashJoin = {
      withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
        new CudfHashJoin(buildKeys, compareNullsEqual)
      }
    }

    def buildDistinctHashJoin(): CudfDistinctHashJoin = {
      withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
        new CudfDistinctHashJoin(buildKeys, compareNullsEqual)
      }
    }

    withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
      val stats = JoinBuildSideStats.fromTable(buildKeys)
      val approxSizeInBytes = buildKeys.getDeviceMemorySize
      if (stats.isDistinct) {
        new CachedDistinctHashJoin(
          stats,
          SharedRecomputableDeviceHandle(
            approxSizeInBytes,
            new CudfDistinctHashJoin(buildKeys, compareNullsEqual)) {
            buildDistinctHashJoin()
          })
      } else {
        new CachedHashJoin(
          stats,
          SharedRecomputableDeviceHandle(
            approxSizeInBytes,
            new CudfHashJoin(buildKeys, compareNullsEqual)) {
            buildHashJoin()
          })
      }
    }
  }
}
