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
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuProjectExec, NvtxRegistry, SpillableColumnarBatch}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.spill.SharedRecomputableDeviceHandle

import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Native build-side state derived from one broadcast batch/key projection.
 *
 * This is not the full build-side data. It contains the projected build-key statistics plus a
 * spillable/recomputable cuDF hash join handle built from those keys.
 */
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

/**
 * Identifies cached build-side state for a given broadcast batch.
 *
 * The same broadcast can feed joins with different key projections or null filtering behavior, so
 * those are included as part of the key.
 */
case class BroadcastCachedBuildSideKey(
    projectedBuildKeys: Seq[String],
    compareNullsEqual: Boolean,
    filterOutNulls: Boolean)

/**
 * Factory to create a cached build-side hash table from a broadcast batch.
 */
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

  private def newHashJoin(buildKeys: Table, compareNullsEqual: Boolean): CudfHashJoin = {
    NvtxRegistry.BROADCAST_HASH_TABLE_BUILD {
      new CudfHashJoin(buildKeys, compareNullsEqual)
    }
  }

  private def newDistinctHashJoin(
      buildKeys: Table,
      compareNullsEqual: Boolean): CudfDistinctHashJoin = {
    NvtxRegistry.BROADCAST_HASH_TABLE_BUILD {
      new CudfDistinctHashJoin(buildKeys, compareNullsEqual)
    }
  }

  def create(
      broadcastBatch: SpillableColumnarBatch,
      boundBuiltKeys: Seq[GpuExpression],
      compareNullsEqual: Boolean,
      filterOutNulls: Boolean): CachedBuildSide = {
    def buildHashJoin(): CudfHashJoin = {
      withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
        newHashJoin(buildKeys, compareNullsEqual)
      }
    }

    def buildDistinctHashJoin(): CudfDistinctHashJoin = {
      withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
        newDistinctHashJoin(buildKeys, compareNullsEqual)
      }
    }

    withBuildKeys(broadcastBatch, boundBuiltKeys, filterOutNulls) { buildKeys =>
      val stats = JoinBuildSideStats.fromTable(buildKeys)
      // cuDF does not expose the size of the native hash-table;
      // using the projected build-key table size as an approximation for spill accounting
      val approxSizeInBytes = buildKeys.getDeviceMemorySize
      if (stats.isDistinct) {
        new CachedDistinctHashJoin(
          stats,
          SharedRecomputableDeviceHandle(
            approxSizeInBytes,
            newDistinctHashJoin(buildKeys, compareNullsEqual)) {
            buildDistinctHashJoin()
          })
      } else {
        new CachedHashJoin(
          stats,
          SharedRecomputableDeviceHandle(
            approxSizeInBytes,
            newHashJoin(buildKeys, compareNullsEqual)) {
            buildHashJoin()
          })
      }
    }
  }
}
