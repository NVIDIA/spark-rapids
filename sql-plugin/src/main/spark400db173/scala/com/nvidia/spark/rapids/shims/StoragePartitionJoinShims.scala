/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "400db173"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Databricks 17.3 stub for StoragePartitionJoinParams which doesn't exist in this version.
 * We provide a minimal stub that satisfies the interface but has no SPJ functionality.
 */
case class SpjParamsStub(
    keyGroupedPartitioning: Option[Seq[Expression]] = None,
    joinKeyPositions: Option[Seq[Int]] = None,
    commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    applyPartialClustering: Boolean = false,
    replicatePartitions: Boolean = false
)

object StoragePartitionJoinShims {
  type SpjParams = SpjParamsStub
  
  def default(): SpjParams = SpjParamsStub()

  /**
   * Databricks 17.3 doesn't have StoragePartitionJoinParams in BatchScanExec,
   * so we return the default stub.
   */
  def fromBatchScan(spjParams: Any): SpjParams = SpjParamsStub()
}

