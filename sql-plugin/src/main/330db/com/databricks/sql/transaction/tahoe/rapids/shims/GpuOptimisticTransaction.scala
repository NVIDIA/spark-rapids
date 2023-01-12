/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * This file was derived from OptimisticTransaction.scala and TransactionalWrite.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids.shims

import com.databricks.sql.transaction.tahoe._
import com.nvidia.spark.rapids._

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.util.Clock


class GpuOptimisticTransaction(
  deltaLog: DeltaLog,
  snapshot: Snapshot,
  rapidsConf: RapidsConf)(implicit clock: Clock)
  extends GpuOptimisticTransactionBase(deltaLog, snapshot, rapidsConf)(clock) {

  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) {
    this(deltaLog, deltaLog.update(), rapidsConf)
  }

  /**
   * Returns a tuple of (data, partition schema). For CDC writes, a `__is_cdc` column is added to
   * the data and `__is_cdc=true/false` is added to the front of the partition schema.
   */
  override def shimPerformCDCPartition(inputData: Dataset[_]): (DataFrame, StructType) = {
    performCDCPartition(inputData: Dataset[_])
  }
}
