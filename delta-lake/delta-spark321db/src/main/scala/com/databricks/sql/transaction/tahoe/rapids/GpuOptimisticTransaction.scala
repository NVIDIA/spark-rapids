/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.commands.cdc.CDCReader
import com.nvidia.spark.rapids._

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
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
    // If this is a CDC write, we need to generate the CDC_PARTITION_COL in order to properly
    // dispatch rows between the main table and CDC event records. This is a virtual partition
    // and will be stripped out later in [[DelayedCommitProtocolEdge]].
    // Note that the ordering of the partition schema is relevant - CDC_PARTITION_COL must
    // come first in order to ensure CDC data lands in the right place.
    if (CDCReader.isCDCEnabledOnTable(metadata) &&
        inputData.schema.fieldNames.contains(CDCReader.CDC_TYPE_COLUMN_NAME)) {
      val augmentedData = inputData.withColumn(
        CDCReader.CDC_PARTITION_COL, col(CDCReader.CDC_TYPE_COLUMN_NAME).isNotNull)
      val partitionSchema = StructType(
        StructField(CDCReader.CDC_PARTITION_COL, StringType) +: metadata.physicalPartitionSchema)
      (augmentedData, partitionSchema)
    } else {
      (inputData.toDF(), metadata.physicalPartitionSchema)
    }
  }
}
