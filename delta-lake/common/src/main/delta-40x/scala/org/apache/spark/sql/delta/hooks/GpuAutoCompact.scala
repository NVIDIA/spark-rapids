/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.GpuOptimisticTransactionBase
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats

/**
 * Delta 4.0 version-specific implementation of GpuAutoCompact.
 * In Delta 4.0, the PostCommitHook.run method signature uses:
 * - DeltaTransaction instead of OptimisticTransactionImpl
 * - Iterator[Action] instead of Seq[Action]
 */
case object GpuAutoCompact extends GpuAutoCompactBase {

  override def run(
      spark: SparkSession,
      txn: DeltaTransaction,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Iterator[Action]): Unit = {
    // Avoid casting to GpuOptimisticTransactionBase and drive auto-compaction using only
    // DeltaTransaction plus commit actions.
    val conf = spark.sessionState.conf
    val autoCompactTypeOpt = getAutoCompactType(conf, postCommitSnapshot.metadata)
    if (shouldSkipAutoCompact(autoCompactTypeOpt, spark, txn)) return

    val committedActions = actions.toSeq
    val addedPartitions: Option[Set[Map[String, String]]] = {
      val partitions = committedActions.collect { case a: AddFile => a.partitionValues }.toSet
      if (partitions.nonEmpty) Some(partitions) else None
    }

    val autoCompactRequest = AutoCompactUtils.prepareAutoCompactRequest(
      spark,
      txn,
      postCommitSnapshot,
      addedPartitions,
      OP_TYPE,
      maxDeletedRowsRatio = None)

    if (autoCompactRequest.shouldCompact) {
      try {
        GpuAutoCompact
          .compact(
            spark,
            txn.deltaLog,
            txn.catalogTable,
            autoCompactRequest.targetPartitionsPredicate,
            OP_TYPE,
            maxDeletedRowsRatio = None
          )
        val partitionsStats = AutoCompactPartitionStats.instance(spark)
        partitionsStats.markPartitionsAsCompacted(
          txn.deltaLog.tableId,
          autoCompactRequest.allowedPartitions
        )
      } catch {
        case e: Throwable =>
          logError(log"Auto Compaction failed with: ${MDC(DeltaLogKeys.ERROR, e.getMessage)}")
          recordDeltaEvent(
            txn.deltaLog,
            opType = "delta.autoCompaction.error",
            data = getErrorData(e))
          throw e
      } finally {
        if (AutoCompactUtils.reservePartitionEnabled(spark)) {
          AutoCompactPartitionReserve.releasePartitions(
            txn.deltaLog.tableId,
            autoCompactRequest.allowedPartitions
          )
        }
      }
    }
  }

  override def run(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit = {
    val conf = spark.sessionState.conf
    val autoCompactTypeOpt = getAutoCompactType(conf, postCommitSnapshot.metadata)
    // Skip Auto Compact if current transaction is not qualified or the table is not qualified
    // based on the value of autoCompactTypeOpt.
    if (shouldSkipAutoCompact(autoCompactTypeOpt, spark, txn)) return
    compactIfNecessary(
      spark,
      txn,
      postCommitSnapshot,
      OP_TYPE,
      maxDeletedRowsRatio = None)
  }
}
