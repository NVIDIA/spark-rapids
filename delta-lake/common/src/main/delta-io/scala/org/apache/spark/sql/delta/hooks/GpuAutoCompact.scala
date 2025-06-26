/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeltaOptimizeContext
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.rapids.GpuOptimisticTransactionBase
import org.apache.spark.sql.delta.rapids.commands.GpuOptimizeExecutor
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.AutoCompactPartitionStats

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression

trait GpuAutoCompactBase extends AutoCompactBase {

  override def run(
      spark: SparkSession,
      txn: OptimisticTransactionImpl,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit = {
    run(spark, txn.asInstanceOf[GpuOptimisticTransactionBase],
      committedVersion, postCommitSnapshot, actions)
  }

  def run(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      committedVersion: Long,
      postCommitSnapshot: Snapshot,
      actions: Seq[Action]): Unit

  // TODO: maybe move all impls to here instead of in GpuAutoCompact
}

case object GpuAutoCompact extends GpuAutoCompactBase {

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
  /**
   * Compact the target table of write transaction `txn` only when there are sufficient amount of
   * small size files.
   */
  /**
   * Compact the target table of write transaction `txn` only when there are sufficient amount of
   * small size files.
   */
  private[delta] def compactIfNecessary(
      spark: SparkSession,
      txn: GpuOptimisticTransactionBase,
      postCommitSnapshot: Snapshot,
      opType: String,
      maxDeletedRowsRatio: Option[Double]
  ): Seq[OptimizeMetrics] = {
    // TODO: GpuDeltaLog?
    val tableId = txn.deltaLog.tableId
    val autoCompactRequest = AutoCompactUtils.prepareAutoCompactRequest(
      spark,
      txn,
      postCommitSnapshot,
      txn.partitionsAddedToOpt.map(_.toSet),
      opType,
      maxDeletedRowsRatio)
    if (autoCompactRequest.shouldCompact) {
      try {
        val metrics = GpuAutoCompact
          .compact(
            spark,
            txn.deltaLog,
            txn.catalogTable,
            autoCompactRequest.targetPartitionsPredicate,
            opType,
            maxDeletedRowsRatio
          )
        val partitionsStats = AutoCompactPartitionStats.instance(spark)
        // Mark partitions as compacted before releasing them.
        // Otherwise an already compacted partition might get picked up by a concurrent thread.
        // But only marks it as compacted, if no exception was thrown by auto compaction so that the
        // partitions stay eligible for subsequent auto compactions.
        partitionsStats.markPartitionsAsCompacted(
          tableId,
          autoCompactRequest.allowedPartitions
        )
        metrics
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
            tableId,
            autoCompactRequest.allowedPartitions
          )
        }
      }
    } else {
      Seq.empty[OptimizeMetrics]
    }
  }
  /**
   * Launch Auto Compaction jobs if there is sufficient capacity.
   * @param spark The spark session of the parent transaction that triggers this Auto Compaction.
   * @param deltaLog The delta log of the parent transaction.
   * @return the optimize metrics of this compaction job.
   */
  /**
   * Launch Auto Compaction jobs if there is sufficient capacity.
   * @param spark The spark session of the parent transaction that triggers this Auto Compaction.
   * @param deltaLog The delta log of the parent transaction.
   * @return the optimize metrics of this compaction job.
   */
  private[delta] override def compact(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      partitionPredicates: Seq[Expression] = Nil,
      opType: String = OP_TYPE,
      maxDeletedRowsRatio: Option[Double] = None)
  : Seq[OptimizeMetrics] = recordDeltaOperation(deltaLog, opType) {
    val maxFileSize = spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MAX_FILE_SIZE)
    val minFileSizeOpt = Some(spark.conf.get(DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_FILE_SIZE)
      .getOrElse(maxFileSize / 2))
    val maxFileSizeOpt = Some(maxFileSize)
    recordDeltaOperation(deltaLog, s"$opType.execute") {
      val optimizeContext = DeltaOptimizeContext(
        reorg = None,
        minFileSizeOpt,
        maxFileSizeOpt,
        maxDeletedRowsRatio = maxDeletedRowsRatio
      )
      val rapidsConf = new RapidsConf(spark.sessionState.conf)
      val rows = new GpuOptimizeExecutor(
        rapidsConf,
        spark,
        deltaLog.update(catalogTableOpt = catalogTable),
        catalogTable,
        partitionPredicates,
        zOrderByColumns = Seq(),
        isAutoCompact = true,
        optimizeContext
      ).optimize()
      val metrics = rows.map(_.getAs[OptimizeMetrics](1))
      recordDeltaEvent(deltaLog, s"$opType.execute.metrics", data = metrics.head)
      metrics
    }
  }
}
