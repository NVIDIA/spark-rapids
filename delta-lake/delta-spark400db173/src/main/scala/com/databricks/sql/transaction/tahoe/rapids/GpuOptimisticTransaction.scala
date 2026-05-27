/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.databricks.sql.transaction.tahoe.{CommittedTransaction, DeltaLog, DeltaOptions, Snapshot}
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.constraints.Constraint
import com.databricks.sql.transaction.tahoe.files.TransactionalWriteOptions
import com.databricks.sql.transaction.tahoe.hooks.{AutoCompact, PostCommitHook}
import com.nvidia.spark.rapids.{ColumnarFileFormat, RapidsConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, RuntimeReplaceable}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.ColumnarWriteJobStatsTracker
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.util.Clock

class GpuOptimisticTransaction(
    deltaLog: DeltaLog,
    catalogTable: Option[CatalogTable],
    snapshot: Snapshot,
    rapidsConf: RapidsConf)(implicit clock: Clock)
    extends GpuOptimisticTransactionWriteBase(deltaLog, catalogTable, snapshot, rapidsConf)(clock) {

  def this(
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable],
      snapshotOpt: Option[Snapshot],
      rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, catalogTable, snapshotOpt.getOrElse(deltaLog.update()), rapidsConf)
  }

  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) = {
    this(deltaLog, Option.empty[CatalogTable], deltaLog.update(), rapidsConf)
  }

  override protected def normalizeGpuStatsColExpr(expr: Expression): Expression = {
    expr.transform {
      case rr: RuntimeReplaceable => rr.replacement
    }
  }

  override protected def handlePostWriteAutoCompact(
      spark: SparkSession,
      isOptimize: Boolean,
      fileActions: Seq[FileAction]): Unit = {
    val autoCompactEnabled =
      AutoCompact.getAutoCompactType(spark.sessionState.conf, metadata).isDefined
    if (!isOptimize && autoCompactEnabled && fileActions.nonEmpty) {
      registerPostCommitHook(GpuAutoCompactCpuFallback)
    }
  }

  override def registerSQLMetrics(spark: SparkSession, metrics: Map[String, SQLMetric]): Unit = {
    val extended = metrics.get("numSourceRows").fold(metrics) { sourceRows =>
      // Alias for 4.0 commit-time transformMetrics expectations.
      metrics + ("operationNumSourceRows" -> sourceRows)
    }
    super.registerSQLMetrics(spark, extended)
  }

  override protected def getGpuWriteCommitter(
      outputPath: Path,
      keepPartitionIdTag: Boolean,
      forcePreserveInputOrder: Boolean) = {
    getCommitter(
      outputPath,
      keepPartitionIdTag,
      forcePreserveInputOrder,
      false,
      false,
      false)
  }

  override protected def writeUsingGpuFileFormatWriter(
      sparkSession: TrampolineConnectShims.SparkSession,
      plan: SparkPlan,
      fileFormat: ColumnarFileFormat,
      committer: FileCommitProtocol,
      outputSpec: FileFormatWriter.OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      options: Map[String, String]): Unit = {
    GpuDeltaFileFormatWriter.write(
      sparkSession = sparkSession,
      plan = plan,
      fileFormat = fileFormat,
      committer = committer,
      outputSpec = outputSpec,
      hadoopConf = hadoopConf,
      partitionColumns = partitionColumns,
      bucketSpec = None,
      statsTrackers = statsTrackers,
      options = options,
      useStableSort = rapidsConf.stableSort,
      concurrentWriterPartitionFlushSize = rapidsConf.concurrentWriterPartitionFlushSize,
      baseDebugOutputPath = rapidsConf.outputDebugDumpPrefix)
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: TransactionalWriteOptions,
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      isCDCWritePhase: Boolean,
      context: Option[String]): Seq[FileAction] = {
    if (isLiquidClustering || isCDCWritePhase) {
      super.writeFiles(inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, isCDCWritePhase, context)
    } else {
      val (fileActions, _) =
        gpuWriteFiles(
          inputData,
          Option(writeOptions).flatMap(_.deltaOptions),
          additionalConstraints,
          Some(isOptimize),
          keepPartitionIdTag = false,
          writeOptions.forcePreserveInputOrder,
          context)
      fileActions
    }
  }

  override def writeFilesAndGetQueryExecution(
      inputData: Dataset[_],
      writeOptions: TransactionalWriteOptions,
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      isCDCWritePhase: Boolean,
      context: Option[String],
      trailing: Boolean): (Seq[FileAction], QueryExecution) = {
    if (isLiquidClustering || isCDCWritePhase) {
      super.writeFilesAndGetQueryExecution(
        inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, isCDCWritePhase, context, trailing)
    } else {
      gpuWriteFiles(
        inputData,
        Option(writeOptions).flatMap(_.deltaOptions),
        additionalConstraints,
        Some(isOptimize),
        trailing,
        writeOptions.forcePreserveInputOrder,
        context)
    }
  }

  override def writeFilesAndGetExecutedPlan(
      inputData: Dataset[_],
      writeOptions: Either[Option[DeltaOptions], TransactionalWriteOptions],
      isOptimize: Boolean,
      isLiquidClustering: Boolean,
      additionalConstraints: Seq[Constraint],
      context: Option[String],
      trailing: Boolean): (Seq[FileAction], SparkPlan) = {
    if (isLiquidClustering) {
      super.writeFilesAndGetExecutedPlan(
        inputData, writeOptions, isOptimize, isLiquidClustering,
        additionalConstraints, context, trailing)
    } else {
      val deltaOpts = writeOptions match {
        case Left(opt) => opt
        case Right(opts) => Option(opts).flatMap(_.deltaOptions)
      }
      val forcePreserveInputOrder = writeOptions match {
        case Left(_) => false
        case Right(opts) => opts.forcePreserveInputOrder
      }
      val (fileActions, qe) =
        gpuWriteFiles(
          inputData,
          deltaOpts,
          additionalConstraints,
          Some(isOptimize),
          trailing,
          forcePreserveInputOrder,
          context)
      (fileActions, qe.executedPlan)
    }
  }
}

private object GpuAutoCompactCpuFallback extends PostCommitHook {
  override val name: String = AutoCompact.name

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    val rapidsEnabled = RapidsConf.SQL_ENABLED.key
    val original = spark.conf.getOption(rapidsEnabled)
    spark.conf.set(rapidsEnabled, "false")
    try {
      AutoCompact.run(spark, txn)
    } finally {
      original match {
        case Some(value) => spark.conf.set(rapidsEnabled, value)
        case None => spark.conf.unset(rapidsEnabled)
      }
    }
  }
}
