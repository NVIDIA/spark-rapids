/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * This file was derived from DeleteCommand.scala
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

package org.apache.spark.sql.delta.rapids.delta33x

import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.delta.GpuDeltaMetricUpdateUDF

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualNullSafe, Expression, If, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, DeltaTableUtils, DeltaUDF, NumRecordsStats, OptimisticTransaction, RowTracking}
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, FileAction}
import org.apache.spark.sql.delta.commands.{DeleteCommandMetrics, DeleteMetric, DeletionVectorUtils, DeltaCommand}
import org.apache.spark.sql.delta.commands.DeleteCommand.{rewritingFilesMsg, FINDING_TOUCHED_FILES_MSG}
import org.apache.spark.sql.delta.commands.MergeIntoCommandBase.totalBytesAndDistinctPartitionValues
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.rapids.{GpuDeltaLog, GpuOptimisticTransactionBase}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.LongType

/**
 * GPU version of Delta Lake DeleteCommand.
 *
 * Performs a Delete based on the search condition
 *
 * Algorithm:
 *   1) Scan all the files and determine which files have
 *      the rows that need to be deleted.
 *   2) Traverse the affected files and rebuild the touched files.
 *   3) Use the Delta protocol to atomically write the remaining rows to new files and remove
 *      the affected files that are identified in step 1.
 */
case class GpuDeleteCommand(
    gpuDeltaLog: GpuDeltaLog,
    catalogTable: Option[CatalogTable],
    target: LogicalPlan,
    condition: Option[Expression])
    extends LeafRunnableCommand with DeltaCommand with DeleteCommandMetrics {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  override val output: Seq[Attribute] = Seq(AttributeReference("num_affected_rows", LongType)())

  override lazy val metrics = createMetrics

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = gpuDeltaLog.deltaLog
    recordDeltaOperation(gpuDeltaLog.deltaLog, "delta.dml.delete") {
      gpuDeltaLog.withNewTransaction(catalogTable) { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        if (hasBeenExecuted(txn, sparkSession)) {
          sendDriverMetrics(sparkSession, metrics)
          return Seq.empty
        }

        val (deleteActions, deleteMetrics) = performDelete(sparkSession, deltaLog, txn)
        val commitVersion = txn.commitIfNeeded(
          actions = deleteActions,
          op = DeltaOperations.Delete(condition.toSeq),
          tags = RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot))

        recordDeltaEvent(
          deltaLog,
          "delta.dml.delete.stats",
          data = deleteMetrics.copy(commitVersion = commitVersion))
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }

    // Adjust for deletes at partition boundaries. Deletes at partition boundaries is a metadata
    // operation, therefore we don't actually have any information around how many rows were deleted
    // While this info may exist in the file statistics, it's not guaranteed that we have these
    // statistics. To avoid any performance regressions, we currently just return a -1 in such cases
    if (metrics("numRemovedFiles").value > 0 && metrics("numDeletedRows").value == 0) {
      Seq(Row(-1L))
    } else {
      Seq(Row(metrics("numDeletedRows").value))
    }
  }

  def performDelete(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      txn: GpuOptimisticTransactionBase): (Seq[Action], DeleteMetric) = {
    import org.apache.spark.sql.delta.implicits._

    var numRemovedFiles: Long = 0
    var numAddedFiles: Long = 0
    var numAddedChangeFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    var numBytesAdded: Long = 0
    var changeFileBytes: Long = 0
    var numBytesRemoved: Long = 0
    var numFilesBeforeSkipping: Long = 0
    var numBytesBeforeSkipping: Long = 0
    var numFilesAfterSkipping: Long = 0
    var numBytesAfterSkipping: Long = 0
    var numPartitionsAfterSkipping: Option[Long] = None
    var numPartitionsRemovedFrom: Option[Long] = None
    var numPartitionsAddedTo: Option[Long] = None
    var numDeletedRows: Option[Long] = None
    var numCopiedRows: Option[Long] = None
    // Deletion vectors are not supported yet.
    val numDeletionVectorsAdded: Long = 0
    val numDeletionVectorsRemoved: Long = 0
    val numDeletionVectorsUpdated: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val deleteActions: Seq[Action] = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val reportRowLevelMetrics = conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)
        val allFiles = txn.filterFiles(Nil, keepNumRecords = reportRowLevelMetrics)

        numRemovedFiles = allFiles.size
        scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
        val (numBytes, numPartitions) = totalBytesAndDistinctPartitionValues(allFiles)
        numBytesRemoved = numBytes
        numFilesBeforeSkipping = numRemovedFiles
        numBytesBeforeSkipping = numBytes
        numFilesAfterSkipping = numRemovedFiles
        numBytesAfterSkipping = numBytes
        numDeletedRows = getDeletedRowsFromAddFilesAndUpdateMetrics(allFiles)

        if (txn.metadata.partitionColumns.nonEmpty) {
          numPartitionsAfterSkipping = Some(numPartitions)
          numPartitionsRemovedFrom = Some(numPartitions)
          numPartitionsAddedTo = Some(0)
        }
        val operationTimestamp = System.currentTimeMillis()
        allFiles.map(_.removeWithTimestamp(operationTimestamp))
      case Some(cond) =>
        val (metadataPredicates, otherPredicates) =
          DeltaTableUtils.splitMetadataAndDataPredicates(
            cond, txn.metadata.partitionColumns, sparkSession)

        numFilesBeforeSkipping = txn.snapshot.numOfFiles
        numBytesBeforeSkipping = txn.snapshot.sizeInBytes

        if (otherPredicates.isEmpty) {
          // Case 2: The condition can be evaluated using metadata only.
          //         Delete a set of files without the need of scanning any data files.
          val operationTimestamp = System.currentTimeMillis()
          val reportRowLevelMetrics = conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)
          val candidateFiles =
            txn.filterFiles(metadataPredicates, keepNumRecords = reportRowLevelMetrics)

          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          numRemovedFiles = candidateFiles.size
          numBytesRemoved = candidateFiles.map(_.size).sum
          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          numDeletedRows = getDeletedRowsFromAddFilesAndUpdateMetrics(candidateFiles)

          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
            numPartitionsRemovedFrom = Some(numCandidatePartitions)
            numPartitionsAddedTo = Some(0)
          }
          candidateFiles.map(_.removeWithTimestamp(operationTimestamp))
        } else {
          // Case 3: Delete the rows based on the condition.

          // Should we write the DVs to represent the deleted rows?
          val shouldWriteDVs = shouldWritePersistentDeletionVectors(sparkSession, txn)

          val candidateFiles = txn.filterFiles(
            metadataPredicates ++ otherPredicates,
            keepNumRecords = shouldWriteDVs)
          // `candidateFiles` contains the files filtered using statistics and delete condition
          // They may or may not contains any rows that need to be deleted.

          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
          }

          val nameToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

          val fileIndex = new TahoeBatchFileIndex(
            sparkSession, "delete", candidateFiles, deltaLog, deltaLog.dataPath, txn.snapshot)

          if (shouldWriteDVs) {
            // this should be unreachable because we fall back to CPU
            // if deletion vectors are enabled. The tracking issue for adding deletion vector
            // support is https://github.com/NVIDIA/spark-rapids/issues/8554
            throw new IllegalStateException("Deletion vectors are not supported on GPU")

          } else {
            // Keep everything from the resolved target except a new TahoeFileIndex
            // that only involves the affected files instead of all files.
            val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
            val data = Dataset.ofRows(sparkSession, newTarget)
            val deletedRowCount = metrics("numDeletedRows")
            val deletedRowUdf = DeltaUDF.boolean {
              new GpuDeltaMetricUpdateUDF(deletedRowCount)
            }.asNondeterministic()
            val filesToRewrite =
              withStatusCode("DELTA", FINDING_TOUCHED_FILES_MSG) {
                if (candidateFiles.isEmpty) {
                  Array.empty[String]
                } else {
                  data.filter(new Column(cond))
                    .select(input_file_name())
                    .filter(deletedRowUdf())
                    .distinct()
                    .as[String]
                    .collect()
                }
              }

            numRemovedFiles = filesToRewrite.length
            scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
            if (filesToRewrite.isEmpty) {
              // Case 3.1: no row matches and no delete will be triggered
              if (txn.metadata.partitionColumns.nonEmpty) {
                numPartitionsRemovedFrom = Some(0)
                numPartitionsAddedTo = Some(0)
              }
              Nil
            } else {
              // Case 3.2: some files need an update to remove the deleted files
              // Do the second pass and just read the affected files
              val baseRelation = buildBaseRelation(
                sparkSession, txn, "delete", deltaLog.dataPath, filesToRewrite, nameToAddFileMap)
              // Keep everything from the resolved target except a new TahoeFileIndex
              // that only involves the affected files instead of all files.
              val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
              val targetDF = RowTracking.preserveRowTrackingColumns(
                dfWithoutRowTrackingColumns = Dataset.ofRows(sparkSession, newTarget),
                snapshot = txn.snapshot)
              val filterCond = Not(EqualNullSafe(cond, Literal.TrueLiteral))
              val rewrittenActions = rewriteFiles(txn, targetDF, filterCond, filesToRewrite.length)
              val (changeFiles, rewrittenFiles) = rewrittenActions
                .partition(_.isInstanceOf[AddCDCFile])
              numAddedFiles = rewrittenFiles.size
              val removedFiles = filesToRewrite.map(f =>
                getTouchedFile(deltaLog.dataPath, f, nameToAddFileMap))
              val (removedBytes, removedPartitions) =
                totalBytesAndDistinctPartitionValues(removedFiles)
              numBytesRemoved = removedBytes
              val (rewrittenBytes, rewrittenPartitions) =
                totalBytesAndDistinctPartitionValues(rewrittenFiles)
              numBytesAdded = rewrittenBytes
              if (txn.metadata.partitionColumns.nonEmpty) {
                numPartitionsRemovedFrom = Some(removedPartitions)
                numPartitionsAddedTo = Some(rewrittenPartitions)
              }
              numAddedChangeFiles = changeFiles.size
              changeFileBytes = changeFiles.collect { case f: AddCDCFile => f.size }.sum
              rewriteTimeMs =
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) - scanTimeMs
              numDeletedRows = Some(metrics("numDeletedRows").value)
              numCopiedRows = Some(metrics("numTouchedRows").value -
                metrics("numDeletedRows").value)

              val operationTimestamp = System.currentTimeMillis()
              removeFilesFromPaths(deltaLog, nameToAddFileMap, filesToRewrite,
                operationTimestamp) ++ rewrittenActions
            }
          }
        }
    }
    metrics("numRemovedFiles").set(numRemovedFiles)
    metrics("numAddedFiles").set(numAddedFiles)
    val executionTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
    metrics("executionTimeMs").set(executionTimeMs)
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    metrics("numAddedChangeFiles").set(numAddedChangeFiles)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numAddedBytes").set(numBytesAdded)
    metrics("numRemovedBytes").set(numBytesRemoved)
    metrics("numFilesBeforeSkipping").set(numFilesBeforeSkipping)
    metrics("numBytesBeforeSkipping").set(numBytesBeforeSkipping)
    metrics("numFilesAfterSkipping").set(numFilesAfterSkipping)
    metrics("numBytesAfterSkipping").set(numBytesAfterSkipping)
    metrics("numDeletionVectorsAdded").set(numDeletionVectorsAdded)
    metrics("numDeletionVectorsRemoved").set(numDeletionVectorsRemoved)
    metrics("numDeletionVectorsUpdated").set(numDeletionVectorsUpdated)
    numPartitionsAfterSkipping.foreach(metrics("numPartitionsAfterSkipping").set)
    numPartitionsAddedTo.foreach(metrics("numPartitionsAddedTo").set)
    numPartitionsRemovedFrom.foreach(metrics("numPartitionsRemovedFrom").set)
    numCopiedRows.foreach(metrics("numCopiedRows").set)
    txn.registerSQLMetrics(sparkSession, metrics)
    sendDriverMetrics(sparkSession, metrics)

    val numRecordsStats = NumRecordsStats.fromActions(deleteActions)
    val deleteMetrics = DeleteMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numFilesAfterSkipping,
        numAddedFiles,
        numRemovedFiles,
        numAddedFiles,
        numAddedChangeFiles = numAddedChangeFiles,
        numFilesBeforeSkipping,
        numBytesBeforeSkipping,
        numFilesAfterSkipping,
        numBytesAfterSkipping,
        numPartitionsAfterSkipping,
        numPartitionsAddedTo,
        numPartitionsRemovedFrom,
        numCopiedRows,
        numDeletedRows,
        numBytesAdded,
        numBytesRemoved,
        changeFileBytes = changeFileBytes,
        scanTimeMs,
        rewriteTimeMs,
        numDeletionVectorsAdded,
        numDeletionVectorsRemoved,
        numDeletionVectorsUpdated,
        numLogicalRecordsAdded = numRecordsStats.numLogicalRecordsAdded,
        numLogicalRecordsRemoved = numRecordsStats.numLogicalRecordsRemoved)

    val actionsToCommit = if (deleteActions.nonEmpty) {
      createSetTransaction(sparkSession, deltaLog).toSeq ++ deleteActions
    } else {
      Seq.empty
    }

    (actionsToCommit, deleteMetrics)
  }

  /**
   * Returns the list of `AddFile`s and `AddCDCFile`s that have been re-written.
   */
  private def rewriteFiles(
      txn: GpuOptimisticTransactionBase,
      baseData: DataFrame,
      filterCondition: Expression,
      numFilesToRewrite: Long): Seq[FileAction] = {
    val shouldWriteCdc = DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(txn.metadata)

    // number of total rows that we have seen / are either copying or deleting (sum of both).
    val numTouchedRows = metrics("numTouchedRows")
    val numTouchedRowsUdf = DeltaUDF.boolean {
      new GpuDeltaMetricUpdateUDF(numTouchedRows)
    }.asNondeterministic()

    withStatusCode(
      "DELTA", rewritingFilesMsg(numFilesToRewrite)) {
      val dfToWrite = if (shouldWriteCdc) {
        import org.apache.spark.sql.delta.commands.cdc.CDCReader._
        // The logic here ends up being surprisingly elegant, with all source rows ending up in
        // the output. Recall that we flipped the user-provided delete condition earlier, before the
        // call to `rewriteFiles`. All rows which match this latest `filterCondition` are retained
        // as table data, while all rows which don't match are removed from the rewritten table data
        // but do get included in the output as CDC events.
        baseData
            .filter(numTouchedRowsUdf())
            .withColumn(
              CDC_TYPE_COLUMN_NAME,
              new Column(If(filterCondition, CDC_TYPE_NOT_CDC, CDC_TYPE_DELETE))
            )
      } else {
        baseData
            .filter(numTouchedRowsUdf())
            .filter(new Column(filterCondition))
      }

      txn.writeFiles(dfToWrite)
    }
  }

  def shouldWritePersistentDeletionVectors(
      spark: SparkSession, txn: OptimisticTransaction): Boolean = {
    spark.conf.get(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }

}
