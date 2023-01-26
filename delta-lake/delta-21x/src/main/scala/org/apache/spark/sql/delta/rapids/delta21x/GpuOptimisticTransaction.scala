/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.delta.rapids.delta21x

import java.net.URI

import scala.collection.mutable.ListBuffer

import ai.rapids.cudf.ColumnView
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints}
import org.apache.spark.sql.delta.rapids.GpuOptimisticTransactionBase
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker, GpuFileFormatWriter, GpuWriteJobStatsTracker}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Clock, SerializableConfiguration}

/**
 * Used to perform a set of reads in a transaction and then commit a set of updates to the
 * state of the log.  All reads from the DeltaLog, MUST go through this instance rather
 * than directly to the DeltaLog otherwise they will not be check for logical conflicts
 * with concurrent updates.
 *
 * This class is not thread-safe.
 *
 * @param deltaLog The Delta Log for the table this transaction is modifying.
 * @param snapshot The snapshot that this transaction is reading at.
 * @param rapidsConf RAPIDS Accelerator config settings.
 */
class GpuOptimisticTransaction
    (deltaLog: DeltaLog, snapshot: Snapshot, rapidsConf: RapidsConf)
    (implicit clock: Clock)
  extends GpuOptimisticTransactionBase(deltaLog, snapshot, rapidsConf)(clock) {

  /** Creates a new OptimisticTransaction.
   *
   * @param deltaLog The Delta Log for the table this transaction is modifying.
   * @param rapidsConf RAPIDS Accelerator config settings
   */
  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) {
    this(deltaLog, deltaLog.update(), rapidsConf)
  }

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (normalizedQueryExecution, output, generatedColumnConstraints, _) =
      normalizeData(deltaLog, data)

    // Build a new plan with a stub GpuDeltaWrite node to work around undesired transitions between
    // columns and rows when AQE is involved. Without this node in the plan, AdaptiveSparkPlanExec
    // could be the root node of the plan. In that case we do not have enough context to know
    // whether the AdaptiveSparkPlanExec should be columnar or not, since the GPU overrides do not
    // see how the parent is using the AdaptiveSparkPlanExec outputs. By using this stub node that
    // appears to be a data writing node to AQE (it derives from V2CommandExec), the
    // AdaptiveSparkPlanExec will be planned as a child of this new node. That provides enough
    // context to plan the AQE sub-plan properly with respect to columnar and row transitions.
    // We could force the AQE node to be columnar here by explicitly replacing the node, but that
    // breaks the connection between the queryExecution and the node that will actually execute.
    val gpuWritePlan = Dataset.ofRows(spark, RapidsDeltaWrite(normalizedQueryExecution.logical))
    val queryExecution = gpuWritePlan.queryExecution

    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val optionalStatsTracker =
      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
        val partitionColNames = partitionSchema.map(_.name).toSet

        // schema should be normalized, therefore we can do an equality check
        val statsDataSchema = output.filterNot(c => partitionColNames.contains(c.name))

        val indexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)
        val prefixLength =
          spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)

        val statsCollection = new GpuStatisticsCollection {
          override val tableDataSchema: StructType = statsDataSchema.toStructType
          override val dataSchema: StructType = tableDataSchema
          override val numIndexedCols: Int = indexedCols
          override val stringPrefixLength: Int = prefixLength
        }

        val statsColExpr: Expression = {
          val dummyDF = Dataset.ofRows(spark, LocalRelation(statsDataSchema))
          dummyDF.select(to_json(statsCollection.statsCollector))
              .queryExecution.analyzed.expressions.head
        }

        val statsSchema = statsCollection.statCollectionSchema
        val batchStatsToRow = (columnViews: Array[ColumnView], row: InternalRow) => {
          GpuStatisticsCollection.batchStatsToRow(statsSchema, columnViews, row)
        }
        Some(new GpuDeltaJobStatisticsTracker(statsDataSchema, statsColExpr, batchStatsToRow))
      } else {
        None
      }

    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      // Remove any unnecessary row conversions added as part of Spark planning
      val queryPhysicalPlan = queryExecution.executedPlan match {
        case GpuColumnarToRowExec(child, _) => child
        case p => p
      }
      val gpuRapidsWrite = queryPhysicalPlan match {
        case g: GpuRapidsDeltaWriteExec => Some(g)
        case _ => None
      }

      val empty2NullPlan = convertEmptyToNullIfNeeded(queryPhysicalPlan,
        partitioningColumns, constraints)
      val planWithInvariants = addInvariantChecks(empty2NullPlan, constraints)
      val physicalPlan = convertToGpu(planWithInvariants)

      val statsTrackers: ListBuffer[ColumnarWriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicColumnarWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
        gpuRapidsWrite.foreach { grw =>
          val hadoopConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
          val tracker = new GpuWriteJobStatsTracker(hadoopConf, grw.basicMetrics, grw.taskMetrics)
          statsTrackers.append(tracker)
        }
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filterKeys { key =>
            key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
                key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }.toMap
      }

      val gpuFileFormat = deltaLog.fileFormat(metadata) match {
        case _: DeltaParquetFileFormat => new GpuParquetFileFormat
        case f => throw new IllegalStateException(s"file format $f is not supported")
      }

      try {
        GpuFileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = gpuFileFormat,
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTracker.toSeq ++ statsTrackers,
          options = options,
          rapidsConf.stableSort,
          rapidsConf.concurrentWriterPartitionFlushSize)
      } catch {
        case s: SparkException =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      }
    }

    val resultFiles = committer.addedStatuses.map { a =>
      a.copy(stats = optionalStatsTracker.map(
        _.recordedStats(new Path(new URI(a.path)).getName)).getOrElse(a.stats))
    }

    resultFiles.toSeq ++ committer.changeFiles
  }
}
