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

package com.databricks.sql.transaction.tahoe.rapids.shims

import java.net.URI

import scala.collection.mutable.ListBuffer

import ai.rapids.cudf.ColumnView
import com.databricks.sql.transaction.tahoe._
import com.databricks.sql.transaction.tahoe.actions.FileAction
import com.databricks.sql.transaction.tahoe.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging
import com.databricks.sql.transaction.tahoe.schema.InvariantViolationException
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.{GpuDeltaJobStatisticsTracker, GpuRapidsDeltaWriteExec, GpuStatisticsCollection, RapidsDeltaWrite}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.rapids.{BasicColumnarWriteJobStatsTracker, ColumnarWriteJobStatsTracker, GpuFileFormatWriter, GpuWriteJobStatsTracker}
import org.apache.spark.sql.rapids.GpuV1WriteUtils.GpuEmpty2Null
import org.apache.spark.sql.rapids.delta.GpuIdentityColumn
import org.apache.spark.sql.types.{StringType, StructType}
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
abstract class GpuOptimisticTransactionBase
    (deltaLog: DeltaLog, snapshot: Snapshot, rapidsConf: RapidsConf)
    (implicit clock: Clock)
  extends OptimisticTransaction(deltaLog, snapshot)(clock)
  with DeltaLogging {

  /**
   * Creates a new OptimisticTransaction.
   *
   * @param deltaLog The Delta Log for the table this transaction is modifying.
   * @param rapidsConf RAPIDS Accelerator config settings.
   */
  def this(deltaLog: DeltaLog, rapidsConf: RapidsConf)(implicit clock: Clock) {
    this(deltaLog, deltaLog.update(), rapidsConf)
  }

  /**
   * Adds checking of constraints on the table
   * @param plan Plan to generate the table to check against constraints
   * @param constraints Constraints to check on the table
   * @return GPU columnar plan to execute
   */
  private def addInvariantChecks(plan: SparkPlan, constraints: Seq[Constraint]): SparkPlan = {
    val cpuInvariants =
      DeltaInvariantCheckerExec.buildInvariantChecks(plan.output, constraints, plan.session)
    GpuCheckDeltaInvariant.maybeConvertToGpu(cpuInvariants, rapidsConf) match {
      case Some(gpuInvariants) =>
        val gpuPlan = convertToGpu(plan)
        GpuDeltaInvariantCheckerExec(gpuPlan, gpuInvariants)
      case None =>
        val cpuPlan = convertToCpu(plan)
        DeltaInvariantCheckerExec(cpuPlan, constraints)
    }
  }

  /** GPU version of convertEmptyToNullIfNeeded */
  private def gpuConvertEmptyToNullIfNeeded(
      plan: GpuExec,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (!spark.conf.get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        GpuAlias(GpuEmpty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) GpuProjectExec(projectList.toList, plan) else plan
  }

  /**
   * If there is any string partition column and there are constraints defined, add a projection to
   * convert empty string to null for that column. The empty strings will be converted to null
   * eventually even without this convert, but we want to do this earlier before check constraints
   * so that empty strings are correctly rejected. Note that this should not cause the downstream
   * logic in `FileFormatWriter` to add duplicate conversions because the logic there checks the
   * partition column using the original plan's output. When the plan is modified with additional
   * projections, the partition column check won't match and will not add more conversion.
   *
   * @param plan The original SparkPlan.
   * @param partCols The partition columns.
   * @param constraints The defined constraints.
   * @return A SparkPlan potentially modified with an additional projection on top of `plan`
   */
  override def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    // Reuse the CPU implementation if the plan ends up on the CPU, otherwise do the
    // equivalent on the GPU.
    plan match {
      case g: GpuExec => gpuConvertEmptyToNullIfNeeded(g, partCols, constraints)
      case _ => super.convertEmptyToNullIfNeeded(plan, partCols, constraints)
    }
  }

  override def writeFiles(
      inputData: Dataset[_],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    writeFiles(inputData, None, additionalConstraints)
  }

  private[shims] def shimPerformCDCPartition(inputData: Dataset[_]): (DataFrame, StructType)

  override def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = shimPerformCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (normalizedQueryExecution, output, generatedColumnConstraints, dataHighWaterMarks) =
      normalizeData(deltaLog, data)
    val highWaterMarks = trackHighWaterMarks.getOrElse(dataHighWaterMarks)

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

    val partitionColNames = partitionSchema.map(_.name).toSet

    // schema should be normalized, therefore we can do an equality check
    val statsDataSchema = output.filterNot(c => partitionColNames.contains(c.name))

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val optionalStatsTracker =
      if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {
        val indexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)
        val prefixLength =
          spark.sessionState.conf.getConf(DeltaSQLConf.DATA_SKIPPING_STRING_PREFIX_LENGTH)

        val statsCollection = new GpuStatisticsCollection {
          override def dataSchema: StructType = statsDataSchema.toStructType
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

    val identityTracker = GpuIdentityColumn.createIdentityColumnStatsTracker(
      spark,
      statsDataSchema,
      metadata.schema,
      highWaterMarks)

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

      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) => writeOptions.options
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
          statsTrackers = optionalStatsTracker.toSeq ++ identityTracker.toSeq ++ statsTrackers,
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

    identityTracker.foreach { tracker =>
      updatedIdentityHighWaterMarks.appendAll(tracker.highWaterMarks.toSeq)
    }
    resultFiles.toSeq ++ committer.changeFiles
  }

  private def convertToCpu(plan: SparkPlan): SparkPlan = plan match {
    case GpuRowToColumnarExec(p, _) => p
    case p: GpuExec => GpuColumnarToRowExec(p)
    case p => p
  }

  private def convertToGpu(plan: SparkPlan): SparkPlan = plan match {
    case GpuColumnarToRowExec(p, _) => p
    case p: GpuExec => p
    case p => GpuRowToColumnarExec(p, TargetSize(rapidsConf.gpuTargetBatchSizeBytes))
  }
}
