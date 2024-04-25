/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
 * This file was derived from MergeIntoCommand.scala
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

package org.apache.spark.sql.delta.rapids.delta24x

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.nvidia.spark.rapids.delta._
import com.nvidia.spark.rapids.{BaseExprMeta, GpuOverrides, RapidsConf}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BasePredicate, Expression, Literal, NamedExpression, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.commands.merge.MergeIntoMaterializeSource
import org.apache.spark.sql.delta.rapids.GpuDeltaLog
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable

case class GpuMergeDataSizes(
                              @JsonDeserialize(contentAs = classOf[java.lang.Long])
                              rows: Option[Long] = None,
                              @JsonDeserialize(contentAs = classOf[java.lang.Long])
                              files: Option[Long] = None,
                              @JsonDeserialize(contentAs = classOf[java.lang.Long])
                              bytes: Option[Long] = None,
                              @JsonDeserialize(contentAs = classOf[java.lang.Long])
                              partitions: Option[Long] = None)

/**
 * Represents the state of a single merge clause:
 * - merge clause's (optional) predicate
 * - action type (insert, update, delete)
 * - action's expressions
 */
case class GpuMergeClauseStats(
                                condition: Option[String],
                                actionType: String,
                                actionExpr: Seq[String])

object GpuMergeClauseStats {
  def apply(mergeClause: DeltaMergeIntoClause): GpuMergeClauseStats = {
    GpuMergeClauseStats(
      condition = mergeClause.condition.map(_.sql),
      mergeClause.clauseType.toLowerCase(),
      actionExpr = mergeClause.actions.map(_.sql))
  }
}

/** State for a GPU merge operation */
case class GpuMergeStats(
                          // Merge condition expression
                          conditionExpr: String,

                          // Expressions used in old MERGE stats, now always Null
                          updateConditionExpr: String,
                          updateExprs: Seq[String],
                          insertConditionExpr: String,
                          insertExprs: Seq[String],
                          deleteConditionExpr: String,

                          // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE
                          matchedStats: Seq[GpuMergeClauseStats],
                          notMatchedStats: Seq[GpuMergeClauseStats],
                          notMatchedBySourceStats: Seq[GpuMergeClauseStats],

                          // Timings
                          executionTimeMs: Long,
                          scanTimeMs: Long,
                          rewriteTimeMs: Long,

                          // Data sizes of source and target at different stages of processing
                          source: GpuMergeDataSizes,
                          targetBeforeSkipping: GpuMergeDataSizes,
                          targetAfterSkipping: GpuMergeDataSizes,
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          sourceRowsInSecondScan: Option[Long],

                          // Data change sizes
                          targetFilesRemoved: Long,
                          targetFilesAdded: Long,
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetChangeFilesAdded: Option[Long],
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetChangeFileBytes: Option[Long],
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetBytesRemoved: Option[Long],
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetBytesAdded: Option[Long],
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetPartitionsRemovedFrom: Option[Long],
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          targetPartitionsAddedTo: Option[Long],
                          targetRowsCopied: Long,
                          targetRowsUpdated: Long,
                          targetRowsMatchedUpdated: Long,
                          targetRowsNotMatchedBySourceUpdated: Long,
                          targetRowsInserted: Long,
                          targetRowsDeleted: Long,
                          targetRowsMatchedDeleted: Long,
                          targetRowsNotMatchedBySourceDeleted: Long,

                          // MergeMaterializeSource stats
                          materializeSourceReason: Option[String] = None,
                          @JsonDeserialize(contentAs = classOf[java.lang.Long])
                          materializeSourceAttempts: Option[Long] = None
                        )

object GpuMergeStats {

  def fromMergeSQLMetrics(
                           metrics: Map[String, SQLMetric],
                           condition: Expression,
                           matchedClauses: Seq[DeltaMergeIntoMatchedClause],
                           notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
                           notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
                           isPartitioned: Boolean): GpuMergeStats = {

    def metricValueIfPartitioned(metricName: String): Option[Long] = {
      if (isPartitioned) Some(metrics(metricName).value) else None
    }

    GpuMergeStats(
      // Merge condition expression
      conditionExpr = condition.sql,

      // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/
      // NOT MATCHED BY SOURCE
      matchedStats = matchedClauses.map(GpuMergeClauseStats(_)),
      notMatchedStats = notMatchedClauses.map(GpuMergeClauseStats(_)),
      notMatchedBySourceStats = notMatchedBySourceClauses.map(GpuMergeClauseStats(_)),

      // Timings
      executionTimeMs = metrics("executionTimeMs").value,
      scanTimeMs = metrics("scanTimeMs").value,
      rewriteTimeMs = metrics("rewriteTimeMs").value,

      // Data sizes of source and target at different stages of processing
      source = GpuMergeDataSizes(rows = Some(metrics("numSourceRows").value)),
      targetBeforeSkipping =
        GpuMergeDataSizes(
          files = Some(metrics("numTargetFilesBeforeSkipping").value),
          bytes = Some(metrics("numTargetBytesBeforeSkipping").value)),
      targetAfterSkipping =
        GpuMergeDataSizes(
          files = Some(metrics("numTargetFilesAfterSkipping").value),
          bytes = Some(metrics("numTargetBytesAfterSkipping").value),
          partitions = metricValueIfPartitioned("numTargetPartitionsAfterSkipping")),
      sourceRowsInSecondScan =
        metrics.get("numSourceRowsInSecondScan").map(_.value).filter(_ >= 0),

      // Data change sizes
      targetFilesAdded = metrics("numTargetFilesAdded").value,
      targetChangeFilesAdded = metrics.get("numTargetChangeFilesAdded").map(_.value),
      targetChangeFileBytes = metrics.get("numTargetChangeFileBytes").map(_.value),
      targetFilesRemoved = metrics("numTargetFilesRemoved").value,
      targetBytesAdded = Some(metrics("numTargetBytesAdded").value),
      targetBytesRemoved = Some(metrics("numTargetBytesRemoved").value),
      targetPartitionsRemovedFrom = metricValueIfPartitioned("numTargetPartitionsRemovedFrom"),
      targetPartitionsAddedTo = metricValueIfPartitioned("numTargetPartitionsAddedTo"),
      targetRowsCopied = metrics("numTargetRowsCopied").value,
      targetRowsUpdated = metrics("numTargetRowsUpdated").value,
      targetRowsMatchedUpdated = metrics("numTargetRowsMatchedUpdated").value,
      targetRowsNotMatchedBySourceUpdated = metrics("numTargetRowsNotMatchedBySourceUpdated").value,
      targetRowsInserted = metrics("numTargetRowsInserted").value,
      targetRowsDeleted = metrics("numTargetRowsDeleted").value,
      targetRowsMatchedDeleted = metrics("numTargetRowsMatchedDeleted").value,
      targetRowsNotMatchedBySourceDeleted = metrics("numTargetRowsNotMatchedBySourceDeleted").value,

      // Deprecated fields
      updateConditionExpr = null,
      updateExprs = null,
      insertConditionExpr = null,
      insertExprs = null,
      deleteConditionExpr = null)
  }
}

/**
 * GPU version of Delta Lake's MergeIntoCommand.
 *
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 * the condition and verify that no two source rows match with the same target row.
 * This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 * for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source                    Source data to merge from
 * @param target                    Target table to merge into
 * @param gpuDeltaLog               Delta log to use
 * @param condition                 Condition for a source row to match with a target row
 * @param matchedClauses            All info related to matched clauses.
 * @param notMatchedClauses         All info related to not matched clauses.
 * @param notMatchedBySourceClauses All info related to not matched by source clauses.
 * @param migratedSchema            The final schema of the target - may be changed by schema
 *                                  evolution.
 */
case class GpuMergeIntoCommand(
                                @transient source: LogicalPlan,
                                @transient target: LogicalPlan,
                                @transient gpuDeltaLog: GpuDeltaLog,
                                condition: Expression,
                                matchedClauses: Seq[DeltaMergeIntoMatchedClause],
                                notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
                                notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
                                migratedSchema: Option[StructType])(
                                @transient val rapidsConf: RapidsConf)
  extends LeafRunnableCommand
    with DeltaCommand
    with PredicateHelper
    with AnalysisHelper
    with ImplicitMetadataOperation
    with MergeIntoMaterializeSource {

  import GpuMergeIntoCommand._
  import SQLMetrics._

  override val otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

  override val canMergeSchema: Boolean = conf.getConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient private lazy val targetDeltaLog: DeltaLog = gpuDeltaLog.deltaLog
  /**
   * Map to get target output attributes by name.
   * The case sensitivity of the map is set accordingly to Spark configuration.
   */
  @transient private lazy val targetOutputAttributesMap: Map[String, Attribute] = {
    val attrMap: Map[String, Attribute] = target
      .outputSet.view
      .map(attr => attr.name -> attr).toMap
    if (conf.caseSensitiveAnalysis) {
      attrMap
    } else {
      CaseInsensitiveMap(attrMap)
    }
  }

  /** Whether this merge statement has only a single insert (NOT MATCHED) clause. */
  private def isSingleInsertOnly: Boolean =
    matchedClauses.isEmpty && notMatchedBySourceClauses.isEmpty && notMatchedClauses.length == 1

  /** Whether this merge statement has no insert (NOT MATCHED) clause. */
  private def hasNoInserts: Boolean = notMatchedClauses.isEmpty

  // We over-count numTargetRowsDeleted when there are multiple matches;
  // this is the amount of the overcount, so we can subtract it to get a correct final metric.
  //  private var multipleMatchDeleteOnlyOvercount: Option[Long] = None

  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numSourceRowsInSecondScan" ->
      createMetric(sc, "number of source rows (during repeated scan)"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsMatchedUpdated" ->
      createMetric(sc, "number of rows updated by a matched clause"),
    "numTargetRowsNotMatchedBySourceUpdated" ->
      createMetric(sc, "number of rows updated by a not matched by source clause"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsMatchedDeleted" ->
      createMetric(sc, "number of rows deleted by a matched clause"),
    "numTargetRowsNotMatchedBySourceDeleted" ->
      createMetric(sc, "number of rows deleted by a not matched by source clause"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"),
    "numTargetChangeFilesAdded" ->
      createMetric(sc, "number of change data capture files generated"),
    "numTargetChangeFileBytes" ->
      createMetric(sc, "total size of change data capture files generated"),
    "numTargetBytesBeforeSkipping" -> createMetric(sc, "number of target bytes before skipping"),
    "numTargetBytesAfterSkipping" -> createMetric(sc, "number of target bytes after skipping"),
    "numTargetBytesRemoved" -> createMetric(sc, "number of target bytes removed"),
    "numTargetBytesAdded" -> createMetric(sc, "number of target bytes added"),
    "numTargetPartitionsAfterSkipping" ->
      createMetric(sc, "number of target partitions after skipping"),
    "numTargetPartitionsRemovedFrom" ->
      createMetric(sc, "number of target partitions from which files were removed"),
    "numTargetPartitionsAddedTo" ->
      createMetric(sc, "number of target partitions to which files were added"),
    "executionTimeMs" ->
      createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createTimingMetric(sc, "time taken to rewrite the matched files"))

  override def run(spark: SparkSession): Seq[Row] = {
    metrics("executionTimeMs").set(0)
    metrics("scanTimeMs").set(0)
    metrics("rewriteTimeMs").set(0)

    if (migratedSchema.isDefined) {
      // Block writes of void columns in the Delta log. Currently void columns are not properly
      // supported and are dropped on read, but this is not enough for merge command that is also
      // reading the schema from the Delta log. Until proper support we prefer to fail merge
      // queries that add void columns.
      val newNullColumn = SchemaUtils.findNullTypeColumn(migratedSchema.get)
      if (newNullColumn.isDefined) {
        throw new AnalysisException(
          s"""Cannot add column '${newNullColumn.get}' with type 'void'. Please explicitly specify a
             |non-void type.""".stripMargin.replaceAll("\n", " ")
        )
      }
    }
    runMerge(spark)
    //    val (materializeSource, _) = shouldMaterializeSource(spark, source, isSingleInsertOnly)
    //    if (!materializeSource) {
    //    } else {
    //      // If it is determined that source should be materialized, wrap the execution with retries,
    //      // in case the data of the materialized source is lost.
    //      runWithMaterializedSourceLostRetries(
    //        spark, targetDeltaLog, metrics, runMerge)
    //    }
  }

  protected def runMerge(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
      val startTime = System.nanoTime()
      gpuDeltaLog.withNewTransaction { deltaTxn =>
        if (hasBeenExecuted(deltaTxn, spark)) {
          sendDriverMetrics(spark, metrics)
          return Seq.empty
        }
        if (target.schema.size != deltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = deltaTxn.metadata.schema)
        }

        if (canMergeSchema) {
          updateMetadata(
            spark, deltaTxn, migratedSchema.getOrElse(target.schema),
            deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }

        // If materialized, prepare the DF reading the materialize source
        // Otherwise, prepare a regular DF from source plan.
        val materializeSourceReason = prepareSourceDFAndReturnMaterializeReason(
          spark,
          source,
          condition,
          matchedClauses,
          notMatchedClauses,
          isSingleInsertOnly)

        val deltaActions = {
          if (isSingleInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            writeInsertsOnlyWhenNoMatchedClauses(spark, deltaTxn)
          } else {
            val filesToRewrite = findTouchedFiles(spark, deltaTxn)
            val newWrittenFiles = lowShuffleMerge(spark, deltaTxn, filesToRewrite)
            //              writeAllChanges(spark, deltaTxn, filesToRewrite)
            filesToRewrite.map(_.remove) ++ newWrittenFiles
          }
        }

        val finalActions = createSetTransaction(spark, targetDeltaLog).toSeq ++ deltaActions
        // Metrics should be recorded before commit (where they are written to delta logs).
        metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
        deltaTxn.registerSQLMetrics(spark, metrics)

        // This is a best-effort sanity check.
        if (metrics("numSourceRowsInSecondScan").value >= 0 &&
          metrics("numSourceRows").value != metrics("numSourceRowsInSecondScan").value) {
          log.warn(s"Merge source has ${metrics("numSourceRows")} rows in initial scan but " +
            s"${metrics("numSourceRowsInSecondScan")} rows in second scan")
          if (conf.getConf(DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED)) {
            throw DeltaErrors.sourceNotDeterministicInMergeException(spark)
          }
        }

        deltaTxn.commitIfNeeded(
          finalActions,
          DeltaOperations.Merge(
            Option(condition),
            matchedClauses.map(DeltaOperations.MergePredicate(_)),
            notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
            notMatchedBySourceClauses.map(DeltaOperations.MergePredicate(_))))

        // Record metrics
        var stats = GpuMergeStats.fromMergeSQLMetrics(
          metrics,
          condition,
          matchedClauses,
          notMatchedClauses,
          notMatchedBySourceClauses,
          deltaTxn.metadata.partitionColumns.nonEmpty)
        stats = stats.copy(
          materializeSourceReason = Some(materializeSourceReason.toString),
          materializeSourceAttempts = Some(attempt))

        recordDeltaEvent(targetDeltaLog, "delta.dml.merge.stats", data = stats)

      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
    }
    sendDriverMetrics(spark, metrics)
    Seq(Row(metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value +
      metrics("numTargetRowsInserted").value, metrics("numTargetRowsUpdated").value,
      metrics("numTargetRowsDeleted").value, metrics("numTargetRowsInserted").value))
  }

  /**
   * Find the target table files that contain the rows that satisfy the merge condition. This is
   * implemented as an inner-join between the source query/table and the target table using
   * the merge condition.
   */
  private def findTouchedFiles(
                                spark: SparkSession,
                                deltaTxn: OptimisticTransaction
                              ): Seq[AddFile] = recordMergeOperation(sqlMetricName = "scanTimeMs") {

    // Accumulator to collect all the distinct touched files
    val touchedFilesAccum = new SetAccumulator[String]()
    spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

    // UDFs to records touched files names and add them to the accumulator
    val recordTouchedFileName = DeltaUDF.intFromString(
      new GpuDeltaRecordTouchedFileNameUDF(touchedFilesAccum)).asNondeterministic()

    // Prune non-matching files if we don't need to collect them for NOT MATCHED BY SOURCE clauses.
    val dataSkippedFiles =
      if (notMatchedBySourceClauses.isEmpty) {
        val targetOnlyPredicates =
          splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))
        deltaTxn.filterFiles(targetOnlyPredicates)
      } else {
        deltaTxn.filterFiles()
      }

    // UDF to increment metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val sourceDF = getSourceDF()
      .filter(new Column(incrSourceRowCountExpr))

    // Join the source and target table using the merge condition to find touched files. An inner
    // join collects all candidate files for MATCHED clauses, a right outer join also includes
    // candidates for NOT MATCHED BY SOURCE clauses.
    // In addition, we attach two columns
    // - a monotonically increasing row id for target rows to later identify whether the same
    //     target row is modified by multiple user or not
    // - the target file name the row is from to later identify the files touched by matched rows
    val joinType = if (notMatchedBySourceClauses.isEmpty) "inner" else "right_outer"
    val targetDF = buildTargetPlanWithFiles(spark, deltaTxn, dataSkippedFiles)
      .withColumn(ROW_ID_COL, monotonically_increasing_id())
      .withColumn(TARGET_FILENAME_COL, input_file_name())
    val joinToFindTouchedFiles = sourceDF.join(targetDF, new Column(condition), joinType)

    // Process the matches from the inner join to record touched files and find multiple matches
    val collectTouchedFiles = joinToFindTouchedFiles
      .select(col(ROW_ID_COL), recordTouchedFileName(col(TARGET_FILENAME_COL)).as("one"))

    // Calculate frequency of matches per source row
    val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))

    // Get multiple matches and simultaneously collect (using touchedFilesAccum) the file names
    // multipleMatchCount = # of target rows with more than 1 matching source row (duplicate match)
    // multipleMatchSum = total # of duplicate matched rows
    import org.apache.spark.sql.delta.implicits._
    val (multipleMatchCount, multipleMatchSum) = matchedRowCounts
      .filter("count > 1")
      .select(coalesce(count(new Column("*")), lit(0)), coalesce(sum("count"), lit(0)))
      .as[(Long, Long)]
      .collect()
      .head

    val hasMultipleMatches = multipleMatchCount > 0

    // Throw error if multiple matches are ambiguous or cannot be computed correctly.
    val canBeComputedUnambiguously = {
      // Multiple matches are not ambiguous when there is only one unconditional delete as
      // all the matched row pairs in the 2nd join in `writeAllChanges` will get deleted.
      val isUnconditionalDelete = matchedClauses.headOption match {
        case Some(DeltaMergeIntoMatchedDeleteClause(None)) => true
        case _ => false
      }
      matchedClauses.size == 1 && isUnconditionalDelete
    }

    if (hasMultipleMatches && !canBeComputedUnambiguously) {
      throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
    }

    if (hasMultipleMatches) {
      // This is only allowed for delete-only queries.
      // This query will count the duplicates for numTargetRowsDeleted in Job 2,
      // because we count matches after the join and not just the target rows.
      // We have to compensate for this by subtracting the duplicates later,
      // so we need to record them here.
      val duplicateCount = multipleMatchSum - multipleMatchCount
      println(s"Has multiple matches, duplicate count: $duplicateCount")
      //      multipleMatchDeleteOnlyOvercount = Some(duplicateCount)
    }

    // Get the AddFiles using the touched file names.
    val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
    logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

    val nameToAddFileMap = generateCandidateFileMap(targetDeltaLog.dataPath, dataSkippedFiles)
    val touchedAddFiles = touchedFileNames.map(f =>
      getTouchedFile(targetDeltaLog.dataPath, f, nameToAddFileMap))

    // When the target table is empty, and the optimizer optimized away the join entirely
    // numSourceRows will be incorrectly 0. We need to scan the source table once to get the correct
    // metric here.
    if (metrics("numSourceRows").value == 0 &&
      (dataSkippedFiles.isEmpty || targetDF.take(1).isEmpty)) {
      val numSourceRows = sourceDF.count()
      metrics("numSourceRows").set(numSourceRows)
    }

    // Update metrics
    metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
    val (afterSkippingBytes, afterSkippingPartitions) =
      totalBytesAndDistinctPartitionValues(dataSkippedFiles)
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
    metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
    val (removedBytes, removedPartitions) = totalBytesAndDistinctPartitionValues(touchedAddFiles)
    metrics("numTargetFilesRemoved") += touchedAddFiles.size
    metrics("numTargetBytesRemoved") += removedBytes
    metrics("numTargetPartitionsRemovedFrom") += removedPartitions
    touchedAddFiles
  }

  /**
   * This is an optimization of the case when there is no update clause for the merge.
   * We perform an left anti join on the source data to find the rows to be inserted.
   *
   * This will currently only optimize for the case when there is a _single_ notMatchedClause.
   */
  private def writeInsertsOnlyWhenNoMatchedClauses(
                                                    spark: SparkSession,
                                                    deltaTxn: OptimisticTransaction
                                                  ): Seq[FileAction] = recordMergeOperation(sqlMetricName = "rewriteTimeMs") {

    // UDFs to update metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    val incrInsertedCountExpr = makeMetricUpdateUDF("numTargetRowsInserted")

    val outputColNames = getTargetOutputCols(deltaTxn).map(_.name)
    // we use head here since we know there is only a single notMatchedClause
    val outputExprs = notMatchedClauses.head.resolvedActions.map(_.expr)
    val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
      new Column(Alias(expr, name)())
    }

    // source DataFrame
    val sourceDF = getSourceDF()
      .filter(new Column(incrSourceRowCountExpr))
      .filter(new Column(notMatchedClauses.head.condition.getOrElse(Literal.TrueLiteral)))

    // Skip data based on the merge condition
    val conjunctivePredicates = splitConjunctivePredicates(condition)
    val targetOnlyPredicates =
      conjunctivePredicates.filter(_.references.subsetOf(target.outputSet))
    val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

    // target DataFrame
    val targetDF = buildTargetPlanWithFiles(spark, deltaTxn, dataSkippedFiles)

    val insertDf = sourceDF.join(targetDF, new Column(condition), "leftanti")
      .select(outputCols: _*)
      .filter(new Column(incrInsertedCountExpr))

    val newFiles = deltaTxn
      .writeFiles(repartitionIfNeeded(spark, insertDf, deltaTxn.metadata.partitionColumns))
      .filter {
        // In some cases (e.g. insert-only when all rows are matched, insert-only with an empty
        // source, insert-only with an unsatisfied condition) we can write out an empty insertDf.
        // This is hard to catch before the write without collecting the DF ahead of time. Instead,
        // we can just accept only the AddFiles that actually add rows or
        // when we don't know the number of records
        case a: AddFile => a.numLogicalRecords.forall(_ > 0)
        case _ => true
      }

    // Update metrics
    metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
    metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
    val (afterSkippingBytes, afterSkippingPartitions) =
      totalBytesAndDistinctPartitionValues(dataSkippedFiles)
    metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
    metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
    metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
    metrics("numTargetFilesRemoved") += 0
    metrics("numTargetBytesRemoved") += 0
    metrics("numTargetPartitionsRemovedFrom") += 0
    val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
    metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
    metrics("numTargetBytesAdded") += addedBytes
    metrics("numTargetPartitionsAddedTo") += addedPartitions
    newFiles
  }

  /**
   * Write new files by reading the touched files and updating/inserting data using the source
   * query/table. This is implemented using a full|right-outer-join using the merge condition.
   *
   * Note that unlike the insert-only code paths with just one control column INCR_ROW_COUNT_COL,
   * this method has two additional control columns ROW_DROPPED_COL for dropping deleted rows and
   * CDC_TYPE_COL_NAME used for handling CDC when enabled.
   */
  //  private def writeAllChanges(
  //                               spark: SparkSession,
  //                               deltaTxn: OptimisticTransaction,
  //                               filesToRewrite: Seq[AddFile]
  //                             ): Seq[FileAction] = recordMergeOperation(sqlMetricName = "rewriteTimeMs") {
  //    import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
  //
  //    val cdcEnabled = DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(deltaTxn.metadata)
  //
  //    var targetOutputCols = getTargetOutputCols(deltaTxn)
  //
  //    val mergedOutputCols = targetOutputCols ++
  //      List(AttributeReference(TARGET_ROW_PRESENT_COL, BooleanType, nullable = false)(),
  //        AttributeReference(TARGET_FILENAME_COL, StringType, nullable = false)(),
  //        AttributeReference(TARGET_ROW_ID_COL, LongType, nullable = false)())
  //
  //    var outputRowSchema = deltaTxn.metadata.schema
  //
  //    // When we have duplicate matches (only allowed when the whenMatchedCondition is a delete with
  //    // no match condition) we will incorrectly generate duplicate CDC rows.
  //    // Duplicate matches can be due to:
  //    // - Duplicate rows in the source w.r.t. the merge condition
  //    // - A target-only or source-only merge condition, which essentially turns our join into a cross
  //    //   join with the target/source satisfiying the merge condition.
  //    // These duplicate matches are dropped from the main data output since this is a delete
  //    // operation, but the duplicate CDC rows are not removed by default.
  //    // See https://github.com/delta-io/delta/issues/1274
  //
  //    // We address this specific scenario by adding row ids to the target before performing our join.
  //    // There should only be one CDC delete row per target row so we can use these row ids to dedupe
  //    // the duplicate CDC delete rows.
  //
  //    // We also need to address the scenario when there are duplicate matches with delete and we
  //    // insert duplicate rows. Here we need to additionally add row ids to the source before the
  //    // join to avoid dropping these valid duplicate inserted rows and their corresponding cdc rows.
  //
  //    // When there is an insert clause, we set SOURCE_ROW_ID_COL=null for all delete rows because we
  //    // need to drop the duplicate matches.
  //    val isDeleteWithDuplicateMatchesAndCdc = multipleMatchDeleteOnlyOvercount.nonEmpty && cdcEnabled
  //
  //    // Generate a new target dataframe that has same output attributes exprIds as the target plan.
  //    // This allows us to apply the existing resolved update/insert expressions.
  //    val baseTargetDF = buildTargetPlanWithFiles(spark, deltaTxn, filesToRewrite)
  //    val joinType = if (hasNoInserts &&
  //      spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
  //      "rightOuter"
  //    } else {
  //      "fullOuter"
  //    }
  //
  //    logDebug(
  //      s"""writeAllChanges using $joinType join:
  //         |  source.output: ${source.outputSet}
  //         |  target.output: ${target.outputSet}
  //         |  condition: $condition
  //         |  newTarget.output: ${baseTargetDF.queryExecution.logical.outputSet}
  //       """.stripMargin)
  //
  //    // UDFs to update metrics
  //    // Make UDFs that appear in the custom join processor node deterministic, as they always
  //    // return true and update a metric. Catalyst precludes non-deterministic UDFs that are not
  //    // allowed outside a very specific set of Catalyst nodes (Project, Filter, Window, Aggregate).
  //    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRowsInSecondScan",
  //      deterministic = true)
  //    val incrUpdatedCountExpr = makeMetricUpdateUDF("numTargetRowsUpdated", deterministic = true)
  //    val incrUpdatedMatchedCountExpr = makeMetricUpdateUDF("numTargetRowsMatchedUpdated",
  //      deterministic = true)
  //    val incrUpdatedNotMatchedBySourceCountExpr =
  //      makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceUpdated", deterministic = true)
  //    val incrInsertedCountExpr = makeMetricUpdateUDF("numTargetRowsInserted", deterministic = true)
  //    val incrNoopCountExpr = makeMetricUpdateUDF("numTargetRowsCopied", deterministic = true)
  //    val incrDeletedCountExpr = makeMetricUpdateUDF("numTargetRowsDeleted", deterministic = true)
  //    val incrDeletedMatchedCountExpr = makeMetricUpdateUDF("numTargetRowsMatchedDeleted",
  //      deterministic = true)
  //    val incrDeletedNotMatchedBySourceCountExpr =
  //      makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceDeleted", deterministic = true)
  //
  //    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
  //    // with value `true`, one to each side of the join. Whether this field is null or not after
  //    // the outer join, will allow us to identify whether the resultant joined row was a
  //    // matched inner result or an unmatched result with null on one side.
  //    // We add row IDs to the targetDF if we have a delete-when-matched clause with duplicate
  //    // matches and CDC is enabled, and additionally add row IDs to the source if we also have an
  //    // insert clause. See above at isDeleteWithDuplicateMatchesAndCdc definition for more details.
  //    var sourceDF = getSourceDF()
  //      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))
  //    val targetDF = baseTargetDF
  //      .withColumn(TARGET_ROW_PRESENT_COL, lit(true))
  //      .withColumn(TARGET_FILENAME_COL, input_file_name())
  //      .withColumn(TARGET_ROW_ID_COL, monotonically_increasing_id())
  //    if (isDeleteWithDuplicateMatchesAndCdc) {
  //      if (notMatchedClauses.nonEmpty) { // insert clause
  //        sourceDF = sourceDF.withColumn(SOURCE_ROW_ID_COL, monotonically_increasing_id())
  //      }
  //    }
  //    val joinedDF = sourceDF.join(targetDF, new Column(condition), joinType)
  //    val joinedPlan = joinedDF.queryExecution.analyzed
  //
  //    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
  //      tryResolveReferencesForExpressions(spark, exprs, joinedPlan)
  //    }
  //
  //    // ==== Generate the expressions to process full-outer join output and generate target rows ====
  //    // If there are N columns in the target table, there will be N + 3 columns after processing
  //    // - N columns for target table
  //    // - ROW_DROPPED_COL to define whether the generated row should dropped or written
  //    // - INCR_ROW_COUNT_COL containing a UDF to update the output row row counter
  //    // - CDC_TYPE_COLUMN_NAME containing the type of change being performed in a particular row
  //
  //    // To generate these N + 3 columns, we will generate N + 3 expressions and apply them to the
  //    // rows in the joinedDF. The CDC column will be either used for CDC generation or dropped before
  //    // performing the final write, and the other two will always be dropped after executing the
  //    // metrics UDF and filtering on ROW_DROPPED_COL.
  //
  //    // We produce rows for both the main table data (with CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC),
  //    // and rows for the CDC data which will be output to CDCReader.CDC_LOCATION.
  //    // See [[CDCReader]] for general details on how partitioning on the CDC type column works.
  //
  //    // In the following functions `updateOutput`, `deleteOutput` and `insertOutput`, we
  //    // produce a Seq[Expression] for each intended output row.
  //    // Depending on the clause and whether CDC is enabled, we output between 0 and 3 rows, as a
  //    // Seq[Seq[Expression]]
  //
  //    // There is one corner case outlined above at isDeleteWithDuplicateMatchesAndCdc definition.
  //    // When we have a delete-ONLY merge with duplicate matches we have N + 4 columns:
  //    // N target cols, TARGET_ROW_ID_COL, ROW_DROPPED_COL, INCR_ROW_COUNT_COL, CDC_TYPE_COLUMN_NAME
  //    // When we have a delete-when-matched merge with duplicate matches + an insert clause, we have
  //    // N + 5 columns:
  //    // N target cols, TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL, ROW_DROPPED_COL, INCR_ROW_COUNT_COL,
  //    // CDC_TYPE_COLUMN_NAME
  //    // These ROW_ID_COL will always be dropped before the final write.
  //
  //    if (isDeleteWithDuplicateMatchesAndCdc) {
  //      targetOutputCols = targetOutputCols :+ UnresolvedAttribute(TARGET_ROW_ID_COL)
  //      outputRowSchema = outputRowSchema.add(TARGET_ROW_ID_COL, DataTypes.LongType)
  //      if (notMatchedClauses.nonEmpty) { // there is an insert clause, make SRC_ROW_ID_COL=null
  //        targetOutputCols = targetOutputCols :+ Alias(Literal(null), SOURCE_ROW_ID_COL)()
  //        outputRowSchema = outputRowSchema.add(SOURCE_ROW_ID_COL, DataTypes.LongType)
  //      }
  //    }
  //
  //    if (cdcEnabled) {
  //      outputRowSchema = outputRowSchema
  //        .add(ROW_DROPPED_COL, DataTypes.BooleanType)
  //        .add(INCR_ROW_COUNT_COL, DataTypes.BooleanType)
  //        .add(CDC_TYPE_COLUMN_NAME, DataTypes.StringType)
  //    }
  //
  //    def updateOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
  //    : Seq[Seq[Expression]] = {
  //      val updateExprs = {
  //        // Generate update expressions and set ROW_DELETED_COL = false and
  //        // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC
  //        val mainDataOutput = resolvedActions.map(_.expr) :+ FalseLiteral :+
  //          incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
  //        if (cdcEnabled) {
  //          // For update preimage, we have do a no-op copy with ROW_DELETED_COL = false and
  //          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_PREIMAGE and INCR_ROW_COUNT_COL as a no-op
  //          // (because the metric will be incremented in `mainDataOutput`)
  //          val preImageOutput = targetOutputCols :+ FalseLiteral :+
  //            Literal(CDC_TYPE_UPDATE_PREIMAGE)
  //          // For update postimage, we have the same expressions as for mainDataOutput but with
  //          // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
  //          // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_POSTIMAGE
  //          val postImageOutput = mainDataOutput.dropRight(2) :+
  //            Literal(CDC_TYPE_UPDATE_POSTIMAGE)
  //          Seq(mainDataOutput, preImageOutput, postImageOutput)
  //        } else {
  //          Seq(mainDataOutput)
  //        }
  //      }
  //      updateExprs.map(resolveOnJoinedPlan)
  //    }
  //
  //    def deleteOutput(incrMetricExpr: Expression): Seq[Seq[Expression]] = {
  //      val deleteExprs = {
  //        // Generate expressions to set the ROW_DELETED_COL = true and CDC_TYPE_COLUMN_NAME =
  //        // CDC_TYPE_NOT_CDC  and TARGET_MODIFIED_COL = true
  //        val mainDataOutput = targetOutputCols :+ TrueLiteral :+ incrMetricExpr :+
  //          CDC_TYPE_NOT_CDC_LITERAL
  //        if (cdcEnabled) {
  //          // For delete we do a no-op copy with ROW_DELETED_COL = false, INCR_ROW_COUNT_COL as a
  //          // no-op (because the metric will be incremented in `mainDataOutput`) and
  //          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_DELETE
  //          val deleteCdcOutput = targetOutputCols :+ FalseLiteral :+ CDC_TYPE_DELETE
  //          Seq(mainDataOutput, deleteCdcOutput)
  //        } else {
  //          Seq(mainDataOutput)
  //        }
  //      }
  //      deleteExprs.map(resolveOnJoinedPlan)
  //    }
  //
  //    def insertOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
  //    : Seq[Seq[Expression]] = {
  //      // Generate insert expressions and set ROW_DELETED_COL = false and
  //      // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC and TARGET_MODIFIED_COL = false
  //      val insertExprs = resolvedActions.map(_.expr)
  //      val mainDataOutput = resolveOnJoinedPlan(
  //        if (isDeleteWithDuplicateMatchesAndCdc) {
  //          // Must be delete-when-matched merge with duplicate matches + insert clause
  //          // Therefore we must keep the target row id and source row id. Since this is a not-matched
  //          // clause we know the target row-id will be null. See above at
  //          // isDeleteWithDuplicateMatchesAndCdc definition for more details.
  //          insertExprs :+
  //            Alias(Literal(null), TARGET_ROW_ID_COL)() :+ UnresolvedAttribute(SOURCE_ROW_ID_COL) :+
  //            FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
  //        } else {
  //          insertExprs :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
  //        }
  //      )
  //      if (cdcEnabled) {
  //        // For insert we have the same expressions as for mainDataOutput, but with
  //        // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
  //        // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_INSERT
  //        val insertCdcOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+ Literal(CDC_TYPE_INSERT)
  //        Seq(mainDataOutput, insertCdcOutput)
  //      } else {
  //        Seq(mainDataOutput)
  //      }
  //    }
  //
  //    def clauseOutput(clause: DeltaMergeIntoClause): Seq[Seq[Expression]] = clause match {
  //      case u: DeltaMergeIntoMatchedUpdateClause =>
  //        updateOutput(u.resolvedActions, And(incrUpdatedCountExpr, incrUpdatedMatchedCountExpr))
  //      case _: DeltaMergeIntoMatchedDeleteClause =>
  //        deleteOutput(And(incrDeletedCountExpr, incrDeletedMatchedCountExpr))
  //      case i: DeltaMergeIntoNotMatchedInsertClause =>
  //        insertOutput(i.resolvedActions, incrInsertedCountExpr)
  //      case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
  //        updateOutput(
  //          u.resolvedActions,
  //          And(incrUpdatedCountExpr, incrUpdatedNotMatchedBySourceCountExpr))
  //      case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
  //        deleteOutput(And(incrDeletedCountExpr, incrDeletedNotMatchedBySourceCountExpr))
  //    }
  //
  //    def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
  //      // if condition is None, then expression always evaluates to true
  //      val condExpr = clause.condition.getOrElse(TrueLiteral)
  //      resolveOnJoinedPlan(Seq(condExpr)).head
  //    }
  //
  //    val targetRowHasNoMatch = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head
  //    val sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr)).head
  //    val matchedConditions = matchedClauses.map(clauseCondition)
  //    val matchedOutputs = matchedClauses.map(clauseOutput)
  //    val notMatchedConditions = notMatchedClauses.map(clauseCondition)
  //    val notMatchedOutputs = notMatchedClauses.map(clauseOutput)
  //    val notMatchedBySourceConditions = notMatchedBySourceClauses.map(clauseCondition)
  //    val notMatchedBySourceOutputs = notMatchedBySourceClauses.map(clauseOutput)
  //    val noopCopyOutput =
  //      resolveOnJoinedPlan(targetOutputCols :+ FalseLiteral :+ incrNoopCountExpr :+
  //        CDC_TYPE_NOT_CDC_LITERAL)
  //    val deleteRowOutput =
  //      resolveOnJoinedPlan(targetOutputCols :+ TrueLiteral :+ TrueLiteral :+
  //        CDC_TYPE_NOT_CDC_LITERAL)
  //
  //    def addMergeJoinProcessor(
  //                               spark: SparkSession,
  //                               joinedPlan: LogicalPlan,
  //                               targetRowHasNoMatch: Expression,
  //                               sourceRowHasNoMatch: Expression,
  //                               matchedConditions: Seq[Expression],
  //                               matchedOutputs: Seq[Seq[Seq[Expression]]],
  //                               notMatchedConditions: Seq[Expression],
  //                               notMatchedOutputs: Seq[Seq[Seq[Expression]]],
  //                               notMatchedBySourceConditions: Seq[Expression],
  //                               notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
  //                               noopCopyOutput: Seq[Expression],
  //                               deleteRowOutput: Seq[Expression]): Dataset[Row] = {
  //
  //      def wrap(e: Expression): BaseExprMeta[Expression] = {
  //        GpuOverrides.wrapExpr(e, rapidsConf, None)
  //      }
  //
  //      val targetRowHasNoMatchMeta = wrap(targetRowHasNoMatch)
  //      val sourceRowHasNoMatchMeta = wrap(sourceRowHasNoMatch)
  //      val matchedConditionsMetas = matchedConditions.map(wrap)
  //      val matchedOutputsMetas = matchedOutputs.map(_.map(_.map(wrap)))
  //      val notMatchedConditionsMetas = notMatchedConditions.map(wrap)
  //      val notMatchedOutputsMetas = notMatchedOutputs.map(_.map(_.map(wrap)))
  //      val notMatchedBySourceConditionsMetas = notMatchedBySourceConditions.map(wrap)
  //      val notMatchedBySourceOutputsMetas = notMatchedBySourceOutputs.map(_.map(_.map(wrap)))
  //      val noopCopyOutputMetas = noopCopyOutput.map(wrap)
  //      val deleteRowOutputMetas = deleteRowOutput.map(wrap)
  //      val allMetas = Seq(targetRowHasNoMatchMeta, sourceRowHasNoMatchMeta) ++
  //        matchedConditionsMetas ++ matchedOutputsMetas.flatten.flatten ++
  //        notMatchedConditionsMetas ++ notMatchedOutputsMetas.flatten.flatten ++
  //        notMatchedBySourceConditionsMetas ++ notMatchedBySourceOutputsMetas.flatten.flatten ++
  //        noopCopyOutputMetas ++ deleteRowOutputMetas
  //      allMetas.foreach(_.tagForGpu())
  //      val canReplace = allMetas.forall(_.canExprTreeBeReplaced) && rapidsConf.isOperatorEnabled(
  //        "spark.rapids.sql.exec.RapidsProcessDeltaMergeJoinExec", false, false)
  //      if (rapidsConf.shouldExplainAll || (rapidsConf.shouldExplain && !canReplace)) {
  //        val exprExplains = allMetas.map(_.explain(rapidsConf.shouldExplainAll))
  //        val execWorkInfo = if (canReplace) {
  //          "will run on GPU"
  //        } else {
  //          "cannot run on GPU because not all merge processing expressions can be replaced"
  //        }
  //        logWarning(s"<RapidsProcessDeltaMergeJoinExec> $execWorkInfo:\n" +
  //          s"  ${exprExplains.mkString("  ")}")
  //      }
  //
  //      if (canReplace) {
  //        val processedJoinPlan = RapidsProcessDeltaMergeJoin(
  //          joinedPlan,
  //          outputRowSchema.toAttributes,
  //          targetRowHasNoMatch = targetRowHasNoMatch,
  //          sourceRowHasNoMatch = sourceRowHasNoMatch,
  //          matchedConditions = matchedConditions,
  //          matchedOutputs = matchedOutputs,
  //          notMatchedConditions = notMatchedConditions,
  //          notMatchedOutputs = notMatchedOutputs,
  //          notMatchedBySourceConditions = notMatchedBySourceConditions,
  //          notMatchedBySourceOutputs = notMatchedBySourceOutputs,
  //          noopCopyOutput = noopCopyOutput,
  //          deleteRowOutput = deleteRowOutput)
  //        Dataset.ofRows(spark, processedJoinPlan)
  //      } else {
  //
  //        val joinedRowEncoder = RowEncoder(joinedPlan.schema)
  //        val processedRowSchema = joinedPlan.schema
  //          .add(ROW_DROPPED_COL, DataTypes.BooleanType)
  //          .add(INCR_ROW_COUNT_COL, DataTypes.BooleanType)
  //          .add(CDC_TYPE_COLUMN_NAME, DataTypes.StringType)
  //        val processedRowEncoder = RowEncoder(processedRowSchema).resolveAndBind()
  //
  //        val processor = new JoinedRowProcessor(
  //          targetRowHasNoMatch = targetRowHasNoMatch,
  //          sourceRowHasNoMatch = sourceRowHasNoMatch,
  //          matchedConditions = matchedConditions,
  //          matchedOutputs = matchedOutputs,
  //          notMatchedConditions = notMatchedConditions,
  //          notMatchedOutputs = notMatchedOutputs,
  //          notMatchedBySourceConditions = notMatchedBySourceClauses.map(clauseCondition),
  //          notMatchedBySourceOutputs = notMatchedBySourceClauses.map(clauseOutput),
  //          joinedAttributes = joinedPlan.output,
  //          joinedRowEncoder = joinedRowEncoder,
  //          processedRowEncoder = processedRowEncoder)
  //
  //        Dataset.ofRows(spark, joinedPlan)
  //          .mapPartitions(processor.processPartition)(processedRowEncoder)
  //      }
  //    }
  //
  //    var mergedDF = addMergeJoinProcessor(spark, joinedPlan,
  //      targetRowHasNoMatch = targetRowHasNoMatch,
  //      sourceRowHasNoMatch = sourceRowHasNoMatch,
  //      matchedConditions = matchedConditions,
  //      matchedOutputs = matchedOutputs,
  //      notMatchedConditions = notMatchedConditions,
  //      notMatchedOutputs = notMatchedOutputs,
  //      notMatchedBySourceConditions = notMatchedBySourceConditions,
  //      notMatchedBySourceOutputs = notMatchedBySourceOutputs,
  //      noopCopyOutput = noopCopyOutput,
  //      deleteRowOutput = deleteRowOutput)
  //
  //    if (isDeleteWithDuplicateMatchesAndCdc) {
  //      // When we have a delete when matched clause with duplicate matches we have to remove
  //      // duplicate CDC rows. This scenario is further explained at
  //      // isDeleteWithDuplicateMatchesAndCdc definition.
  //
  //      // To remove duplicate CDC rows generated by the duplicate matches we dedupe by
  //      // TARGET_ROW_ID_COL since there should only be one CDC delete row per target row.
  //      // When there is an insert clause in addition to the delete clause we additionally dedupe by
  //      // SOURCE_ROW_ID_COL and CDC_TYPE_COLUMN_NAME to avoid dropping valid duplicate inserted rows
  //      // and their corresponding CDC rows.
  //      val columnsToDedupeBy = if (notMatchedClauses.nonEmpty) { // insert clause
  //        Seq(TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL, CDC_TYPE_COLUMN_NAME)
  //      } else {
  //        Seq(TARGET_ROW_ID_COL)
  //      }
  //      mergedDF = mergedDF
  //        .dropDuplicates(columnsToDedupeBy)
  //        .drop(ROW_DROPPED_COL, INCR_ROW_COUNT_COL, TARGET_ROW_ID_COL, SOURCE_ROW_ID_COL)
  //    } else {
  //      mergedDF = mergedDF
  //        .drop(ROW_DROPPED_COL, INCR_ROW_COUNT_COL, CDC_TYPE_COLUMN_NAME)
  //    }
  //
  //    logInfo("writeModifiedChanges: join output plan:\n" + mergedDF.queryExecution.analyzed)
  //
  //
  //    // Write to Delta
  //    val newFiles = {
  ////      val mergedPlan = mergedDF.queryExecution.analyzed
  ////      val mergedRowEncoder: ExpressionEncoder[Row] = RowEncoder(mergedPlan.schema)
  ////        .resolveAndBind()
  ////      val outputRowEncoder: ExpressionEncoder[Row] = RowEncoder(outputRowSchema)
  ////        .resolveAndBind()
  ////
  ////      val outputDF = mergedDF
  ////        .drop(TARGET_ROW_PRESENT_COL, TARGET_ROW_ID_COL, TARGET_FILENAME_COL)
  ////        .mapPartitions { rows =>
  ////          val fromRow = mergedRowEncoder.createSerializer()
  ////          val outputProj = UnsafeProjection.create(outputRowEncoder.schema, mergedPlan.schema)
  ////
  ////          val toRow = outputRowEncoder.createDeserializer()
  ////
  ////          rows.map(row => toRow(outputProj(fromRow(row))))
  ////        }(outputRowEncoder)
  ////
  ////      logInfo(s"Merged DF schema is: ${mergedDF.queryExecution.analyzed.schema}, " +
  ////        s"Output schema is: $outputRowSchema")
  ////      //      val outputDF = mergedDF.select(outputRowSchema.fields.map(f => col(f.name)): _*)
  //
  //      deltaTxn
  //        .writeFiles(repartitionIfNeeded(spark, mergedDF, deltaTxn.metadata.partitionColumns))
  //    }
  //
  //    // Update metrics
  //    val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
  //    metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
  //    metrics("numTargetChangeFilesAdded") += newFiles.count(_.isInstanceOf[AddCDCFile])
  //    metrics("numTargetChangeFileBytes") += newFiles.collect { case f: AddCDCFile => f.size }.sum
  //    metrics("numTargetBytesAdded") += addedBytes
  //    metrics("numTargetPartitionsAddedTo") += addedPartitions
  //    if (multipleMatchDeleteOnlyOvercount.isDefined) {
  //      // Compensate for counting duplicates during the query.
  //      val actualRowsDeleted =
  //        metrics("numTargetRowsDeleted").value - multipleMatchDeleteOnlyOvercount.get
  //      assert(actualRowsDeleted >= 0)
  //      metrics("numTargetRowsDeleted").set(actualRowsDeleted)
  //      val actualRowsMatchedDeleted =
  //        metrics("numTargetRowsMatchedDeleted").value - multipleMatchDeleteOnlyOvercount.get
  //      assert(actualRowsMatchedDeleted >= 0)
  //      metrics("numTargetRowsMatchedDeleted").set(actualRowsMatchedDeleted)
  //    }
  //
  //    newFiles
  //  }

  private def lowShuffleMerge(spark: SparkSession,
                              deltaTxn: OptimisticTransaction,
                              filesToRewrite: Seq[AddFile]): Seq[FileAction] = {
    val executor = new LowShuffleMergeExecutor(spark, deltaTxn, filesToRewrite, this)
    executor.execute()
  }

  /**
   * Build a new logical plan using the given `files` that has the same output columns (exprIds)
   * as the `target` logical plan, so that existing update/insert expressions can be applied
   * on this new plan.
   */
  private def buildTargetPlanWithFiles(
                                        spark: SparkSession,
                                        deltaTxn: OptimisticTransaction,
                                        files: Seq[AddFile]): DataFrame = {
    val targetOutputCols = getTargetOutputCols(deltaTxn)
    val targetOutputColsMap = {
      val colsMap: Map[String, NamedExpression] = targetOutputCols.view
        .map(col => col.name -> col).toMap
      if (conf.caseSensitiveAnalysis) {
        colsMap
      } else {
        CaseInsensitiveMap(colsMap)
      }
    }

    var plan = {
      // We have to do surgery to use the attributes from `targetOutputCols` to scan the table.
      // In cases of schema evolution, they may not be the same type as the original attributes.
      val original =
        deltaTxn.deltaLog.createDataFrame(deltaTxn.snapshot, files).queryExecution.analyzed
      val transformed = original.transform {
        case LogicalRelation(base, _, catalogTbl, isStreaming) =>
          LogicalRelation(
            base,
            // We can ignore the new columns which aren't yet AttributeReferences.
            targetOutputCols.collect { case a: AttributeReference => a },
            catalogTbl,
            isStreaming)
      }

      // In case of schema evolution & column mapping, we would also need to rebuild the file format
      // because under column mapping, the reference schema within DeltaParquetFileFormat
      // that is used to populate metadata needs to be updated
      if (deltaTxn.metadata.columnMappingMode != NoMapping) {
        val updatedFileFormat = deltaTxn.deltaLog.fileFormat(deltaTxn.protocol, deltaTxn.metadata)
        DeltaTableUtils.replaceFileFormat(transformed, updatedFileFormat)
      } else {
        transformed
      }
    }

    logDebug("Target plan with files:\n" + plan.treeString)
    plan = replaceFileFormat(plan)

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = targetOutputColsMap.get(newAttrib.name)
          .getOrElse {
            throw DeltaErrors.failedFindAttributeInOutputColumns(
              newAttrib.name, targetOutputCols.mkString(","))
          }.asInstanceOf[AttributeReference]

        if (existingTargetAttrib.exprId == newAttrib.exprId) {
          // It's not valid to alias an expression to its own exprId (this is considered a
          // non-unique exprId by the analyzer), so we just use the attribute directly.
          newAttrib
        } else {
          Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
        }
    }

    Dataset.ofRows(spark, Project(aliases, plan))
  }

  /** Expressions to increment SQL metrics */
  private def makeMetricUpdateUDF(name: String, deterministic: Boolean = false): Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    var u = DeltaUDF.boolean(new GpuDeltaMetricUpdateUDF(metric))
    if (!deterministic) {
      u = u.asNondeterministic()
    }
    u.apply().expr
  }

  private def getTargetOutputCols(txn: OptimisticTransaction): Seq[NamedExpression] = {
    txn.metadata.schema.map { col =>
      targetOutputAttributesMap
        .get(col.name)
        .map { a =>
          AttributeReference(col.name, col.dataType, col.nullable)(a.exprId)
        }
        .getOrElse(Alias(Literal(null), col.name)())
    }
  }


  /**
   * Execute the given `thunk` and return its result while recording the time taken to do it.
   *
   * @param sqlMetricName name of SQL metric to update with the time taken by the thunk
   * @param thunk         the code to execute
   */
  private def recordMergeOperation[A](sqlMetricName: String)(thunk: => A): A = {
    val startTimeNs = System.nanoTime()
    val r = thunk
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
    if (sqlMetricName != null && timeTakenMs > 0) {
      metrics(sqlMetricName) += timeTakenMs
    }
    r
  }
}

object GpuMergeIntoCommand extends Logging {
  /**
   * Spark UI will track all normal accumulators along with Spark tasks to show them on Web UI.
   * However, the accumulator used by `MergeIntoCommand` can store a very large value since it
   * tracks all files that need to be rewritten. We should ask Spark UI to not remember it,
   * otherwise, the UI data may consume lots of memory. Hence, we use the prefix `internal.metrics.`
   * to make this accumulator become an internal accumulator, so that it will not be tracked by
   * Spark UI.
   */
  val TOUCHED_FILES_ACCUM_NAME = "internal.metrics.MergeIntoDelta.touchedFiles"

  val ROW_ID_COL = "_row_id_"
  val TARGET_ROW_ID_COL = "_target_row_id_"
  val SOURCE_ROW_ID_COL = "_source_row_id_"
  val TARGET_FILENAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"
  val ROW_DROPPED_COL = GpuDeltaMergeConstants.ROW_DROPPED_COL
  val INCR_ROW_COUNT_COL = "_incr_row_count_"

  private val SOURCE_ROW_PRESENT_FIELD = StructField(SOURCE_ROW_PRESENT_COL,
    BooleanType, nullable = false)
  private val TARGET_ROW_PRESENT_FIELD = StructField(TARGET_ROW_PRESENT_COL,
    BooleanType, nullable = false)
  private val ROW_DROPPED_FIELD = StructField(ROW_DROPPED_COL, BooleanType, nullable = false)
  private val TARGET_ROW_ID_FIELD = StructField(TARGET_ROW_ID_COL, LongType, nullable = false)
  private val TARGET_FILENAME_FIELD = StructField(TARGET_FILENAME_COL, StringType, nullable = false)

  // Some Delta versions use Literal(null) which translates to a literal of NullType instead
  // of the Literal(null, StringType) which is needed, so using a fixed version here
  // rather than the version from Delta Lake.
  val CDC_TYPE_NOT_CDC_LITERAL = Literal(null, StringType)

  /**
   * @param targetRowHasNoMatch          whether a joined row is a target row with no match in the source
   *                                     table
   * @param sourceRowHasNoMatch          whether a joined row is a source row with no match in the target
   *                                     table
   * @param matchedConditions            condition for each match clause
   * @param matchedOutputs               corresponding output for each match clause. for each clause, we
   *                                     have 1-3 output rows, each of which is a sequence of expressions
   *                                     to apply to the joined row
   * @param notMatchedConditions         condition for each not-matched clause
   * @param notMatchedOutputs            corresponding output for each not-matched clause. for each clause,
   *                                     we have 1-2 output rows, each of which is a sequence of
   *                                     expressions to apply to the joined row
   * @param notMatchedBySourceConditions condition for each not-matched-by-source clause
   * @param notMatchedBySourceOutputs    corresponding output for each not-matched-by-source
   *                                     clause. for each clause, we have 1-3 output rows, each of
   *                                     which is a sequence of expressions to apply to the joined
   *                                     row
   * @param noopCopyOutput               no-op expression to copy a target row to the output
   * @param deleteRowOutput              expression to drop a row from the final output. this is used for
   *                                     source rows that don't match any not-matched clauses
   * @param joinedAttributes             schema of our outer-joined dataframe
   * @param joinedRowEncoder             joinedDF row encoder
   * @param processedRowEncoder          final output row encoder
   */
  class JoinedRowProcessor(
                            targetRowHasNoMatch: Expression,
                            sourceRowHasNoMatch: Expression,
                            matchedConditions: Seq[Expression],
                            matchedOutputs: Seq[Seq[Seq[Expression]]],
                            notMatchedConditions: Seq[Expression],
                            notMatchedOutputs: Seq[Seq[Seq[Expression]]],
                            notMatchedBySourceConditions: Seq[Expression],
                            notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]],
                            joinedAttributes: Seq[Attribute],
                            joinedRowEncoder: ExpressionEncoder[Row],
                            processedRowEncoder: ExpressionEncoder[Row]) extends Serializable {

    private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
      UnsafeProjection.create(exprs, joinedAttributes)
    }

    private def generatePredicate(expr: Expression): BasePredicate = {
      GeneratePredicate.generate(expr, joinedAttributes)
    }

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {

      val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
      val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
      val matchedPreds = matchedConditions.map(generatePredicate)
      val matchedProjs = matchedOutputs.map(_.map(generateProjection))
      val notMatchedPreds = notMatchedConditions.map(generatePredicate)
      val notMatchedProjs = notMatchedOutputs.map(_.map(generateProjection))
      val notMatchedBySourcePreds = notMatchedBySourceConditions.map(generatePredicate)
      val notMatchedBySourceProjs = notMatchedBySourceOutputs.map(_.map(generateProjection))
      //      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      // this is accessing ROW_DROPPED_COL. If ROW_DROPPED_COL is not in outputRowEncoder.schema
      // then CDC must be disabled and it's the column after our output cols
      def shouldDeleteRow(row: InternalRow): Boolean = {
        row.getBoolean(
          processedRowEncoder.schema.getFieldIndex(ROW_DROPPED_COL)
            .getOrElse(processedRowEncoder.schema.fields.size)
        )
      }

      def processRow(inputRow: InternalRow): Iterator[InternalRow] = {
        // Identify which set of clauses to execute: matched, not-matched or not-matched-by-source
        val (predicates, projections) = if (targetRowHasNoMatchPred.eval(inputRow)) {
          // Target row did not match any source row, so update the target row.
          (notMatchedBySourcePreds, notMatchedBySourceProjs)
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
          // Source row did not match with any target row, so insert the new source row
          (notMatchedPreds, notMatchedProjs)
        } else {
          // Source row matched with target row, so update the target row
          (matchedPreds, matchedProjs)
        }

        // find (predicate, projection) pair whose predicate satisfies inputRow
        val pair = (predicates zip projections).find {
          case (predicate, _) => predicate.eval(inputRow)
        }

        pair match {
          case Some((_, projections)) =>
            projections.map(_.apply(inputRow)).iterator
          case None => Iterator.empty
        }
      }

      val toRow = joinedRowEncoder.createSerializer()
      val fromRow = processedRowEncoder.createDeserializer()
      rowIterator
        .map(toRow)
        .flatMap(processRow)
        .filter(!shouldDeleteRow(_))
        .map(fromRow)
    }
  }

  /** Count the number of distinct partition values among the AddFiles in the given set. */
  def totalBytesAndDistinctPartitionValues(files: Seq[FileAction]): (Long, Int) = {
    val distinctValues = new mutable.HashSet[Map[String, String]]()
    var bytes = 0L
    val iter = files.collect { case a: AddFile => a }.iterator
    while (iter.hasNext) {
      val file = iter.next()
      distinctValues += file.partitionValues
      bytes += file.size
    }
    // If the only distinct value map is an empty map, then it must be an unpartitioned table.
    // Return 0 in that case.
    val numDistinctValues =
      if (distinctValues.size == 1 && distinctValues.head.isEmpty) 0 else distinctValues.size
    (bytes, numDistinctValues)
  }


  /**
   * Replace the file index in a logical plan and return the updated plan.
   * It's a common pattern that, in Delta commands, we use data skipping to determine a subset of
   * files that can be affected by the command, so we replace the whole-table file index in the
   * original logical plan with a new index of potentially affected files, while everything else in
   * the original plan, e.g., resolved references, remain unchanged.
   *
   * In addition we also request a metadata column and a row index column from the Scan to help
   * generate the Deletion Vectors.
   *
   * @param target    the logical plan in which we replace the file index
   * @param fileIndex the new file index
   */
  private def replaceFileFormat(target: LogicalPlan): LogicalPlan = {
    target.transformUp {
      case l@LogicalRelation(
      hfsr@HadoopFsRelation(_, _, _, _, format: DeltaParquetFileFormat, _), _, _, _) =>
        // Disable splitting and filter pushdown in order to generate the row-indexes
        val newFormat = format.copy(isSplittable = false, disablePushDowns = true)
        val newBaseRelation = hfsr.copy(
          fileFormat = newFormat)(hfsr.sparkSession)

        l.copy(relation = newBaseRelation)
    }
  }

  /**
   * Repartitions the output DataFrame by the partition columns if table is partitioned
   * and `merge.repartitionBeforeWrite.enabled` is set to true.
   */
  protected def repartitionIfNeeded(
                                     spark: SparkSession,
                                     df: DataFrame,
                                     partitionColumns: Seq[String]): DataFrame = {
    if (partitionColumns.nonEmpty && spark.conf.get(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE)) {
      df.repartition(partitionColumns.map(col): _*)
    } else {
      df
    }
  }

  class LowShuffleMergeExecutor(spark: SparkSession,
                                deltaTxn: OptimisticTransaction,
                                filesToRewrite: Seq[AddFile],
                                cmd: GpuMergeIntoCommand) extends Logging {

    // UDFs to update metrics
    // Make UDFs that appear in the custom join processor node deterministic, as they always
    // return true and update a metric. Catalyst precludes non-deterministic UDFs that are not
    // allowed outside a very specific set of Catalyst nodes (Project, Filter, Window, Aggregate).
    private val incrSourceRowCountExpr = cmd.makeMetricUpdateUDF("numSourceRowsInSecondScan",
      deterministic = true)
    private val incrUpdatedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsUpdated",
      deterministic = true)
    private val incrUpdatedMatchedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsMatchedUpdated",
      deterministic = true)
    private val incrUpdatedNotMatchedBySourceCountExpr =
      cmd.makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceUpdated", deterministic = true)
    private val incrInsertedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsInserted",
      deterministic = true)
    private val incrDeletedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsDeleted",
      deterministic = true)
    private val incrDeletedMatchedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsMatchedDeleted",
      deterministic = true)
    private val incrDeletedNotMatchedBySourceCountExpr =
      cmd.makeMetricUpdateUDF("numTargetRowsNotMatchedBySourceDeleted", deterministic = true)

    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the outer join, will allow us to identify whether the resultant joined row was a
    // matched inner result or an unmatched result with null on one side.
    // We add row IDs to the targetDF if we have a delete-when-matched clause with duplicate
    // matches and CDC is enabled, and additionally add row IDs to the source if we also have an
    // insert clause. See above at isDeleteWithDuplicateMatchesAndCdc definition for more details.
    private val sourceDF = cmd.getSourceDF()
      .withColumn(SOURCE_ROW_PRESENT_FIELD.name, new Column(incrSourceRowCountExpr))
    private val targetDF = {
      // Generate a new target dataframe that has same output attributes exprIds as the target plan.
      // This allows us to apply the existing resolved update/insert expressions.
      val baseTargetDF = cmd.buildTargetPlanWithFiles(spark, deltaTxn, filesToRewrite)

      baseTargetDF
        .withColumn(TARGET_ROW_PRESENT_FIELD.name, lit(true))
        .withColumn(TARGET_FILENAME_FIELD.name, input_file_name())
        .withColumn(TARGET_ROW_ID_FIELD.name, monotonically_increasing_id())
    }

    private val joinType = if (cmd.hasNoInserts &&
      spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)) {
      "rightOuter"
    } else {
      "fullOuter"
    }

    private val joinedDF = {
      sourceDF.join(targetDF, new Column(cmd.condition), joinType)
    }

    private val joinedPlan = joinedDF.queryExecution.analyzed

    private val outputRowSchema = deltaTxn.metadata.schema
    private val targetOutputCols = cmd.getTargetOutputCols(deltaTxn)

    def execute(): Seq[FileAction] = {
      logDebug(
        s"""WriteChanges using $joinType join:
           |  source.output: ${cmd.source.outputSet}
           |  target.output: ${cmd.target.outputSet}
           |  condition: ${cmd.condition}
           |  newTarget.output: ${joinedDF.queryExecution.logical.outputSet}
       """.stripMargin)

      val targetRowHasNoMatch = resolveOnJoinedPlan(Seq(
        col(SOURCE_ROW_PRESENT_FIELD.name).isNull.expr)).head
      val sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(
        col(TARGET_ROW_PRESENT_FIELD.name).isNull.expr))
        .head
      val matchedConditions = cmd.matchedClauses.map(clauseCondition)
      val matchedOutputs = cmd.matchedClauses.map(clauseOutput)
      val notMatchedConditions = cmd.notMatchedClauses.map(clauseCondition)
      val notMatchedOutputs = cmd.notMatchedClauses.map(clauseOutput)
      val notMatchedBySourceConditions = cmd.notMatchedBySourceClauses.map(clauseCondition)
      val notMatchedBySourceOutputs = cmd.notMatchedBySourceClauses.map(clauseOutput)

      val processedDF = addMergeJoinProcessor(spark, joinedPlan,
        targetRowHasNoMatch = targetRowHasNoMatch,
        sourceRowHasNoMatch = sourceRowHasNoMatch,
        matchedConditions = matchedConditions,
        matchedOutputs = matchedOutputs,
        notMatchedConditions = notMatchedConditions,
        notMatchedOutputs = notMatchedOutputs,
        notMatchedBySourceConditions = notMatchedBySourceConditions,
        notMatchedBySourceOutputs = notMatchedBySourceOutputs)


      // Write to Delta
      val newFiles = cmd.withStatusCode("DELTA", "Writing modified data") {
        writeModified(processedDF)
      } ++ cmd.withStatusCode("DELTA", "Writing unmodified data") {
        writeUnmodified(processedDF)
      }

      // Update metrics
      val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
      cmd.metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
      cmd.metrics("numTargetChangeFilesAdded") += newFiles.count(_.isInstanceOf[AddCDCFile])
      cmd.metrics("numTargetChangeFileBytes") += newFiles.collect {
          case f: AddCDCFile => f.size
        }
        .sum
      cmd.metrics("numTargetBytesAdded") += addedBytes
      cmd.metrics("numTargetPartitionsAddedTo") += addedPartitions
      //      if (multipleMatchDeleteOnlyOvercount.isDefined) {
      //        // Compensate for counting duplicates during the query.
      //        val actualRowsDeleted =
      //          metrics("numTargetRowsDeleted").value - multipleMatchDeleteOnlyOvercount.get
      //        assert(actualRowsDeleted >= 0)
      //        metrics("numTargetRowsDeleted").set(actualRowsDeleted)
      //        val actualRowsMatchedDeleted =
      //          metrics("numTargetRowsMatchedDeleted").value -
      //          multipleMatchDeleteOnlyOvercount.get
      //        assert(actualRowsMatchedDeleted >= 0)
      //        metrics("numTargetRowsMatchedDeleted").set(actualRowsMatchedDeleted)
      //      }

      newFiles
    }

    private def writeModified(processedDF: DataFrame): Seq[FileAction] = {
      val outputDF = processedDF
        .drop(ROW_DROPPED_FIELD.name, TARGET_ROW_ID_FIELD.name, TARGET_FILENAME_FIELD.name)
      logInfo("Calculating changes: join output plan:\n" + outputDF.queryExecution.analyzed)

      deltaTxn
        .writeFiles(repartitionIfNeeded(spark, outputDF, deltaTxn.metadata.partitionColumns))
    }

    private def writeUnmodified(processedDF: DataFrame): Seq[FileAction] = {
      // TODO: We need to filter out the rows that were modified in the processedDF
      val modifiedTargetRows = processedDF
        .select(TARGET_ROW_ID_FIELD.name, TARGET_FILENAME_FIELD.name)

      val baseTargetDF = cmd.buildTargetPlanWithFiles(spark, deltaTxn, filesToRewrite)
        .withColumn(TARGET_ROW_ID_FIELD.name, monotonically_increasing_id())
        .withColumn(TARGET_FILENAME_FIELD.name, input_file_name())

      val joinCondition = baseTargetDF(TARGET_FILENAME_FIELD.name) ===
        modifiedTargetRows(TARGET_FILENAME_FIELD.name) &&
        baseTargetDF(TARGET_ROW_ID_FIELD.name) ===
          modifiedTargetRows(TARGET_ROW_ID_FIELD.name)

      val unModifiedTargetRows = baseTargetDF.join(modifiedTargetRows,
          joinCondition, "anti")
        .drop(TARGET_ROW_ID_FIELD.name, TARGET_FILENAME_FIELD.name)

      deltaTxn.writeFiles(unModifiedTargetRows)
    }

    private def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      cmd.tryResolveReferencesForExpressions(spark, exprs, joinedPlan)
    }

    private def updateOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
    : Seq[Seq[Expression]] = {
      //      val updateExprs = {
      //        // Generate update expressions and set ROW_DELETED_COL = false and
      //        // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC
      //        val mainDataOutput = resolvedActions.map(_.expr) :+ FalseLiteral :+
      //          incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
      //        if (cdcEnabled) {
      //          // For update preimage, we have do a no-op copy with ROW_DELETED_COL = false and
      //          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_PREIMAGE and INCR_ROW_COUNT_COL as a no-op
      //          // (because the metric will be incremented in `mainDataOutput`)
      //          val preImageOutput = targetOutputCols :+ FalseLiteral :+
      //            Literal(CDC_TYPE_UPDATE_PREIMAGE)
      //          // For update postimage, we have the same expressions as for mainDataOutput but with
      //          // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
      //          // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_UPDATE_POSTIMAGE
      //          val postImageOutput = mainDataOutput.dropRight(2) :+
      //            Literal(CDC_TYPE_UPDATE_POSTIMAGE)
      //          Seq(mainDataOutput, preImageOutput, postImageOutput)
      //        } else {
      //          Seq(mainDataOutput)
      //        }
      //      }
      val updateExprs = {
        val mainDataOutput = resolvedActions.map(_.expr) :+
          FalseLiteral :+
          UnresolvedAttribute(TARGET_ROW_ID_COL) :+
          UnresolvedAttribute(TARGET_FILENAME_COL) :+
          incrMetricExpr
        Seq(mainDataOutput)
      }
      updateExprs.map(resolveOnJoinedPlan)
    }

    private def deleteOutput(incrMetricExpr: Expression): Seq[Seq[Expression]] = {
      //      val deleteExprs = {
      //        // Generate expressions to set the ROW_DELETED_COL = true and CDC_TYPE_COLUMN_NAME =
      //        // CDC_TYPE_NOT_CDC  and TARGET_MODIFIED_COL = true
      //        val mainDataOutput = targetOutputCols :+ TrueLiteral :+ incrMetricExpr :+
      //          CDC_TYPE_NOT_CDC_LITERAL
      //        if (cdcEnabled) {
      //          // For delete we do a no-op copy with ROW_DELETED_COL = false, INCR_ROW_COUNT_COL as a
      //          // no-op (because the metric will be incremented in `mainDataOutput`) and
      //          // CDC_TYPE_COLUMN_NAME = CDC_TYPE_DELETE
      //          val deleteCdcOutput = targetOutputCols :+ FalseLiteral :+ CDC_TYPE_DELETE
      //          Seq(mainDataOutput, deleteCdcOutput)
      //        } else {
      //          Seq(mainDataOutput)
      //        }
      //      }

      val deleteExprs = {
        val mainDataOutput = targetOutputCols :+
          TrueLiteral :+
          UnresolvedAttribute(TARGET_ROW_ID_COL) :+
          UnresolvedAttribute(TARGET_FILENAME_COL) :+
          incrMetricExpr

        Seq(mainDataOutput)
      }
      deleteExprs.map(resolveOnJoinedPlan)
    }

    private def insertOutput(resolvedActions: Seq[DeltaMergeAction], incrMetricExpr: Expression)
    : Seq[Seq[Expression]] = {
      //      // Generate insert expressions and set ROW_DELETED_COL = false and
      //      // CDC_TYPE_COLUMN_NAME = CDC_TYPE_NOT_CDC and TARGET_MODIFIED_COL = false
      //      val insertExprs = resolvedActions.map(_.expr)
      //      val mainDataOutput = resolveOnJoinedPlan(
      //        //        if (isDeleteWithDuplicateMatchesAndCdc) {
      //        // Must be delete-when-matched merge with duplicate matches + insert clause
      //        // Therefore we must keep the target row id and source row id.
      //        // Since this is a not-matched
      //        // clause we know the target row-id will be null. See above at
      //        // isDeleteWithDuplicateMatchesAndCdc definition for more details.
      //        //          insertExprs :+
      //        //            Alias(Literal(null), TARGET_ROW_ID_COL)()
      //        //            :+ UnresolvedAttribute(SOURCE_ROW_ID_COL)
      //        //            :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
      //        //        } else {
      //        //          insertExprs :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
      //        //        }
      //        insertExprs :+ FalseLiteral :+ incrMetricExpr :+ CDC_TYPE_NOT_CDC_LITERAL
      //      )
      //      if (cdcEnabled) {
      //        // For insert we have the same expressions as for mainDataOutput, but with
      //        // INCR_ROW_COUNT_COL as a no-op (because the metric will be incremented in
      //        // `mainDataOutput`), and CDC_TYPE_COLUMN_NAME = CDC_TYPE_INSERT
      //        val insertCdcOutput = mainDataOutput.dropRight(2) :+ TrueLiteral :+ Literal(CDC_TYPE_INSERT)
      //        Seq(mainDataOutput, insertCdcOutput)
      //      } else {
      //        Seq(mainDataOutput)
      //      }
      val insertExprs = {
        val mainDataOutput = resolvedActions.map(_.expr) :+
          FalseLiteral :+
          UnresolvedAttribute(TARGET_ROW_ID_COL) :+
          UnresolvedAttribute(TARGET_FILENAME_COL) :+
          incrMetricExpr
        Seq(mainDataOutput)
      }
      insertExprs.map(resolveOnJoinedPlan)
    }

    private def clauseOutput(clause: DeltaMergeIntoClause): Seq[Seq[Expression]] = clause match {
      case u: DeltaMergeIntoMatchedUpdateClause =>
        updateOutput(u.resolvedActions, And(incrUpdatedCountExpr, incrUpdatedMatchedCountExpr))
      case _: DeltaMergeIntoMatchedDeleteClause =>
        deleteOutput(And(incrDeletedCountExpr, incrDeletedMatchedCountExpr))
      case i: DeltaMergeIntoNotMatchedInsertClause =>
        insertOutput(i.resolvedActions, incrInsertedCountExpr)
      case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
        updateOutput(
          u.resolvedActions,
          And(incrUpdatedCountExpr, incrUpdatedNotMatchedBySourceCountExpr))
      case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
        deleteOutput(And(incrDeletedCountExpr, incrDeletedNotMatchedBySourceCountExpr))
    }

    private def clauseCondition(clause: DeltaMergeIntoClause): Expression = {
      // if condition is None, then expression always evaluates to true
      val condExpr = clause.condition.getOrElse(TrueLiteral)
      resolveOnJoinedPlan(Seq(condExpr)).head
    }

    private def wrap(e: Expression): BaseExprMeta[Expression] = {
      GpuOverrides.wrapExpr(e, cmd.rapidsConf, None)
    }

    private def addMergeJoinProcessor(
                                       spark: SparkSession,
                                       joinedPlan: LogicalPlan,
                                       targetRowHasNoMatch: Expression,
                                       sourceRowHasNoMatch: Expression,
                                       matchedConditions: Seq[Expression],
                                       matchedOutputs: Seq[Seq[Seq[Expression]]],
                                       notMatchedConditions: Seq[Expression],
                                       notMatchedOutputs: Seq[Seq[Seq[Expression]]],
                                       notMatchedBySourceConditions: Seq[Expression],
                                       notMatchedBySourceOutputs: Seq[Seq[Seq[Expression]]])
    : Dataset[Row] = {

      val targetRowHasNoMatchMeta = wrap(targetRowHasNoMatch)
      val sourceRowHasNoMatchMeta = wrap(sourceRowHasNoMatch)
      val matchedConditionsMetas = matchedConditions.map(wrap)
      val matchedOutputsMetas = matchedOutputs.map(_.map(_.map(wrap)))
      val notMatchedConditionsMetas = notMatchedConditions.map(wrap)
      val notMatchedOutputsMetas = notMatchedOutputs.map(_.map(_.map(wrap)))
      val notMatchedBySourceConditionsMetas = notMatchedBySourceConditions.map(wrap)
      val notMatchedBySourceOutputsMetas = notMatchedBySourceOutputs.map(_.map(_.map(wrap)))
      val allMetas = Seq(targetRowHasNoMatchMeta, sourceRowHasNoMatchMeta) ++
        matchedConditionsMetas ++ matchedOutputsMetas.flatten.flatten ++
        notMatchedConditionsMetas ++ notMatchedOutputsMetas.flatten.flatten ++
        notMatchedBySourceConditionsMetas ++ notMatchedBySourceOutputsMetas.flatten.flatten

      allMetas.foreach(_.tagForGpu())
      val canReplace = allMetas.forall(_.canExprTreeBeReplaced) && cmd.rapidsConf.isOperatorEnabled(
        "spark.rapids.sql.exec.RapidsProcessDeltaMergeJoinExec", false, false)
      if (cmd.rapidsConf.shouldExplainAll || (cmd.rapidsConf.shouldExplain && !canReplace)) {
        val exprExplains = allMetas.map(_.explain(cmd.rapidsConf.shouldExplainAll))
        val execWorkInfo = if (canReplace) {
          "will run on GPU"
        } else {
          "cannot run on GPU because not all merge processing expressions can be replaced"
        }
        logWarning(s"<RapidsProcessDeltaMergeJoinExec> $execWorkInfo:\n" +
          s"  ${exprExplains.mkString("  ")}")
      }

      val joinedRowEncoder = RowEncoder(joinedPlan.schema)
      val processedRowSchema = outputRowSchema
        .add(ROW_DROPPED_FIELD)
        .add(TARGET_ROW_ID_FIELD)
        .add(TARGET_FILENAME_FIELD)

      val processedRowEncoder = RowEncoder(processedRowSchema).resolveAndBind()

      val processor = new JoinedRowProcessor(
        targetRowHasNoMatch = targetRowHasNoMatch,
        sourceRowHasNoMatch = sourceRowHasNoMatch,
        matchedConditions = matchedConditions,
        matchedOutputs = matchedOutputs,
        notMatchedConditions = notMatchedConditions,
        notMatchedOutputs = notMatchedOutputs,
        notMatchedBySourceConditions = cmd.notMatchedBySourceClauses.map(clauseCondition),
        notMatchedBySourceOutputs = cmd.notMatchedBySourceClauses.map(clauseOutput),
        joinedAttributes = joinedPlan.output,
        joinedRowEncoder = joinedRowEncoder,
        processedRowEncoder = processedRowEncoder)

      Dataset.ofRows(spark, joinedPlan)
        .mapPartitions(processor.processPartition)(processedRowEncoder)
    }
  }
}
