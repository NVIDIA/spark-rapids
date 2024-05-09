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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.{BaseExprMeta, GpuOverrides, RapidsConf}
import com.nvidia.spark.rapids.delta._
import com.nvidia.spark.rapids.delta.RapidsRepartitionByFilePath.{FILE_PATH_FIELD, ROW_ID_FIELD}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BasePredicate, Expression, Literal, NamedExpression, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeAction, DeltaMergeIntoClause, DeltaMergeIntoMatchedClause, DeltaMergeIntoMatchedDeleteClause, DeltaMergeIntoMatchedUpdateClause, DeltaMergeIntoNotMatchedBySourceClause, DeltaMergeIntoNotMatchedBySourceDeleteClause, DeltaMergeIntoNotMatchedBySourceUpdateClause, DeltaMergeIntoNotMatchedClause, DeltaMergeIntoNotMatchedInsertClause, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, DeltaTableUtils, NoMapping, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaOperations.MergePredicate
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.rapids.GpuDeltaLog
import org.apache.spark.sql.delta.rapids.delta24x.GpuLowShuffleMergeCommand.{InsertOnlyMergeExecutor, LowShuffleMergeExecutor}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{AnalysisHelper, SetAccumulator}
import org.apache.spark.sql.execution.{ExtendedMode, SQLExecution}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}

/**
 * GPU version of Delta Lake's low shuffle merge implementation.
 *
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 * Different from the original implementation, it optimized writing unmodified target rows.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 * the condition and verify that no two source rows match with the same target row.
 * This is implemented as an inner-join using the given condition. See [[findTouchedFiles]]
 * for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows
 * without copying unmodified rows.
 *
 * Phase 3: Read the touched files again and write new files with unmodified rows in target table,
 * trying to keep its original order and avoid shuffle as much as possible.
 *
 * Phase 4: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source            Source data to merge from
 * @param target            Target table to merge into
 * @param gpuDeltaLog       Delta log to use
 * @param condition         Condition for a source row to match with a target row
 * @param matchedClauses    All info related to matched clauses.
 * @param notMatchedClauses All info related to not matched clause.
 * @param migratedSchema    The final schema of the target - may be changed by schema evolution.
 */
case class GpuLowShuffleMergeCommand(
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
    with DeltaCommand with PredicateHelper with AnalysisHelper with ImplicitMetadataOperation {

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
  private def isSingleInsertOnly: Boolean = matchedClauses.isEmpty && notMatchedClauses.length == 1

  /** Whether this merge statement has only MATCHED clauses. */
  //  private def isMatchedOnly: Boolean = notMatchedClauses.isEmpty && matchedClauses.nonEmpty

  /** Whether this merge statement has no insert (NOT MATCHED) clause. */
  private def hasNoInserts: Boolean = notMatchedClauses.isEmpty


  override lazy val metrics = Map[String, SQLMetric](
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numSourceRowsInSecondScan" ->
      createMetric(sc, "number of source rows (during repeated scan)"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsMatchedUpdated" -> createMetric(sc, "number of target rows updated when matched"),
    "numTargetRowsMatchedDeleted" -> createMetric(sc, "number of target rows deleted when matched"),
    "numTargetRowsNotMatchedBySourceUpdated" -> createMetric(sc,
      "number of target rows updated when not matched by source"),
    "numTargetRowsNotMatchedBySourceDeleted" -> createMetric(sc,
      "number of target rows deleted when not matched by source"),
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
      createMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createMetric(sc, "time taken to rewrite the matched files"))

  override def run(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.lowshufflemerge") {
      val startTime = System.nanoTime()
      gpuDeltaLog.withNewTransaction { deltaTxn =>
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

        val deltaActions = {
          if (isSingleInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            new InsertOnlyMergeExecutor(spark, this, deltaTxn).execute()
          } else {
            new LowShuffleMergeExecutor(spark, deltaTxn, this).execute()
          }
        }

        // Metrics should be recorded before commit (where they are written to delta logs).
        metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
        deltaTxn.registerSQLMetrics(spark, metrics)

        // This is a best-effort sanity check.
        if (metrics("numSourceRowsInSecondScan").value >= 0 &&
          metrics("numSourceRows").value != metrics("numSourceRowsInSecondScan").value) {
          log.warn(s"Merge source has ${metrics("numSourceRows").value} rows in initial scan but " +
            s"${metrics("numSourceRowsInSecondScan").value} rows in second scan")
          if (conf.getConf(DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED)) {
            throw DeltaErrors.sourceNotDeterministicInMergeException(spark)
          }
        }

        deltaTxn.commit(
          deltaActions,
          DeltaOperations.Merge(
            Option(condition),
            matchedClauses.map(DeltaOperations.MergePredicate(_)),
            notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
            // We do not support notMatchedBySourcePredicates yet and fall back to CPU
            // See https://github.com/NVIDIA/spark-rapids/issues/8415
            notMatchedBySourcePredicates = Seq.empty[MergePredicate]
          ))

        // Record metrics
        val stats = GpuMergeStats.fromMergeSQLMetrics(
          metrics,
          condition,
          matchedClauses,
          notMatchedClauses,
          notMatchedBySourceClauses,
          deltaTxn.metadata.partitionColumns.nonEmpty)
        recordDeltaEvent(targetDeltaLog, "delta.dml.merge.stats", data = stats)

      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
    }
    // This is needed to make the SQL metrics visible in the Spark UI. Also this needs
    // to be outside the recordMergeOperation because this method will update some metric.
    val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(spark.sparkContext, executionId, metrics.values.toSeq)
    Seq(Row(metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value +
      metrics("numTargetRowsInserted").value, metrics("numTargetRowsUpdated").value,
      metrics("numTargetRowsDeleted").value, metrics("numTargetRowsInserted").value))
  }

  private def getSourceDF(spark: SparkSession): DataFrame = {
    // UDF to increment metrics
    val incrSourceRowCountExpr = makeMetricUpdateUDF("numSourceRows")
    Dataset.ofRows(spark, source)
      .filter(new Column(incrSourceRowCountExpr))
  }


  /** Expressions to increment SQL metrics */
  private def makeMetricUpdateUDF(name: String, deterministic: Boolean = false): Expression = {
    // only capture the needed metric in a local variable
    val metric = metrics(name)
    var u = udf(new GpuDeltaMetricUpdateUDF(metric))
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
        .getOrElse(Alias(Literal(null), col.name)()
        )
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

object GpuLowShuffleMergeCommand {
  /**
   * Spark UI will track all normal accumulators along with Spark tasks to show them on Web UI.
   * However, the accumulator used by `MergeIntoCommand` can store a very large value since it
   * tracks all files that need to be rewritten. We should ask Spark UI to not remember it,
   * otherwise, the UI data may consume lots of memory. Hence, we use the prefix `internal.metrics.`
   * to make this accumulator become an internal accumulator, so that it will not be tracked by
   * Spark UI.
   */
  val TOUCHED_FILES_ACCUM_NAME = "internal.metrics.MergeIntoDelta.touchedFiles"

  val ROW_ID_COL = RapidsRepartitionByFilePath.ROW_ID_COL
  val FILE_PATH_COL = RapidsRepartitionByFilePath.FILE_PATH_COL
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"
  val ROW_DROPPED_COL = GpuDeltaMergeConstants.ROW_DROPPED_COL
  val ROW_DROPPED_FIELD = StructField(ROW_DROPPED_COL, BooleanType, nullable = false)
  // This column is used to track whether a row is matched or not in merged output
  val ROW_MATCHED_COL = "_row_matched_"
  val ROW_MATCHED_FIELD = StructField(ROW_MATCHED_COL, BooleanType, nullable = false)
  val INCR_ROW_COUNT_COL = "_incr_row_count_"

  // Some Delta versions use Literal(null) which translates to a literal of NullType instead
  // of the Literal(null, StringType) which is needed, so using a fixed version here
  // rather than the version from Delta Lake.
  val CDC_TYPE_NOT_CDC_LITERAL = Literal(null, StringType)

  /**
   * @param targetRowHasNoMatch  whether a joined row is a target row with no match in the source
   *                             table
   * @param sourceRowHasNoMatch  whether a joined row is a source row with no match in the target
   *                             table
   * @param matchedConditions    condition for each match clause
   * @param matchedOutputs       corresponding output for each match clause. for each clause, we
   *                             have 1-3 output rows, each of which is a sequence of expressions
   *                             to apply to the joined row
   * @param notMatchedConditions condition for each not-matched clause
   * @param notMatchedOutputs    corresponding output for each not-matched clause. for each clause,
   *                             we have 1-2 output rows, each of which is a sequence of
   *                             expressions to apply to the joined row
   * @param noopCopyOutput       no-op expression to copy a target row to the output
   * @param deleteRowOutput      expression to drop a row from the final output. this is used for
   *                             source rows that don't match any not-matched clauses
   * @param joinedAttributes     schema of our outer-joined dataframe
   * @param joinedRowEncoder     joinedDF row encoder
   * @param outputRowEncoder     final output row encoder
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

  trait MergeExecutor extends AnalysisHelper with Logging {
    def spark: SparkSession

    def cmd: GpuLowShuffleMergeCommand

    def deltaTxn: OptimisticTransaction

    def execute(): Seq[FileAction]


    /**
     * Build a DataFrame using the given `files` that has the same output columns (exprIds)
     * as the `target` logical plan, so that existing update/insert expressions can be applied
     * on this new plan.
     */
    protected def buildTargetDFWithFiles(files: Seq[AddFile]): DataFrame = {
      val targetOutputCols = cmd.getTargetOutputCols(deltaTxn)
      val targetOutputColsMap = {
        val colsMap: Map[String, NamedExpression] = targetOutputCols.view
          .map(col => col.name -> col).toMap
        if (cmd.conf.caseSensitiveAnalysis) {
          colsMap
        } else {
          CaseInsensitiveMap(colsMap)
        }
      }

      val plan = {
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

        // In case of schema evolution & column mapping, we would also need to rebuild the file
        // format because under column mapping, the reference schema within DeltaParquetFileFormat
        // that is used to populate metadata needs to be updated
        if (deltaTxn.metadata.columnMappingMode != NoMapping) {
          val updatedFileFormat = deltaTxn.deltaLog.fileFormat(
            deltaTxn.deltaLog.unsafeVolatileSnapshot.protocol, deltaTxn.metadata)
          DeltaTableUtils.replaceFileFormat(transformed, updatedFileFormat)
        } else {
          transformed
        }
      }

      // For each plan output column, find the corresponding target output column (by name) and
      // create an alias
      val aliases = plan.output.map {
        case newAttrib: AttributeReference =>
          val existingTargetAttrib = targetOutputColsMap.get(newAttrib.name)
            .getOrElse {
              throw new AnalysisException(
                s"Could not find ${newAttrib.name} among the existing target output " +
                  targetOutputCols.mkString(","))
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
  }

  /**
   * This is an optimization of the case when there is no update clause for the merge.
   * We perform an left anti join on the source data to find the rows to be inserted.
   *
   * This will currently only optimize for the case when there is a _single_ notMatchedClause.
   */
  class InsertOnlyMergeExecutor(override val spark: SparkSession,
      override val cmd: GpuLowShuffleMergeCommand,
      override val deltaTxn: OptimisticTransaction
  ) extends MergeExecutor {
    override def execute(): Seq[FileAction] = {
      cmd.recordMergeOperation(sqlMetricName = "rewriteTimeMs") {

        // UDFs to update metrics
        val incrSourceRowCountExpr = cmd.makeMetricUpdateUDF("numSourceRows")
        val incrInsertedCountExpr = cmd.makeMetricUpdateUDF("numTargetRowsInserted")

        val outputColNames = cmd.getTargetOutputCols(deltaTxn).map(_.name)
        // we use head here since we know there is only a single notMatchedClause
        val outputExprs = cmd.notMatchedClauses.head.resolvedActions.map(_.expr)
        val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
          new Column(Alias(expr, name)())
        }

        // source DataFrame
        val sourceDF = Dataset.ofRows(spark, cmd.source)
          .filter(new Column(incrSourceRowCountExpr))
          .filter(new Column(cmd.notMatchedClauses.head.condition.getOrElse(Literal.TrueLiteral)))

        // Skip data based on the merge condition
        val conjunctivePredicates = cmd.splitConjunctivePredicates(cmd.condition)
        val targetOnlyPredicates =
          conjunctivePredicates.filter(_.references.subsetOf(cmd.target.outputSet))
        val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

        // target DataFrame
        val targetDF = buildTargetDFWithFiles(dataSkippedFiles)

        val insertDf = sourceDF.join(targetDF, new Column(cmd.condition), "leftanti")
          .select(outputCols: _*)
          .filter(new Column(incrInsertedCountExpr))

        val newFiles = deltaTxn
          .writeFiles(cmd.repartitionIfNeeded(spark, insertDf, deltaTxn.metadata.partitionColumns))

        // Update metrics
        cmd.metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
        cmd.metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
        val (afterSkippingBytes, afterSkippingPartitions) =
          totalBytesAndDistinctPartitionValues(dataSkippedFiles)
        cmd.metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
        cmd.metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
        cmd.metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
        cmd.metrics("numTargetFilesRemoved") += 0
        cmd.metrics("numTargetBytesRemoved") += 0
        cmd.metrics("numTargetPartitionsRemovedFrom") += 0
        val (addedBytes, addedPartitions) = totalBytesAndDistinctPartitionValues(newFiles)
        cmd.metrics("numTargetFilesAdded") += newFiles.count(_.isInstanceOf[AddFile])
        cmd.metrics("numTargetBytesAdded") += addedBytes
        cmd.metrics("numTargetPartitionsAddedTo") += addedPartitions
        newFiles
      }
    }
  }


  /**
   * This is an optimized algorithm for merge statement, where we avoid shuffling the unmodified
   * target data.
   *
   * The algorithm is as follows:
   * 1. Find touched target files in the target table by joining the source and target data.
   * 2. Read the touched files again and write new files with updated and/or inserted rows
   * without coping unmodified data from target table.
   * 3. Calculating unmodified data by reading the touched files again and do anti join against
   * output of [[JoinedRowProcessor]] using `__metadata_file_path` and `row_id`. Here we avoid
   * shuffle of target files with too methods:
   *  a. Pushing down a flag to [[org.apache.spark.sql.execution.FileSourceScanExec]] to enforce
   *  one file per partition.
   *  b. Adding [[RapidsRepartitionByFilePath]] to repartition the data by `__metadata_file_path`
   *  without shuffle.
   */
  class LowShuffleMergeExecutor(
      override val spark: SparkSession,
      override val deltaTxn: OptimisticTransaction,
      override val cmd: GpuLowShuffleMergeCommand) extends MergeExecutor {

    // We over-count numTargetRowsDeleted when there are multiple matches;
    // this is the amount of the overcount, so we can subtract it to get a correct final metric.
    private var multipleMatchDeleteOnlyOvercount: Option[Long] = None
    private val filesToRewrite = this.findTouchedFiles()


    // UDFs to update metrics
    // Make UDFs that appear in the custom join processor node deterministic, as they always
    // return true and update a metric. Catalyst precludes non-deterministic UDFs that are not
    // allowed outside a very specific set of Catalyst nodes (Project, Filter, Window, Aggregate).
    private val incrSourceRowCountExpr = cmd.makeMetricUpdateUDF("numSourceRowsInSecondScan",
      deterministic = true)

    // Apply an outer join to find both, matches and non-matches. We are adding two boolean fields
    // with value `true`, one to each side of the join. Whether this field is null or not after
    // the outer join, will allow us to identify whether the resultant joined row was a
    // matched inner result or an unmatched result with null on one side.
    // We add row IDs to the targetDF if we have a delete-when-matched clause with duplicate
    // matches and CDC is enabled, and additionally add row IDs to the source if we also have an
    // insert clause. See above at isDeleteWithDuplicateMatchesAndCdc definition for more details.
    private val sourceDF = cmd.getSourceDF(spark)
      .withColumn(SOURCE_ROW_PRESENT_COL, new Column(incrSourceRowCountExpr))

    private val targetDF = {
      // Generate a new target dataframe that has same output attributes exprIds as the target plan.
      // This allows us to apply the existing resolved update/insert expressions.
      val baseTargetDF = buildTargetDFWithFiles(filesToRewrite)

      // Here we enforce that the targetDF to read each file per partition. This is to ensure
      // that the `_metadata_row_id` column is correctly populated for each row in the targetDF.
      val newPlan = baseTargetDF.queryExecution.analyzed.transform {
        case r@LogicalRelation(fs: HadoopFsRelation, _, _, _) =>
          val newFs = fs.copy(options = fs.options +
            (RapidsConf.FORCE_ONE_FILE_PER_PARITION.key -> "true"))(spark)
          r.copy(relation = newFs)
      }

      Dataset.ofRows(spark, newPlan)
        .withColumns(Map(
          TARGET_ROW_PRESENT_COL -> lit(true),
          ROW_ID_COL -> monotonically_increasing_id(),
          FILE_PATH_COL -> input_file_name(),
        ))
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

    override def execute(): Seq[FileAction] = {
      logDebug(
        s"""WriteChanges using $joinType join:
           |  source.output: ${cmd.source.outputSet}
           |  target.output: ${cmd.target.outputSet}
           |  condition: ${cmd.condition}
           |  newTarget.output: ${joinedDF.queryExecution.logical.outputSet}
       """.stripMargin)

      val targetRowHasNoMatch = resolveOnJoinedPlan(Seq(
        col(SOURCE_ROW_PRESENT_COL).isNull.expr)).head
      val sourceRowHasNoMatch = resolveOnJoinedPlan(Seq(
        col(TARGET_ROW_PRESENT_COL).isNull.expr))
        .head
      val matchedConditions = cmd.matchedClauses.map(clauseCondition)
      val matchedOutputs = cmd.matchedClauses.map(clauseOutput)
      val notMatchedConditions = cmd.notMatchedClauses.map(clauseCondition)
      val notMatchedOutputs = cmd.notMatchedClauses.map(clauseOutput)
      val notMatchedBySourceConditions = cmd.notMatchedBySourceClauses.map(clauseCondition)
      val notMatchedBySourceOutputs = cmd.notMatchedBySourceClauses.map(clauseOutput)

      // Schema of processedDF:
      // targetSchema + ROW_DROPPED_COL + ROW_MATCHED_COL + ROW_ID_COL + FILE_PATH_COL
      // It consists of several parts:
      // 1. Unmatched source rows which are inserted
      // 2. Unmatched source rows which are deleted
      // 3. Target rows which are updated
      // 4. Target rows which are deleted
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

      // Update output metrics
      cmd.withStatusCode("DELTA", "Updating metrics") {
        updateOutputMetrics(processedDF)
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

      if (multipleMatchDeleteOnlyOvercount.isDefined) {
        // Compensate for counting duplicates during the query.
        val actualRowsDeleted =
          cmd.metrics("numTargetRowsDeleted").value - multipleMatchDeleteOnlyOvercount.get
        assert(actualRowsDeleted >= 0)
        cmd.metrics("numTargetRowsDeleted").set(actualRowsDeleted)
      }

      filesToRewrite.map(_.remove) ++ newFiles
    }

    // Now the `JoinedRowProcessor` is calculated more than once, so we can't update metrics as
    // original approach.
    private def updateOutputMetrics(processedDF: DataFrame): Unit = {
      val result = processedDF.selectExpr(
        s"count_if($ROW_ID_COL IS NULL AND !$ROW_DROPPED_COL) as insertedCount",
        s"""count_if($ROW_DROPPED_COL AND $ROW_MATCHED_COL AND $ROW_ID_COL IS NOT NULL)
           |as deletedMatchedCount""".stripMargin,
        s"""count_if($ROW_DROPPED_COL AND !$ROW_MATCHED_COL AND $ROW_ID_COL IS NOT NULL)
           |as deletedNotMatchedBySourceCount""".stripMargin,
        s"""count_if(!$ROW_DROPPED_COL AND $ROW_MATCHED_COL AND $ROW_ID_COL IS NOT NULL)
           |as updatedMatchedCount""".stripMargin,
        s"""count_if(!$ROW_DROPPED_COL AND !$ROW_MATCHED_COL AND $ROW_ID_COL IS NOT NULL)
           |as updatedNotMatchedBySourceCount""".stripMargin,
      ).head()

      val insertedCount = result.getLong(0)
      val deletedMatchedCount = result.getLong(1)
      val deletedNotMatchedBySourceCount = result.getLong(2)
      val updatedMatchedCount = result.getLong(3)
      val updatedNotMatchedBySourceCount = result.getLong(4)

      cmd.metrics("numTargetRowsInserted").set(insertedCount)
      cmd.metrics("numTargetRowsMatchedDeleted").set(deletedMatchedCount)
      cmd.metrics("numTargetRowsNotMatchedBySourceDeleted").set(deletedNotMatchedBySourceCount)
      cmd.metrics("numTargetRowsDeleted").set(deletedMatchedCount + deletedNotMatchedBySourceCount)
      cmd.metrics("numTargetRowsMatchedUpdated").set(updatedMatchedCount)
      cmd.metrics("numTargetRowsNotMatchedBySourceUpdated").set(updatedNotMatchedBySourceCount)
      cmd.metrics("numTargetRowsUpdated").set(updatedMatchedCount + updatedNotMatchedBySourceCount)
    }

    /**
     * Find the target table files that contain the rows that satisfy the merge condition. This is
     * implemented as an inner-join between the source query/table and the target table using
     * the merge condition.
     */
    private def findTouchedFiles(): Seq[AddFile] =
      cmd.recordMergeOperation(sqlMetricName = "scanTimeMs") {

        // Accumulator to collect all the distinct touched files
        val touchedFilesAccum = new SetAccumulator[String]()
        spark.sparkContext.register(touchedFilesAccum, TOUCHED_FILES_ACCUM_NAME)

        // UDFs to records touched files names and add them to the accumulator
        val recordTouchedFileName = udf(new GpuDeltaRecordTouchedFileNameUDF(touchedFilesAccum))
          .asNondeterministic()

        // Skip data based on the merge condition
        val targetOnlyPredicates =
          cmd.splitConjunctivePredicates(cmd.condition)
            .filter(_.references.subsetOf(cmd.target.outputSet))
        val dataSkippedFiles = deltaTxn.filterFiles(targetOnlyPredicates)

        val sourceDF = cmd.getSourceDF(spark)

        // Apply inner join to between source and target using the merge condition to find matches
        // In addition, we attach two columns
        // - a monotonically increasing row id for target rows to later identify whether the same
        //     target row is modified by multiple user or not
        // - the target file name the row is from to later identify the files touched by matched
        //      rows
        val targetDF = buildTargetDFWithFiles(dataSkippedFiles)
          .withColumn(ROW_ID_COL, monotonically_increasing_id())
          .withColumn(FILE_PATH_COL, input_file_name())
        val joinToFindTouchedFiles = sourceDF.join(targetDF, new Column(cmd.condition), "inner")

        // Process the matches from the inner join to record touched files and find multiple matches
        val collectTouchedFiles = joinToFindTouchedFiles
          .select(col(ROW_ID_COL), recordTouchedFileName(col(FILE_PATH_COL)).as("one"))

        // Calculate frequency of matches per source row
        val matchedRowCounts = collectTouchedFiles.groupBy(ROW_ID_COL).agg(sum("one").as("count"))

        // Get multiple matches and simultaneously collect (using touchedFilesAccum) the file names
        // multipleMatchCount = # of target rows with
        // more than 1 matching source row (duplicate match)
        // multipleMatchSum = total # of duplicate matched rows
        import spark.implicits._
        val (multipleMatchCount, multipleMatchSum) = matchedRowCounts
          .filter("count > 1")
          .select(coalesce(count("*"), lit(0)), coalesce(sum("count"), lit(0)))
          .as[(Long, Long)]
          .collect()
          .head

        val hasMultipleMatches = multipleMatchCount > 0

        // Throw error if multiple matches are ambiguous or cannot be computed correctly.
        val canBeComputedUnambiguously = {
          // Multiple matches are not ambiguous when there is only one unconditional delete as
          // all the matched row pairs in the 2nd join in `writeAllChanges` will get deleted.
          val isUnconditionalDelete = cmd.matchedClauses.headOption match {
            case Some(DeltaMergeIntoMatchedDeleteClause(None)) => true
            case _ => false
          }
          cmd.matchedClauses.size == 1 && isUnconditionalDelete
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
          multipleMatchDeleteOnlyOvercount = Some(duplicateCount)
        }

        // Get the AddFiles using the touched file names.
        val touchedFileNames = touchedFilesAccum.value.iterator().asScala.toSeq
        logTrace(s"findTouchedFiles: matched files:\n\t${touchedFileNames.mkString("\n\t")}")

        val nameToAddFileMap = cmd.generateCandidateFileMap(
          cmd.targetDeltaLog.dataPath,
          dataSkippedFiles)
        val touchedAddFiles = touchedFileNames.map(f =>
          cmd.getTouchedFile(cmd.targetDeltaLog.dataPath, f, nameToAddFileMap))

        // When the target table is empty, and the optimizer optimized away the join entirely
        // numSourceRows will be incorrectly 0.
        // We need to scan the source table once to get the correct
        // metric here.
        if (cmd.metrics("numSourceRows").value == 0 &&
          (dataSkippedFiles.isEmpty || targetDF.take(1).isEmpty)) {
          val numSourceRows = sourceDF.count()
          cmd.metrics("numSourceRows").set(numSourceRows)
        }

        // Update metrics
        cmd.metrics("numTargetFilesBeforeSkipping") += deltaTxn.snapshot.numOfFiles
        cmd.metrics("numTargetBytesBeforeSkipping") += deltaTxn.snapshot.sizeInBytes
        val (afterSkippingBytes, afterSkippingPartitions) =
          totalBytesAndDistinctPartitionValues(dataSkippedFiles)
        cmd.metrics("numTargetFilesAfterSkipping") += dataSkippedFiles.size
        cmd.metrics("numTargetBytesAfterSkipping") += afterSkippingBytes
        cmd.metrics("numTargetPartitionsAfterSkipping") += afterSkippingPartitions
        val (removedBytes, removedPartitions) =
          totalBytesAndDistinctPartitionValues(touchedAddFiles)
        cmd.metrics("numTargetFilesRemoved") += touchedAddFiles.size
        cmd.metrics("numTargetBytesRemoved") += removedBytes
        cmd.metrics("numTargetPartitionsRemovedFrom") += removedPartitions
        touchedAddFiles
      }

    private def writeModified(processedDF: DataFrame): Seq[FileAction] = {
      val outputDF = processedDF
        .filter(!col(ROW_DROPPED_COL)) // Filter out dropped rows
        .drop(ROW_DROPPED_COL, ROW_MATCHED_COL, ROW_ID_COL, FILE_PATH_COL)

      deltaTxn
        .writeFiles(cmd.repartitionIfNeeded(spark, outputDF, deltaTxn.metadata.partitionColumns))
    }

    private def writeUnmodified(processedDF: DataFrame): Seq[FileAction] = {

      // We filter out the modified target rows ids and file paths
      val modifiedTargetRows = processedDF
        .filter(col(ROW_ID_COL).isNotNull) // Filter out unmatched source rows
        .select(
          (col(ROW_ID_COL) * -1).as(ROW_ID_COL),
          col(FILE_PATH_COL))

      var baseTargetDF = targetDF.drop(TARGET_ROW_PRESENT_COL)

      baseTargetDF = repartitionByFilePath(baseTargetDF)

      // We make the join condition as following:
      // left._metadata_file_path = right._metadata_file_path &&
      //  (left._metadata_row_id + right._metadata_row_id) = 0
      // to ensure that the generated physical plan only requires repartition
      // by `_metadata_file_path`, so that we can avoid shuffle of target table.
      val joinCondition = baseTargetDF(FILE_PATH_COL) === modifiedTargetRows(FILE_PATH_COL) &&
        ((baseTargetDF(ROW_ID_COL) + modifiedTargetRows(ROW_ID_COL)) === 0)


      val unModifiedTargetRows = baseTargetDF.join(modifiedTargetRows,
          joinCondition, "anti")
        .drop(ROW_DROPPED_COL, ROW_MATCHED_COL, ROW_ID_COL, FILE_PATH_COL)

      logDebug("Unmodified target rows:\n" +
        unModifiedTargetRows.queryExecution.explainString(ExtendedMode))

      // Note we don't need to repartition here since they are unmodified.
      deltaTxn.writeFiles(unModifiedTargetRows)
    }

    private def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      resolveReferencesForExpressions(spark, exprs, joinedPlan)
    }

    private def updateOutput(resolvedActions: Seq[DeltaMergeAction], matched: Literal)
    : Seq[Seq[Expression]] = {
      val updateExprs = {
        val mainDataOutput = resolvedActions.map(_.expr) :+
          Literal.FalseLiteral :+
          matched :+
          UnresolvedAttribute(ROW_ID_COL) :+
          UnresolvedAttribute(FILE_PATH_COL)

        Seq(mainDataOutput)
      }
      updateExprs.map(resolveOnJoinedPlan)
    }

    private def deleteOutput(matched: Literal): Seq[Seq[Expression]] = {
      val deleteExprs = {
        val mainDataOutput = targetOutputCols :+
          TrueLiteral :+
          matched :+
          UnresolvedAttribute(ROW_ID_COL) :+
          UnresolvedAttribute(FILE_PATH_COL)

        Seq(mainDataOutput)
      }
      deleteExprs.map(resolveOnJoinedPlan)
    }

    private def insertOutput(resolvedActions: Seq[DeltaMergeAction])
    : Seq[Seq[Expression]] = {
      val insertExprs = {
        val mainDataOutput = resolvedActions.map(_.expr) :+
          Literal.FalseLiteral :+
          Literal.FalseLiteral :+
          UnresolvedAttribute(ROW_ID_COL) :+
          UnresolvedAttribute(FILE_PATH_COL)

        Seq(mainDataOutput)
      }
      insertExprs.map(resolveOnJoinedPlan)
    }

    private def clauseOutput(clause: DeltaMergeIntoClause): Seq[Seq[Expression]] = clause match {
      case u: DeltaMergeIntoMatchedUpdateClause =>
        updateOutput(u.resolvedActions, Literal.TrueLiteral)
      case _: DeltaMergeIntoMatchedDeleteClause =>
        deleteOutput(Literal.TrueLiteral)
      case i: DeltaMergeIntoNotMatchedInsertClause =>
        insertOutput(i.resolvedActions)
      case u: DeltaMergeIntoNotMatchedBySourceUpdateClause =>
        updateOutput(
          u.resolvedActions,
          Literal.FalseLiteral)
      case _: DeltaMergeIntoNotMatchedBySourceDeleteClause =>
        deleteOutput(Literal.FalseLiteral)
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

      logDebug(
        s"""Joined plan: \n ${joinedDF.explain(true)}
           |Joined plan schema: ${joinedPlan.schema}
           |""".stripMargin)
      val joinedRowEncoder = RowEncoder(joinedPlan.schema)
      val processedRowSchema = outputRowSchema
        .add(ROW_DROPPED_FIELD)
        .add(ROW_MATCHED_FIELD)
        .add(ROW_ID_FIELD.copy(nullable = true))
        .add(FILE_PATH_FIELD.copy(nullable = true))

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

    private def repartitionByFilePath(input: DataFrame): DataFrame = {
      logInfo(s"Adding RapidsRepartitionByFilePath plan node")
      val newPlan = input.queryExecution.analyzed.transformUp({
        case p@Project(projectList, _: LogicalRelation) =>
          if (projectList.exists(_.name == FILE_PATH_COL)) {
            RapidsRepartitionByFilePath(None, p)
          } else {
            p
          }
      })
      Dataset.ofRows(spark, newPlan)
    }
  }
}
