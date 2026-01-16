/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "341db"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import ai.rapids.cudf.GroupByOptions
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuBindReferences, GpuBoundReference, GpuColumnVector, GpuExec, GpuExpression, GpuMetric, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.window.{GpuDenseRank, GpuRank, GpuRowNumber}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, DenseRank, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, Rank, RowNumber, SortOrder, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.window.{Final, Partial, WindowExec, WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait RankFunctionType
case object RankFunction extends RankFunctionType
case object DenseRankFunction extends RankFunctionType
case object RowNumberFunction extends RankFunctionType

class GpuWindowGroupLimitExecMeta(limitExec: WindowGroupLimitExec,
                                  conf: RapidsConf,
                                  parent: Option[RapidsMeta[_, _, _]],
                                  rule: DataFromReplacementRule)
    extends SparkPlanMeta[WindowGroupLimitExec](limitExec, conf, parent, rule) {

  private val partitionSpec: Seq[BaseExprMeta[Expression]] =
    limitExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    limitExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val rankFunction = GpuOverrides.wrapExpr(limitExec.rankLikeFunction, conf, Some(this))

  override def tagPlanForGpu(): Unit = {
    wrapped.rankLikeFunction match {
      case DenseRank(_) =>
      case Rank(_) =>
      case RowNumber() =>
      case _ => willNotWorkOnGpu("Only rank, dense_rank and row_number are " +
                                 "currently supported for window group limits")
    }

    wrapped.mode match {
      case Partial =>
      case Final =>
        // Check if this Final limit is redundant because it will be followed by
        // a WindowExec + FilterExec that does the same work
        if (conf.isWindowGroupLimitOptEnabled && isFinalLimitRedundant) {
          shouldBeRemoved("redundant Final WindowGroupLimit: subsequent WindowExec and " +
            "FilterExec will perform the same rank computation and filtering")
        }
      case _ => willNotWorkOnGpu("Unsupported WindowGroupLimitMode: " +
                                 s"${wrapped.mode.getClass.getName}")
    }
  }

  /**
   * Checks if this Final WindowGroupLimitExec is redundant.
   *
   * The Final limit is redundant when:
   * 1. The parent is a WindowExec that computes the same rank function with same specs
   * 2. The grandparent is a FilterExec that filters on the rank result
   *
   * When both conditions are met, the WindowExec + FilterExec will do the same
   * rank computation and filtering that this Final limit would do, making this
   * operator a no-op that we can skip.
   */
  private def isFinalLimitRedundant: Boolean = {
    // Get the parent's wrapped plan (should be WindowExec)
    val parentWindowExecOpt = parent.flatMap { p =>
      p.wrapped match {
        case w: WindowExec => Some(w)
        case _ => None
      }
    }

    parentWindowExecOpt.exists { windowExec =>
      // Check if WindowExec has matching partition and order specs
      val specsMatch = 
        windowExec.partitionSpec.map(_.semanticHash()) == 
          limitExec.partitionSpec.map(_.semanticHash()) &&
        windowExec.orderSpec.map(_.semanticHash()) == 
          limitExec.orderSpec.map(_.semanticHash())

      if (!specsMatch) {
        return false
      }

      // Check if WindowExec contains a matching rank function
      val hasMatchingRankFunction = findMatchingRankExpression(windowExec).isDefined

      if (!hasMatchingRankFunction) {
        return false
      }

      // Check if grandparent is a FilterExec
      val grandparentFilterOpt = parent.flatMap(_.parent).flatMap { gp =>
        gp.wrapped match {
          case f: FilterExec => Some(f)
          case _ => None
        }
      }

      // For the optimization to be safe, we need a FilterExec as grandparent
      // The filter should reference the rank output and enforce the limit
      grandparentFilterOpt.exists { filterExec =>
        findMatchingRankExpression(windowExec).exists { case (rankExprId, _) =>
          filterReferencesRankWithLimit(filterExec, rankExprId, limitExec.limit)
        }
      }
    }
  }

  /**
   * Finds a window expression in the WindowExec that matches the rank function
   * in this WindowGroupLimitExec.
   *
   * @return Option of (expression id, rank function type) if found
   */
  private def findMatchingRankExpression(windowExec: WindowExec): 
      Option[(Long, Expression)] = {
    // Get window expressions from WindowExec
    val windowExprs = try {
      val method = windowExec.getClass.getMethod("windowExpression")
      method.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
    } catch {
      case _: NoSuchMethodException =>
        try {
          val method = windowExec.getClass.getMethod("projectList")
          method.invoke(windowExec).asInstanceOf[Seq[NamedExpression]]
        } catch {
          case _: NoSuchMethodException => return None
        }
    }

    // Find window expression with matching rank function
    windowExprs.collectFirst {
      case alias @ Alias(WindowExpression(rankFunc, spec: WindowSpecDefinition), _)
          if matchesRankFunction(rankFunc) && matchesWindowSpec(spec) =>
        (alias.exprId.id, rankFunc)
    }
  }

  /**
   * Checks if the given expression matches the rank function type in this limit.
   */
  private def matchesRankFunction(expr: Expression): Boolean = {
    (expr, limitExec.rankLikeFunction) match {
      case (_: Rank, _: Rank) => true
      case (_: DenseRank, _: DenseRank) => true
      case (_: RowNumber, _: RowNumber) => true
      case _ => false
    }
  }

  /**
   * Checks if the window spec matches the partition and order specs of this limit.
   */
  private def matchesWindowSpec(spec: WindowSpecDefinition): Boolean = {
    spec.partitionSpec.map(_.semanticHash()) == 
      limitExec.partitionSpec.map(_.semanticHash()) &&
    spec.orderSpec.map(_.semanticHash()) == 
      limitExec.orderSpec.map(_.semanticHash())
  }

  /**
   * Checks if the filter condition references the rank column and enforces
   * a limit that is compatible with (same or more restrictive than) this limit.
   *
   * We look for patterns like:
   *   - rank <= limit
   *   - rank < limit + 1
   *   - limit >= rank
   *   - limit + 1 > rank
   */
  private def filterReferencesRankWithLimit(
      filterExec: FilterExec,
      rankExprId: Long,
      limit: Int): Boolean = {
    
    def referencesRankColumn(expr: Expression): Boolean = {
      expr.references.exists(_.exprId.id == rankExprId)
    }

    def extractLiteralInt(expr: Expression): Option[Int] = expr match {
      case Literal(value: Int, _) => Some(value)
      case Literal(value: Long, _) if value.isValidInt => Some(value.toInt)
      case _ => None
    }

    // Check if the condition is a comparison that references the rank column
    // and has a limit value that is >= our limit (so it's at least as restrictive)
    filterExec.condition match {
      // rank <= n  where n >= limit
      case LessThanOrEqual(left, right) if referencesRankColumn(left) =>
        extractLiteralInt(right).exists(_ >= limit)
      
      // rank < n  where n > limit (equivalent to rank <= n-1 where n-1 >= limit)
      case LessThan(left, right) if referencesRankColumn(left) =>
        extractLiteralInt(right).exists(_ > limit)
      
      // n >= rank  where n >= limit
      case GreaterThanOrEqual(left, right) if referencesRankColumn(right) =>
        extractLiteralInt(left).exists(_ >= limit)
      
      // n > rank  where n > limit
      case GreaterThan(left, right) if referencesRankColumn(right) =>
        extractLiteralInt(left).exists(_ > limit)

      case _ => false
    }
  }

  override def convertToGpu(): GpuExec = {
    GpuWindowGroupLimitExec(partitionSpec.map(_.convertToGpu()),
                            orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
                            rankFunction.convertToGpu(),
                            limitExec.limit,
                            limitExec.mode,
                            childPlans.head.convertIfNeeded())
  }
}

class GpuWindowGroupLimitingIterator(input: Iterator[ColumnarBatch],
                                     boundPartitionSpec: Seq[GpuExpression],
                                     boundOrderSpec: Seq[SortOrder],
                                     rankFunction: RankFunctionType,
                                     limit: Int,
                                     numOutputBatches: GpuMetric,
                                     numOutputRows: GpuMetric)
  extends Iterator[ColumnarBatch]
  with Logging {

  override def hasNext: Boolean = input.hasNext

  // Caches input column schema on first read.
  private var inputTypes: Option[Array[DataType]] = None

  private val partByPositions: Array[Int] =
    boundPartitionSpec.map {
      _.asInstanceOf[GpuBoundReference].ordinal
    }.toArray
  private val sortColumns: Seq[Expression] = boundOrderSpec.map {
    _.child
  }

  private def readInputBatch: SpillableColumnarBatch = {
    def optionallySetInputTypes(inputCB: ColumnarBatch): Unit = {
      if (inputTypes.isEmpty) {
        inputTypes = Some(GpuColumnVector.extractTypes(inputCB))
      }
    }

    val inputCB = input.next()
    optionallySetInputTypes(inputCB)
    SpillableColumnarBatch(inputCB, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  private var subIterator: Option[Iterator[ColumnarBatch]] = None

  override def next(): ColumnarBatch = {

    if (!hasNext) {
      throw new NoSuchElementException()
    }

    if (subIterator.exists(_.hasNext)) {
      subIterator.map(_.next).get
    }
    else {
      val iter = withRetry(readInputBatch, splitSpillableInHalfByRows) { spillableInputCB =>
        withResource(spillableInputCB.getColumnarBatch()) { inputCB =>
          val filterMap = withResource(calculateRank(rankFunction, inputCB)) { ranks =>
            withResource(cudf.Scalar.fromInt(limit)) { limitScalar =>
              // For a query with `WHERE rank < n`, the limit passed to the exec
              // is `n - 1`.  The comparison should be `LESS_EQUAL`.
              ranks.binaryOp(cudf.BinaryOp.LESS_EQUAL, limitScalar, cudf.DType.BOOL8)
            }
          }
          withResource(filterMap) { _ =>
            withResource(GpuColumnVector.from(inputCB)) { inputTable =>
              withResource(inputTable.filter(filterMap)) {
                GpuColumnVector.from(_, inputTypes.get)
              }
            }
          }
        }
      }
      val result = iter.next()
      subIterator = Some(iter)
      numOutputBatches += 1
      numOutputRows += result.numRows()
      result
    }
  }

  private def hasGroupingColumns: Boolean = partByPositions.nonEmpty

  private def getRankFunction(rankFunctionType: RankFunctionType) = {
    rankFunctionType match {
      case RankFunction => GpuRank(sortColumns)
      case DenseRankFunction => GpuDenseRank(sortColumns)
      case RowNumberFunction => GpuRowNumber
      case _ => throw new IllegalArgumentException("Unexpected ranking function")
    }
  }

  private def calculateRank(rankFunctionType: RankFunctionType,
                            inputCB: ColumnarBatch): cudf.ColumnVector = {
    if (hasGroupingColumns) {
      calculateRankWithGroupByScan(rankFunctionType, inputCB)
    }
    else {
      calculateRankWithScan(rankFunctionType, inputCB)
    }
  }

  private def extractSortColumn(inputCB: ColumnarBatch, sortColumnProjection: Seq[Expression]) =
    withResource(GpuProjectExec.project(inputCB, sortColumnProjection)) {
      sortColumnCB =>
        withResource(GpuColumnVector.from(sortColumnCB)) { sortColumnTable =>
          require(sortColumnTable.getNumberOfColumns == 1,
            "Expected single (consolidated) sort-by column.")
          sortColumnTable.getColumn(0).incRefCount()
        }
    }

  private def calculateRankWithGroupByScan(rankFunctionType: RankFunctionType,
                                           inputCB: ColumnarBatch): cudf.ColumnVector = {
    // For multiple order-by columns order-by columns, the group-scan's input projection
    // is a single STRUCT column (containing the order-by columns as members).
    val rankFunction = getRankFunction(rankFunctionType)
    val sortColumnProjection = rankFunction.groupByScanInputProjection(isRunningBatched = false)

    // Append the projected sort-column to the end of the input table.
    val gbyScanInputTable = withResource(GpuColumnVector.from(inputCB)) { inputTable =>
      withResource(extractSortColumn(inputCB, sortColumnProjection)) { sortColumn =>
        val columnsWithSortByAdded = Range(0, inputTable.getNumberOfColumns).map {
          inputTable.getColumn
        }.toArray :+ sortColumn
        new cudf.Table(columnsWithSortByAdded: _*)
      }
    }

    // Now, perform groupBy-scan:
    val sortColumnIndex = gbyScanInputTable.getNumberOfColumns - 1 // Last column.
    val gbyScanAggregation = rankFunction.groupByScanAggregation(isRunningBatched = false).head.agg
    val sortedGroupByOptions = GroupByOptions.builder().withKeysSorted(true).build
    withResource(gbyScanInputTable) { _ =>
      withResource(gbyScanInputTable.groupBy(sortedGroupByOptions, partByPositions: _*)
        .scan(gbyScanAggregation.onColumn(sortColumnIndex))) { gbyScanOutput =>
        // The last column should be the grouped ranks.
        gbyScanOutput.getColumn(gbyScanOutput.getNumberOfColumns - 1).incRefCount()
      }
    }
  }

  private def calculateRankWithScan(rankFunctionType: RankFunctionType,
                                    inputCB: ColumnarBatch): cudf.ColumnVector = {

    // For multiple order-by columns order-by columns, the group-scan's input projection
    // is a single STRUCT column (containing the order-by columns as members).
    val rankFunction = getRankFunction(rankFunctionType)
    val sortColumnProjection = rankFunction.scanInputProjection(isRunningBatched = false)
    val scanAggregation = rankFunction.scanAggregation(isRunningBatched = false).head.agg
    withResource(extractSortColumn(inputCB, sortColumnProjection)) { sortColumn =>
      sortColumn.scan(scanAggregation, cudf.ScanType.INCLUSIVE,
                      cudf.NullPolicy.INCLUDE)
    }
  }
}

/**
 * Analog of o.a.s.s.e.window.WindowGroupLimitExec.  Responsible for top-k limit push-down,
 * for queries that contain boolean filter predicates on the result of ranking window functions
 * such as `RANK()`, `DENSE_RANK()`, and `ROW_NUMBER()`.
 *
 * @see <a href="https://issues.apache.org/jira/browse/SPARK-37099">SPARK-37099</a>
 *
 * Consider this query:
 * <pre>{@code
 * SELECT * FROM (
 *   SELECT *, RANK() OVER (PARTITION BY a ORDER BY b) rnk
 *   FROM input_table
 * ) WHERE rnk < 5;
 * }</pre>
 *
 * This translates to the following plan:
 * <pre>
 * == Physical Plan ==
 * *(3) Filter (rnk#22 < 5)
 *      +- Window [rank(b#1) windowspecdefinition(a#0L, b#1 ASC NULLS FIRST,
 *                                                specifiedwindowframe(RowFrame,
                                                    unboundedpreceding$(),
 *                                                  currentrow$())) AS rnk#22],
 *                [a#0L],
 *                [b#1 ASC NULLS FIRST]
 *         +- WindowGroupLimit [a#0L], [b#1 ASC NULLS FIRST], rank(b#1), 4, Final
 *            +- *(2) Sort [a#0L ASC NULLS FIRST, b#1 ASC NULLS FIRST], false, 0
 *               +- Exchange hashpartitioning(a#0L, 1), ENSURE_REQUIREMENTS, [plan_id=113]
 *                  +- WindowGroupLimit [a#0L], [b#1 ASC NULLS FIRST], rank(b#1), 4, Partial
 *                     +- *(1) Sort [a#0L ASC NULLS FIRST, b#1 ASC NULLS FIRST], false, 0
 *                        +- *(1) ColumnarToRow
 *                           +- FileScan parquet [a#0L,b#1,c#2L] ...
 * </pre>
 *
 * WindowGroupLimitExec works by using an appropriate row-wise iterator to go row-by-row, keeping
 * track of the current rank, and skipping over the rows that do not satisfy the rank predicate
 * (i.e. "< 5").
 *
 * GpuWindowGroupLimitExec differs slightly from Apache Spark in that it cannot go row-by-row.
 * Instead, it calculates the ranks for all the rows in the column, together, and then filters out
 * any that do not satisfy the predicate.
 *
 * The performance speedup comes from two sources:
 * 1. Preventing `WindowGroupLimit` from falling off the GPU, thereby eliminating *two*
 *    row-column-row transpose operations.
 * 2. Basic rank computations via cudf.ColumnVector scan operations, which tend to be fast.
 *
 * Note: No "running window" optimizations are required.  Each column batch is filtered
 * independently. If the rank-limit is `5`, only 5 ranks per group are permitted per column batch.
 * The final window aggregation and the subsequent filter will produce the right result downstream,
 * but without the costly shuffles.
 */
case class GpuWindowGroupLimitExec(
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def getRankFunctionType(expr: Expression): RankFunctionType = expr match {
    case GpuRank(_) => RankFunction
    case GpuDenseRank(_) => DenseRankFunction
    case GpuRowNumber => RowNumberFunction
    case _ =>
      throw new UnsupportedOperationException("Only rank, dense_rank and row_number are " +
                                              "currently supported for group limits")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output,
      allMetrics)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output, allMetrics)

    child.executeColumnar().mapPartitions { input =>
      new GpuWindowGroupLimitingIterator(input,
                                         boundPartitionSpec,
                                         boundOrderSpec,
                                         getRankFunctionType(rankLikeFunction),
                                         limit,
                                         numOutputBatches,
                                         numOutputRows)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Row-wise execution unsupported!")
}
