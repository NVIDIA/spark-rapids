/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf
import ai.rapids.cudf.GroupByOptions
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuBindReferences, GpuBoundReference, GpuColumnVector, GpuExec, GpuExpression, GpuMetric, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitSpillableInHalfByRows, withRetry}
import com.nvidia.spark.rapids.window.{GpuDenseRank, GpuRank}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DenseRank, Expression, Rank, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.{Final, Partial, WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait RankFunctionType
case object RankFunction extends RankFunctionType
case object DenseRankFunction extends RankFunctionType

class GpuWindowGroupLimitExecMeta(limitExec: WindowGroupLimitExec,
                                  conf: RapidsConf,
                                  parent:Option[RapidsMeta[_, _, _]],
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
      // case RowNumber() => // TODO: Future.
      case _ => willNotWorkOnGpu("Only Rank() and DenseRank() are " +
                                 "currently supported for window group limits")
    }

    wrapped.mode match {
      case Partial =>
      case Final =>
      case _ => willNotWorkOnGpu("Unsupported WindowGroupLimitMode: " +
                                 s"${wrapped.mode.getClass.getName}")
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

  private def getRankFunctionType(expr: Expression): RankFunctionType = expr match {
    case GpuRank(_) => RankFunction
    case GpuDenseRank(_) => DenseRankFunction
    case _ =>
      throw new UnsupportedOperationException("Only Rank() is currently supported for group limits")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

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
