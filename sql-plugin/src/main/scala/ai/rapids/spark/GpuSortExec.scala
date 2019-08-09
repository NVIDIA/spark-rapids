/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, NvtxRange, Table}
import ai.rapids.spark.GpuExpressionsUtils.evaluateBoundExpressions
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, NullOrdering, NullsFirst, NullsLast, RowOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

case class GpuSortExec(
    sortOrder: Seq[GpuSortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryExecNode with GpuExec {

  private val sparkSortOrder = sortOrder.map(_.toSortOrder)

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(PreferSingleBatch)

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sparkSortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sparkSortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "sortTime" -> SQLMetrics.createTimingMetric(sparkContext, "sort time"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sortTime = longMetric("sortTime")

    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      val sorter = createBatchGpuSorter()
      val sortedIterator = sorter.sort(cbIter)
      sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
      sortedIterator
    }
  }

  private def createBatchGpuSorter(): GpuColumnarBatchSorter = {
    val boundSortExprs = GpuBindReferences.bindReferences(sortOrder, output)
    new GpuColumnarBatchSorter(schema, boundSortExprs)
  }
}

class GpuColumnarBatchSorter(
    schema: StructType,
    sortOrder: Seq[GpuSortOrder]) {

  private var totalSortTimeNanos = 0L
  private val numSortCols = sortOrder.length

  def getSortTimeNanos: Long = totalSortTimeNanos

  def sort(batchIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]  = {

    if (sortOrder.isEmpty) {
      // shouldn't ever get here as catalyst seems to optimize out but just in case
      return batchIter
    }
    var inputCvs: Seq[GpuColumnVector] = null
    var resultCb: ColumnarBatch = null
    var success = false
    var numRows = 0L
    var concatTbl: Table = null

    try {
      val inputTbls = new ArrayBuffer[Table]()
      try {
        while (batchIter.hasNext) {
          val batch = batchIter.next()
          val nvtxRange = new NvtxRange("sort input batch", NvtxColor.WHITE)
          try {
            try {
              numRows += batch.numRows
              if (numRows > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException(s"Too many rows to sort")
              }
              inputCvs = getGpuCvsAndBindReferences(batch, sortOrder)
              inputTbls += new cudf.Table(inputCvs.map(_.getBase): _*)
            } finally {
              inputCvs.foreach(_.close())
              batch.close()
            }
          } finally {
            nvtxRange.close()
            inputCvs.safeClose()
            batch.close()
          }
        }
        val nvtxRange = new NvtxRange("sort concatenate", NvtxColor.YELLOW)
        try {
          if (numRows == 0 || inputTbls.isEmpty) return Iterator.empty
          if (inputTbls.length > 1) {
            concatTbl = Table.concatenate(inputTbls: _*)
          } else {
            concatTbl = inputTbls.remove(0)
          }
        } finally {
          nvtxRange.close()
        }
      } finally {
        inputTbls.safeClose()
      }

      val nvtxRange = new NvtxRange("sort", NvtxColor.ORANGE)
      try {
        val orderByArgs = sortOrder.zipWithIndex.map { case (order, index) =>
          if (order.isAscending) Table.asc(index) else Table.desc(index)
        }

        val startTimestamp = System.nanoTime
        resultCb = doGpuSort(concatTbl, orderByArgs)
        totalSortTimeNanos += System.nanoTime - startTimestamp
        success = true
      } finally {
        nvtxRange.close()
      }

      new Iterator[ColumnarBatch] {

        TaskContext.get().addTaskCompletionListener[Unit](_ => closeBatch())

        private def closeBatch(): Unit = {
          if (resultCb != null) {
            resultCb.close()
            resultCb = null
          }
        }

        override def hasNext: Boolean = resultCb != null

        override def next(): ColumnarBatch = {
          val ret = resultCb
          resultCb = null
          ret
        }
      }
    } finally {
      if (!success) {
        if (resultCb != null) {
          resultCb.close()
        }
      }
      if (concatTbl != null) {
        concatTbl.close()
      }
    }
  }

  /*
   * This function takes the input batch and the bound sort order references and
   * evaluates each column in case its an expression. It then appends the original columns
   * after the sort key columns. The sort key columns will be dropped after sorting.
   */
  private def getGpuCvsAndBindReferences(
      batch: ColumnarBatch,
      boundInputReferences: Seq[GpuSortOrder]): Seq[GpuColumnVector] = {
    val sortCvs = new ArrayBuffer[GpuColumnVector](numSortCols)
    var batchWithCategories: ColumnarBatch = null
    try {
      batchWithCategories = GpuColumnVector.convertToStringCategoriesIfNeeded(batch)
      val childExprs = boundInputReferences.map(_.child)
      sortCvs ++= evaluateBoundExpressions(batchWithCategories, childExprs)
    } catch {
      case t: Throwable =>
        sortCvs.safeClose()
        batchWithCategories.safeClose()
        throw t
    }
    sortCvs ++ GpuColumnVector.extractColumns(batchWithCategories)
  }

  private def areNullsSmallest: Boolean = {
    (sortOrder.head.isAscending && sortOrder.head.nullOrdering == NullsFirst) ||
      (!sortOrder.head.isAscending && sortOrder.head.nullOrdering == NullsLast)
  }

  private def doGpuSort(
      tbl: Table,
      orderByArgs: Seq[Table.OrderByArg]): ColumnarBatch = {
    var resultTbl: cudf.Table = null
    try {
      resultTbl = tbl.orderBy(areNullsSmallest, orderByArgs: _*)
      GpuColumnVector.from(resultTbl, numSortCols, resultTbl.getNumberOfColumns)
    } finally {
      if (resultTbl != null) {
        resultTbl.close()
      }
    }
  }
}

/**
 * GpuSortOrder where the child is a GpuExpression.
 *
 * As far as I can tell the sameOrderExpressions can stay as is. It's used to see if the
 * ordering already matches for things like inserting shuffles and optimizing out redundant sorts
 * and as long as the plugin isn't acting differently then the CPU that should just work.
 */
case class GpuSortOrder(
    child: GpuExpression,
    direction: SortDirection,
    nullOrdering: NullOrdering,
    sameOrderExpressions: Set[Expression])
  extends GpuUnevaluableUnaryExpression {

  /** Sort order is not foldable because we don't have an eval for it. */
  override def foldable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (RowOrdering.isOrderable(dataType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"cannot sort data type ${dataType.catalogString}")
    }
  }

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  override def toString: String = s"$child ${direction.sql} ${nullOrdering.sql}"
  override def sql: String = child.sql + " " + direction.sql + " " + nullOrdering.sql

  def isAscending: Boolean = direction == Ascending

  def toSortOrder: SortOrder = SortOrder(child, direction, nullOrdering, sameOrderExpressions)
}
