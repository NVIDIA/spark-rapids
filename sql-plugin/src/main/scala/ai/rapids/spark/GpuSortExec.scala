/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, NvtxRange, Table}
import ai.rapids.spark.GpuMetricNames._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, NullOrdering, NullsFirst, NullsLast, RowOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.execution.{SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

class GpuSortMeta(
    sort: SortExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[SortExec](sort, conf, parent, rule) {
  override def convertToGpu(): GpuExec =
    GpuSortExec(childExprs.map(_.convertToGpu()).asInstanceOf[Seq[GpuSortOrder]],
      sort.global,
      childPlans(0).convertIfNeeded())

  override def tagPlanForGpu(): Unit = {
    if (GpuOverrides.isAnyStringLit(sort.sortOrder)) {
      willNotWorkOnGpu("string literal values are not supported in a sort")
    }
  }
}

case class GpuSortExec(
    sortOrder: Seq[GpuSortOrder],
    global: Boolean,
    child: SparkPlan,
    coalesceGoal: CoalesceGoal = RequireSingleBatch,
    testSpillFrequency: Int = 0
)
  extends UnaryExecNode with GpuExec {

  private val sparkSortOrder = sortOrder.map(_.toSortOrder)

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = Seq(coalesceGoal)

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sparkSortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sparkSortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override lazy val additionalMetrics = Map(
    "sortTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "sort time"),
    "peakDevMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak device memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val sortTime = longMetric("sortTime")

    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      val sorter = createBatchGpuSorter()
      val sortedIterator = sorter.sort(cbIter)
      sortTime += sorter.getSortTimeNanos
      metrics("peakDevMemory") += sorter.getPeakMemoryUsage
      sortedIterator
    }
  }

  private def createBatchGpuSorter(): GpuColumnarBatchSorter = {
    val boundSortExprs = GpuBindReferences.bindReferences(sortOrder, output)
    new GpuColumnarBatchSorter(boundSortExprs, this, coalesceGoal == RequireSingleBatch)
  }
}

class GpuColumnarBatchSorter(
    sortOrder: Seq[GpuSortOrder],
    exec: GpuExec,
    singleBatchOnly: Boolean,
    shouldUpdateMetrics: Boolean = true) extends Serializable {

  private var totalSortTimeNanos = 0L
  private var maxDeviceMemory = 0L
  private var haveSortedBatch = false
  private val numSortCols = sortOrder.length
  private val totalTimeMetric : Option[SQLMetric] = initMetric(TOTAL_TIME)
  private val outputBatchesMetric : Option[SQLMetric] = initMetric(NUM_OUTPUT_BATCHES)
  private val outputRowsMetric : Option[SQLMetric] = initMetric(NUM_OUTPUT_ROWS)

  private def initMetric(metricName: String): Option[SQLMetric] = if (shouldUpdateMetrics) {
    Some(exec.longMetric(metricName))
  } else {
    None
  }

  def getSortTimeNanos: Long = totalSortTimeNanos

  def getPeakMemoryUsage: Long = maxDeviceMemory

  def sort(batchIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]  = {

    // Sort order shouldn't be empty for Sort exec,
    // in any other case empty sort order translates to an ascending sort on all columns with nulls as smallest
    new Iterator[ColumnarBatch] {
      var resultBatch: Option[ColumnarBatch] = None

      TaskContext.get().addTaskCompletionListener[Unit](_ => closeBatch())

      private def closeBatch(): Unit = resultBatch.foreach(_.close())

      private def loadNextBatch(): Option[ColumnarBatch] = {
        if (batchIter.hasNext) {
          if (singleBatchOnly && haveSortedBatch) {
            throw new UnsupportedOperationException("Expected single batch to sort")
          }
          haveSortedBatch = true
          val inputBatch = batchIter.next()
          try {
            if (inputBatch.numCols() > 0) {
              Some(sortBatch(inputBatch))
            } else {
              Some(new ColumnarBatch(Array.empty, inputBatch.numRows()))
            }
          } finally {
            inputBatch.close()
          }
        } else {
          None
        }
      }

      private def sortBatch(inputBatch: ColumnarBatch): ColumnarBatch = {
        val nvtxRange = initNvtxRange
        try {
          var inputTbl: Table = null
          var inputCvs: Seq[GpuColumnVector] = Nil
          try {
            if (sortOrder.nonEmpty) {
              inputCvs = SortUtils.getGpuColVectorsAndBindReferences(inputBatch, sortOrder)
              inputTbl = new cudf.Table(inputCvs.map(_.getBase): _*)
            } else if (inputBatch.numCols() > 0) {
              inputTbl = GpuColumnVector.from(inputBatch)
            }
            val orderByArgs = getOrderArgs(inputTbl)
            val startTimestamp = System.nanoTime()
            val batch = doGpuSort(inputTbl, orderByArgs)
            updateMetricValues(inputTbl, startTimestamp, batch)
            batch
          } finally {
            inputCvs.foreach(_.close())
            if (inputTbl != null) {
              inputTbl.close()
            }
          }
        } finally {
          nvtxRange.close()
        }
      }

      override def hasNext: Boolean = {
        if (resultBatch.isDefined) {
          true
        } else {
          resultBatch = loadNextBatch()
          resultBatch.isDefined
        }
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val ret = resultBatch.get
        resultBatch = None
        ret
      }
    }
  }

  private def getOrderArgs(inputTbl: Table): Seq[Table.OrderByArg] = {
    val orderByArgs = if (sortOrder.nonEmpty) {
      sortOrder.zipWithIndex.map { case (order, index) =>
        if (order.isAscending) {
          Table.asc(index, order.nullOrdering == NullsFirst)
        } else {
          Table.desc(index, order.nullOrdering == NullsLast)
        }
      }
    } else {
      (0 until inputTbl.getNumberOfColumns).map { index =>
        Table.asc(index, true)
      }
    }
    orderByArgs
  }

  private def updateMetricValues(inputTbl: Table, startTimestamp: Long,
    batch: ColumnarBatch): Unit = {
    if (shouldUpdateMetrics) {
      totalSortTimeNanos += System.nanoTime - startTimestamp
      outputBatchesMetric.get += 1
      outputRowsMetric.get += batch.numRows
      val devMemUsed = GpuColumnVector.getTotalDeviceMemoryUsed(inputTbl) + GpuColumnVector.getTotalDeviceMemoryUsed(batch)
      maxDeviceMemory = scala.math.max(maxDeviceMemory, devMemUsed)
    }
  }

  private def initNvtxRange = {
    if (shouldUpdateMetrics) {
      new NvtxWithMetrics("sort batch", NvtxColor.WHITE, totalTimeMetric.get)
    } else {
      new NvtxRange("sort batch", NvtxColor.WHITE)
    }
  }

  private def doGpuSort(
      tbl: Table,
      orderByArgs: Seq[Table.OrderByArg]): ColumnarBatch = {
    var resultTbl: cudf.Table = null
    try {
      resultTbl = tbl.orderBy(orderByArgs: _*)
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
 *
 * Keep the original child Expression around so that when we convert back to a SortOrder we
 * can pass that in. If we don't do that then GpuExpressions will end up being used to
 * check if the sort order satisfies the child order and things won't match up (specifically
 * AttributeReference.semanticEquals won't match GpuAttributeReference.
 *
 */
case class GpuSortOrder(
    child: GpuExpression,
    direction: SortDirection,
    nullOrdering: NullOrdering,
    sameOrderExpressions: Set[Expression],
    private val origChild: Expression)
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

  def toSortOrder: SortOrder = SortOrder(origChild, direction, nullOrdering, sameOrderExpressions)
}

object GpuSortOrder {
  def apply(
      child: GpuExpression,
      origChild: Expression,
      direction: SortDirection,
      sameOrderExpressions: Set[Expression] = Set.empty): GpuSortOrder = {
    new GpuSortOrder(child, direction, direction.defaultNullOrdering, sameOrderExpressions, origChild)
  }
}
