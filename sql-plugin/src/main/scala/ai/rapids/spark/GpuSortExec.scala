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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, Table}
import ai.rapids.spark.GpuExpressionsUtils.evaluateBoundExpressions
import ai.rapids.spark.GpuMetricNames._
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, NullOrdering, NullsFirst, NullsLast, RowOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.execution.{SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.execution.metric.SQLMetrics

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
    val keyDataTypes = sort.sortOrder.map(_.dataType)
    if ((keyDataTypes.contains(FloatType) || keyDataTypes.contains(DoubleType)) && conf.hasNans) {
      willNotWorkOnGpu("floats/doubles are not supported in sort, due to " +
        "incompatibility with NaN. If you don't have any NaNs in your data you can set " +
        s"${RapidsConf.HAS_NANS}=false to bypass this.")
    }

    // note that dataframe.sort always sets this to true
    if (sort.global && !conf.enableTotalOrderSort) {
      willNotWorkOnGpu(s"Don't support total ordering on GPU yet")
    }
  }
}

case class GpuSortExec(
    sortOrder: Seq[GpuSortOrder],
    global: Boolean,
    child: SparkPlan,
    coalesceGoal: CoalesceGoal = RequireSingleBatch,
    testSpillFrequency: Int = 0,
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
    singleBatchOnly: Boolean) {

  private var totalSortTimeNanos = 0L
  private var maxDeviceMemory = 0L
  private val numSortCols = sortOrder.length
  private val totalTime = exec.longMetric(TOTAL_TIME)
  private val numOutputBatches = exec.longMetric(NUM_OUTPUT_BATCHES)
  private val numOutputRows = exec.longMetric(NUM_OUTPUT_ROWS)

  def getSortTimeNanos: Long = totalSortTimeNanos

  def getPeakMemoryUsage: Long = maxDeviceMemory

  def sort(batchIter: Iterator[ColumnarBatch]): Iterator[ColumnarBatch]  = {
    if (sortOrder.isEmpty) {
      // shouldn't ever get here as catalyst seems to optimize out but just in case
      return batchIter
    }

    new Iterator[ColumnarBatch] {
      var resultBatch: Option[ColumnarBatch] = None

      TaskContext.get().addTaskCompletionListener[Unit](_ => closeBatch())

      private def closeBatch(): Unit = resultBatch.foreach(_.close())

      private def loadNextBatch(): Option[ColumnarBatch] = {
        if (batchIter.hasNext) {
          if (singleBatchOnly && numOutputRows.value > 0) {
            throw new UnsupportedOperationException("Expected single batch to sort")
          }
          val inputBatch = batchIter.next()
          try {
            Some(sortBatch(inputBatch))
          } finally {
            inputBatch.close()
          }
        } else {
          None
        }
      }

      private def sortBatch(inputBatch: ColumnarBatch): ColumnarBatch = {
        val nvtxRange = new NvtxWithMetrics("sort batch", NvtxColor.WHITE, totalTime)
        try {
          val inputCvs = getGpuCvsAndBindReferences(inputBatch, sortOrder)
          val inputTbl = try {
            new cudf.Table(inputCvs.map(_.getBase): _*)
          } finally {
            inputCvs.foreach(_.close())
          }
          try {
            val orderByArgs = sortOrder.zipWithIndex.map { case (order, index) =>
              if (order.isAscending) {
                Table.asc(index, order.nullOrdering == NullsFirst)
              } else {
                Table.desc(index, order.nullOrdering == NullsLast)
              }
            }

            val startTimestamp = System.nanoTime()
            val batch = doGpuSort(inputTbl, orderByArgs)
            totalSortTimeNanos += System.nanoTime - startTimestamp
            numOutputBatches += 1
            numOutputRows += batch.numRows
            val devMemUsed = GpuColumnVector.getTotalDeviceMemoryUsed(inputTbl)
              + GpuColumnVector.getTotalDeviceMemoryUsed(batch)
            maxDeviceMemory = scala.math.max(maxDeviceMemory, devMemUsed)
            batch
          } finally {
            inputTbl.close()
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

  /*
   * This function takes the input batch and the bound sort order references and
   * evaluates each column in case its an expression. It then appends the original columns
   * after the sort key columns. The sort key columns will be dropped after sorting.
   */
  private def getGpuCvsAndBindReferences(
      batch: ColumnarBatch,
      boundInputReferences: Seq[GpuSortOrder]): Seq[GpuColumnVector] = {
    val sortCvs = new ArrayBuffer[GpuColumnVector](numSortCols)
    val childExprs = boundInputReferences.map(_.child)
    sortCvs ++= evaluateBoundExpressions(batch, childExprs)
    sortCvs ++ GpuColumnVector.extractColumns(batch)
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
