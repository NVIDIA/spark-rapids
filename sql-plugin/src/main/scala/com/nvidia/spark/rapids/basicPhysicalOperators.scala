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

package com.nvidia.spark.rapids

import ai.rapids.cudf
import ai.rapids.cudf.{NvtxColor, Scalar, Table}
import com.nvidia.spark.rapids.GpuMetricNames._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, IsNotNull, NamedExpression, NullIntolerant, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.rapids.GpuPredicateHelper
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuProjectExec {
  def projectAndClose[A <: Expression](cb: ColumnarBatch, boundExprs: Seq[A],
      totalTime: SQLMetric): ColumnarBatch = {
    val nvtxRange = new NvtxWithMetrics("ProjectExec", NvtxColor.CYAN, totalTime)
    try {
      project(cb, boundExprs)
    } finally {
      cb.close()
      nvtxRange.close()
    }
  }

  def project[A <: Expression](cb: ColumnarBatch, boundExprs: Seq[A]): ColumnarBatch = {
    val newColumns = boundExprs.safeMap {
      expr => {
        val result = expr.columnarEval(cb)
        result match {
          case cv: ColumnVector => cv
          case other =>
            val scalar = GpuScalar.from(other, expr.dataType)
            try {
              GpuColumnVector.from(scalar, cb.numRows())
            } finally {
              scalar.close()
            }
        }
      }}.toArray
    new ColumnarBatch(newColumns, cb.numRows())
  }
}

case class GpuProjectExec(projectList: Seq[Expression], child: SparkPlan)
    extends UnaryExecNode with GpuExec {

  private val sparkProjectList = projectList.asInstanceOf[Seq[NamedExpression]]

  override def output: Seq[Attribute] = sparkProjectList.map(_.toAttribute)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val boundProjectList = GpuBindReferences.bindGpuReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows()
      GpuProjectExec.projectAndClose(cb, boundProjectList, totalTime)
    }
  }
}

/**
 * Run a filter on a batch.  The batch will be consumed.
 */
object GpuFilter extends Arm {
  def apply(
      batch: ColumnarBatch,
      boundCondition: Expression,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      filterTime: SQLMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("filter batch", NvtxColor.YELLOW, filterTime)) { _ =>
      val filteredBatch = GpuFilter(batch, boundCondition)
      numOutputBatches += 1
      numOutputRows += filteredBatch.numRows()
      filteredBatch
    }
  }

  def apply(
      batch: ColumnarBatch,
      boundCondition: Expression) : ColumnarBatch = {
    var filterConditionCv: GpuColumnVector = null
    var tbl: cudf.Table = null
    var filtered: cudf.Table = null
    try {
      filterConditionCv = boundCondition.columnarEval(batch).asInstanceOf[GpuColumnVector]
      tbl = GpuColumnVector.from(batch)
      filtered = tbl.filter(filterConditionCv.getBase)
      GpuColumnVector.from(filtered)
    } finally {
      Seq(filtered, tbl, filterConditionCv, batch).safeClose()
    }
  }
}

case class GpuFilterExec(condition: Expression, child: SparkPlan)
    extends UnaryExecNode with GpuPredicateHelper with GpuExec {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case GpuIsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  /**
   * Potentially a lot is removed.
   */
  override def coalesceAfter: Boolean = true

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val boundCondition = GpuBindReferences.bindReference(condition, child.output)
    val rdd = child.executeColumnar()
    rdd.map { batch =>
      GpuFilter(batch, boundCondition, numOutputRows, numOutputBatches, totalTime)
    }
  }
}

/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class GpuRangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range,
    targetSizeBytes: Long)
    extends LeafExecNode with GpuExec {

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(sparkContext.defaultParallelism)
  val numElements: BigInt = range.numElements

  override val output: Seq[Attribute] = range.output

  override def outputOrdering: Seq[SortOrder] = range.outputOrdering

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override def doCanonicalize(): SparkPlan = {
    GpuRangeExec(
      range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range],
      targetSizeBytes)
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val maxRowCountPerBatch = Math.min(targetSizeBytes/8, Int.MaxValue)

    sqlContext
        .sparkContext
        .parallelize(0 until numSlices, numSlices)
        .mapPartitionsWithIndex { (i, _) =>
          val partitionStart = (i * numElements) / numSlices * step + start
          val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
          def getSafeMargin(bi: BigInt): Long =
            if (bi.isValidLong) {
              bi.toLong
            } else if (bi > 0) {
              Long.MaxValue
            } else {
              Long.MinValue
            }
          val safePartitionStart = getSafeMargin(partitionStart) // inclusive
          val safePartitionEnd = getSafeMargin(partitionEnd) // exclusive, unless start == this
          val taskContext = TaskContext.get()

          val iter: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
            private[this] var number: Long = safePartitionStart
            private[this] var done: Boolean = false
            private[this] val inputMetrics = taskContext.taskMetrics().inputMetrics

            override def hasNext: Boolean =
              if (!done) {
                if (step > 0) {
                  number < safePartitionEnd
                } else {
                  number > safePartitionEnd
                }
              } else false

            override def next(): ColumnarBatch =
              withResource(new NvtxWithMetrics("GpuRange", NvtxColor.DARK_GREEN, totalTime)){
                _ =>
                  GpuSemaphore.acquireIfNecessary(taskContext)
                  val start = number
                  val remainingSteps = (safePartitionEnd - start) / step
                  // Start is inclusive so we need to produce at least one row
                  val rowsThisBatch = Math.max(1, Math.min(remainingSteps, maxRowCountPerBatch))
                  val endInclusive = start + ((rowsThisBatch - 1) * step)
                  number = endInclusive + step
                  if (number < endInclusive ^ step < 0) {
                    // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
                    // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a
                    // step back, we are pretty sure that we have an overflow.
                    done = true
                  }

                  val ret = withResource(Scalar.fromLong(start)) { startScalar =>
                    withResource(Scalar.fromLong(step)) { stepScalar =>
                      withResource(
                        ai.rapids.cudf.ColumnVector.sequence(
                          startScalar, stepScalar, rowsThisBatch.toInt)) { vec =>
                        withResource(new Table(vec)) { tab =>
                          GpuColumnVector.from(tab)
                        }
                      }
                    }
                  }

                  assert (rowsThisBatch == ret.numRows())
                  numOutputRows += rowsThisBatch
                  TrampolineUtil.incInputRecordsRows(inputMetrics, rowsThisBatch)
                  numOutputBatches += 1
                  ret
              }
          }
          new InterruptibleIterator(taskContext, iter)
        }
  }

  override def simpleString(maxFields: Int): String = {
    s"GpuRange ($start, $end, step=$step, splits=$numSlices)"
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")
}


case class GpuUnionExec(children: Seq[SparkPlan]) extends SparkPlan with GpuExec {
  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(TrampolineUtil.structTypeMerge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)

    sparkContext.union(children.map(_.executeColumnar())).map { batch =>
      withResource(new NvtxWithMetrics("Union", NvtxColor.CYAN, totalTime)) { _ =>
        numOutputBatches += 1
        numOutputRows += batch.numRows
        batch
      }
    }
  }
}

case class GpuCoalesceExec(numPartitions: Int, child: SparkPlan)
    extends UnaryExecNode with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (numPartitions == 1 && child.executeColumnar().getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      child.executeColumnar().coalesce(numPartitions, shuffle = false)
    }
  }
}

object GpuCoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[ColumnarBatch](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}
