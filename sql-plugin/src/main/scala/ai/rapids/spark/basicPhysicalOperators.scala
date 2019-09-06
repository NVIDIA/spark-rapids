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

import ai.rapids.cudf
import ai.rapids.cudf.NvtxColor
import ai.rapids.spark.RapidsPluginImplicits._
import ai.rapids.spark.GpuMetricNames._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, IsNotNull, NamedExpression, NullIntolerant, PredicateHelper, SortOrder}
import org.apache.spark.sql.execution.{SparkPlan, TrampolineUtil, UnaryExecNode}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric

object GpuProjectExec {
  def projectAndClose[A <: GpuExpression](cb: ColumnarBatch, boundExprs: Seq[A],
      totalTime: SQLMetric): ColumnarBatch = {
    val nvtxRange = new NvtxWithMetrics("ProjectExec", NvtxColor.CYAN, totalTime)
    try {
      try {
        project(cb, boundExprs)
      } finally {
        cb.close()
      }
    } finally {
      nvtxRange.close()
    }
  }

  def project[A <: GpuExpression](cb: ColumnarBatch, boundExprs: Seq[A]): ColumnarBatch = {
    val newColumns = boundExprs.safeMap {
      expr => {
        val result = expr.columnarEval(cb)
        result match {
          case cv: ColumnVector => cv
          case other => GpuColumnVector.from(GpuScalar.from(other), cb.numRows())
        }
      }}.toArray
    new ColumnarBatch(newColumns, cb.numRows())
  }
}

case class GpuProjectExec(projectList: Seq[GpuExpression], child: SparkPlan)
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
    val boundProjectList = GpuBindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.map { cb =>
      numOutputBatches += 1
      numOutputRows += cb.numRows()
      GpuProjectExec.projectAndClose(cb, boundProjectList, totalTime)
    }
  }
}

case class GpuFilterExec(condition: GpuExpression, child: SparkPlan)
  extends UnaryExecNode with PredicateHelper with GpuExec {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
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
      val nvtxRange = new NvtxWithMetrics("filter batch", NvtxColor.YELLOW, totalTime)
      try {
        filterBatch(batch, boundCondition, numOutputRows, numOutputBatches)
      } finally {
        nvtxRange.close()
      }
    }
  }

  private def filterBatch(
      batch: ColumnarBatch,
      boundCondition: GpuExpression,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric): ColumnarBatch = {
    val batchWithCategories = try {
      GpuColumnVector.convertToStringCategoriesIfNeeded(batch)
    } finally {
      batch.close()
    }

    var filterConditionCv: GpuColumnVector = null
    var tbl: cudf.Table = null
    var filtered: cudf.Table = null
    var error: Throwable = null
    val filteredBatch = try {
      filterConditionCv = boundCondition.columnarEval(batchWithCategories).asInstanceOf[GpuColumnVector]
      tbl = GpuColumnVector.from(batchWithCategories)
      filtered = tbl.filter(filterConditionCv.getBase)
      numOutputRows += filtered.getRowCount
      GpuColumnVector.from(filtered)
    } finally {
      Seq(filtered, tbl, filterConditionCv, batchWithCategories).safeClose()
    }

    numOutputBatches += 1
    numOutputRows += filteredBatch.numRows()
    filteredBatch
  }
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
}
