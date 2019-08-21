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
import ai.rapids.cudf.DType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, IsNotNull, NamedExpression, NullIntolerant, PredicateHelper, SortOrder}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan, TrampolineUtil, UnaryExecNode}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetrics

object GpuProjectExec {
  def projectAndClose[A <: GpuExpression](cb: ColumnarBatch, boundExprs: Seq[A]): ColumnarBatch =
    try {
      project(cb, boundExprs)
    } finally {
      cb.close()
    }

  def project[A <: GpuExpression](cb: ColumnarBatch, boundExprs: Seq[A]): ColumnarBatch = {
    val newColumns = boundExprs.map(
      expr => {
        val result = expr.asInstanceOf[GpuExpression].columnarEval(cb)
        result match {
          case cv: ColumnVector => cv
          case other => GpuColumnVector.from(GpuScalar.from(other), cb.numRows())
        }
      }).toArray
    new ColumnarBatch(newColumns, cb.numRows())
  }
}

class GpuProjectExec(projectList: Seq[GpuExpression], child: SparkPlan)
  extends ProjectExec(projectList.asInstanceOf[Seq[NamedExpression]], child) with GpuExec {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList = GpuBindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.map(cb => GpuProjectExec.projectAndClose(cb, boundProjectList))
  }
}

case class GpuFilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with PredicateHelper with GpuExec {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, _) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

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

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("Row-based execution should not occur for this class")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val boundCondition = GpuBindReferences.bindReference(condition.asInstanceOf[GpuExpression], child.output)
    val rdd = child.executeColumnar()

    rdd.map(batch => {
      val cols = new ArrayBuffer[GpuColumnVector](batch.numCols)
      var success = false
      var error: Throwable = null
      try {
        for (i <- 0 until batch.numCols) {
          val col = batch.column(i).asInstanceOf[GpuColumnVector]
          col.getBase.getType match {
            // filter does not work on strings, so turn them to categories
            // TODO we need a faster way to work with these values!!!
            case DType.STRING =>
              val catCv = col.getBase.asStringCategories
              var successFrom = false
              try {
                cols += GpuColumnVector.from(catCv)
                successFrom = true
              } finally {
                if (!successFrom) {
                  catCv.close()
                }
              }
            case _ => cols += col.incRefCount()
          }
        }
        success = true
      } finally {
        if (!success) {
          // this foreach will not throw
          cols.foreach(col => {
            try {
              col.close()
            } catch {
              case t: Throwable =>
                if (error == null) {
                  error = t
                } else {
                  error.addSuppressed(t)
                }
            }
          })
        }
        // throw error if there were problems closing
        if (error != null) {
          throw error
        }
      }

      var filterConditionCv: GpuColumnVector = null
      var tbl: cudf.Table = null
      var filtered: cudf.Table = null
      val filteredBatch = try {
        val batchWithCategories = new ColumnarBatch(cols.toArray, batch.numRows)
        filterConditionCv = boundCondition.columnarEval(batchWithCategories).asInstanceOf[GpuColumnVector]
        tbl = new cudf.Table(cols.map(_.getBase): _*)
        filtered = tbl.filter(filterConditionCv.getBase)
        numOutputRows += filtered.getRowCount
        GpuColumnVector.from(filtered)
      } finally {
        // this foreach will not throw
        (Seq(filtered, tbl, filterConditionCv, batch) ++ cols).foreach (toClose => {
          try {
            if (toClose != null) {
              toClose.close()
            }
          } catch {
            case t: Throwable =>
              if (error == null) {
                error = t
              } else {
                error.addSuppressed(t)
              }
          }
        })
        if (error != null) {
          throw error
        }
      }

      filteredBatch
    })
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
    throw new IllegalStateException("Row-based execution should not occur for this class")

  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
}
