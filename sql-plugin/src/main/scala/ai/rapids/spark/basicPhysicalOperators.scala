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
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable.ArrayBuffer

class GpuProjectExec(projectList: Seq[GpuExpression], child: SparkPlan)
  extends ProjectExec(projectList.asInstanceOf[Seq[NamedExpression]], child) with GpuExec {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] = GpuBindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.map(cb => try {
      val newColumns = boundProjectList.map(
        expr => {
          val result = expr.asInstanceOf[GpuExpression].columnarEval(cb)
          result match {
            case cv: ColumnVector => cv
            case other => GpuColumnVector.from(GpuScalar.from(other), cb.numRows())
          }
        }).toArray
      new ColumnarBatch(newColumns, cb.numRows())
    } finally {
      cb.close()
    })
  }
}

class GpuFilterExec(condition: GpuExpression, child: SparkPlan)
  extends FilterExec(condition, child) with GpuExec {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val boundCondition = GpuBindReferences.bindReference(condition, child.output)
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
            case DType.STRING => {
              val catCv = col.getBase.asStringCategories
              var successFrom = false
              try {
                cols += GpuColumnVector.from(catCv)
                successFrom = true
              } finally {
                if (!successFrom) {
                  catCv.close
                }
              }
            }
            case _ => cols += col.inRefCount()
          }
        }
        success = true
      } finally {
        if (!success) {
          // this foreach will not throw
          cols.foreach(col => {
            try {
              col.close
            } catch {
              case t: Throwable => {
                if (error == null) {
                  error = t
                } else {
                  error.addSuppressed(t)
                }
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
              toClose.close
            }
          } catch {
            case t: Throwable => {
              if (error == null) {
                error = t
              } else {
                error.addSuppressed(t)
              }
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

class GpuUnionExec(children: Seq[SparkPlan]) extends UnionExec(children) with GpuExec {
  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
}
