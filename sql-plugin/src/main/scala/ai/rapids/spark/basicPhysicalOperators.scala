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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuProjectExec(projectList: Seq[GpuExpression], child: SparkPlan)
  extends ProjectExec(projectList.asInstanceOf[Seq[NamedExpression]], child) with GpuExec {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] = GpuBindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    AutoCloseColumnBatchIterator.map(rdd,
      (cb: ColumnarBatch) => {
        val newColumns = boundProjectList.map(
          expr => {
            val result = expr.asInstanceOf[GpuExpression].columnarEval(cb)
            // TODO it is possible for a constant to be returned that we need to
            // create a columnVector for, might be a special sub-class
            // that only stores a single value.
            result.asInstanceOf[ColumnVector]
          }).toArray
        new ColumnarBatch(newColumns, cb.numRows())
      }
    )
  }
}

class GpuFilterExec(condition: GpuExpression, child: SparkPlan)
  extends FilterExec(condition, child) with GpuExec {

  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val boundCondition: Any = GpuBindReferences.bindReference(condition, child.output)
    val rdd = child.executeColumnar()
    AutoCloseColumnBatchIterator.map(rdd,
      (cb: ColumnarBatch) => {
        val boundExpression = boundCondition.asInstanceOf[GpuExpression]
        val evalCv = boundExpression.columnarEval(cb)
        var cols = Seq[GpuColumnVector]()
        var rowCount = 0
        try {
          val gpuEvalCv = evalCv.asInstanceOf[GpuColumnVector]

          // rebuild the columns, but with new filtered columns
          for (i <- 0 until cb.numCols()) {
            val colBase = cb.column(i).asInstanceOf[GpuColumnVector].getBase
            val filtered = colBase.filter(gpuEvalCv.getBase)
            cols = (cols :+ GpuColumnVector.from(filtered))
            rowCount = filtered.getRowCount().intValue() // all columns have the same # of rows
          }
        } finally {
          if (evalCv != null && evalCv.isInstanceOf[GpuColumnVector]) {
            evalCv.asInstanceOf[GpuColumnVector].close();
          }
        }
        numOutputRows += rowCount

        new ColumnarBatch(cols.toArray, rowCount)
      }
    )
  }
}

class GpuUnionExec(children: Seq[SparkPlan]) extends UnionExec(children) with GpuExec {
  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    sparkContext.union(children.map(_.executeColumnar()))
}