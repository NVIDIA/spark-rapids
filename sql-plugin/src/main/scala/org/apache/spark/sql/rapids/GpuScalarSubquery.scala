/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import com.nvidia.spark.rapids.{GpuExec, GpuExpression}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuScalarSubquery(
    plan: BaseSubqueryExec,
    exprId: ExprId)
  extends ExecSubqueryExpression with GpuExpression {

  override def dataType: DataType = plan.schema.fields.head.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def withNewPlan(query: BaseSubqueryExec): GpuScalarSubquery = copy(plan = query)

  override def semanticEquals(other: Expression): Boolean = other match {
    case s: GpuScalarSubquery => plan.sameResult(s.plan)
    case _ => false
  }
  private var result: Any = _

  override def updateResult(): Unit = {
    val subPlanRDD = plan.child.execute()
    val sc = plan.sqlContext.sparkContext

    // As ScalarSubquery, we expect a single row and column result from the child plan.
    def fetchScalar(iter: Iterator[InternalRow]): Any = {
      if (!iter.hasNext) {
        null
      } else {
        val firstRow = iter.next()
        assert(!iter.hasNext,
          "Found multiple rows output from the child plan of SubqueryExec")
        assert(firstRow.numFields == 1,
          "Found multiple columns output from the child plan of SubqueryExec")
        firstRow.get(0, dataType)
      }
    }

    def resultHandler(taskResult: Any): Unit = {
      if (taskResult != null) {
        assert(result == null,
          "Found multiple records from the results of different partitions")
        result = taskResult
      }
    }

    sc.runJob(
      rdd = subPlanRDD,
      func = (_: TaskContext, iter: Iterator[InternalRow]) => fetchScalar(iter),
      partitions = 0 until subPlanRDD.getNumPartitions,
      resultHandler = (_: Int, ret: Any) => resultHandler(ret)
    )
  }

  override def columnarEval(batch: ColumnarBatch): Any = result
}

// A GPU placeholder for SubqueryExec, which should NOT be executed
case class GpuSubqueryExec(name: String, child: SparkPlan)
  extends BaseSubqueryExec with UnaryExecNode with GpuExec {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"Cannot execute plan: $this")
  }
}
