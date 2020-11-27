/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301db

import com.nvidia.spark.rapids.{GpuBindReferences, GpuBoundReference, GpuProjectExec, GpuWindowExpression}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.GpuWindowInPandasExecBase
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

/*
 * This GpuWindowInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for Python processes
 */
case class GpuWindowInPandasExec(
    projectList: Seq[Expression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends GpuWindowInPandasExecBase {

  override final def pythonModuleKey: String = "databricks"

  // On Databricks, the projectList contains not only the window expression, but may also contains
  // the input attributes. So we need to extract the window expressions from it.
  override def windowExpression: Seq[Expression] = projectList.filter { expr =>
    expr.find(node => node.isInstanceOf[GpuWindowExpression]).isDefined
  }

  // On Databricks, the projectList is expected to be the final output, and it is nondeterministic.
  // It may contain the input attributes or not, or even part of the input attributes. So
  // we need to project the joined batch per this projectList.
  // But for the schema, just return it directly.
  override def output: Seq[Attribute] = projectList
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  override def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch = {
    // Project the data
    withResource(joinedBatch) { joinBatch =>
      GpuProjectExec.project(joinBatch, outReferences)
    }
  }

  private val outReferences = {
    val allExpressions = windowFramesWithExpressions.map(_._2).flatten
    val references = allExpressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      GpuBoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = allExpressions.zip(references).toMap
    // Bound the project list for GPU
    GpuBindReferences.bindGpuReferences(
      projectList.map(_.transform(unboundToRefMap)), child.output)
  }

}
