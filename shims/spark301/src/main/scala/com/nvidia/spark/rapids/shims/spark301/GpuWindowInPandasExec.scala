/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.GpuWindowInPandasExecBase
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This GpuWindowInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for Python processes
 */
case class GpuWindowInPandasExec(
    windowExpression: Seq[Expression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends GpuWindowInPandasExecBase {

  override final def pythonModuleKey: String = "spark"

  // Apache Spark expects input columns before the result columns
  override def output: Seq[Attribute] = child.output ++ windowExpression
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  // Return the join batch directly per Apache Spark's expectation.
  override def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch = joinedBatch
}
