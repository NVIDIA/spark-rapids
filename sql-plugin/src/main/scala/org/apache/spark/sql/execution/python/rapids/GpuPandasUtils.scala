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

package org.apache.spark.sql.execution.python.rapids

import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.PandasGroupUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This is to expose the APIs of PandasGroupUtils to rapids Execs
 */
private[sql] object GpuPandasUtils {

  def executePython[T](
      data: Iterator[T],
      output: Seq[Attribute],
      runner: BasePythonRunner[T, ColumnarBatch]): Iterator[InternalRow] = {
    PandasGroupUtils.executePython(data, output, runner)
  }

  def groupAndProject(
    input: Iterator[InternalRow],
    groupingAttributes: Seq[Attribute],
    inputSchema: Seq[Attribute],
    dedupSchema: Seq[Attribute]):
  Iterator[(InternalRow, Iterator[InternalRow])] = {
    PandasGroupUtils.groupAndProject(input, groupingAttributes, inputSchema, dedupSchema)
  }

  def resolveArgOffsets(
      child: SparkPlan, groupingAttributes: Seq[Attribute]): (Seq[Attribute], Array[Int]) = {
    PandasGroupUtils.resolveArgOffsets(child, groupingAttributes)
  }
}
