/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350emr"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BaseOutputAdapterExec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Trivial GPU implementation of Spark's [[OutputAdapterExec]] to extend [[GpuExec]]
 */
case class GpuOutputAdapterExec(
    override val output: Seq[Attribute],
    override val child: SparkPlan) extends ShimUnaryExecNode
    with BaseOutputAdapterExec with GpuExec {

  // The adapter effectively produces new attributes that are not found in input attributes
  override def producedAttributes: AttributeSet = outputSet -- inputSet

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
      s" mismatch:\n$this")
  }
}