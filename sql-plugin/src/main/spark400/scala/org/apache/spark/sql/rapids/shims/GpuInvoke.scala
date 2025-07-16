/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "400"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This class wrap a target GPU object to adapt CPU Invoke
 * Refers to `InvokeExprMeta` to get what target GPU object is generated.
 * All checks and conversion are in `InvokeExprMeta`
 * @param targetGpuObject target GPU object which do the actual columnar work
 */
case class GpuInvoke(targetGpuObject: GpuExpression) extends ShimExpression with GpuExpression {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = Nil
  override def dataType: DataType =  targetGpuObject.dataType

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    targetGpuObject.columnarEval(batch)
  }
}
