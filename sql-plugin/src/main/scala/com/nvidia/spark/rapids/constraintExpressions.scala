/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

// Avoid deriving from TaggingExpression since it is a UnaryExpression that changed in Spark 3.2
case class GpuKnownFloatingPointNormalized(child: Expression) extends GpuExpression {
  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  override def columnarEval(batch: ColumnarBatch): Any = {
    child.columnarEval(batch)
  }
}
