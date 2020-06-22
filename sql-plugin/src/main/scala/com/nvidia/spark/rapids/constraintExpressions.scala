/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import org.apache.spark.sql.catalyst.expressions.{Expression, TaggingExpression}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This is a TaggingExpression in spark, which gets matched in NormalizeFloatingNumbers (which is
 * a Rule).
 */
case class GpuKnownFloatingPointNormalized(child: Expression) extends TaggingExpression
    with GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = {
    child.asInstanceOf[GpuExpression].columnarEval(batch)
  }
}
